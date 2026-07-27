import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
	RoleTransactionLock,
	beginClaimHead,
	commitClaim,
	enqueueAssignment,
	listClaiming,
	listQueue,
	queueCount,
	readLockOwnerForTest,
	recoverClaiming,
	withRoleLock,
	withRoleLockAsync,
	QueueCorruptionError,
	cancelQueuedAssignment,
} from "../src/herdr/role-queue.js";
import { PoolRegistry, isArchivalTombstone, isNonReusableLiveWorker, selectClosablePoolWorkers } from "../src/herdr/pool-registry.js";
import { resolvePoolIdentity, stableWorkerId } from "../src/herdr/pool-identity.js";
import { installMomoWorker } from "../src/extensions/worker-runtime.js";
import { workerControlPaths, assignmentSpoolPaths } from "../src/herdr/assignment-spool.js";
import { atomicWriteJson } from "../src/ipc/spool.js";
import { tryReadIpcJson } from "../src/ipc/validate.js";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { createHerdrChildSessionFactory } from "../src/delegation/herdr-factory.js";
import { HerdrClient } from "../src/herdr/client.js";
import { getRole } from "../src/roles.js";

const tempDirs: string[] = [];

afterEach(() => {
	while (tempDirs.length > 0) {
		const dir = tempDirs.pop();
		if (dir) rmSync(dir, { recursive: true, force: true });
	}
});

function tempDir(prefix: string): string {
	const dir = mkdtempSync(path.join(tmpdir(), prefix));
	tempDirs.push(dir);
	return dir;
}

describe("RoleTransactionLock", () => {
	it("releases only with matching token; never deletes successor", async () => {
		const cacheRoot = tempDir("momo-lock-cache-");
		const cwd = tempDir("momo-lock-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const a = new RoleTransactionLock(pool.poolRoot, "scout");
		await a.acquire(2_000);
		const tokenA = a.getTokenForTest();
		expect(tokenA).toBeTruthy();

		// Simulate successor: release should check token.
		const ownerPath = path.join(a.lockDir, "owner.json");
		const successorToken = "b".repeat(32);
		writeFileSync(
			ownerPath,
			`${JSON.stringify({
				version: 1,
				token: successorToken,
				pid: process.pid,
				at: new Date().toISOString(),
				heartbeatAt: new Date().toISOString(),
			})}\n`,
			{ mode: 0o600 },
		);
		a.release();
		expect(existsSync(a.lockDir)).toBe(true);
		const owner = readLockOwnerForTest(a.lockDir);
		expect(owner?.token).toBe(successorToken);
		rmSync(a.lockDir, { recursive: true, force: true });
	});

	it("dead stale owner is not removed or replaced; acquisition times out", async () => {
		const cacheRoot = tempDir("momo-lock-dead-stale-");
		const cwd = tempDir("momo-lock-dead-stale-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const lockDir = path.join(pool.poolRoot, "roles", "scout", "tx.lock");
		mkdirSync(path.dirname(lockDir), { recursive: true, mode: 0o700 });
		mkdirSync(lockDir, { mode: 0o700 });
		const staleToken = "a".repeat(32);
		writeFileSync(
			path.join(lockDir, "owner.json"),
			`${JSON.stringify({
				version: 1,
				token: staleToken,
				pid: 1_999_999_999, // dead
				at: new Date(Date.now() - 120_000).toISOString(),
				heartbeatAt: new Date(Date.now() - 120_000).toISOString(),
			})}\n`,
			{ mode: 0o600 },
		);
		const lock = new RoleTransactionLock(pool.poolRoot, "scout", { staleMs: 1 });
		await expect(lock.acquire(200)).rejects.toThrow(/Timed out/);
		expect(existsSync(lockDir)).toBe(true);
		expect(readLockOwnerForTest(lockDir)?.token).toBe(staleToken);
		rmSync(lockDir, { recursive: true, force: true });
	});

	it("owner-less lock dir is not auto-deleted; acquisition times out", async () => {
		const cacheRoot = tempDir("momo-lock-grace-");
		const cwd = tempDir("momo-lock-cwd3-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const lockDir = path.join(pool.poolRoot, "roles", "planner", "tx.lock");
		mkdirSync(lockDir, { recursive: true, mode: 0o700 });
		const lock = new RoleTransactionLock(pool.poolRoot, "planner", {
			missingOwnerGraceMs: 1,
			staleMs: 1,
		});
		await expect(lock.acquire(150)).rejects.toThrow(/Timed out/);
		expect(existsSync(lockDir)).toBe(true);
		rmSync(lockDir, { recursive: true, force: true });
	});

	it("normal owner release allows a successor to acquire", async () => {
		const cacheRoot = tempDir("momo-lock-release-succ-");
		const cwd = tempDir("momo-lock-release-succ-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const first = new RoleTransactionLock(pool.poolRoot, "reviewer");
		await first.acquire(1_000);
		const firstToken = first.getTokenForTest();
		expect(firstToken).toBeTruthy();
		first.release();
		expect(existsSync(first.lockDir)).toBe(false);

		const successor = new RoleTransactionLock(pool.poolRoot, "reviewer");
		await successor.acquire(1_000);
		expect(successor.getTokenForTest()).toBeTruthy();
		expect(successor.getTokenForTest()).not.toBe(firstToken);
		successor.release();
	});
});

describe("durable FIFO claim", () => {
	it("keeps queue item until commitClaim; recovers claiming", () => {
		const cacheRoot = tempDir("momo-claim-cache-");
		const cwd = tempDir("momo-claim-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const entry = enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: "abcdef0123456789",
			workerId: "scout_x",
			generation: 1,
			parentEpoch: "epoch1",
			task: "t1",
		});
		expect(listQueue(pool.poolRoot, "scout")).toHaveLength(1);
		const claimed = withRoleLock(pool.poolRoot, "scout", () =>
			beginClaimHead(pool.poolRoot, "scout", 1),
		);
		expect(claimed?.assignmentId).toBe(entry.assignmentId);
		expect(listQueue(pool.poolRoot, "scout")).toHaveLength(0);
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(1);
		// Crash window: claiming remains until commit.
		commitClaim(pool.poolRoot, "scout", entry);
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(0);
		expect(queueCount(pool.poolRoot, "scout")).toBe(0);
	});

	it("fails closed on corrupt queue JSON", () => {
		const cacheRoot = tempDir("momo-qcorrupt-");
		const cwd = tempDir("momo-qcorrupt-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const dir = path.join(pool.poolRoot, "roles", "scout", "queue");
		mkdirSync(dir, { recursive: true, mode: 0o700 });
		writeFileSync(path.join(dir, "00000001-abcdef0123456789.json"), "{not-json\n", {
			mode: 0o600,
		});
		expect(() => listQueue(pool.poolRoot, "scout")).toThrow();
	});

	it("cancels exact queued assignment only", () => {
		const cacheRoot = tempDir("momo-qcancel-");
		const cwd = tempDir("momo-qcancel-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		enqueueAssignment(pool.poolRoot, "planner", {
			assignmentId: "aaaaaaaaaaaaaaaa",
			workerId: "p1",
			generation: 2,
			parentEpoch: "e1",
			task: "a",
		});
		enqueueAssignment(pool.poolRoot, "planner", {
			assignmentId: "bbbbbbbbbbbbbbbb",
			workerId: "p1",
			generation: 2,
			parentEpoch: "e1",
			task: "b",
		});
		expect(cancelQueuedAssignment(pool.poolRoot, "planner", "aaaaaaaaaaaaaaaa")).toBe(true);
		expect(listQueue(pool.poolRoot, "planner").map((e) => e.assignmentId)).toEqual([
			"bbbbbbbbbbbbbbbb",
		]);
	});
});

describe("per-role registry", () => {
	it("allows concurrent different-role updates without lost writes", async () => {
		const cacheRoot = tempDir("momo-reg-cache-");
		const cwd = tempDir("momo-reg-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		await Promise.all([
			withRoleLockAsync(pool.poolRoot, "scout", () => {
				pool.upsert({
					workerId: "scout_1",
					generation: 1,
					generationTombstone: 1,
					role: "scout",
					status: "idle",
					updatedAt: new Date().toISOString(),
				});
			}),
			withRoleLockAsync(pool.poolRoot, "planner", () => {
				pool.upsert({
					workerId: "planner_1",
					generation: 2,
					generationTombstone: 2,
					role: "planner",
					status: "busy",
					updatedAt: new Date().toISOString(),
				});
			}),
		]);
		expect(pool.getByRole("scout")?.generation).toBe(1);
		expect(pool.getByRole("planner")?.generation).toBe(2);
		expect(pool.nextGeneration("scout")).toBe(2);
	});

	it("retains monotonic generation tombstone across archive", () => {
		const cacheRoot = tempDir("momo-tomb-");
		const cwd = tempDir("momo-tomb-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		pool.upsert({
			workerId: "impl_1",
			generation: 3,
			generationTombstone: 3,
			role: "implementer",
			paneId: "w1:p1",
			agentName: "momo_impl",
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		pool.archiveRoleKeepingTombstone("implementer", new Date().toISOString());
		expect(pool.getByRole("implementer")?.generationTombstone).toBe(3);
		expect(pool.nextGeneration("implementer")).toBe(4);
		expect(isArchivalTombstone(pool.getByRole("implementer")!)).toBe(true);
		expect(isNonReusableLiveWorker(pool.getByRole("implementer")!)).toBe(false);
	});

	it("rejects live unhealthy worker but treats archival tombstone as recreatable", () => {
		const cacheRoot = tempDir("momo-tomb-reuse-");
		const cwd = tempDir("momo-tomb-reuse-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		pool.upsert({
			workerId: "scout_live",
			generation: 2,
			generationTombstone: 2,
			role: "scout",
			paneId: "w1:p9",
			agentName: "momo_scout",
			status: "unhealthy",
			updatedAt: new Date().toISOString(),
		});
		expect(isArchivalTombstone(pool.getByRole("scout")!)).toBe(false);
		expect(isNonReusableLiveWorker(pool.getByRole("scout")!)).toBe(true);

		pool.archiveRoleKeepingTombstone("scout", new Date().toISOString());
		const tomb = pool.getByRole("scout")!;
		expect(isArchivalTombstone(tomb)).toBe(true);
		expect(isNonReusableLiveWorker(tomb)).toBe(false);
		expect(tomb.generation).toBe(0);
		expect(tomb.paneId).toBeUndefined();
		expect(pool.nextGeneration("scout")).toBe(3);
	});
});

describe("crash windows and recovery", () => {
	it("recovers incomplete claim before durable active commit", () => {
		const cacheRoot = tempDir("momo-crash-claim-");
		const cwd = tempDir("momo-crash-claim-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const entry = enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: "cccccccccccccccc",
			workerId: "scout_x",
			generation: 1,
			parentEpoch: "e1",
			task: "recover-me",
		});
		withRoleLock(pool.poolRoot, "scout", () => {
			beginClaimHead(pool.poolRoot, "scout", 1);
		});
		// Crash before commitClaim: item is in claiming/, not queue/.
		expect(listQueue(pool.poolRoot, "scout")).toHaveLength(0);
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(1);
		const recovered = withRoleLock(pool.poolRoot, "scout", () =>
			recoverClaiming(pool.poolRoot, "scout", 1),
		);
		expect(recovered[0]?.assignmentId).toBe(entry.assignmentId);
		commitClaim(pool.poolRoot, "scout", entry);
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(0);
	});

	it("generation fence rejects foreign-generation claim recovery", () => {
		const cacheRoot = tempDir("momo-gen-fence-");
		const cwd = tempDir("momo-gen-fence-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		enqueueAssignment(pool.poolRoot, "planner", {
			assignmentId: "dddddddddddddddd",
			workerId: "p1",
			generation: 1,
			parentEpoch: "e1",
			task: "old-gen",
		});
		withRoleLock(pool.poolRoot, "planner", () => {
			beginClaimHead(pool.poolRoot, "planner", 1);
		});
		expect(() =>
			withRoleLock(pool.poolRoot, "planner", () => recoverClaiming(pool.poolRoot, "planner", 2)),
		).toThrow(/generation fence/);
		expect(listClaiming(pool.poolRoot, "planner")).toHaveLength(1);
	});
});

describe("multi-parent same-role contention", () => {
	it("serializes same-role registry updates under role lock", async () => {
		const cacheRoot = tempDir("momo-contend-");
		const cwd = tempDir("momo-contend-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		pool.upsert({
			workerId: "scout_1",
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		await Promise.all(
			Array.from({ length: 8 }, (_, i) =>
				withRoleLockAsync(pool.poolRoot, "scout", async () => {
					const cur = pool.getByRole("scout")!;
					pool.upsert({
						...cur,
						status: i % 2 === 0 ? "busy" : "idle",
						updatedAt: new Date().toISOString(),
					});
				}),
			),
		);
		const final = pool.getByRole("scout");
		expect(final?.generation).toBe(1);
		expect(["idle", "busy"]).toContain(final?.status);
	});
});

describe("adoption heartbeat freshness", () => {
	it("refuses adoption when heartbeat is stale", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const { workerControlPaths } = await import("../src/herdr/assignment-spool.js");
		const { atomicWriteJson } = await import("../src/ipc/spool.js");
		const { HerdrClient } = await import("../src/herdr/client.js");
		const cacheRoot = tempDir("momo-adopt-stale-");
		const cwd = tempDir("momo-adopt-stale-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = "scout_adopt1";
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			cwd,
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId,
			generation: 1,
			role: "scout",
			cwd,
			paneId: "w1:p1",
			agentName: "momo_scout",
			createdAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date(Date.now() - 60_000).toISOString(),
			seq: 1,
		});
		const notes: string[] = [];
		const client = new HerdrClient({
			runCommand: async () => ({
				code: 0,
				stdout: JSON.stringify({
					id: "ok",
					result: { pane_id: "w1:p1", name: "momo_scout", agent: "pi", agent_status: "idle" },
				}),
				stderr: "",
			}),
		});
		await adoptPoolWorkers(pool, client, identity.poolKey, {
			ui: { notify: (m: string) => notes.push(m) },
		});
		expect(notes.join("\n")).toMatch(/stale heartbeat|Did not adopt/i);
		expect(pool.getByRole("scout")?.status).toBe("unhealthy");
	});
});

describe("archive then reprovision", () => {
	const previousPiDir = process.env.PI_CODING_AGENT_DIR;

	afterEach(() => {
		if (previousPiDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
		else process.env.PI_CODING_AGENT_DIR = previousPiDir;
	});

	function installFakeHerdrExtension(): void {
		const agentDir = tempDir("momo-pi-agent-");
		mkdirSync(path.join(agentDir, "extensions"), { recursive: true });
		writeFileSync(path.join(agentDir, "extensions", "herdr-agent-state.ts"), "// fake\n", "utf8");
		process.env.PI_CODING_AGENT_DIR = agentDir;
	}

	it("cleanup/archive then new delegation provisions generation N+1 instead of throwing", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-reprov-cache-");
		const cwd = tempDir("momo-reprov-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		pool.upsert({
			workerId: stableWorkerId(identity.poolKey, "scout"),
			generation: 5,
			generationTombstone: 5,
			role: "scout",
			paneId: "w1:p-old",
			agentName: "momo_scout_old",
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		pool.archiveRoleKeepingTombstone("scout", new Date().toISOString());
		expect(isArchivalTombstone(pool.getByRole("scout")!)).toBe(true);
		expect(pool.nextGeneration("scout")).toBe(6);

		let provisionedGen: string | undefined;
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
					provisionedGen = envArgs
						.find((v) => v.startsWith("MOMO_WORKER_GENERATION="))
						?.slice("MOMO_WORKER_GENERATION=".length);
					const control =
						envArgs.find((v) => v.startsWith("MOMO_CONTROL_DIR="))?.slice("MOMO_CONTROL_DIR=".length) ??
						envArgs.find((v) => v.startsWith("MOMO_IPC_DIR="))?.slice("MOMO_IPC_DIR=".length);
					const workerId =
						envArgs.find((v) => v.startsWith("MOMO_WORKER_ID="))?.slice("MOMO_WORKER_ID=".length) ?? "";
					const runId =
						envArgs.find((v) => v.startsWith("MOMO_RUN_ID="))?.slice("MOMO_RUN_ID=".length) ?? "";
					if (control) {
						mkdirSync(control, { recursive: true, mode: 0o700 });
						atomicWriteJson(path.join(control, "ready.json"), {
							version: 1,
							runId,
							workerId,
							readyAt: new Date().toISOString(),
						});
					}
					return {
						code: 0,
						stdout: JSON.stringify({ id: "s", result: { pane: { pane_id: "w1:p-new" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: {
								pane_id: "w1:p-new",
								name: args[2],
								agent: "pi",
								interactive_ready: true,
							},
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-reprov",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			pollIntervalMs: 20,
			readyTimeoutMs: 2_000,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});
		const session = await factory({ cwd, role: getRole("scout") });
		await session.prompt("after cleanup");
		expect(provisionedGen).toBe("6");
		expect(pool.getByRole("scout")?.generation).toBe(6);
		expect(pool.getByRole("scout")?.generationTombstone).toBeGreaterThanOrEqual(6);
		expect(pool.getByRole("scout")?.paneId).toBe("w1:p-new");
		await session.dispose();
	});

	it("still refuses genuinely unhealthy live workers", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-refuse-live-");
		const cwd = tempDir("momo-refuse-live-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		pool.upsert({
			workerId: stableWorkerId(identity.poolKey, "planner"),
			generation: 2,
			generationTombstone: 2,
			role: "planner",
			paneId: "w1:p2",
			agentName: "momo_planner",
			status: "unhealthy",
			updatedAt: new Date().toISOString(),
		});
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-refuse",
			client: new HerdrClient({
				runCommand: async () => ({
					code: 0,
					stdout: JSON.stringify({ id: "ok", result: {} }),
					stderr: "",
				}),
			}),
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			sleep: async () => {},
		});
		const session = await factory({ cwd, role: getRole("planner") });
		await expect(session.prompt("nope")).rejects.toThrow(/unhealthy|cannot be reused/i);
	});
});

describe("worker registry transitions under role lock", () => {
	afterEach(() => {
		vi.useRealTimers();
	});

	it("claimNextOrIdle nested registry updates do not re-enter the role lock", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-lock-nested-");
		const cwd = tempDir("momo-lock-nested-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: "aaaaaaaaaaaaaaaa",
			workerId,
			generation: 1,
			parentEpoch: "e1",
			task: "queued-one",
		});

		const handlers = new Map<string, Function[]>();
		let runtimeReady = false;
		const pi = {
			handlers,
			on(event: string, handler: Function) {
				const list = handlers.get(event) ?? [];
				list.push(handler);
				handlers.set(event, list);
			},
			sendUserMessage: vi.fn(),
			setActiveTools: vi.fn(),
			registerTool: vi.fn(),
			async emit(event: string, payload: unknown = {}, ctx?: ExtensionContext) {
				if (event === "session_start") runtimeReady = true;
				void runtimeReady;
				for (const handler of handlers.get(event) ?? []) {
					await handler(payload, ctx);
				}
			},
		};
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "scout",
				MOMO_IPC_DIR: control.root,
				MOMO_CONTROL_DIR: control.root,
				MOMO_WORKER_ID: workerId,
				MOMO_WORKER_GENERATION: "1",
				MOMO_POOL_KEY: identity.poolKey,
				MOMO_POOL_ROOT: pool.poolRoot,
				MOMO_RUN_ID: "g1",
				MOMO_CWD: cwd,
			},
			poolRegistry: pool,
			now: () => 1_700_000_000_000,
			sleep: async () => {},
		});
		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;
		// Would hang/timeout if claimNextOrIdleLocked re-entered withRoleLock via markRegistry.
		await pi.emit("session_start", {}, ctx);
		expect(pool.getByRole("scout")?.status).toBe("busy");
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe("aaaaaaaaaaaaaaaa");
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).toHaveBeenCalled();
	});
});

describe("starting reservation wait (no duplicate provision)", () => {
	const previousPiDir = process.env.PI_CODING_AGENT_DIR;

	afterEach(() => {
		if (previousPiDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
		else process.env.PI_CODING_AGENT_DIR = previousPiDir;
		vi.useRealTimers();
	});

	function installFakeHerdrExtension(): void {
		const agentDir = tempDir("momo-pi-agent-");
		mkdirSync(path.join(agentDir, "extensions"), { recursive: true });
		writeFileSync(path.join(agentDir, "extensions", "herdr-agent-state.ts"), "// fake\n", "utf8");
		process.env.PI_CODING_AGENT_DIR = agentDir;
	}

	it("two factories: A split paused, B waits — one split/start, same gen/pane, FIFO queue", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-dup-prov-");
		const cwd = tempDir("momo-dup-prov-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const splits: string[] = [];
		const starts: string[] = [];
		let releaseSplit!: () => void;
		const splitGate = new Promise<void>((resolve) => {
			releaseSplit = resolve;
		});
		let splitEntered = 0;

		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					splitEntered += 1;
					splits.push("split");
					const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
					const control =
						envArgs.find((v) => v.startsWith("MOMO_CONTROL_DIR="))?.slice("MOMO_CONTROL_DIR=".length) ??
						envArgs.find((v) => v.startsWith("MOMO_IPC_DIR="))?.slice("MOMO_IPC_DIR=".length);
					const workerId =
						envArgs.find((v) => v.startsWith("MOMO_WORKER_ID="))?.slice("MOMO_WORKER_ID=".length) ?? "";
					const runId =
						envArgs.find((v) => v.startsWith("MOMO_RUN_ID="))?.slice("MOMO_RUN_ID=".length) ?? "";
					if (splitEntered === 1) {
						await splitGate;
					}
					if (control) {
						mkdirSync(control, { recursive: true, mode: 0o700 });
						atomicWriteJson(path.join(control, "ready.json"), {
							version: 1,
							runId,
							workerId,
							readyAt: new Date().toISOString(),
						});
					}
					return {
						code: 0,
						stdout: JSON.stringify({ id: "s", result: { pane: { pane_id: "w1:p-shared" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					starts.push(String(args[2] ?? ""));
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: {
								pane_id: "w1:p-shared",
								name: args[2],
								agent: "pi",
								interactive_ready: true,
							},
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});

		const sleep = async (ms: number): Promise<void> => {
			await new Promise<void>((r) => setTimeout(r, ms));
		};
		const factoryOpts = {
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-dup",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			pollIntervalMs: 20,
			readyTimeoutMs: 5_000,
			sleep,
		} as const;
		const factoryA = createHerdrChildSessionFactory(factoryOpts);
		const factoryB = createHerdrChildSessionFactory({
			...factoryOpts,
			parentId: "parent-dup-b",
			parentEpoch: "epoch-b",
		});

		const sessionA = await factoryA({ cwd, role: getRole("scout") });
		const promptA = sessionA.prompt("task-a");
		// Wait until A has reserved starting (and entered split).
		for (let i = 0; i < 50 && splitEntered < 1; i += 1) await sleep(20);
		expect(splitEntered).toBe(1);
		expect(pool.getByRole("scout")?.status).toBe("starting");
		expect(pool.getByRole("scout")?.paneId).toBeUndefined();

		const sessionB = await factoryB({ cwd, role: getRole("scout") });
		const promptB = sessionB.prompt("task-b");
		// Give B time to observe starting and enter wait-ready (must not split again).
		await sleep(100);
		expect(splits).toHaveLength(1);

		releaseSplit();
		await promptA;
		await promptB;

		expect(splits).toHaveLength(1);
		expect(starts).toHaveLength(1);
		const record = pool.getByRole("scout")!;
		expect(record.generation).toBe(1);
		expect(record.paneId).toBe("w1:p-shared");
		expect(record.status).toBe("busy");
		expect(record.activeAssignmentId).toBeTruthy();
		expect(queueCount(pool.poolRoot, "scout")).toBe(1);
		expect(listQueue(pool.poolRoot, "scout")[0]?.task).toBe("task-b");
		await sessionA.dispose();
		await sessionB.dispose();
	}, 15_000);
});

describe("dispatch/claim durable order fault injection", () => {
	it("worker publishClaimLocked: registry busy failure does not publish active; claiming recoverable", async () => {
		vi.useFakeTimers();
		try {
			const cacheRoot = tempDir("momo-fault-claim-");
			const cwd = tempDir("momo-fault-claim-cwd-");
			const identity = resolvePoolIdentity({
				cwd,
				canonicalRoot: cwd,
				workspaceId: "ws",
				socketPath: "s",
			});
			const pool = new PoolRegistry(identity.poolKey, cacheRoot);
			const workerId = stableWorkerId(identity.poolKey, "planner");
			pool.upsert({
				workerId,
				generation: 1,
				generationTombstone: 1,
				role: "planner",
				paneId: "w1:p1",
				agentName: "momo_planner",
				status: "idle",
				updatedAt: new Date().toISOString(),
			});
			const entry = enqueueAssignment(pool.poolRoot, "planner", {
				assignmentId: "cccccccccccccccc",
				workerId,
				generation: 1,
				parentEpoch: "e1",
				task: "fault-me",
			});
			const control = workerControlPaths(pool.poolRoot, "planner");
			mkdirSync(control.root, { recursive: true, mode: 0o700 });

			const originalUpsert = pool.upsert.bind(pool);
			pool.upsert = ((record) => {
				if (record.status === "busy") {
					throw new Error("injected registry busy failure");
				}
				return originalUpsert(record);
			}) as typeof pool.upsert;

			const handlers = new Map<string, Function[]>();
			const pi = {
				handlers,
				on(event: string, handler: Function) {
					const list = handlers.get(event) ?? [];
					list.push(handler);
					handlers.set(event, list);
				},
				sendUserMessage: vi.fn(),
				setActiveTools: vi.fn(),
				registerTool: vi.fn(),
				async emit(event: string, payload: unknown = {}, ctx?: ExtensionContext) {
					for (const handler of handlers.get(event) ?? []) {
						await handler(payload, ctx);
					}
				},
			};
			installMomoWorker(pi as unknown as ExtensionAPI, {
				env: {
					MOMO_WORKER: "1",
					MOMO_ROLE: "planner",
					MOMO_IPC_DIR: control.root,
					MOMO_CONTROL_DIR: control.root,
					MOMO_WORKER_ID: workerId,
					MOMO_WORKER_GENERATION: "1",
					MOMO_POOL_KEY: identity.poolKey,
					MOMO_POOL_ROOT: pool.poolRoot,
					MOMO_RUN_ID: "g1",
					MOMO_CWD: cwd,
				},
				poolRegistry: pool,
				now: () => 1_700_000_000_000,
				sleep: async () => {},
			});
			const ctx = {
				hasUI: true,
				isIdle: () => true,
				abort: vi.fn(),
				cwd,
			} as unknown as ExtensionContext;

			await expect(pi.emit("session_start", {}, ctx)).rejects.toThrow(
				/injected registry busy failure/,
			);
			expect(existsSync(control.active)).toBe(false);
			expect(listClaiming(pool.poolRoot, "planner")).toHaveLength(1);
			expect(pool.getByRole("planner")?.status).toBe("idle");

			pool.upsert = originalUpsert;
			const recovered = withRoleLock(pool.poolRoot, "planner", () =>
				recoverClaiming(pool.poolRoot, "planner", 1),
			);
			expect(recovered[0]?.assignmentId).toBe(entry.assignmentId);
		} finally {
			vi.useRealTimers();
		}
	});

	it("parent dispatchActiveLocked: registry busy failure does not publish active.json", async () => {
		const previousPiDir = process.env.PI_CODING_AGENT_DIR;
		const agentDir = tempDir("momo-pi-agent-fault-");
		mkdirSync(path.join(agentDir, "extensions"), { recursive: true });
		writeFileSync(path.join(agentDir, "extensions", "herdr-agent-state.ts"), "// fake\n", "utf8");
		process.env.PI_CODING_AGENT_DIR = agentDir;
		try {
			const cacheRoot = tempDir("momo-fault-parent-");
			const cwd = tempDir("momo-fault-parent-cwd-");
			const identity = resolvePoolIdentity({
				cwd,
				canonicalRoot: cwd,
				workspaceId: "ws",
				socketPath: "s",
			});
			const pool = new PoolRegistry(identity.poolKey, cacheRoot);
			const workerId = stableWorkerId(identity.poolKey, "scout");
			const control = workerControlPaths(pool.poolRoot, "scout");
			mkdirSync(control.root, { recursive: true, mode: 0o700 });
			pool.upsert({
				workerId,
				generation: 1,
				generationTombstone: 1,
				role: "scout",
				paneId: "w1:p1",
				agentName: "momo_scout",
				cwd: identity.canonicalRoot,
				status: "idle",
				updatedAt: new Date().toISOString(),
			});

			const originalUpsert = pool.upsert.bind(pool);
			pool.upsert = ((record) => {
				if (record.status === "busy") {
					throw new Error("injected registry busy failure");
				}
				return originalUpsert(record);
			}) as typeof pool.upsert;

			const factory = createHerdrChildSessionFactory({
				cwd,
				parentPaneId: "w1:p0",
				parentId: "parent-fault",
				client: new HerdrClient({
					runCommand: async () => ({
						code: 0,
						stdout: JSON.stringify({ id: "ok", result: {} }),
						stderr: "",
					}),
				}),
				poolRegistry: pool,
				cacheRoot,
				canonicalRoot: cwd,
				workspaceId: "ws",
				socketPath: "s",
				sleep: async () => {},
			});
			const session = await factory({ cwd, role: getRole("scout") });
			const assignmentId = (session as unknown as { runId: string }).runId;
			const cmd = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentId).command;
			await expect(session.prompt("boom")).rejects.toThrow(/injected registry busy failure/);
			expect(existsSync(control.active)).toBe(false);
			expect(existsSync(cmd)).toBe(true); // command may exist; active must not
			expect(pool.getByRole("scout")?.status).toBe("idle");
			expect(pool.getByRole("scout")?.activeAssignmentId).toBeUndefined();
			pool.upsert = originalUpsert;
			await session.dispose();
		} finally {
			if (previousPiDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
			else process.env.PI_CODING_AGENT_DIR = previousPiDir;
		}
	});

	it("parent dispatchActiveLocked writes command, then busy, then active", async () => {
		const previousPiDir = process.env.PI_CODING_AGENT_DIR;
		const agentDir = tempDir("momo-pi-agent-");
		mkdirSync(path.join(agentDir, "extensions"), { recursive: true });
		writeFileSync(path.join(agentDir, "extensions", "herdr-agent-state.ts"), "// fake\n", "utf8");
		process.env.PI_CODING_AGENT_DIR = agentDir;
		try {
			const cacheRoot = tempDir("momo-order-");
			const cwd = tempDir("momo-order-cwd-");
			const identity = resolvePoolIdentity({
				cwd,
				canonicalRoot: cwd,
				workspaceId: "ws",
				socketPath: "s",
			});
			const pool = new PoolRegistry(identity.poolKey, cacheRoot);
			const workerId = stableWorkerId(identity.poolKey, "scout");
			const control = workerControlPaths(pool.poolRoot, "scout");
			mkdirSync(control.root, { recursive: true, mode: 0o700 });
			atomicWriteJson(path.join(control.root, "ready.json"), {
				version: 1,
				runId: "g1",
				workerId,
				readyAt: new Date().toISOString(),
			});
			pool.upsert({
				workerId,
				generation: 1,
				generationTombstone: 1,
				role: "scout",
				paneId: "w1:p1",
				agentName: "momo_scout",
				cwd: identity.canonicalRoot,
				status: "idle",
				updatedAt: new Date().toISOString(),
			});

			const order: string[] = [];
			let assignmentId = "";
			const originalUpsert = pool.upsert.bind(pool);
			pool.upsert = ((record) => {
				if (record.status === "busy") {
					assignmentId = record.activeAssignmentId ?? "";
					const cmd = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentId).command;
					order.push("registry-busy");
					expect(existsSync(cmd)).toBe(true);
					expect(existsSync(control.active)).toBe(false);
				}
				return originalUpsert(record);
			}) as typeof pool.upsert;

			const client = new HerdrClient({
				runCommand: async () => ({
					code: 0,
					stdout: JSON.stringify({ id: "ok", result: {} }),
					stderr: "",
				}),
			});
			const factory = createHerdrChildSessionFactory({
				cwd,
				parentPaneId: "w1:p0",
				parentId: "parent-order",
				client,
				poolRegistry: pool,
				cacheRoot,
				canonicalRoot: cwd,
				workspaceId: "ws",
				socketPath: "s",
				sleep: async () => {},
			});
			const session = await factory({ cwd, role: getRole("scout") });
			expect(existsSync(control.active)).toBe(false);
			await session.prompt("only-dispatch");
			expect(order).toEqual(["registry-busy"]);
			expect(existsSync(control.active)).toBe(true);
			expect(pool.getByRole("scout")?.status).toBe("busy");
			expect(pool.getByRole("scout")?.activeAssignmentId).toBe(assignmentId);
			pool.upsert = originalUpsert;
			await session.dispose();
		} finally {
			if (previousPiDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
			else process.env.PI_CODING_AGENT_DIR = previousPiDir;
		}
	});
});

describe("adoption generation/worker fence", () => {
	it("stale adoption snapshot cannot overwrite newer busy assignment", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const cacheRoot = tempDir("momo-adopt-race-");
		const cwd = tempDir("momo-adopt-race-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			cwd,
			status: "busy",
			activeAssignmentId: "oldoldoldoldold1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId,
			generation: 1,
			role: "scout",
			cwd,
			paneId: "w1:p1",
			agentName: "momo_scout",
			createdAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 1,
		});
		const oldPaths = assignmentSpoolPaths(pool.poolRoot, "scout", "oldoldoldoldold1");
		mkdirSync(oldPaths.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(oldPaths.result, {
			version: 1,
			runId: "oldoldoldoldold1",
			workerId,
			status: "completed",
			messages: [{ role: "assistant", content: [{ type: "text", text: "done" }] }],
			finishedAt: new Date().toISOString(),
		});

		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "agent" && args[1] === "get") {
					pool.upsert({
						workerId,
						generation: 1,
						generationTombstone: 1,
						role: "scout",
						paneId: "w1:p1",
						agentName: "momo_scout",
						cwd,
						status: "busy",
						activeAssignmentId: "newnewnewnewnew2",
						updatedAt: new Date().toISOString(),
					});
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "g",
							result: {
								type: "agent_info",
								agent: { agent_status: "idle", pane_id: "w1:p1", name: "momo_scout" },
							},
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});

		await adoptPoolWorkers(pool, client, identity.poolKey, { ui: { notify: () => undefined } });
		const final = pool.getByRole("scout")!;
		expect(final.status).toBe("busy");
		expect(final.activeAssignmentId).toBe("newnewnewnewnew2");
	});

	it("missing Herdr paneId is an adoption identity mismatch (fail closed)", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const cacheRoot = tempDir("momo-adopt-nopane-");
		const cwd = tempDir("momo-adopt-nopane-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			cwd,
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId,
			generation: 1,
			role: "scout",
			cwd,
			paneId: "w1:p1",
			agentName: "momo_scout",
			createdAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 1,
		});
		const notes: string[] = [];
		const client = new HerdrClient({
			runCommand: async () => ({
				code: 0,
				stdout: JSON.stringify({
					id: "ok",
					result: {
						type: "agent_info",
						// Successful but incomplete — no pane_id.
						agent: { agent_status: "idle", name: "momo_scout" },
					},
				}),
				stderr: "",
			}),
		});
		await adoptPoolWorkers(pool, client, identity.poolKey, {
			ui: { notify: (m: string) => notes.push(m) },
		});
		expect(notes.join("\n")).toMatch(/pane identity|Did not adopt/i);
		expect(pool.getByRole("scout")?.status).toBe("unhealthy");
	});

	it("adoption of live worker without registry cwd fails closed", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const cacheRoot = tempDir("momo-adopt-nocwd-");
		const cwd = tempDir("momo-adopt-nocwd-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			// Missing registry cwd — pre-fix candidate.
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId,
			generation: 1,
			role: "scout",
			cwd,
			paneId: "w1:p1",
			agentName: "momo_scout",
			createdAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 1,
		});
		const notes: string[] = [];
		const client = new HerdrClient({
			runCommand: async () => ({
				code: 0,
				stdout: JSON.stringify({
					id: "ok",
					result: {
						type: "agent_info",
						agent: { agent_status: "idle", pane_id: "w1:p1", name: "momo_scout" },
					},
				}),
				stderr: "",
			}),
		});
		await adoptPoolWorkers(pool, client, identity.poolKey, {
			ui: { notify: (m: string) => notes.push(m) },
		});
		expect(notes.join("\n")).toMatch(/canonical cwd|Did not adopt/i);
		expect(pool.getByRole("scout")?.status).toBe("unhealthy");
	});

	it("stale busy-A snapshot does not overwrite current idle (success path)", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const cacheRoot = tempDir("momo-adopt-stale-idle-");
		const cwd = tempDir("momo-adopt-stale-idle-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "implementer");
		const control = workerControlPaths(pool.poolRoot, "implementer");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentA = "staleidleaaaaa01";
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p1",
			agentName: "momo_implementer",
			cwd,
			status: "busy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId,
			generation: 1,
			role: "implementer",
			cwd,
			paneId: "w1:p1",
			agentName: "momo_implementer",
			createdAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 1,
		});
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentA);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(pathsA.started, {
			version: 1,
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			startedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: assignmentA,
			generation: 1,
			parentEpoch: "e1",
			dispatchedAt: new Date().toISOString(),
		});

		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "agent" && args[1] === "get") {
					// Current advanced to idle before adoption lock runs.
					pool.upsert({
						workerId,
						generation: 1,
						generationTombstone: 1,
						role: "implementer",
						paneId: "w1:p1",
						agentName: "momo_implementer",
						cwd,
						status: "idle",
						updatedAt: new Date().toISOString(),
					});
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "g",
							result: {
								type: "agent_info",
								agent: { agent_status: "idle", pane_id: "w1:p1", name: "momo_implementer" },
							},
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});

		await adoptPoolWorkers(pool, client, identity.poolKey, { ui: { notify: () => undefined } });
		const final = pool.getByRole("implementer")!;
		expect(final.status).toBe("idle");
		expect(final.uncertainWrite).toBeUndefined();
		expect(final.activeAssignmentId).toBeUndefined();
	});

	it("stale busy-A snapshot does not overwrite current busy-B (success path)", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const cacheRoot = tempDir("momo-adopt-stale-b-");
		const cwd = tempDir("momo-adopt-stale-b-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "implementer");
		const control = workerControlPaths(pool.poolRoot, "implementer");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentA = "stalebsuccessaa1";
		const assignmentB = "stalebsuccessbb2";
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p1",
			agentName: "momo_implementer",
			cwd,
			status: "busy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId,
			generation: 1,
			role: "implementer",
			cwd,
			paneId: "w1:p1",
			agentName: "momo_implementer",
			createdAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 1,
		});
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentA);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(pathsA.result, {
			version: 1,
			runId: assignmentA,
			workerId,
			status: "completed",
			messages: [{ role: "assistant", content: [{ type: "text", text: "a" }] }],
			finishedAt: new Date().toISOString(),
		});

		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "agent" && args[1] === "get") {
					pool.upsert({
						workerId,
						generation: 1,
						generationTombstone: 1,
						role: "implementer",
						paneId: "w1:p1",
						agentName: "momo_implementer",
						cwd,
						status: "busy",
						activeAssignmentId: assignmentB,
						activeParentEpoch: "e2",
						updatedAt: new Date().toISOString(),
					});
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "g",
							result: {
								type: "agent_info",
								agent: { agent_status: "working", pane_id: "w1:p1", name: "momo_implementer" },
							},
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});

		await adoptPoolWorkers(pool, client, identity.poolKey, { ui: { notify: () => undefined } });
		const final = pool.getByRole("implementer")!;
		expect(final.status).toBe("busy");
		expect(final.activeAssignmentId).toBe(assignmentB);
		expect(final.activeParentEpoch).toBe("e2");
		expect(final.uncertainWrite).toBeUndefined();
	});

	it("stale busy-A snapshot does not overwrite current idle (catch path)", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const cacheRoot = tempDir("momo-adopt-catch-idle-");
		const cwd = tempDir("momo-adopt-catch-idle-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "implementer");
		const control = workerControlPaths(pool.poolRoot, "implementer");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentA = "catchidleaaaaa01";
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p1",
			agentName: "momo_implementer",
			cwd,
			status: "busy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId,
			generation: 1,
			role: "implementer",
			cwd,
			paneId: "w1:p1",
			agentName: "momo_implementer",
			createdAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 1,
		});

		const notes: string[] = [];
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "agent" && args[1] === "get") {
					// Current finished A → idle before catch lock; pane mismatch forces catch.
					pool.upsert({
						workerId,
						generation: 1,
						generationTombstone: 1,
						role: "implementer",
						paneId: "w1:p1",
						agentName: "momo_implementer",
						cwd,
						status: "idle",
						updatedAt: new Date().toISOString(),
					});
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "g",
							result: {
								type: "agent_info",
								agent: {
									agent_status: "idle",
									pane_id: "w1:other",
									name: "momo_implementer",
								},
							},
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		await adoptPoolWorkers(pool, client, identity.poolKey, {
			ui: { notify: (m: string) => notes.push(m) },
		});

		expect(notes.join("\n")).toMatch(/pane identity|Did not adopt/i);
		const final = pool.getByRole("implementer")!;
		expect(final.status).toBe("idle");
		expect(final.uncertainWrite).toBeUndefined();
		expect(final.activeAssignmentId).toBeUndefined();
	});

	it("stale busy-A snapshot does not overwrite current busy-B (catch path)", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const cacheRoot = tempDir("momo-adopt-catch-b-");
		const cwd = tempDir("momo-adopt-catch-b-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "implementer");
		const control = workerControlPaths(pool.poolRoot, "implementer");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentA = "catchbsuccessaa1";
		const assignmentB = "catchbsuccessbb2";
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p1",
			agentName: "momo_implementer",
			cwd,
			status: "busy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId,
			generation: 1,
			role: "implementer",
			cwd,
			paneId: "w1:p1",
			agentName: "momo_implementer",
			createdAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 1,
		});

		const notes: string[] = [];
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "agent" && args[1] === "get") {
					pool.upsert({
						workerId,
						generation: 1,
						generationTombstone: 1,
						role: "implementer",
						paneId: "w1:p1",
						agentName: "momo_implementer",
						cwd,
						status: "busy",
						activeAssignmentId: assignmentB,
						activeParentEpoch: "e2",
						updatedAt: new Date().toISOString(),
					});
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "g",
							result: {
								type: "agent_info",
								agent: {
									agent_status: "working",
									pane_id: "w1:other",
									name: "momo_implementer",
								},
							},
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		await adoptPoolWorkers(pool, client, identity.poolKey, {
			ui: { notify: (m: string) => notes.push(m) },
		});

		expect(notes.join("\n")).toMatch(/pane identity|Did not adopt/i);
		const final = pool.getByRole("implementer")!;
		expect(final.status).toBe("busy");
		expect(final.activeAssignmentId).toBe(assignmentB);
		expect(final.activeParentEpoch).toBe("e2");
		expect(final.uncertainWrite).toBeUndefined();
	});

	it("missing pane/agent on active implementer marks uncertain not unhealthy", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const cacheRoot = tempDir("momo-adopt-missing-");
		const cwd = tempDir("momo-adopt-missing-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "implementer");
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			// Missing paneId/agentName → early path.
			status: "busy",
			activeAssignmentId: "missingpaneaaa01",
			activeParentEpoch: "e1",
			cwd,
			updatedAt: new Date().toISOString(),
		});
		const client = new HerdrClient({
			runCommand: async () => ({ code: 0, stdout: "{}", stderr: "" }),
		});
		await adoptPoolWorkers(pool, client, identity.poolKey, { ui: { notify: () => undefined } });
		expect(pool.getByRole("implementer")?.status).toBe("uncertain");
		expect(pool.getByRole("implementer")?.uncertainWrite).toBe(true);
		expect(pool.getByRole("implementer")?.activeAssignmentId).toBe("missingpaneaaa01");
	});

	it("concurrent adopt of pane-less starting leaves intact; provisioner finalizes", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const cacheRoot = tempDir("momo-adopt-starting-");
		const cwd = tempDir("momo-adopt-starting-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		// Pane-less starting reservation (normal in-flight provision).
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			status: "starting",
			cwd,
			updatedAt: new Date().toISOString(),
		});
		const client = new HerdrClient({
			runCommand: async () => ({ code: 0, stdout: "{}", stderr: "" }),
		});
		const notes: string[] = [];
		await adoptPoolWorkers(pool, client, identity.poolKey, {
			ui: { notify: (m: string) => notes.push(m) },
		});
		const mid = pool.getByRole("scout")!;
		expect(mid.status).toBe("starting");
		expect(mid.paneId).toBeUndefined();
		expect(mid.agentName).toBeUndefined();
		expect(notes.join("\n")).not.toMatch(/unhealthy|uncertain/i);

		// Provisioning owner finalizes after adopt no-op.
		pool.upsert({
			...mid,
			paneId: "w1:p1",
			agentName: "momo_scout",
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		expect(pool.getByRole("scout")?.status).toBe("idle");
		expect(pool.getByRole("scout")?.paneId).toBe("w1:p1");
	});

	it("snapshot idle→current busy B during validation failure stays B", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const cacheRoot = tempDir("momo-adopt-idle-to-b-");
		const cwd = tempDir("momo-adopt-idle-to-b-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentB = "idletobusybbbb02";
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			cwd,
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId,
			generation: 1,
			role: "scout",
			cwd,
			paneId: "w1:p1",
			agentName: "momo_scout",
			createdAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 1,
		});

		const notes: string[] = [];
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "agent" && args[1] === "get") {
					pool.upsert({
						workerId,
						generation: 1,
						generationTombstone: 1,
						role: "scout",
						paneId: "w1:p1",
						agentName: "momo_scout",
						cwd,
						status: "busy",
						activeAssignmentId: assignmentB,
						activeParentEpoch: "e2",
						updatedAt: new Date().toISOString(),
					});
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "g",
							result: {
								type: "agent_info",
								agent: { agent_status: "working", pane_id: "w1:other", name: "momo_scout" },
							},
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		await adoptPoolWorkers(pool, client, identity.poolKey, {
			ui: { notify: (m: string) => notes.push(m) },
		});
		expect(notes.join("\n")).toMatch(/pane identity|Did not adopt/i);
		const final = pool.getByRole("scout")!;
		expect(final.status).toBe("busy");
		expect(final.activeAssignmentId).toBe(assignmentB);
		expect(final.activeParentEpoch).toBe("e2");
		expect(final.uncertainWrite).toBeUndefined();
	});

	it("snapshot starting→finalized/busy before early lock unchanged", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const cacheRoot = tempDir("momo-adopt-start-adv-");
		const cwd = tempDir("momo-adopt-start-adv-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "planner");
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "planner",
			status: "starting",
			cwd,
			updatedAt: new Date().toISOString(),
		});

		const client = new HerdrClient({
			runCommand: async () => ({ code: 0, stdout: "{}", stderr: "" }),
		});

		// Hold the role lock so adopt takes a starting snapshot, then waits;
		// finalize to busy before releasing so early lock sees a divergent current.
		let releaseHold!: () => void;
		const holdGate = new Promise<void>((resolve) => {
			releaseHold = resolve;
		});
		const hold = withRoleLockAsync(pool.poolRoot, "planner", async () => {
			await holdGate;
		});
		await Promise.resolve();
		await Promise.resolve();

		const adopt = adoptPoolWorkers(pool, client, identity.poolKey, {
			ui: { notify: () => undefined },
		});
		await Promise.resolve();
		await Promise.resolve();

		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "planner",
			paneId: "w1:p9",
			agentName: "momo_planner",
			cwd,
			status: "busy",
			activeAssignmentId: "startadvanced001",
			activeParentEpoch: "e9",
			updatedAt: new Date().toISOString(),
		});
		releaseHold();
		await hold;
		await adopt;

		const final = pool.getByRole("planner")!;
		expect(final.status).toBe("busy");
		expect(final.paneId).toBe("w1:p9");
		expect(final.agentName).toBe("momo_planner");
		expect(final.activeAssignmentId).toBe("startadvanced001");
		expect(final.uncertainWrite).toBeUndefined();
	});
});

describe("worker generation/worker registry fence", () => {
	afterEach(() => {
		vi.useRealTimers();
	});

	function makePi() {
		const handlers = new Map<string, Function[]>();
		return {
			handlers,
			on(event: string, handler: Function) {
				const list = handlers.get(event) ?? [];
				list.push(handler);
				handlers.set(event, list);
			},
			sendUserMessage: vi.fn(),
			setActiveTools: vi.fn(),
			registerTool: vi.fn(),
			async emit(event: string, payload: unknown = {}, ctx?: ExtensionContext) {
				for (const handler of handlers.get(event) ?? []) {
					await handler(payload, ctx);
				}
			},
		};
	}

	it("stale gen N cannot overwrite archival tombstone on session_start", async () => {
		const cacheRoot = tempDir("momo-fence-tomb-");
		const cwd = tempDir("momo-fence-tomb-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 3,
			generationTombstone: 3,
			role: "scout",
			paneId: "w1:p3",
			agentName: "momo_scout",
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		pool.archiveRoleKeepingTombstone("scout", new Date().toISOString());
		expect(isArchivalTombstone(pool.getByRole("scout")!)).toBe(true);
		const tombstoneBefore = structuredClone(pool.getByRole("scout")!);

		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: "tombstonetask001",
			workerId,
			generation: 3,
			parentEpoch: "e-old",
			task: "must-not-claim",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: "should-remain",
			generation: 3,
			parentEpoch: "e-old",
			dispatchedAt: new Date().toISOString(),
		});

		const pi = makePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "scout",
				MOMO_IPC_DIR: control.root,
				MOMO_CONTROL_DIR: control.root,
				MOMO_WORKER_ID: workerId,
				MOMO_WORKER_GENERATION: "3",
				MOMO_POOL_KEY: identity.poolKey,
				MOMO_POOL_ROOT: pool.poolRoot,
				MOMO_RUN_ID: "g3",
				MOMO_CWD: cwd,
			},
			poolRegistry: pool,
			now: () => 1_700_000_000_000,
			sleep: async () => {},
		});
		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;

		await expect(pi.emit("session_start", {}, ctx)).rejects.toThrow(
			/superseded by archival tombstone|no longer owns/i,
		);
		const after = pool.getByRole("scout")!;
		expect(after.generation).toBe(0);
		expect(after.generationTombstone).toBe(tombstoneBefore.generationTombstone);
		expect(isArchivalTombstone(after)).toBe(true);
		expect(queueCount(pool.poolRoot, "scout")).toBe(1);
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(0);
		expect(existsSync(control.active)).toBe(true);
		expect(pi.setActiveTools).toHaveBeenCalled();
	});

	it("stale gen N cannot overwrite live N+1 on session_start", async () => {
		const cacheRoot = tempDir("momo-fence-n1-");
		const cwd = tempDir("momo-fence-n1-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "planner");
		const control = workerControlPaths(pool.poolRoot, "planner");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 4,
			generationTombstone: 4,
			role: "planner",
			paneId: "w1:p4",
			agentName: "momo_planner",
			status: "busy",
			activeAssignmentId: "newgenassignment1",
			updatedAt: new Date().toISOString(),
		});
		const before = structuredClone(pool.getByRole("planner")!);
		enqueueAssignment(pool.poolRoot, "planner", {
			assignmentId: "oldgenqueued0001",
			workerId,
			generation: 3,
			parentEpoch: "e-old",
			task: "stale-must-not-claim",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: "newgenassignment1",
			generation: 4,
			parentEpoch: "e-new",
			dispatchedAt: new Date().toISOString(),
		});

		const pi = makePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "planner",
				MOMO_IPC_DIR: control.root,
				MOMO_CONTROL_DIR: control.root,
				MOMO_WORKER_ID: workerId,
				MOMO_WORKER_GENERATION: "3",
				MOMO_POOL_KEY: identity.poolKey,
				MOMO_POOL_ROOT: pool.poolRoot,
				MOMO_RUN_ID: "g3",
				MOMO_CWD: cwd,
			},
			poolRegistry: pool,
			now: () => 1_700_000_000_000,
			sleep: async () => {},
		});
		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;

		await expect(pi.emit("session_start", {}, ctx)).rejects.toThrow(/no longer owns/i);
		const after = pool.getByRole("planner")!;
		expect(after.generation).toBe(4);
		expect(after.status).toBe("busy");
		expect(after.activeAssignmentId).toBe(before.activeAssignmentId);
		expect(queueCount(pool.poolRoot, "planner")).toBe(1);
		expect(listClaiming(pool.poolRoot, "planner")).toHaveLength(0);
		expect(existsSync(control.active)).toBe(true);
	});

	it("after N→N+1 swap, finish does not claim/commit or rewrite registry/control", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-fence-swap-");
		const cwd = tempDir("momo-fence-swap-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 2,
			generationTombstone: 2,
			role: "scout",
			paneId: "w1:p2",
			agentName: "momo_scout",
			status: "idle",
			updatedAt: new Date().toISOString(),
		});

		const pi = makePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "scout",
				MOMO_IPC_DIR: control.root,
				MOMO_CONTROL_DIR: control.root,
				MOMO_WORKER_ID: workerId,
				MOMO_WORKER_GENERATION: "2",
				MOMO_POOL_KEY: identity.poolKey,
				MOMO_POOL_ROOT: pool.poolRoot,
				MOMO_RUN_ID: "g2",
				MOMO_CWD: cwd,
			},
			poolRegistry: pool,
			now: () => 1_700_000_000_000,
			sleep: async () => {},
		});
		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);

		const assignmentId = "swapfinish000001";
		const paths = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "finish-me",
			issuedAt: new Date().toISOString(),
			runId: assignmentId,
			workerId,
			generation: 2,
			parentEpoch: "e2",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId,
			generation: 2,
			parentEpoch: "e2",
			dispatchedAt: new Date().toISOString(),
		});
		pool.upsert({
			workerId,
			generation: 2,
			generationTombstone: 2,
			role: "scout",
			paneId: "w1:p2",
			agentName: "momo_scout",
			status: "busy",
			activeAssignmentId: assignmentId,
			updatedAt: new Date().toISOString(),
		});
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).toHaveBeenCalled();

		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: "mustnotclaim00002",
			workerId,
			generation: 2,
			parentEpoch: "e2",
			task: "queued-after-swap",
		});

		// Newer generation takes the role before finish advances the queue.
		pool.upsert({
			workerId,
			generation: 3,
			generationTombstone: 3,
			role: "scout",
			paneId: "w1:p3",
			agentName: "momo_scout_new",
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		const n1Before = structuredClone(pool.getByRole("scout")!);

		await pi.emit(
			"message_end",
			{
				message: {
					role: "assistant",
					content: [{ type: "text", text: "done" }],
					stopReason: "stop",
				},
			},
			ctx,
		);
		await pi.emit("agent_settled", {}, ctx);

		const after = pool.getByRole("scout")!;
		expect(after.generation).toBe(3);
		expect(after.status).toBe("idle");
		expect(after.paneId).toBe(n1Before.paneId);
		expect(after.agentName).toBe(n1Before.agentName);
		expect(after.activeAssignmentId).toBeUndefined();
		expect(queueCount(pool.poolRoot, "scout")).toBe(1);
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(0);
		// Fence failure must not remove control or claim the queued successor.
		expect(existsSync(control.active)).toBe(true);
		const activeRaw = JSON.parse(readFileSync(control.active, "utf8")) as {
			assignmentId?: string;
			generation?: number;
		};
		expect(activeRaw.assignmentId).toBe(assignmentId);
		expect(activeRaw.assignmentId).not.toBe("mustnotclaim00002");
		expect(pi.setActiveTools).toHaveBeenCalled();
	});

	it("after archive to tombstone, finish does not restore gen N over tombstone", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-fence-arch-");
		const cwd = tempDir("momo-fence-arch-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "reviewer");
		const control = workerControlPaths(pool.poolRoot, "reviewer");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 5,
			generationTombstone: 5,
			role: "reviewer",
			paneId: "w1:p5",
			agentName: "momo_reviewer",
			status: "idle",
			updatedAt: new Date().toISOString(),
		});

		const pi = makePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "reviewer",
				MOMO_IPC_DIR: control.root,
				MOMO_CONTROL_DIR: control.root,
				MOMO_WORKER_ID: workerId,
				MOMO_WORKER_GENERATION: "5",
				MOMO_POOL_KEY: identity.poolKey,
				MOMO_POOL_ROOT: pool.poolRoot,
				MOMO_RUN_ID: "g5",
				MOMO_CWD: cwd,
			},
			poolRegistry: pool,
			now: () => 1_700_000_000_000,
			sleep: async () => {},
		});
		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);

		const assignmentId = "archfinish000001";
		const paths = assignmentSpoolPaths(pool.poolRoot, "reviewer", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "finish-arch",
			issuedAt: new Date().toISOString(),
			runId: assignmentId,
			workerId,
			generation: 5,
			parentEpoch: "e5",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId,
			generation: 5,
			parentEpoch: "e5",
			dispatchedAt: new Date().toISOString(),
		});
		pool.upsert({
			...pool.getByRole("reviewer")!,
			status: "busy",
			activeAssignmentId: assignmentId,
			updatedAt: new Date().toISOString(),
		});
		await vi.advanceTimersByTimeAsync(200);

		enqueueAssignment(pool.poolRoot, "reviewer", {
			assignmentId: "archqueued000002",
			workerId,
			generation: 5,
			parentEpoch: "e5",
			task: "queued",
		});
		pool.archiveRoleKeepingTombstone("reviewer", new Date().toISOString());
		expect(isArchivalTombstone(pool.getByRole("reviewer")!)).toBe(true);

		await pi.emit(
			"message_end",
			{
				message: {
					role: "assistant",
					content: [{ type: "text", text: "done" }],
					stopReason: "stop",
				},
			},
			ctx,
		);
		await pi.emit("agent_settled", {}, ctx);

		const after = pool.getByRole("reviewer")!;
		expect(after.generation).toBe(0);
		expect(isArchivalTombstone(after)).toBe(true);
		expect(queueCount(pool.poolRoot, "reviewer")).toBe(1);
		expect(listClaiming(pool.poolRoot, "reviewer")).toHaveLength(0);
	});
});

describe("durable claim crash recovery", () => {
	afterEach(() => {
		vi.useRealTimers();
	});

	function makePi() {
		const handlers = new Map<string, Function[]>();
		return {
			handlers,
			on(event: string, handler: Function) {
				const list = handlers.get(event) ?? [];
				list.push(handler);
				handlers.set(event, list);
			},
			sendUserMessage: vi.fn(),
			setActiveTools: vi.fn(),
			registerTool: vi.fn(),
			async emit(event: string, payload: unknown = {}, ctx?: ExtensionContext) {
				for (const handler of handlers.get(event) ?? []) {
					await handler(payload, ctx);
				}
			},
		};
	}

	it("active→commit crash: commits claiming on startup; one prompt; FIFO successor once", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-claim-active-");
		const cwd = tempDir("momo-claim-active-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "implementer");
		const control = workerControlPaths(pool.poolRoot, "implementer");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentA = "claimcrashaaaa01";
		const assignmentB = "claimcrashbbbb02";
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p1",
			agentName: "momo_implementer",
			status: "busy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentA);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(pathsA.command, {
			version: 1,
			type: "prompt",
			task: "task-a",
			issuedAt: new Date().toISOString(),
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "e1",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: assignmentA,
			generation: 1,
			parentEpoch: "e1",
			dispatchedAt: new Date().toISOString(),
		});
		// Simulate crash after active.json before commitClaim: A still in claiming.
		enqueueAssignment(pool.poolRoot, "implementer", {
			assignmentId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			task: "task-a",
		});
		withRoleLock(pool.poolRoot, "implementer", () => {
			beginClaimHead(pool.poolRoot, "implementer", 1);
		});
		enqueueAssignment(pool.poolRoot, "implementer", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			task: "task-b",
		});
		expect(listClaiming(pool.poolRoot, "implementer")).toHaveLength(1);

		const { WriterLeaseManager } = await import("../src/lease/writer-lease.js");
		const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_700_000_000_000 });
		const pi = makePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "implementer",
				MOMO_IPC_DIR: control.root,
				MOMO_CONTROL_DIR: control.root,
				MOMO_WORKER_ID: workerId,
				MOMO_WORKER_GENERATION: "1",
				MOMO_POOL_KEY: identity.poolKey,
				MOMO_POOL_ROOT: pool.poolRoot,
				MOMO_RUN_ID: "g1",
				MOMO_CWD: cwd,
			},
			poolRegistry: pool,
			leaseManager: leases,
			now: () => 1_700_000_000_000,
			sleep: async () => {},
		});
		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);
		expect(listClaiming(pool.poolRoot, "implementer")).toHaveLength(0);
		expect(queueCount(pool.poolRoot, "implementer")).toBe(1);

		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("task-a");

		await pi.emit(
			"message_end",
			{
				message: {
					role: "assistant",
					content: [{ type: "text", text: "done-a" }],
					stopReason: "stop",
				},
			},
			ctx,
		);
		await pi.emit("agent_settled", {}, ctx);
		expect(listClaiming(pool.poolRoot, "implementer")).toHaveLength(0);
		expect(pool.getByRole("implementer")?.activeAssignmentId).toBe(assignmentB);
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).toHaveBeenCalledTimes(2);
		expect(pi.sendUserMessage).toHaveBeenLastCalledWith("task-b");
	});

	it("registry-busy→active crash: republishes active (with or without claiming) once", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-claim-busy-");
		const cwd = tempDir("momo-claim-busy-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentId = "busytactive00001";
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			status: "busy",
			activeAssignmentId: assignmentId,
			activeParentEpoch: "e-parent",
			updatedAt: new Date().toISOString(),
		});
		const paths = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "recover-me",
			issuedAt: new Date().toISOString(),
			runId: assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "e-parent",
		});
		// No active.json — crash between registry busy and active publish.
		expect(existsSync(control.active)).toBe(false);

		const pi = makePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "scout",
				MOMO_IPC_DIR: control.root,
				MOMO_CONTROL_DIR: control.root,
				MOMO_WORKER_ID: workerId,
				MOMO_WORKER_GENERATION: "1",
				MOMO_POOL_KEY: identity.poolKey,
				MOMO_POOL_ROOT: pool.poolRoot,
				MOMO_RUN_ID: "g1",
				MOMO_CWD: cwd,
			},
			poolRegistry: pool,
			now: () => 1_700_000_000_000,
			sleep: async () => {},
		});
		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);
		expect(existsSync(control.active)).toBe(true);
		const active = JSON.parse(readFileSync(control.active, "utf8")) as {
			assignmentId: string;
			generation: number;
		};
		expect(active.assignmentId).toBe(assignmentId);
		expect(active.generation).toBe(1);
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("recover-me");
	});

	it("registry-busy→active with matching claiming commits claim and runs once", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-claim-busy-c-");
		const cwd = tempDir("momo-claim-busy-c-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentId = "busytclaim000002";
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			status: "busy",
			activeAssignmentId: assignmentId,
			activeParentEpoch: "e-parent",
			updatedAt: new Date().toISOString(),
		});
		const paths = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "claim-recover",
			issuedAt: new Date().toISOString(),
			runId: assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "e-parent",
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "e-parent",
			task: "claim-recover",
		});
		withRoleLock(pool.poolRoot, "scout", () => {
			beginClaimHead(pool.poolRoot, "scout", 1);
		});
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(1);
		expect(existsSync(control.active)).toBe(false);

		const pi = makePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "scout",
				MOMO_IPC_DIR: control.root,
				MOMO_CONTROL_DIR: control.root,
				MOMO_WORKER_ID: workerId,
				MOMO_WORKER_GENERATION: "1",
				MOMO_POOL_KEY: identity.poolKey,
				MOMO_POOL_ROOT: pool.poolRoot,
				MOMO_RUN_ID: "g1",
				MOMO_CWD: cwd,
			},
			poolRegistry: pool,
			now: () => 1_700_000_000_000,
			sleep: async () => {},
		});
		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);
		expect(existsSync(control.active)).toBe(true);
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(0);
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("claim-recover");
	});

	it("claiming/active mismatch fails closed without queue advance", async () => {
		const cacheRoot = tempDir("momo-claim-mismatch-");
		const cwd = tempDir("momo-claim-mismatch-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			status: "busy",
			activeAssignmentId: "activeassign0001",
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		const paths = assignmentSpoolPaths(pool.poolRoot, "scout", "activeassign0001");
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "active-task",
			issuedAt: new Date().toISOString(),
			runId: "activeassign0001",
			workerId,
			generation: 1,
			parentEpoch: "e1",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: "activeassign0001",
			generation: 1,
			parentEpoch: "e1",
			dispatchedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: "otherclaiming001",
			workerId,
			generation: 1,
			parentEpoch: "e1",
			task: "wrong-claim",
		});
		withRoleLock(pool.poolRoot, "scout", () => {
			beginClaimHead(pool.poolRoot, "scout", 1);
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: "queuedsuccessor01",
			workerId,
			generation: 1,
			parentEpoch: "e1",
			task: "must-not-run",
		});

		const pi = makePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "scout",
				MOMO_IPC_DIR: control.root,
				MOMO_CONTROL_DIR: control.root,
				MOMO_WORKER_ID: workerId,
				MOMO_WORKER_GENERATION: "1",
				MOMO_POOL_KEY: identity.poolKey,
				MOMO_POOL_ROOT: pool.poolRoot,
				MOMO_RUN_ID: "g1",
				MOMO_CWD: cwd,
			},
			poolRegistry: pool,
			now: () => 1_700_000_000_000,
			sleep: async () => {},
		});
		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);
		expect(pool.getByRole("scout")?.status).toMatch(/unhealthy|uncertain/);
		expect(queueCount(pool.poolRoot, "scout")).toBe(2);
		expect(pi.sendUserMessage).not.toHaveBeenCalled();
	});
});

describe("no terminal-key escalation on shared panes", () => {
	const previousPiDir = process.env.PI_CODING_AGENT_DIR;

	afterEach(() => {
		if (previousPiDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
		else process.env.PI_CODING_AGENT_DIR = previousPiDir;
	});

	it("A→B transition before abort: cancel IPC for A only; no keys affect B", async () => {
		const agentDir = tempDir("momo-pi-nokeys-");
		mkdirSync(path.join(agentDir, "extensions"), { recursive: true });
		writeFileSync(path.join(agentDir, "extensions", "herdr-agent-state.ts"), "// fake\n", "utf8");
		process.env.PI_CODING_AGENT_DIR = agentDir;

		const cacheRoot = tempDir("momo-nokeys-cache-");
		const cwd = tempDir("momo-nokeys-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const keys: string[][] = [];
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "agent" && args[1] === "send-keys") {
					keys.push([...args]);
				}
				if (args[0] === "pane" && args[1] === "split") {
					const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
					const control =
						envArgs.find((v) => v.startsWith("MOMO_CONTROL_DIR="))?.slice("MOMO_CONTROL_DIR=".length) ??
						"";
					const wid =
						envArgs.find((v) => v.startsWith("MOMO_WORKER_ID="))?.slice("MOMO_WORKER_ID=".length) ?? "";
					const runId =
						envArgs.find((v) => v.startsWith("MOMO_RUN_ID="))?.slice("MOMO_RUN_ID=".length) ?? "";
					if (control) {
						mkdirSync(control, { recursive: true, mode: 0o700 });
						atomicWriteJson(path.join(control, "ready.json"), {
							version: 1,
							runId,
							workerId: wid,
							readyAt: new Date().toISOString(),
						});
					}
					return {
						code: 0,
						stdout: JSON.stringify({ id: "s", result: { pane: { pane_id: "w1:p-shared" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: {
								pane_id: "w1:p-shared",
								name: args[2],
								agent: "pi",
								interactive_ready: true,
							},
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-nokeys",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			pollIntervalMs: 20,
			readyTimeoutMs: 2_000,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});
		const sessionA = await factory({ cwd, role: getRole("scout") });
		const proxyA = sessionA as unknown as {
			assignmentId: string;
			paths: { cancel: string };
			abort: () => Promise<void>;
		};
		await sessionA.prompt("task-a");
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe(proxyA.assignmentId);

		const assignmentB = "successorbbbb001";
		const pathsB = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentB);
		mkdirSync(pathsB.root, { recursive: true, mode: 0o700 });
		// A→B transition before A's escalation window: B owns the pane assignment.
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p-shared",
			agentName: pool.getByRole("scout")?.agentName ?? "momo_scout",
			status: "busy",
			activeAssignmentId: assignmentB,
			activeParentEpoch: "epoch-b",
			updatedAt: new Date().toISOString(),
		});

		await proxyA.abort();
		expect(keys).toHaveLength(0);
		expect(tryReadIpcJson(proxyA.paths.cancel)).toMatchObject({
			runId: proxyA.assignmentId,
			reason: "parent_abort",
		});
		expect(tryReadIpcJson(pathsB.cancel)).toBeUndefined();
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe(assignmentB);
		await sessionA.dispose();
	});
});

describe("at-most-once startup reconcile", () => {
	afterEach(() => {
		vi.useRealTimers();
	});

	function makePi() {
		const handlers = new Map<string, Function[]>();
		return {
			handlers,
			on(event: string, handler: Function) {
				const list = handlers.get(event) ?? [];
				list.push(handler);
				handlers.set(event, list);
			},
			sendUserMessage: vi.fn(),
			setActiveTools: vi.fn(),
			registerTool: vi.fn(),
			async emit(event: string, payload: unknown = {}, ctx?: ExtensionContext) {
				for (const handler of handlers.get(event) ?? []) {
					await handler(payload, ctx);
				}
			},
		};
	}

	function installRole(options: {
		pool: PoolRegistry;
		identity: ReturnType<typeof resolvePoolIdentity>;
		role: "scout" | "implementer" | "planner" | "reviewer";
		generation?: number;
		cwd: string;
		leaseManager?: import("../src/lease/writer-lease.js").WriterLeaseManager;
	}) {
		const generation = options.generation ?? 1;
		const workerId = stableWorkerId(options.identity.poolKey, options.role);
		const control = workerControlPaths(options.pool.poolRoot, options.role);
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const pi = makePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: options.role,
				MOMO_IPC_DIR: control.root,
				MOMO_CONTROL_DIR: control.root,
				MOMO_WORKER_ID: workerId,
				MOMO_WORKER_GENERATION: String(generation),
				MOMO_POOL_KEY: options.identity.poolKey,
				MOMO_POOL_ROOT: options.pool.poolRoot,
				MOMO_RUN_ID: `g${generation}`,
				MOMO_CWD: options.cwd,
			},
			poolRegistry: options.pool,
			...(options.leaseManager ? { leaseManager: options.leaseManager } : {}),
			now: () => 1_700_000_000_000,
			sleep: async () => {},
		});
		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd: options.cwd,
		} as unknown as ExtensionContext;
		return { pi, ctx, workerId, control, generation };
	}

	it("pre-start active recovers and prompts once", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-once-pre-");
		const cwd = tempDir("momo-once-pre-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const { pi, ctx, workerId, control } = installRole({ pool, identity, role: "scout", cwd });
		const assignmentId = "prestartactive001";
		const paths = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			status: "busy",
			activeAssignmentId: assignmentId,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "once-only",
			issuedAt: new Date().toISOString(),
			runId: assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "e1",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId,
			generation: 1,
			parentEpoch: "e1",
			dispatchedAt: new Date().toISOString(),
		});
		await pi.emit("session_start", {}, ctx);
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("once-only");
		expect(existsSync(paths.started)).toBe(true);
	});

	it("started/no-result: implementer uncertain, read roles unhealthy; no prompt/advance", async () => {
		vi.useFakeTimers();
		for (const role of ["implementer", "scout"] as const) {
			const cacheRoot = tempDir(`momo-once-started-${role}-`);
			const cwd = tempDir(`momo-once-started-${role}-cwd-`);
			const identity = resolvePoolIdentity({
				cwd,
				canonicalRoot: cwd,
				workspaceId: "ws",
				socketPath: "s",
			});
			const pool = new PoolRegistry(identity.poolKey, cacheRoot);
			const { WriterLeaseManager } = await import("../src/lease/writer-lease.js");
			const leases =
				role === "implementer"
					? new WriterLeaseManager({ cacheRoot, now: () => 1_700_000_000_000 })
					: undefined;
			const { pi, ctx, workerId, control } = installRole({
				pool,
				identity,
				role,
				cwd,
				...(leases ? { leaseManager: leases } : {}),
			});
			const assignmentA = "startednorerun001";
			const assignmentB = "shouldnotclaim002";
			const paths = assignmentSpoolPaths(pool.poolRoot, role, assignmentA);
			mkdirSync(paths.root, { recursive: true, mode: 0o700 });
			pool.upsert({
				workerId,
				generation: 1,
				generationTombstone: 1,
				role,
				paneId: "w1:p1",
				agentName: `momo_${role}`,
				status: "busy",
				activeAssignmentId: assignmentA,
				activeParentEpoch: "e1",
				updatedAt: new Date().toISOString(),
			});
			atomicWriteJson(paths.command, {
				version: 1,
				type: "prompt",
				task: "already-started",
				issuedAt: new Date().toISOString(),
				runId: assignmentA,
				workerId,
				generation: 1,
				parentEpoch: "e1",
			});
			atomicWriteJson(control.active, {
				version: 1,
				assignmentId: assignmentA,
				generation: 1,
				parentEpoch: "e1",
				dispatchedAt: new Date().toISOString(),
			});
			atomicWriteJson(paths.started, {
				version: 1,
				runId: assignmentA,
				workerId,
				generation: 1,
				parentEpoch: "e1",
				startedAt: new Date().toISOString(),
			});
			enqueueAssignment(pool.poolRoot, role, {
				assignmentId: assignmentB,
				workerId,
				generation: 1,
				parentEpoch: "e1",
				task: "successor",
			});
			await pi.emit("session_start", {}, ctx);
			await vi.advanceTimersByTimeAsync(200);
			expect(pi.sendUserMessage).not.toHaveBeenCalled();
			expect(queueCount(pool.poolRoot, role)).toBe(1);
			expect(pool.getByRole(role)?.activeAssignmentId).toBe(assignmentA);
			if (role === "implementer") {
				expect(pool.getByRole(role)?.status).toBe("uncertain");
				expect(pool.getByRole(role)?.uncertainWrite).toBe(true);
			} else {
				expect(pool.getByRole(role)?.status).toBe("unhealthy");
			}
			expect(existsSync(control.active)).toBe(true);
		}
	});

	it("clean terminal result never reruns and advances B once", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-once-clean-");
		const cwd = tempDir("momo-once-clean-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const { pi, ctx, workerId, control } = installRole({ pool, identity, role: "scout", cwd });
		const assignmentA = "cleanresultaaaa01";
		const assignmentB = "cleanresultbbbb02";
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentA);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			status: "busy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(pathsA.command, {
			version: 1,
			type: "prompt",
			task: "done-a",
			issuedAt: new Date().toISOString(),
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "e1",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: assignmentA,
			generation: 1,
			parentEpoch: "e1",
			dispatchedAt: new Date().toISOString(),
		});
		atomicWriteJson(pathsA.started, {
			version: 1,
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			startedAt: new Date().toISOString(),
		});
		atomicWriteJson(pathsA.result, {
			version: 1,
			runId: assignmentA,
			workerId,
			status: "completed",
			messages: [{ role: "assistant", content: [{ type: "text", text: "a" }] }],
			finishedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			task: "done-a",
		});
		withRoleLock(pool.poolRoot, "scout", () => {
			beginClaimHead(pool.poolRoot, "scout", 1);
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			task: "task-b",
		});
		await pi.emit("session_start", {}, ctx);
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(0);
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe(assignmentB);
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("task-b");
	});

	it("preserves canonical cwd across busy→idle and sequential claim publish", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-cwd-persist-");
		const cwd = tempDir("momo-cwd-persist-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const { pi, ctx, workerId, control } = installRole({ pool, identity, role: "scout", cwd });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			cwd: identity.canonicalRoot,
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		const assignmentA = "cwdpersistaaaa01";
		const assignmentB = "cwdpersistbbbb02";
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			task: "task-a",
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			task: "task-b",
		});
		await pi.emit("session_start", {}, ctx);
		expect(pool.getByRole("scout")?.status).toBe("busy");
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe(assignmentA);
		expect(pool.getByRole("scout")?.cwd).toBe(identity.canonicalRoot);

		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("task-a");
		await pi.emit(
			"message_end",
			{
				message: {
					role: "assistant",
					content: [{ type: "text", text: "done-a" }],
					stopReason: "stop",
				},
			},
			ctx,
		);
		await pi.emit("agent_settled", {}, ctx);
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe(assignmentB);
		expect(pool.getByRole("scout")?.status).toBe("busy");
		expect(pool.getByRole("scout")?.cwd).toBe(identity.canonicalRoot);

		await vi.advanceTimersByTimeAsync(200);
		await pi.emit(
			"message_end",
			{
				message: {
					role: "assistant",
					content: [{ type: "text", text: "done-b" }],
					stopReason: "stop",
				},
			},
			ctx,
		);
		await pi.emit("agent_settled", {}, ctx);
		expect(pool.getByRole("scout")?.status).toBe("idle");
		expect(pool.getByRole("scout")?.cwd).toBe(identity.canonicalRoot);
		expect(pool.getByRole("scout")?.activeAssignmentId).toBeUndefined();
		void control;
		vi.useRealTimers();
	});

	it("uncertain result retains evidence and does not advance B", async () => {
		const cacheRoot = tempDir("momo-once-uncert-");
		const cwd = tempDir("momo-once-uncert-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const { pi, ctx, workerId, control } = installRole({
			pool,
			identity,
			role: "implementer",
			cwd,
		});
		const assignmentA = "uncertainaaaaa01";
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentA);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p1",
			agentName: "momo_implementer",
			status: "busy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(pathsA.command, {
			version: 1,
			type: "prompt",
			task: "uncertain-a",
			issuedAt: new Date().toISOString(),
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "e1",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: assignmentA,
			generation: 1,
			parentEpoch: "e1",
			dispatchedAt: new Date().toISOString(),
		});
		atomicWriteJson(pathsA.result, {
			version: 1,
			runId: assignmentA,
			workerId,
			status: "failed",
			uncertainWrite: true,
			messages: [],
			errorMessage: "lease ambiguous",
			finishedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "implementer", {
			assignmentId: "uncertainbbbbb02",
			workerId,
			generation: 1,
			parentEpoch: "e1",
			task: "no-b",
		});
		await pi.emit("session_start", {}, ctx);
		expect(pi.sendUserMessage).not.toHaveBeenCalled();
		expect(pool.getByRole("implementer")?.status).toBe("uncertain");
		expect(pool.getByRole("implementer")?.activeAssignmentId).toBe(assignmentA);
		expect(existsSync(control.active)).toBe(true);
		expect(queueCount(pool.poolRoot, "implementer")).toBe(1);
	});

	it("malformed active forms fail closed without overwrite or prompt", async () => {
		for (const [label, payload] of [
			["null", "null\n"],
			["array", "[1]\n"],
			["primitive", "\"x\"\n"],
			["missing-fields", `${JSON.stringify({ version: 1 })}\n`],
		] as const) {
			const cacheRoot = tempDir(`momo-once-bad-${label}-`);
			const cwd = tempDir(`momo-once-bad-${label}-cwd-`);
			const identity = resolvePoolIdentity({
				cwd,
				canonicalRoot: cwd,
				workspaceId: "ws",
				socketPath: "s",
			});
			const pool = new PoolRegistry(identity.poolKey, cacheRoot);
			const { pi, ctx, workerId, control } = installRole({ pool, identity, role: "scout", cwd });
			pool.upsert({
				workerId,
				generation: 1,
				generationTombstone: 1,
				role: "scout",
				paneId: "w1:p1",
				agentName: "momo_scout",
				status: "busy",
				activeAssignmentId: "malformedactive01",
				activeParentEpoch: "e1",
				updatedAt: new Date().toISOString(),
			});
			writeFileSync(control.active, payload, { mode: 0o600 });
			const before = readFileSync(control.active, "utf8");
			await pi.emit("session_start", {}, ctx);
			expect(readFileSync(control.active, "utf8")).toBe(before);
			expect(pi.sendUserMessage).not.toHaveBeenCalled();
			expect(pool.getByRole("scout")?.status).toMatch(/unhealthy|uncertain/);
		}
	});

	it("epoch mismatches fail closed with no claim commit/prompt/advance", async () => {
		const cacheRoot = tempDir("momo-once-epoch-");
		const cwd = tempDir("momo-once-epoch-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const { pi, ctx, workerId, control } = installRole({ pool, identity, role: "scout", cwd });
		const assignmentId = "epochmismatch001";
		const paths = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			status: "busy",
			activeAssignmentId: assignmentId,
			activeParentEpoch: "epoch-registry",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "epoch-task",
			issuedAt: new Date().toISOString(),
			runId: assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "epoch-command",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId,
			generation: 1,
			parentEpoch: "epoch-active",
			dispatchedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "epoch-claim",
			task: "epoch-task",
		});
		withRoleLock(pool.poolRoot, "scout", () => {
			beginClaimHead(pool.poolRoot, "scout", 1);
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: "epochsuccessor001",
			workerId,
			generation: 1,
			parentEpoch: "epoch-registry",
			task: "no-advance",
		});
		await pi.emit("session_start", {}, ctx);
		expect(pi.sendUserMessage).not.toHaveBeenCalled();
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(1);
		expect(queueCount(pool.poolRoot, "scout")).toBe(2);
		expect(pool.getByRole("scout")?.status).toMatch(/unhealthy|uncertain/);
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe(assignmentId);
	});

	it("invalid started/result identity fails closed", async () => {
		const cacheRoot = tempDir("momo-once-id-");
		const cwd = tempDir("momo-once-id-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const { pi, ctx, workerId, control } = installRole({ pool, identity, role: "scout", cwd });
		const assignmentId = "badidentity00001";
		const paths = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			status: "busy",
			activeAssignmentId: assignmentId,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "x",
			issuedAt: new Date().toISOString(),
			runId: assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "e1",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId,
			generation: 1,
			parentEpoch: "e1",
			dispatchedAt: new Date().toISOString(),
		});
		atomicWriteJson(paths.started, {
			version: 1,
			runId: "otherassignment001",
			workerId,
			generation: 1,
			parentEpoch: "e1",
			startedAt: new Date().toISOString(),
		});
		await pi.emit("session_start", {}, ctx);
		expect(pi.sendUserMessage).not.toHaveBeenCalled();
		expect(pool.getByRole("scout")?.status).toMatch(/unhealthy|uncertain/);
	});
});

describe("clean-result terminal transition races", () => {
	it("adoption of A result + queued B makes B active without reusable idle", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const cacheRoot = tempDir("momo-adopt-ab-");
		const cwd = tempDir("momo-adopt-ab-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentA = "adoptaaaaaaaaaa01";
		const assignmentB = "adoptbbbbbbbbbb02";
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			cwd,
			status: "busy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId,
			generation: 1,
			role: "scout",
			cwd,
			paneId: "w1:p1",
			agentName: "momo_scout",
			createdAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 1,
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: assignmentA,
			generation: 1,
			parentEpoch: "e1",
			dispatchedAt: new Date().toISOString(),
		});
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentA);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(pathsA.command, {
			version: 1,
			type: "prompt",
			task: "a",
			issuedAt: new Date().toISOString(),
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "e1",
		});
		atomicWriteJson(pathsA.result, {
			version: 1,
			runId: assignmentA,
			workerId,
			status: "completed",
			messages: [{ role: "assistant", content: [{ type: "text", text: "a" }] }],
			finishedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			task: "task-b",
		});

		const client = new HerdrClient({
			runCommand: async () => ({
				code: 0,
				stdout: JSON.stringify({
					id: "ok",
					result: {
						type: "agent_info",
						agent: { agent_status: "idle", pane_id: "w1:p1", name: "momo_scout" },
					},
				}),
				stderr: "",
			}),
		});
		await adoptPoolWorkers(pool, client, identity.poolKey, { ui: { notify: () => undefined } });

		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("busy");
		expect(after.activeAssignmentId).toBe(assignmentB);
		expect(after.status).not.toBe("idle");
		const activePtr = tryReadIpcJson(control.active) as { assignmentId?: string };
		expect(activePtr.assignmentId).toBe(assignmentB);
		expect(queueCount(pool.poolRoot, "scout")).toBe(0);
	});

	it("stale A finisher leaves B active untouched", async () => {
		const { completeCleanAssignmentLocked } = await import("../src/herdr/terminal-transition.js");
		const cacheRoot = tempDir("momo-stale-fin-");
		const cwd = tempDir("momo-stale-fin-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentA = "stalefinaaaaaa01";
		const assignmentB = "stalefinbbbbbb02";
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			cwd,
			status: "busy",
			activeAssignmentId: assignmentB,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: assignmentB,
			generation: 1,
			parentEpoch: "e1",
			dispatchedAt: new Date().toISOString(),
		});
		const pathsB = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentB);
		mkdirSync(pathsB.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(pathsB.command, {
			version: 1,
			type: "prompt",
			task: "b",
			issuedAt: new Date().toISOString(),
			runId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "e1",
		});

		const outcome = withRoleLock(pool.poolRoot, "scout", () =>
			completeCleanAssignmentLocked({
				pool,
				role: "scout",
				workerId,
				generation: 1,
				finishedAssignmentId: assignmentA,
			}),
		);
		expect(outcome.kind).toBe("noop_superseded");
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe(assignmentB);
		expect(pool.getByRole("scout")?.status).toBe("busy");
		const ptr = tryReadIpcJson(control.active) as { assignmentId?: string };
		expect(ptr.assignmentId).toBe(assignmentB);
	});

	it("concurrent adoption/finish preserves A→B once FIFO", async () => {
		const { completeCleanAssignmentLocked } = await import("../src/herdr/terminal-transition.js");
		const cacheRoot = tempDir("momo-conc-ab-");
		const cwd = tempDir("momo-conc-ab-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentA = "concfinaaaaaaa01";
		const assignmentB = "concfinbbbbbbb02";
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			cwd,
			status: "busy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: assignmentA,
			generation: 1,
			parentEpoch: "e1",
			dispatchedAt: new Date().toISOString(),
		});
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentA);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(pathsA.result, {
			version: 1,
			runId: assignmentA,
			workerId,
			status: "completed",
			messages: [{ role: "assistant", content: [{ type: "text", text: "a" }] }],
			finishedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			task: "task-b",
		});

		const outcomes = await Promise.all([
			withRoleLockAsync(pool.poolRoot, "scout", () =>
				completeCleanAssignmentLocked({
					pool,
					role: "scout",
					workerId,
					generation: 1,
					finishedAssignmentId: assignmentA,
				}),
			),
			withRoleLockAsync(pool.poolRoot, "scout", () =>
				completeCleanAssignmentLocked({
					pool,
					role: "scout",
					workerId,
					generation: 1,
					finishedAssignmentId: assignmentA,
				}),
			),
		]);
		const advanced = outcomes.filter((o) => o.kind === "advanced");
		const superseded = outcomes.filter((o) => o.kind === "noop_superseded");
		expect(advanced).toHaveLength(1);
		expect(superseded).toHaveLength(1);
		expect(advanced[0]).toEqual({ kind: "advanced", nextAssignmentId: assignmentB });
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe(assignmentB);
		expect(pool.getByRole("scout")?.status).toBe("busy");
		expect(queueCount(pool.poolRoot, "scout")).toBe(0);
	});
});

describe("implementer pre-start lease uncertain", () => {
	function makePi() {
		const handlers = new Map<string, Function[]>();
		return {
			handlers,
			on(event: string, handler: Function) {
				const list = handlers.get(event) ?? [];
				list.push(handler);
				handlers.set(event, list);
			},
			sendUserMessage: vi.fn(),
			setActiveTools: vi.fn(),
			registerTool: vi.fn(),
			async emit(event: string, payload: unknown = {}, ctx?: ExtensionContext) {
				for (const handler of handlers.get(event) ?? []) {
					await handler(payload, ctx);
				}
			},
		};
	}

	it("same-worker lease pre-start => no prompt / uncertain / queue retained", async () => {
		const { WriterLeaseManager, createLeaseToken } = await import("../src/lease/writer-lease.js");
		const cacheRoot = tempDir("momo-lease-pre-");
		const cwd = tempDir("momo-lease-pre-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "implementer");
		const control = workerControlPaths(pool.poolRoot, "implementer");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentA = "leasepreaaaaaa01";
		const assignmentB = "leaseprebbbbbb02";
		const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_700_000_000_000 });
		// Crash after acquire, before started: lease owned by this worker, token unknowable.
		leases.acquire(cwd, workerId, createLeaseToken());
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p1",
			agentName: "momo_implementer",
			cwd,
			status: "busy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentA);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(pathsA.command, {
			version: 1,
			type: "prompt",
			task: "task-a",
			issuedAt: new Date().toISOString(),
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "e1",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: assignmentA,
			generation: 1,
			parentEpoch: "e1",
			dispatchedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "implementer", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			task: "task-b",
		});

		const pi = makePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "implementer",
				MOMO_IPC_DIR: control.root,
				MOMO_CONTROL_DIR: control.root,
				MOMO_WORKER_ID: workerId,
				MOMO_WORKER_GENERATION: "1",
				MOMO_POOL_KEY: identity.poolKey,
				MOMO_POOL_ROOT: pool.poolRoot,
				MOMO_RUN_ID: "g1",
				MOMO_CWD: cwd,
			},
			poolRegistry: pool,
			leaseManager: leases,
			now: () => 1_700_000_000_000,
			sleep: async () => {},
		});
		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);
		expect(pi.sendUserMessage).not.toHaveBeenCalled();
		expect(pool.getByRole("implementer")?.status).toBe("uncertain");
		expect(pool.getByRole("implementer")?.uncertainWrite).toBe(true);
		expect(pool.getByRole("implementer")?.activeAssignmentId).toBe(assignmentA);
		expect(queueCount(pool.poolRoot, "implementer")).toBe(1);
		expect(leases.peekOwner(cwd)?.ownerId).toBe(workerId);
		expect(existsSync(control.active)).toBe(true);
	});

	it("stale adoption of active implementer matching lease => uncertain", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const { WriterLeaseManager, createLeaseToken } = await import("../src/lease/writer-lease.js");
		const cacheRoot = tempDir("momo-adopt-uncert-");
		const cwd = tempDir("momo-adopt-uncert-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "implementer");
		const control = workerControlPaths(pool.poolRoot, "implementer");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentId = "adoptuncertaaa01";
		const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_700_000_000_000 });
		leases.acquire(cwd, workerId, createLeaseToken());
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p1",
			agentName: "momo_implementer",
			cwd,
			status: "busy",
			activeAssignmentId: assignmentId,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId,
			generation: 1,
			role: "implementer",
			cwd,
			paneId: "w1:p1",
			agentName: "momo_implementer",
			createdAt: new Date().toISOString(),
		});
		// Stale heartbeat forces adoption failure path.
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date(Date.now() - 60_000).toISOString(),
			seq: 1,
		});
		const paths = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "x",
			issuedAt: new Date().toISOString(),
			runId: assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "e1",
		});

		const notes: string[] = [];
		const client = new HerdrClient({
			runCommand: async () => ({
				code: 0,
				stdout: JSON.stringify({
					id: "ok",
					result: {
						type: "agent_info",
						agent: { agent_status: "idle", pane_id: "w1:p1", name: "momo_implementer" },
					},
				}),
				stderr: "",
			}),
		});
		await adoptPoolWorkers(pool, client, identity.poolKey, {
			ui: { notify: (m: string) => notes.push(m) },
		});
		expect(notes.join("\n")).toMatch(/stale heartbeat|Did not adopt/i);
		expect(pool.getByRole("implementer")?.status).toBe("uncertain");
		expect(pool.getByRole("implementer")?.uncertainWrite).toBe(true);
		expect(pool.getByRole("implementer")?.activeAssignmentId).toBe(assignmentId);
		expect(leases.peekOwner(cwd)?.ownerId).toBe(workerId);
	});

	it("healthy working fresh implementer remains busy (not force-cleanup exposed)", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const { WriterLeaseManager, createLeaseToken } = await import("../src/lease/writer-lease.js");
		const cacheRoot = tempDir("momo-adopt-working-");
		const cwd = tempDir("momo-adopt-working-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "implementer");
		const control = workerControlPaths(pool.poolRoot, "implementer");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentId = "adoptworkingaa01";
		const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_700_000_000_000 });
		leases.acquire(cwd, workerId, createLeaseToken());
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p1",
			agentName: "momo_implementer",
			cwd,
			status: "busy",
			activeAssignmentId: assignmentId,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId,
			generation: 1,
			role: "implementer",
			cwd,
			paneId: "w1:p1",
			agentName: "momo_implementer",
			createdAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 1,
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId,
			generation: 1,
			parentEpoch: "e1",
			dispatchedAt: new Date().toISOString(),
		});
		const paths = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "live",
			issuedAt: new Date().toISOString(),
			runId: assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "e1",
		});
		atomicWriteJson(paths.started, {
			version: 1,
			runId: assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			startedAt: new Date().toISOString(),
		});
		const client = new HerdrClient({
			runCommand: async () => ({
				code: 0,
				stdout: JSON.stringify({
					id: "ok",
					result: {
						type: "agent_info",
						agent: { agent_status: "working", pane_id: "w1:p1", name: "momo_implementer" },
					},
				}),
				stderr: "",
			}),
		});
		await adoptPoolWorkers(pool, client, identity.poolKey, { ui: { notify: () => undefined } });
		const after = pool.getByRole("implementer")!;
		expect(after.status).toBe("busy");
		expect(after.uncertainWrite).toBeUndefined();
		expect(after.activeAssignmentId).toBe(assignmentId);
		expect(selectClosablePoolWorkers([after], { force: true }).closable).toHaveLength(0);
		expect(leases.peekOwner(cwd)?.ownerId).toBe(workerId);
	});

	it("adoption foreign-lease waiting remains busy and lease untouched", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const { WriterLeaseManager, createLeaseToken } = await import("../src/lease/writer-lease.js");
		const cacheRoot = tempDir("momo-adopt-foreign-");
		const cwd = tempDir("momo-adopt-foreign-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "implementer");
		const control = workerControlPaths(pool.poolRoot, "implementer");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentId = "adoptforeignaa01";
		const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_700_000_000_000 });
		leases.acquire(cwd, "other_worker_id", createLeaseToken());
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p1",
			agentName: "momo_implementer",
			cwd,
			status: "busy",
			activeAssignmentId: assignmentId,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId,
			generation: 1,
			role: "implementer",
			cwd,
			paneId: "w1:p1",
			agentName: "momo_implementer",
			createdAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 1,
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId,
			generation: 1,
			parentEpoch: "e1",
			dispatchedAt: new Date().toISOString(),
		});
		const paths = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "wait",
			issuedAt: new Date().toISOString(),
			runId: assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "e1",
		});
		const client = new HerdrClient({
			runCommand: async () => ({
				code: 0,
				stdout: JSON.stringify({
					id: "ok",
					result: {
						type: "agent_info",
						agent: { agent_status: "idle", pane_id: "w1:p1", name: "momo_implementer" },
					},
				}),
				stderr: "",
			}),
		});
		await adoptPoolWorkers(pool, client, identity.poolKey, { ui: { notify: () => undefined } });
		expect(pool.getByRole("implementer")?.status).toBe("busy");
		expect(pool.getByRole("implementer")?.uncertainWrite).toBeUndefined();
		expect(leases.peekOwner(cwd)?.ownerId).toBe("other_worker_id");
	});

	it("adoption idle/done + valid started => uncertain", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const cacheRoot = tempDir("momo-adopt-started-");
		const cwd = tempDir("momo-adopt-started-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "implementer");
		const control = workerControlPaths(pool.poolRoot, "implementer");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentId = "adoptstarteda001";
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p1",
			agentName: "momo_implementer",
			cwd,
			status: "busy",
			activeAssignmentId: assignmentId,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId,
			generation: 1,
			role: "implementer",
			cwd,
			paneId: "w1:p1",
			agentName: "momo_implementer",
			createdAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 1,
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId,
			generation: 1,
			parentEpoch: "e1",
			dispatchedAt: new Date().toISOString(),
		});
		const paths = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "x",
			issuedAt: new Date().toISOString(),
			runId: assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "e1",
		});
		atomicWriteJson(paths.started, {
			version: 1,
			runId: assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			startedAt: new Date().toISOString(),
		});
		const client = new HerdrClient({
			runCommand: async () => ({
				code: 0,
				stdout: JSON.stringify({
					id: "ok",
					result: {
						type: "agent_info",
						agent: { agent_status: "done", pane_id: "w1:p1", name: "momo_implementer" },
					},
				}),
				stderr: "",
			}),
		});
		await adoptPoolWorkers(pool, client, identity.poolKey, { ui: { notify: () => undefined } });
		expect(pool.getByRole("implementer")?.status).toBe("uncertain");
		expect(pool.getByRole("implementer")?.uncertainWrite).toBe(true);
		expect(pool.getByRole("implementer")?.activeAssignmentId).toBe(assignmentId);
	});

	it("adoption idle + same-worker lease (token ambiguous) => uncertain", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const { WriterLeaseManager, createLeaseToken } = await import("../src/lease/writer-lease.js");
		const cacheRoot = tempDir("momo-adopt-samelease-");
		const cwd = tempDir("momo-adopt-samelease-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "implementer");
		const control = workerControlPaths(pool.poolRoot, "implementer");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentId = "adoptsamelease01";
		const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_700_000_000_000 });
		leases.acquire(cwd, workerId, createLeaseToken());
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p1",
			agentName: "momo_implementer",
			cwd,
			status: "busy",
			activeAssignmentId: assignmentId,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId,
			generation: 1,
			role: "implementer",
			cwd,
			paneId: "w1:p1",
			agentName: "momo_implementer",
			createdAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 1,
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId,
			generation: 1,
			parentEpoch: "e1",
			dispatchedAt: new Date().toISOString(),
		});
		const paths = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "x",
			issuedAt: new Date().toISOString(),
			runId: assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "e1",
		});
		const client = new HerdrClient({
			runCommand: async () => ({
				code: 0,
				stdout: JSON.stringify({
					id: "ok",
					result: {
						type: "agent_info",
						agent: { agent_status: "idle", pane_id: "w1:p1", name: "momo_implementer" },
					},
				}),
				stderr: "",
			}),
		});
		await adoptPoolWorkers(pool, client, identity.poolKey, { ui: { notify: () => undefined } });
		expect(pool.getByRole("implementer")?.status).toBe("uncertain");
		expect(pool.getByRole("implementer")?.uncertainWrite).toBe(true);
		expect(leases.peekOwner(cwd)?.ownerId).toBe(workerId);
	});

	it("force exact owner permits next-generation lease; foreign owner untouched", async () => {
		const { WriterLeaseManager, createLeaseToken } = await import("../src/lease/writer-lease.js");
		const cacheRoot = tempDir("momo-force-owner-");
		const cwd = tempDir("momo-force-owner-cwd-");
		const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_700_000_000_000 });
		const ownerA = "impl_exact_owner_a";
		const ownerB = "impl_foreign_owner_b";
		const tokenA = createLeaseToken();
		leases.acquire(cwd, ownerA, tokenA);
		expect(leases.forceReleaseIfOwner(cwd, ownerB).released).toBe(false);
		expect(leases.peekOwner(cwd)?.ownerId).toBe(ownerA);
		expect(leases.forceReleaseIfOwner(cwd, ownerA).released).toBe(true);
		expect(leases.peekOwner(cwd)).toBeUndefined();
		const nextToken = createLeaseToken();
		leases.acquire(cwd, "impl_next_generation", nextToken);
		expect(leases.peekOwner(cwd)?.ownerId).toBe("impl_next_generation");
	});

	it("foreign lease owner pre-start remains untouched (safe wait path)", async () => {
		const { WriterLeaseManager, createLeaseToken } = await import("../src/lease/writer-lease.js");
		const cacheRoot = tempDir("momo-lease-foreign-");
		const cwd = tempDir("momo-lease-foreign-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "implementer");
		const control = workerControlPaths(pool.poolRoot, "implementer");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const assignmentId = "leaseforeignaa01";
		const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_700_000_000_000 });
		leases.acquire(cwd, "other_worker_id", createLeaseToken());
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p1",
			agentName: "momo_implementer",
			cwd,
			status: "busy",
			activeAssignmentId: assignmentId,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		const paths = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "task-a",
			issuedAt: new Date().toISOString(),
			runId: assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "e1",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId,
			generation: 1,
			parentEpoch: "e1",
			dispatchedAt: new Date().toISOString(),
		});

		const pi = makePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "implementer",
				MOMO_IPC_DIR: control.root,
				MOMO_CONTROL_DIR: control.root,
				MOMO_WORKER_ID: workerId,
				MOMO_WORKER_GENERATION: "1",
				MOMO_POOL_KEY: identity.poolKey,
				MOMO_POOL_ROOT: pool.poolRoot,
				MOMO_RUN_ID: "g1",
				MOMO_CWD: cwd,
			},
			poolRegistry: pool,
			leaseManager: leases,
			now: () => 1_700_000_000_000,
			sleep: async () => {},
		});
		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);
		// Foreign owner: reconcile must not mark uncertain or steal the lease.
		expect(pool.getByRole("implementer")?.status).toBe("busy");
		expect(pool.getByRole("implementer")?.uncertainWrite).toBeUndefined();
		expect(leases.peekOwner(cwd)?.ownerId).toBe("other_worker_id");
		expect(pi.sendUserMessage).not.toHaveBeenCalled();
	});
});

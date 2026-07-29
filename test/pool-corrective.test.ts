import {
	existsSync,
	mkdirSync,
	readFileSync,
	rmSync,
	symlinkSync,
	writeFileSync,
	mkdtempSync,
} from "node:fs";
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
	QueueLockError,
	__setLockAcquireHooksForTest,
} from "../src/herdr/role-queue.js";
import {
	PoolRegistry,
	isArchivalTombstone,
	isNonReusableLiveWorker,
	selectClosablePoolWorkers,
	validatePoolWorkerRecord,
} from "../src/herdr/pool-registry.js";
import { resolvePoolIdentity, stableWorkerId } from "../src/herdr/pool-identity.js";
import {
	assertNoOrphanPaneEvidence,
	listOrphanPaneEvidence,
	orphanEvidenceFileName,
	orphanEvidencePath,
	orphanPanesDir,
	OrphanPaneEvidencePersistenceError,
	roleHasOrphanPaneEvidence,
	writeOrphanPaneEvidence,
	__setOrphanEvidenceWriteForTest,
} from "../src/herdr/orphan-panes.js";
import { installMomoWorker } from "../src/extensions/worker-runtime.js";
import { workerControlPaths, assignmentSpoolPaths } from "../src/herdr/assignment-spool.js";
import { atomicWriteJson, MAX_IPC_JSON_BYTES, DEFAULT_HEARTBEAT_CLOCK_SKEW_MS } from "../src/ipc/spool.js";
import { tryReadIpcJson, validateResult } from "../src/ipc/validate.js";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
	createHerdrChildSessionFactory,
	WorkerReadyTimeoutError,
	__resetProvisioningOwnershipHooksForTest,
	__setJoinerReadyTimeoutLockHookForTest,
	__setPostPlanPreBranchHookForTest,
	__setProvisioningHeartbeatBeforeLockHookForTest,
	type HerdrDiagnosticReport,
} from "../src/delegation/herdr-factory.js";
import { HerdrClient } from "../src/herdr/client.js";
import { getRole } from "../src/roles.js";
import {
	installMomoParent,
	__resetCleanupRoleLockEnteredHookForTest,
} from "../src/extensions/parent.js";

const tempDirs: string[] = [];

afterEach(() => {
	__resetProvisioningOwnershipHooksForTest();
	__setLockAcquireHooksForTest();
	__resetCleanupRoleLockEnteredHookForTest();
	__setOrphanEvidenceWriteForTest();
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

	it("owner temp write/rename failure removes only newly-created lock; next acquire succeeds", async () => {
		const cacheRoot = tempDir("momo-lock-owner-fail-");
		const cwd = tempDir("momo-lock-owner-fail-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		__setLockAcquireHooksForTest({
			writeOwnerAtomic: () => {
				throw Object.assign(new Error("injected owner rename failure"), { code: "EPERM" });
			},
		});
		const failed = new RoleTransactionLock(pool.poolRoot, "scout");
		await expect(failed.acquire(200)).rejects.toThrow(/injected owner rename failure/i);
		expect(existsSync(failed.lockDir)).toBe(false);
		__setLockAcquireHooksForTest();
		const ok = new RoleTransactionLock(pool.poolRoot, "scout");
		await ok.acquire(1_000);
		expect(ok.getTokenForTest()).toBeTruthy();
		ok.release();
		expect(existsSync(ok.lockDir)).toBe(false);
	});

	it("heartbeat initialization failure rolls back newly-created lock; sync acquire succeeds after", () => {
		const cacheRoot = tempDir("momo-lock-hb-fail-");
		const cwd = tempDir("momo-lock-hb-fail-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		__setLockAcquireHooksForTest({
			afterOwnerInitialized: () => {
				throw new Error("injected heartbeat initialization failure");
			},
		});
		const failed = new RoleTransactionLock(pool.poolRoot, "planner");
		expect(() => failed.acquireSync(200)).toThrow(/injected heartbeat initialization failure/i);
		expect(existsSync(failed.lockDir)).toBe(false);
		__setLockAcquireHooksForTest();
		const ok = new RoleTransactionLock(pool.poolRoot, "planner");
		ok.acquireSync(1_000);
		expect(ok.getTokenForTest()).toBeTruthy();
		ok.release();
	});

	it("EEXIST peer lock is never removed on acquire init failure of a different call", async () => {
		const cacheRoot = tempDir("momo-lock-eexist-");
		const cwd = tempDir("momo-lock-eexist-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const peer = new RoleTransactionLock(pool.poolRoot, "reviewer");
		await peer.acquire(1_000);
		const peerToken = peer.getTokenForTest();
		__setLockAcquireHooksForTest({
			writeOwnerAtomic: () => {
				throw new Error("should not write over peer");
			},
		});
		const contender = new RoleTransactionLock(pool.poolRoot, "reviewer");
		await expect(contender.acquire(150)).rejects.toThrow(/Timed out/);
		expect(existsSync(peer.lockDir)).toBe(true);
		expect(readLockOwnerForTest(peer.lockDir)?.token).toBe(peerToken);
		__setLockAcquireHooksForTest();
		peer.release();
	});

	it("rollback-rm failure throws QueueLockError preserving init and rollback", async () => {
		const cacheRoot = tempDir("momo-lock-rollback-");
		const cwd = tempDir("momo-lock-rollback-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		__setLockAcquireHooksForTest({
			writeOwnerAtomic: () => {
				throw new Error("init-owner-boom");
			},
			removeLockDir: () => {
				throw new Error("rollback-rm-boom");
			},
		});
		const lock = new RoleTransactionLock(pool.poolRoot, "implementer");
		let caught: unknown;
		try {
			await lock.acquire(200);
		} catch (error) {
			caught = error;
		}
		expect(caught).toBeInstanceOf(QueueLockError);
		const qe = caught as QueueLockError;
		expect(qe.message).toMatch(/rollback remove failed|lock directory may remain/i);
		expect(qe.message).toMatch(/init-owner-boom/);
		expect(qe.message).toMatch(/rollback-rm-boom/);
		expect(String(qe.initError)).toMatch(/init-owner-boom/);
		expect(String(qe.rollbackError)).toMatch(/rollback-rm-boom/);
		expect(existsSync(lock.lockDir)).toBe(true);
		__setLockAcquireHooksForTest();
		rmSync(lock.lockDir, { recursive: true, force: true });
	});
});

describe("orphan pane cleanup evidence", () => {
	function installFakeHerdrExtension(): void {
		const agentDir = tempDir("momo-pi-agent-");
		mkdirSync(path.join(agentDir, "extensions"), { recursive: true });
		writeFileSync(path.join(agentDir, "extensions", "herdr-agent-state.ts"), "// fake\n", "utf8");
		process.env.PI_CODING_AGENT_DIR = agentDir;
	}

	function createFakePi() {
		const commands = new Map<string, { handler: (args: string, ctx: unknown) => Promise<void> }>();
		return {
			commands,
			registerTool: vi.fn(),
			setActiveTools: vi.fn(),
			registerCommand: vi.fn(
				(name: string, def: { handler: (args: string, ctx: unknown) => Promise<void> }) => {
					commands.set(name, def);
				},
			),
			on: vi.fn(),
		};
	}

	it("close-fail after supersession persists across dispose; blocks provision; cleanup clears", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-orphan-persist-");
		const cwd = tempDir("momo-orphan-persist-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		let releaseSplit!: () => void;
		const splitGate = new Promise<void>((resolve) => {
			releaseSplit = resolve;
		});
		let splitEntered = 0;
		let closeCalls = 0;
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					splitEntered += 1;
					await splitGate;
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: { pane: { pane_id: "w1:p-orphan" } },
						}),
						stderr: "",
					};
				}
				if (args[0] === "pane" && args[1] === "close") {
					closeCalls += 1;
					if (String(args[2]) === "w1:p-orphan" && closeCalls === 1) {
						return {
							code: 1,
							stdout: JSON.stringify({
								id: "c",
								error: { code: "timeout", message: "close timed out" },
							}),
							stderr: "",
						};
					}
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const sleep = async (ms: number) => {
			await new Promise<void>((r) => setTimeout(r, Math.min(ms, 15)));
		};
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-orphan",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 2_000,
			sleep,
		});
		const session = await factory({ cwd, role: getRole("scout") });
		const prompt = session.prompt("task");
		for (let i = 0; i < 80 && splitEntered < 1; i += 1) await sleep(10);
		expect(splitEntered).toBe(1);
		const original = pool.getByRole("scout")!;
		const nowIso = new Date().toISOString();
		pool.upsert({
			...original,
			provisioningOwnerId: "replacement-owner",
			provisioningHeartbeatAt: nowIso,
			updatedAt: nowIso,
		});
		releaseSplit();
		await expect(prompt).rejects.toThrow(/superseded after pane split/i);
		expect(roleHasOrphanPaneEvidence(pool.poolRoot, "scout")).toBe(true);
		const listed = listOrphanPaneEvidence(pool.poolRoot, "scout");
		expect(listed.some((e) => e.ok && e.evidence.paneId === "w1:p-orphan")).toBe(true);
		const successorBefore = pool.getByRole("scout")!;
		expect(successorBefore.provisioningOwnerId).toBe("replacement-owner");
		expect(successorBefore.paneId).toBeUndefined();
		await session.dispose();
		expect(roleHasOrphanPaneEvidence(pool.poolRoot, "scout")).toBe(true);

		// Archive so the next prompt attempts physical provisioning (blocked by orphan evidence).
		pool.archiveRoleKeepingTombstone("scout", new Date().toISOString());
		const blocked = await factory({ cwd, role: getRole("scout") });
		await expect(blocked.prompt("blocked")).rejects.toThrow(/orphan pane cleanup evidence|momo-cleanup/i);
		await blocked.dispose();
		expect(roleHasOrphanPaneEvidence(pool.poolRoot, "scout")).toBe(true);

		// Seed a live successor that must remain untouched by orphan cleanup.
		const workerId = stableWorkerId(identity.poolKey, "scout");
		pool.upsert({
			workerId,
			generation: 2,
			generationTombstone: 2,
			role: "scout",
			paneId: "w1:p-successor",
			agentName: "momo_scout",
			cwd: identity.canonicalRoot,
			status: "busy",
			activeAssignmentId: "succbusyorphan01",
			updatedAt: new Date().toISOString(),
		});
		const pi = createFakePi();
		const notifies: string[] = [];
		installMomoParent(pi as unknown as ExtensionAPI, {
			cwd,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "parent-orphan-clean",
				HERDR_ENV: "1",
				HERDR_PANE_ID: "w1:p0",
				HERDR_WORKSPACE_ID: "ws",
				HERDR_SOCKET_PATH: "s",
			},
			client,
			poolRegistry: pool,
		});
		await pi.commands.get("momo-cleanup")!.handler("", {
			ui: { notify: (m: string) => notifies.push(m) },
		} as never);
		expect(notifies.join("\n")).toMatch(/Closed 1 orphan pane/i);
		expect(roleHasOrphanPaneEvidence(pool.poolRoot, "scout")).toBe(false);
		const afterCleanup = pool.getByRole("scout")!;
		expect(afterCleanup.status).toBe("busy");
		expect(afterCleanup.paneId).toBe("w1:p-successor");
		expect(afterCleanup.activeAssignmentId).toBe("succbusyorphan01");

		// Provision allowed after cleanup once successor is archived.
		pool.archiveRoleKeepingTombstone("scout", new Date().toISOString());
		const client2 = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
					const control =
						envArgs.find((v) => v.startsWith("MOMO_CONTROL_DIR="))?.slice(
							"MOMO_CONTROL_DIR=".length,
						) ??
						envArgs.find((v) => v.startsWith("MOMO_IPC_DIR="))?.slice("MOMO_IPC_DIR=".length);
					const wid =
						envArgs.find((v) => v.startsWith("MOMO_WORKER_ID="))?.slice("MOMO_WORKER_ID=".length) ??
						"";
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
						stdout: JSON.stringify({
							id: "s",
							result: { pane: { pane_id: "w1:p-next" } },
						}),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: {
								pane_id: "w1:p-next",
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
		const factory2 = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-orphan-2",
			client: client2,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 2_000,
			sleep,
		});
		const session2 = await factory2({ cwd, role: getRole("scout") });
		await session2.prompt("after-cleanup");
		expect(pool.getByRole("scout")?.paneId).toBe("w1:p-next");
		expect(pool.getByRole("scout")?.status).toBe("busy");
		await session2.dispose();
	}, 20_000);

	it("malformed/symlink/oversized/mismatched-name/group-mode orphan evidence fail closed", () => {
		const cacheRoot = tempDir("momo-orphan-bad-");
		const cwd = tempDir("momo-orphan-bad-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const dir = orphanPanesDir(pool.poolRoot, "scout");
		mkdirSync(dir, { recursive: true, mode: 0o700 });
		writeFileSync(path.join(dir, "bad.json"), "{not-json\n", { mode: 0o600 });
		writeFileSync(path.join(dir, "huge.json"), "x".repeat(MAX_IPC_JSON_BYTES + 10), {
			mode: 0o600,
		});
		// Valid JSON under a non-canonical filename must not be closable.
		writeFileSync(
			path.join(dir, "not-the-hash.json"),
			`${JSON.stringify({
				version: 1,
				generation: 1,
				workerId: "w".repeat(16),
				paneId: "w1:p-mismatch",
				agentName: "momo_scout",
				reason: "test",
				createdAt: new Date().toISOString(),
			})}\n`,
			{ mode: 0o600 },
		);
		writeOrphanPaneEvidence(pool.poolRoot, "scout", {
			generation: 1,
			workerId: "w".repeat(16),
			paneId: "w1:p-real",
			agentName: "momo_scout",
			reason: "test",
			createdAt: new Date().toISOString(),
		});
		const realPath = orphanEvidencePath(pool.poolRoot, "scout", "w1:p-real");
		expect(path.basename(realPath)).toBe(orphanEvidenceFileName("w1:p-real"));
		const groupy = path.join(dir, "groupy.json");
		writeFileSync(groupy, '{"version":1}\n', { mode: 0o644 });
		// Interrupted atomic write must remain visible and block provisioning.
		const interruptedTmp = path.join(
			dir,
			`${orphanEvidenceFileName("w1:p-temp")}.${process.pid}.${Date.now()}.tmp`,
		);
		writeFileSync(interruptedTmp, '{"partial":true}\n', { mode: 0o600 });
		const linkPath = path.join(dir, "link.json");
		try {
			symlinkSync(realPath, linkPath);
		} catch {
			// Some CI FS may not allow symlinks; still cover other fail-closed cases.
		}
		expect(roleHasOrphanPaneEvidence(pool.poolRoot, "scout")).toBe(true);
		const listed = listOrphanPaneEvidence(pool.poolRoot, "scout");
		expect(
			listed.some((e) => !e.ok && /invalid|JSON|oversized|symlink|canonical|group\/other/i.test(e.reason)),
		).toBe(true);
		expect(listed.some((e) => !e.ok && /canonical paneId hash/i.test(e.reason))).toBe(true);
		expect(
			listed.some(
				(e) => !e.ok && e.filePath === interruptedTmp && /interrupted atomic write|temp/i.test(e.reason),
			),
		).toBe(true);
		expect(listed.some((e) => e.ok && e.evidence.paneId === "w1:p-real")).toBe(true);
		expect(() => assertNoOrphanPaneEvidence(pool.poolRoot, "scout")).toThrow(/momo-cleanup/i);
	});

	it("cleanup pane-not-found removes orphan evidence without touching successor registry", async () => {
		const cacheRoot = tempDir("momo-orphan-nf-");
		const cwd = tempDir("momo-orphan-nf-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		pool.upsert({
			workerId,
			generation: 2,
			generationTombstone: 2,
			role: "scout",
			paneId: "w1:p-successor",
			agentName: "momo_scout",
			cwd: identity.canonicalRoot,
			status: "busy",
			activeAssignmentId: "succbusy00000001",
			updatedAt: new Date().toISOString(),
		});
		writeOrphanPaneEvidence(pool.poolRoot, "scout", {
			generation: 1,
			workerId,
			paneId: "w1:p-gone",
			agentName: "momo_scout",
			reason: "superseded_close_failed",
			createdAt: new Date().toISOString(),
		});
		expect(existsSync(orphanEvidencePath(pool.poolRoot, "scout", "w1:p-gone"))).toBe(true);
		const closed: string[] = [];
		const agentGets: string[] = [];
		const pi = createFakePi();
		installMomoParent(pi as unknown as ExtensionAPI, {
			cwd,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "parent-orphan-nf",
				HERDR_ENV: "1",
				HERDR_PANE_ID: "w1:p0",
				HERDR_WORKSPACE_ID: "ws",
				HERDR_SOCKET_PATH: "s",
			},
			client: new HerdrClient({
				runCommand: async (_file, args) => {
					if (args[0] === "pane" && args[1] === "close") {
						closed.push(String(args[2]));
						return {
							code: 1,
							stdout: JSON.stringify({
								id: "c",
								error: { code: "pane_not_found", message: "gone" },
							}),
							stderr: "",
						};
					}
					if (args[0] === "agent" && args[1] === "get") {
						agentGets.push(String(args[2] ?? ""));
						return {
							code: 0,
							stdout: JSON.stringify({
								id: "g",
								result: {
									type: "agent_info",
									agent: { agent_status: "working", name: "momo_scout" },
								},
							}),
							stderr: "",
						};
					}
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				},
			}),
			poolRegistry: pool,
		});
		const notifies: string[] = [];
		await pi.commands.get("momo-cleanup")!.handler("", {
			ui: { notify: (m: string) => notifies.push(m) },
		} as never);
		expect(closed).toEqual(["w1:p-gone"]);
		expect(closed).not.toContain("w1:p-successor");
		expect(agentGets).toEqual([]);
		expect(roleHasOrphanPaneEvidence(pool.poolRoot, "scout")).toBe(false);
		const successor = pool.getByRole("scout")!;
		expect(successor.status).toBe("busy");
		expect(successor.paneId).toBe("w1:p-successor");
		expect(successor.activeAssignmentId).toBe("succbusy00000001");
		expect(notifies.join("\n")).toMatch(/Closed 1 orphan pane/i);
	});

	for (const status of ["busy", "idle"] as const) {
		it(`refuses orphan paneId colliding with live ${status} successor (no close)`, async () => {
			const cacheRoot = tempDir(`momo-orphan-collide-${status}-`);
			const cwd = tempDir(`momo-orphan-collide-${status}-cwd-`);
			const identity = resolvePoolIdentity({
				cwd,
				canonicalRoot: cwd,
				workspaceId: "ws",
				socketPath: "s",
			});
			const pool = new PoolRegistry(identity.poolKey, cacheRoot);
			const workerId = stableWorkerId(identity.poolKey, "scout");
			const sharedPane = "w1:p-shared-live";
			pool.upsert({
				workerId,
				generation: 2,
				generationTombstone: 2,
				role: "scout",
				paneId: sharedPane,
				agentName: "momo_scout",
				cwd: identity.canonicalRoot,
				status,
				...(status === "busy"
					? { activeAssignmentId: "collidebusy000001" }
					: {
							// Keep paneId registered but not normally closable so only orphan
							// collision refusal is under test.
							paneClosed: true,
							recoveryRequired: true,
						}),
				updatedAt: new Date().toISOString(),
			});
			writeOrphanPaneEvidence(pool.poolRoot, "scout", {
				generation: 1,
				workerId,
				paneId: sharedPane,
				agentName: "momo_scout",
				reason: "stale_evidence_same_pane",
				createdAt: new Date().toISOString(),
			});
			const closed: string[] = [];
			const pi = createFakePi();
			installMomoParent(pi as unknown as ExtensionAPI, {
				cwd,
				env: {
					MOMO_PARENT: "1",
					MOMO_PARENT_ID: `parent-orphan-collide-${status}`,
					HERDR_ENV: "1",
					HERDR_PANE_ID: "w1:p0",
					HERDR_WORKSPACE_ID: "ws",
					HERDR_SOCKET_PATH: "s",
				},
				client: new HerdrClient({
					runCommand: async (_file, args) => {
						if (args[0] === "pane" && args[1] === "close") {
							closed.push(String(args[2]));
							return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
						}
						return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
					},
				}),
				poolRegistry: pool,
			});
			const notifies: string[] = [];
			await pi.commands.get("momo-cleanup")!.handler("", {
				ui: { notify: (m: string) => notifies.push(m) },
			} as never);
			expect(closed).toEqual([]);
			expect(roleHasOrphanPaneEvidence(pool.poolRoot, "scout")).toBe(true);
			expect(notifies.join("\n")).toMatch(/collides with live registry pane/i);
			const live = pool.getByRole("scout")!;
			expect(live.paneId).toBe(sharedPane);
			expect(live.status).toBe(status);
		});
	}

	it("delayed orphan close releases role lock; successor settlement proceeds; replaced evidence retained", async () => {
		const cacheRoot = tempDir("momo-orphan-delayed-close-");
		const cwd = tempDir("momo-orphan-delayed-close-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const assignmentId = "succsettle000001";
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 2,
			generationTombstone: 2,
			role: "scout",
			paneId: "w1:p-successor",
			agentName: "momo_scout",
			cwd: identity.canonicalRoot,
			status: "busy",
			activeAssignmentId: assignmentId,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId,
			generation: 2,
			parentEpoch: "e1",
			dispatchedAt: new Date().toISOString(),
		});
		const originalEvidence = writeOrphanPaneEvidence(pool.poolRoot, "scout", {
			generation: 1,
			workerId,
			paneId: "w1:p-orphan-delay",
			agentName: "momo_scout",
			reason: "superseded_close_failed",
			createdAt: "2020-01-01T00:00:00.000Z",
		});
		let releaseClose!: () => void;
		const closeGate = new Promise<void>((resolve) => {
			releaseClose = resolve;
		});
		let closeEntered = 0;
		const closed: string[] = [];
		const pi = createFakePi();
		installMomoParent(pi as unknown as ExtensionAPI, {
			cwd,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "parent-orphan-delayed",
				HERDR_ENV: "1",
				HERDR_PANE_ID: "w1:p0",
				HERDR_WORKSPACE_ID: "ws",
				HERDR_SOCKET_PATH: "s",
			},
			client: new HerdrClient({
				runCommand: async (_file, args) => {
					if (args[0] === "pane" && args[1] === "close") {
						closed.push(String(args[2]));
						closeEntered += 1;
						await closeGate;
						return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
					}
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				},
			}),
			poolRegistry: pool,
		});
		const notifies: string[] = [];
		const cleanupDone = pi.commands.get("momo-cleanup")!.handler("", {
			ui: { notify: (m: string) => notifies.push(m) },
		} as never);
		for (let i = 0; i < 80 && closeEntered < 1; i += 1) {
			await new Promise<void>((r) => setTimeout(r, 10));
		}
		expect(closeEntered).toBe(1);
		expect(closed).toEqual(["w1:p-orphan-delay"]);

		// Role lock must be free during Herdr close — successor settlement proceeds.
		const { completeCleanAssignmentLocked } = await import(
			"../src/herdr/terminal-transition.js"
		);
		const settleStarted = Date.now();
		const outcome = withRoleLock(pool.poolRoot, "scout", () =>
			completeCleanAssignmentLocked({
				pool,
				role: "scout",
				workerId,
				generation: 2,
				finishedAssignmentId: assignmentId,
			}),
		);
		expect(Date.now() - settleStarted).toBeLessThan(2_000);
		expect(outcome.kind).toBe("idle");
		const afterSettle = pool.getByRole("scout")!;
		expect(afterSettle.status).toBe("idle");
		expect(afterSettle.paneId).toBe("w1:p-successor");
		expect(afterSettle.activeAssignmentId).toBeUndefined();

		// Replace evidence while close is still in flight — must not be deleted.
		writeOrphanPaneEvidence(pool.poolRoot, "scout", {
			...originalEvidence,
			reason: "replaced_during_close",
			createdAt: "2020-01-02T00:00:00.000Z",
		});
		releaseClose();
		await cleanupDone;
		expect(roleHasOrphanPaneEvidence(pool.poolRoot, "scout")).toBe(true);
		const listed = listOrphanPaneEvidence(pool.poolRoot, "scout");
		expect(
			listed.some(
				(e) => e.ok && e.evidence.reason === "replaced_during_close",
			),
		).toBe(true);
		expect(notifies.join("\n")).toMatch(/evidence replaced during close/i);
		expect(notifies.join("\n")).not.toMatch(/Closed 1 orphan pane/i);
		const successor = pool.getByRole("scout")!;
		expect(successor.status).toBe("idle");
		expect(successor.paneId).toBe("w1:p-successor");
		expect(successor.activeAssignmentId).toBeUndefined();
	}, 15_000);

	it("temp-only orphan evidence blocks provision and appears in cleanup notes/list", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-orphan-tmp-");
		const cwd = tempDir("momo-orphan-tmp-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const dir = orphanPanesDir(pool.poolRoot, "scout");
		mkdirSync(dir, { recursive: true, mode: 0o700 });
		const tmpPath = path.join(
			dir,
			`${orphanEvidenceFileName("w1:p-interrupted")}.${process.pid}.tmp`,
		);
		writeFileSync(tmpPath, '{"version":1,"partial":true}\n', { mode: 0o600 });
		expect(roleHasOrphanPaneEvidence(pool.poolRoot, "scout")).toBe(true);
		const listed = listOrphanPaneEvidence(pool.poolRoot, "scout");
		expect(listed).toHaveLength(1);
		expect(listed[0]?.ok).toBe(false);
		if (listed[0]?.ok === false) {
			expect(listed[0].filePath).toBe(tmpPath);
			expect(listed[0].reason).toMatch(/interrupted atomic write|temp orphan evidence/i);
		}
		expect(() => assertNoOrphanPaneEvidence(pool.poolRoot, "scout")).toThrow(
			/orphan pane cleanup evidence|momo-cleanup/i,
		);

		const sleep = async (ms: number) => {
			await new Promise<void>((r) => setTimeout(r, Math.min(ms, 15)));
		};
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-orphan-tmp",
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
			readyTimeoutMs: 2_000,
			sleep,
		});
		const blocked = await factory({ cwd, role: getRole("scout") });
		await expect(blocked.prompt("blocked-by-tmp")).rejects.toThrow(
			/orphan pane cleanup evidence|momo-cleanup/i,
		);
		await blocked.dispose();
		expect(existsSync(tmpPath)).toBe(true);

		const closed: string[] = [];
		const pi = createFakePi();
		installMomoParent(pi as unknown as ExtensionAPI, {
			cwd,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "parent-orphan-tmp-clean",
				HERDR_ENV: "1",
				HERDR_PANE_ID: "w1:p0",
				HERDR_WORKSPACE_ID: "ws",
				HERDR_SOCKET_PATH: "s",
			},
			client: new HerdrClient({
				runCommand: async (_file, args) => {
					if (args[0] === "pane" && args[1] === "close") {
						closed.push(String(args[2]));
						return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
					}
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				},
			}),
			poolRegistry: pool,
		});
		const notifies: string[] = [];
		await pi.commands.get("momo-cleanup")!.handler("", {
			ui: { notify: (m: string) => notifies.push(m) },
		} as never);
		expect(closed).toEqual([]);
		expect(existsSync(tmpPath)).toBe(true);
		expect(roleHasOrphanPaneEvidence(pool.poolRoot, "scout")).toBe(true);
		expect(notifies.join("\n")).toMatch(/retained orphan evidence .*interrupted atomic write|temp orphan evidence/i);
	}, 15_000);

	it("evidence atomic write failure is not silent success", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-orphan-writefail-");
		const cwd = tempDir("momo-orphan-writefail-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		let releaseSplit!: () => void;
		const splitGate = new Promise<void>((resolve) => {
			releaseSplit = resolve;
		});
		let splitEntered = 0;
		__setOrphanEvidenceWriteForTest(() => {
			throw new Error("injected orphan evidence atomic write failure");
		});
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					splitEntered += 1;
					await splitGate;
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: { pane: { pane_id: "w1:p-writefail" } },
						}),
						stderr: "",
					};
				}
				if (args[0] === "pane" && args[1] === "close") {
					return {
						code: 1,
						stdout: JSON.stringify({
							id: "c",
							error: { code: "timeout", message: "close timed out" },
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const sleep = async (ms: number) => {
			await new Promise<void>((r) => setTimeout(r, Math.min(ms, 15)));
		};
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-orphan-writefail",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 2_000,
			sleep,
		});
		const session = await factory({ cwd, role: getRole("scout") });
		const prompt = session.prompt("task");
		for (let i = 0; i < 80 && splitEntered < 1; i += 1) await sleep(10);
		const original = pool.getByRole("scout")!;
		const nowIso = new Date().toISOString();
		pool.upsert({
			...original,
			provisioningOwnerId: "replacement-owner",
			provisioningHeartbeatAt: nowIso,
			updatedAt: nowIso,
		});
		releaseSplit();
		await expect(prompt).rejects.toBeInstanceOf(OrphanPaneEvidencePersistenceError);
		await expect(prompt).rejects.toThrow(
			/close failed|evidence persistence failed|injected orphan evidence atomic write/i,
		);
		expect(roleHasOrphanPaneEvidence(pool.poolRoot, "scout")).toBe(false);
		await session.dispose();
	}, 15_000);

	it("cancel-wins then close+evidence fail reports protocol failure; normal cancel stays quiet", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-orphan-cancel-detach-");
		const cwd = tempDir("momo-orphan-cancel-detach-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		let releaseSplit!: () => void;
		const splitGate = new Promise<void>((resolve) => {
			releaseSplit = resolve;
		});
		let splitEntered = 0;
		const reports: HerdrDiagnosticReport[] = [];
		__setOrphanEvidenceWriteForTest(() => {
			throw new Error("injected orphan evidence atomic write failure");
		});
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					splitEntered += 1;
					await splitGate;
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: { pane: { pane_id: "w1:p-cancel-orphan" } },
						}),
						stderr: "",
					};
				}
				if (args[0] === "pane" && args[1] === "close") {
					return {
						code: 1,
						stdout: JSON.stringify({
							id: "c",
							error: { code: "timeout", message: "close timed out" },
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const sleep = async (ms: number) => {
			await new Promise<void>((r) => setTimeout(r, Math.min(ms, 15)));
		};
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-orphan-cancel-detach",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 2_000,
			sleep,
			reportDiagnostic: (report) => {
				reports.push(report);
			},
		});
		const session = await factory({ cwd, role: getRole("scout") });
		const promptStarted = Date.now();
		const prompt = session.prompt("task");
		for (let i = 0; i < 80 && splitEntered < 1; i += 1) await sleep(10);
		expect(splitEntered).toBe(1);
		const original = pool.getByRole("scout")!;
		const nowIso = new Date().toISOString();
		pool.upsert({
			...original,
			provisioningOwnerId: "replacement-owner",
			provisioningHeartbeatAt: nowIso,
			updatedAt: nowIso,
		});
		// Cancel wins the prompt race before close/evidence failure completes.
		await session.abort();
		await prompt;
		expect(Date.now() - promptStarted).toBeLessThan(2_000);
		releaseSplit();
		for (let i = 0; i < 80 && reports.length === 0; i += 1) await sleep(20);
		expect(reports).toHaveLength(1);
		expect(reports[0]?.kind).toBe("detached_protocol_failure");
		expect(reports[0]?.error).toBeInstanceOf(OrphanPaneEvidencePersistenceError);
		expect(reports[0]?.paneId).toBe("w1:p-cancel-orphan");
		// Cancel often hits throwIfCancelled after split (ownership path); supersession
		// after failed persist uses the split reason. Either is a protocol failure.
		expect(reports[0]?.reason).toMatch(
			/provision_(ownership_superseded|superseded_after_split)_close_failed/,
		);
		expect(reports[0]?.message).toMatch(
			/evidence persistence failed|injected orphan evidence atomic write/i,
		);
		const proxy = session as unknown as { getDiagnosticFailureForTest: () => string | undefined };
		expect(proxy.getDiagnosticFailureForTest()).toMatch(/evidence persistence failed/i);
		await session.dispose();

		// Ordinary cancellation (no protocol cleanup failure) leaves reporter untouched.
		__setOrphanEvidenceWriteForTest();
		reports.length = 0;
		pool.archiveRoleKeepingTombstone("scout", new Date().toISOString());
		const quiet = await factory({ cwd, role: getRole("scout") });
		__setPostPlanPreBranchHookForTest(async () => {
			await quiet.abort();
		});
		const quietStarted = Date.now();
		await quiet.prompt("quiet-cancel");
		expect(Date.now() - quietStarted).toBeLessThan(2_000);
		await sleep(80);
		expect(reports).toHaveLength(0);
		const quietProxy = quiet as unknown as {
			getDiagnosticFailureForTest: () => string | undefined;
		};
		expect(quietProxy.getDiagnosticFailureForTest()).toBeUndefined();
		await quiet.dispose();
	}, 20_000);
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

	it("joiner timeout on pane-less starting archives tombstone so role can reprovision", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-joiner-orphan-");
		const cwd = tempDir("momo-joiner-orphan-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			agentName: "momo_scout",
			status: "starting",
			cwd: identity.canonicalRoot,
			updatedAt: new Date().toISOString(),
		});
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
			parentId: "parent-joiner-orphan",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 80,
			sleep: async (ms) => {
				await new Promise((r) => setTimeout(r, Math.min(ms, 20)));
			},
		});
		const session = await factory({ cwd, role: getRole("scout") });
		await expect(session.prompt("join")).rejects.toThrow(/did not become ready/i);
		const after = pool.getByRole("scout")!;
		expect(isArchivalTombstone(after)).toBe(true);
		expect(after.generationTombstone).toBeGreaterThanOrEqual(1);
		expect(after.paneId).toBeUndefined();
		// Reprovision allowed.
		expect(pool.nextGeneration("scout")).toBe(after.generationTombstone + 1);
		await session.dispose();
	}, 10_000);

	it("joiner timeout on paneful starting marks unhealthy retaining pane for cleanup", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-joiner-paneful-");
		const cwd = tempDir("momo-joiner-paneful-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p-orphan",
			agentName: "momo_scout",
			status: "starting",
			cwd: identity.canonicalRoot,
			updatedAt: new Date().toISOString(),
		});
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
			parentId: "parent-joiner-paneful",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 80,
			sleep: async (ms) => {
				await new Promise((r) => setTimeout(r, Math.min(ms, 20)));
			},
		});
		const session = await factory({ cwd, role: getRole("scout") });
		await expect(session.prompt("join")).rejects.toThrow(/did not become ready/i);
		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("unhealthy");
		expect(after.paneId).toBe("w1:p-orphan");
		expect(after.agentName).toBe("momo_scout");
		expect(isArchivalTombstone(after)).toBe(false);
		expect(selectClosablePoolWorkers([after]).closable).toHaveLength(1);
		await session.dispose();
	}, 10_000);

	it("canceled joiner does not alter live starting provision", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-joiner-cancel-");
		const cwd = tempDir("momo-joiner-cancel-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p-live",
			agentName: "momo_scout",
			status: "starting",
			cwd: identity.canonicalRoot,
			updatedAt: new Date().toISOString(),
		});
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
			parentId: "parent-joiner-cancel",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 5_000,
			sleep: async (ms) => {
				await new Promise((r) => setTimeout(r, Math.min(ms, 20)));
			},
		});
		const session = await factory({ cwd, role: getRole("scout") });
		const prompt = session.prompt("join");
		await new Promise((r) => setTimeout(r, 40));
		await session.abort();
		await prompt.catch(() => undefined);
		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("starting");
		expect(after.paneId).toBe("w1:p-live");
		expect(after.generation).toBe(1);
		await session.dispose();
	}, 10_000);

	it("cancel after reserve before provision branch archives immediately; next reprovisions", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-cancel-prebranch-");
		const cwd = tempDir("momo-cancel-prebranch-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		let splitCalls = 0;
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					splitCalls += 1;
					const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
					const control =
						envArgs.find((v) => v.startsWith("MOMO_CONTROL_DIR="))?.slice(
							"MOMO_CONTROL_DIR=".length,
						) ??
						envArgs.find((v) => v.startsWith("MOMO_IPC_DIR="))?.slice("MOMO_IPC_DIR=".length);
					const workerId =
						envArgs.find((v) => v.startsWith("MOMO_WORKER_ID="))?.slice("MOMO_WORKER_ID=".length) ??
						"";
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
						stdout: JSON.stringify({
							id: "s",
							result: { pane: { pane_id: "w1:p-reprov" } },
						}),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: {
								pane_id: "w1:p-reprov",
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
		const sleep = async (ms: number) => {
			await new Promise<void>((r) => setTimeout(r, Math.min(ms, 15)));
		};
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-cancel-prebranch",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 2_000,
			pollIntervalMs: 20,
			sleep,
		});
		const session = await factory({ cwd, role: getRole("scout") });
		let barrierOwnerId: string | undefined;
		__setPostPlanPreBranchHookForTest(async (plan) => {
			barrierOwnerId = plan.provisioningOwnerId;
			const mid = pool.getByRole("scout")!;
			expect(mid.status).toBe("starting");
			expect(mid.paneId).toBeUndefined();
			expect(mid.provisioningOwnerId).toBe(plan.provisioningOwnerId);
			expect(splitCalls).toBe(0);
			await session.abort();
		});
		const promptStarted = Date.now();
		await session.prompt("cancelled-prebranch");
		expect(Date.now() - promptStarted).toBeLessThan(2_000);
		// Allow supervised rollback to finish archiving.
		for (let i = 0; i < 40; i += 1) {
			const row = pool.getByRole("scout");
			if (row && isArchivalTombstone(row)) break;
			await sleep(20);
		}
		const archived = pool.getByRole("scout")!;
		expect(isArchivalTombstone(archived)).toBe(true);
		expect(archived.paneId).toBeUndefined();
		expect(archived.provisioningOwnerId).toBeUndefined();
		expect(archived.provisioningHeartbeatAt).toBeUndefined();
		expect(splitCalls).toBe(0);
		expect(barrierOwnerId).toBeTruthy();
		await session.dispose();

		// Next delegation reprovisions without waiting out a ready timeout on a strand.
		__resetProvisioningOwnershipHooksForTest();
		const session2 = await factory({ cwd, role: getRole("scout") });
		const reprovisionStarted = Date.now();
		await session2.prompt("reprovision");
		expect(Date.now() - reprovisionStarted).toBeLessThan(2_000);
		expect(splitCalls).toBe(1);
		const live = pool.getByRole("scout")!;
		expect(live.status).toBe("busy");
		expect(live.paneId).toBe("w1:p-reprov");
		expect(live.generation).toBeGreaterThanOrEqual(2);
		expect(live.provisioningOwnerId).toBeUndefined();
		await session2.dispose();
	}, 15_000);

	it("live provision owner heartbeat: joiner timeout renews; both assignments succeed FIFO", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-owner-live-");
		const cwd = tempDir("momo-owner-live-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		let releaseSplit!: () => void;
		const splitGate = new Promise<void>((resolve) => {
			releaseSplit = resolve;
		});
		let splitEntered = 0;
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					splitEntered += 1;
					const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
					const control =
						envArgs.find((v) => v.startsWith("MOMO_CONTROL_DIR="))?.slice(
							"MOMO_CONTROL_DIR=".length,
						) ??
						envArgs.find((v) => v.startsWith("MOMO_IPC_DIR="))?.slice("MOMO_IPC_DIR=".length);
					const workerId =
						envArgs.find((v) => v.startsWith("MOMO_WORKER_ID="))?.slice("MOMO_WORKER_ID=".length) ??
						"";
					const runId =
						envArgs.find((v) => v.startsWith("MOMO_RUN_ID="))?.slice("MOMO_RUN_ID=".length) ?? "";
					await splitGate;
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
						stdout: JSON.stringify({
							id: "s",
							result: { pane: { pane_id: "w1:p-owned" } },
						}),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: {
								pane_id: "w1:p-owned",
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
		const sleep = async (ms: number) => {
			await new Promise<void>((r) => setTimeout(r, Math.min(ms, 15)));
		};
		const factoryOpts = {
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-owner-live",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			pollIntervalMs: 20,
			// Joiner local deadline is shorter than the blocked split hold.
			readyTimeoutMs: 90,
			sleep,
		} as const;
		const factoryA = createHerdrChildSessionFactory(factoryOpts);
		const factoryB = createHerdrChildSessionFactory({
			...factoryOpts,
			parentId: "parent-owner-live-b",
			parentEpoch: "epoch-b",
		});
		const sessionA = await factoryA({ cwd, role: getRole("scout") });
		const promptA = sessionA.prompt("task-a");
		for (let i = 0; i < 80 && splitEntered < 1; i += 1) await sleep(10);
		expect(splitEntered).toBe(1);
		const mid = pool.getByRole("scout")!;
		expect(mid.status).toBe("starting");
		expect(mid.provisioningOwnerId).toBeTruthy();
		expect(mid.provisioningHeartbeatAt).toBeTruthy();

		const sessionB = await factoryB({ cwd, role: getRole("scout") });
		const promptB = sessionB.prompt("task-b");
		// Hold past at least one joiner readyTimeout while owner keeps heartbeating.
		await sleep(220);
		expect(pool.getByRole("scout")?.status).toBe("starting");
		expect(pool.getByRole("scout")?.provisioningOwnerId).toBe(mid.provisioningOwnerId);
		expect(isArchivalTombstone(pool.getByRole("scout")!)).toBe(false);

		releaseSplit();
		await promptA;
		await promptB;
		const record = pool.getByRole("scout")!;
		expect(record.status).toBe("busy");
		expect(record.paneId).toBe("w1:p-owned");
		expect(record.provisioningOwnerId).toBeUndefined();
		expect(record.provisioningHeartbeatAt).toBeUndefined();
		expect(queueCount(pool.poolRoot, "scout")).toBe(1);
		expect(listQueue(pool.poolRoot, "scout")[0]?.task).toBe("task-b");
		await sessionA.dispose();
		await sessionB.dispose();
	}, 20_000);

	it("dead owner stale heartbeat: joiner timeout fences and allows reprovision", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-owner-stale-");
		const cwd = tempDir("momo-owner-stale-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const staleAt = new Date(Date.now() - 60_000).toISOString();
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			agentName: "momo_scout",
			status: "starting",
			cwd: identity.canonicalRoot,
			provisioningOwnerId: "dead-owner-token",
			provisioningHeartbeatAt: staleAt,
			updatedAt: staleAt,
		});
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
			parentId: "parent-owner-stale",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 80,
			sleep: async (ms) => {
				await new Promise((r) => setTimeout(r, Math.min(ms, 20)));
			},
		});
		const session = await factory({ cwd, role: getRole("scout") });
		await expect(session.prompt("join")).rejects.toBeInstanceOf(WorkerReadyTimeoutError);
		const after = pool.getByRole("scout")!;
		expect(isArchivalTombstone(after)).toBe(true);
		expect(after.provisioningOwnerId).toBeUndefined();
		expect(after.provisioningHeartbeatAt).toBeUndefined();
		expect(pool.nextGeneration("scout")).toBe(after.generationTombstone + 1);
		await session.dispose();
	}, 10_000);

	it("invalid ready IPC fails closed without fencing a live starting owner", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-owner-bad-ready-");
		const cwd = tempDir("momo-owner-bad-ready-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const nowIso = new Date().toISOString();
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p-bad-ready",
			agentName: "momo_scout",
			status: "starting",
			cwd: identity.canonicalRoot,
			provisioningOwnerId: "live-owner",
			provisioningHeartbeatAt: nowIso,
			updatedAt: nowIso,
		});
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(control.ready, {
			version: 1,
			runId: "wrong-run",
			workerId: "wrong-worker",
			readyAt: nowIso,
		});
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
			parentId: "parent-bad-ready",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 5_000,
			sleep: async (ms) => {
				await new Promise((r) => setTimeout(r, Math.min(ms, 20)));
			},
		});
		const session = await factory({ cwd, role: getRole("scout") });
		await expect(session.prompt("join")).rejects.toThrow(/ready IPC invalid/i);
		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("starting");
		expect(after.generation).toBe(1);
		expect(after.paneId).toBe("w1:p-bad-ready");
		expect(after.provisioningOwnerId).toBe("live-owner");
		await session.dispose();
	}, 10_000);

	it("provisioning heartbeat cannot mutate newer generation or foreign owner", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-owner-fence-hb-");
		const cwd = tempDir("momo-owner-fence-hb-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		let releaseSplit!: () => void;
		const splitGate = new Promise<void>((resolve) => {
			releaseSplit = resolve;
		});
		let splitEntered = 0;
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					splitEntered += 1;
					await splitGate;
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: { pane: { pane_id: "w1:p-hb" } },
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const sleep = async (ms: number) => {
			await new Promise<void>((r) => setTimeout(r, Math.min(ms, 15)));
		};
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-hb-fence",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 120,
			sleep,
		});
		const session = await factory({ cwd, role: getRole("scout") });
		const prompt = session.prompt("task");
		for (let i = 0; i < 80 && splitEntered < 1; i += 1) await sleep(10);
		expect(splitEntered).toBe(1);
		const owned = pool.getByRole("scout")!;
		expect(owned.provisioningOwnerId).toBeTruthy();
		// Wait for at least one owner heartbeat tick.
		let advanced = owned.provisioningHeartbeatAt!;
		for (let i = 0; i < 40; i += 1) {
			await sleep(20);
			const hb = pool.getByRole("scout")?.provisioningHeartbeatAt;
			if (hb && hb !== owned.provisioningHeartbeatAt) {
				advanced = hb;
				break;
			}
		}
		expect(advanced).not.toBe(owned.provisioningHeartbeatAt);

		const frozenAt = new Date(Date.now() - 1_000).toISOString();
		pool.upsert({
			...owned,
			generation: owned.generation + 1,
			generationTombstone: owned.generation + 1,
			provisioningOwnerId: "foreign-owner",
			provisioningHeartbeatAt: frozenAt,
			updatedAt: frozenAt,
		});
		await sleep(200);
		const after = pool.getByRole("scout")!;
		expect(after.generation).toBe(owned.generation + 1);
		expect(after.provisioningOwnerId).toBe("foreign-owner");
		expect(after.provisioningHeartbeatAt).toBe(frozenAt);

		releaseSplit();
		await prompt.catch(() => undefined);
		await session.dispose();
		// Dispose stops the timer; foreign ownership must remain untouched.
		await sleep(120);
		const final = pool.getByRole("scout")!;
		expect(final.provisioningOwnerId === "foreign-owner" || final.generation === 0).toBe(true);
	}, 15_000);

	it("provisioning ownership metadata clears on leave-starting and timers stop", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-owner-clear-");
		const cwd = tempDir("momo-owner-clear-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
					const control =
						envArgs.find((v) => v.startsWith("MOMO_CONTROL_DIR="))?.slice(
							"MOMO_CONTROL_DIR=".length,
						) ??
						envArgs.find((v) => v.startsWith("MOMO_IPC_DIR="))?.slice("MOMO_IPC_DIR=".length);
					const workerId =
						envArgs.find((v) => v.startsWith("MOMO_WORKER_ID="))?.slice("MOMO_WORKER_ID=".length) ??
						"";
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
						stdout: JSON.stringify({
							id: "s",
							result: { pane: { pane_id: "w1:p-clear" } },
						}),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: {
								pane_id: "w1:p-clear",
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
			parentId: "parent-owner-clear",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 2_000,
			pollIntervalMs: 20,
			sleep: async (ms) => {
				await new Promise((r) => setTimeout(r, Math.min(ms, 15)));
			},
		});
		const session = await factory({ cwd, role: getRole("scout") });
		await session.prompt("task");
		const busy = pool.getByRole("scout")!;
		expect(busy.status).toBe("busy");
		expect(busy.provisioningOwnerId).toBeUndefined();
		expect(busy.provisioningHeartbeatAt).toBeUndefined();
		const snapshot = { ...busy };
		await new Promise((r) => setTimeout(r, 150));
		const after = pool.getByRole("scout")!;
		expect(after.provisioningOwnerId).toBeUndefined();
		expect(after.provisioningHeartbeatAt).toBeUndefined();
		expect(after.updatedAt).toBe(snapshot.updatedAt);
		await session.dispose();
	}, 15_000);

	it("validatePoolWorkerRecord requires ownership as a starting-only pair", () => {
		const base = {
			workerId: "w".repeat(16),
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			status: "starting",
			updatedAt: new Date().toISOString(),
			provisioningOwnerId: "owner-1",
			provisioningHeartbeatAt: new Date().toISOString(),
		};
		const ok = validatePoolWorkerRecord(base);
		expect(ok.provisioningOwnerId).toBe("owner-1");
		expect(validatePoolWorkerRecord({ ...base, provisioningOwnerId: undefined, provisioningHeartbeatAt: undefined }).provisioningOwnerId).toBeUndefined();
		expect(() =>
			validatePoolWorkerRecord({
				...base,
				provisioningHeartbeatAt: "not-a-date",
			}),
		).toThrow(/provisioningHeartbeatAt/);
		expect(() =>
			validatePoolWorkerRecord({
				...base,
				provisioningHeartbeatAt: undefined,
			}),
		).toThrow(/pair/i);
		expect(() =>
			validatePoolWorkerRecord({
				...base,
				provisioningOwnerId: undefined,
			}),
		).toThrow(/pair/i);
		expect(() =>
			validatePoolWorkerRecord({
				...base,
				status: "idle",
				paneId: "w1:p1",
				agentName: "momo_scout",
			}),
		).toThrow(/only allowed on starting/i);
		expect(() =>
			validatePoolWorkerRecord({
				...base,
				provisioningHeartbeatAt: new Date(
					Date.now() + DEFAULT_HEARTBEAT_CLOCK_SKEW_MS + 60_000,
				).toISOString(),
			}),
		).toThrow(/future/i);
	});

	it("atomic joiner stale decision fences before concurrent refresh can interleave", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-owner-atomic-");
		const cwd = tempDir("momo-owner-atomic-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const staleAt = new Date(Date.now() - 60_000).toISOString();
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			agentName: "momo_scout",
			status: "starting",
			cwd: identity.canonicalRoot,
			provisioningOwnerId: "stale-owner",
			provisioningHeartbeatAt: staleAt,
			updatedAt: staleAt,
		});

		let refreshSawStatus: string | undefined;
		let refreshWroteFresh = false;
		let refreshPromise: Promise<void> | undefined;
		__setJoinerReadyTimeoutLockHookForTest(async () => {
			refreshPromise = withRoleLockAsync(pool.poolRoot, "scout", () => {
				const record = pool.getByRole("scout")!;
				refreshSawStatus = record.status;
				if (record.status === "starting") {
					const nowIso = new Date().toISOString();
					pool.upsert({
						...record,
						provisioningHeartbeatAt: nowIso,
						updatedAt: nowIso,
					});
					refreshWroteFresh = true;
				}
			});
			// Let the refresh acquire attempt block on the held role lock.
			await new Promise((r) => setTimeout(r, 40));
		});

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
			parentId: "parent-owner-atomic",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 80,
			sleep: async (ms) => {
				await new Promise((r) => setTimeout(r, Math.min(ms, 20)));
			},
		});
		const session = await factory({ cwd, role: getRole("scout") });
		await expect(session.prompt("join")).rejects.toBeInstanceOf(WorkerReadyTimeoutError);
		expect(refreshPromise).toBeTruthy();
		await refreshPromise;
		expect(refreshWroteFresh).toBe(false);
		expect(refreshSawStatus).not.toBe("starting");
		const after = pool.getByRole("scout")!;
		expect(isArchivalTombstone(after)).toBe(true);
		await session.dispose();
	}, 15_000);

	it("provisioning heartbeat lock errors are not uncaught and retry keeps ownership live", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-owner-hb-err-");
		const cwd = tempDir("momo-owner-hb-err-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		let releaseSplit!: () => void;
		const splitGate = new Promise<void>((resolve) => {
			releaseSplit = resolve;
		});
		let splitEntered = 0;
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					splitEntered += 1;
					await splitGate;
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: { pane: { pane_id: "w1:p-hb-err" } },
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		let faults = 0;
		__setProvisioningHeartbeatBeforeLockHookForTest(() => {
			faults += 1;
			if (faults <= 2) {
				throw new Error("injected heartbeat lock fault");
			}
		});
		const sleep = async (ms: number) => {
			await new Promise<void>((r) => setTimeout(r, Math.min(ms, 15)));
		};
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-hb-err",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 120,
			sleep,
		});
		const session = await factory({ cwd, role: getRole("scout") });
		const prompt = session.prompt("task");
		for (let i = 0; i < 80 && splitEntered < 1; i += 1) await sleep(10);
		expect(splitEntered).toBe(1);
		const owned = pool.getByRole("scout")!;
		expect(owned.status).toBe("starting");
		expect(owned.provisioningOwnerId).toBeTruthy();
		let recoveredHb: string | undefined;
		for (let i = 0; i < 60; i += 1) {
			await sleep(20);
			const hb = pool.getByRole("scout")?.provisioningHeartbeatAt;
			if (hb && hb !== owned.provisioningHeartbeatAt) {
				recoveredHb = hb;
				break;
			}
		}
		expect(faults).toBeGreaterThanOrEqual(2);
		expect(recoveredHb).toBeTruthy();
		expect(pool.getByRole("scout")?.status).toBe("starting");
		expect(pool.getByRole("scout")?.provisioningOwnerId).toBe(owned.provisioningOwnerId);
		releaseSplit();
		await prompt.catch(() => undefined);
		await session.dispose();
	}, 15_000);

	it("same-generation owner replacement: stale owner cannot persist pane or rollback", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-owner-replace-");
		const cwd = tempDir("momo-owner-replace-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		let releaseSplit!: () => void;
		const splitGate = new Promise<void>((resolve) => {
			releaseSplit = resolve;
		});
		let splitEntered = 0;
		const closed: string[] = [];
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					splitEntered += 1;
					await splitGate;
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: { pane: { pane_id: "w1:p-stale-owner" } },
						}),
						stderr: "",
					};
				}
				if (args[0] === "pane" && args[1] === "close") {
					closed.push(String(args[2] ?? ""));
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const sleep = async (ms: number) => {
			await new Promise<void>((r) => setTimeout(r, Math.min(ms, 15)));
		};
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-owner-replace",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 2_000,
			sleep,
		});
		const session = await factory({ cwd, role: getRole("scout") });
		const prompt = session.prompt("task");
		for (let i = 0; i < 80 && splitEntered < 1; i += 1) await sleep(10);
		expect(splitEntered).toBe(1);
		const original = pool.getByRole("scout")!;
		expect(original.provisioningOwnerId).toBeTruthy();
		const replacementOwner = "replacement-owner-token";
		const nowIso = new Date().toISOString();
		pool.upsert({
			...original,
			provisioningOwnerId: replacementOwner,
			provisioningHeartbeatAt: nowIso,
			updatedAt: nowIso,
			// Still starting at the same generation — only ownership changed.
		});
		releaseSplit();
		await expect(prompt).rejects.toThrow(/superseded after pane split/i);
		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("starting");
		expect(after.generation).toBe(original.generation);
		expect(after.provisioningOwnerId).toBe(replacementOwner);
		expect(after.paneId).toBeUndefined();
		expect(closed).toContain("w1:p-stale-owner");
		await session.dispose();
	}, 15_000);

	it("future provisioning heartbeat within skew is not classified fresh", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-owner-future-");
		const cwd = tempDir("momo-owner-future-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		// Within shared skew grace for registry storage, but must not count as fresh.
		const futureAt = new Date(Date.now() + Math.floor(DEFAULT_HEARTBEAT_CLOCK_SKEW_MS / 2)).toISOString();
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			agentName: "momo_scout",
			status: "starting",
			cwd: identity.canonicalRoot,
			provisioningOwnerId: "future-owner",
			provisioningHeartbeatAt: futureAt,
			updatedAt: futureAt,
		});
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
			parentId: "parent-owner-future",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 80,
			sleep: async (ms) => {
				await new Promise((r) => setTimeout(r, Math.min(ms, 20)));
			},
		});
		const session = await factory({ cwd, role: getRole("scout") });
		await expect(session.prompt("join")).rejects.toBeInstanceOf(WorkerReadyTimeoutError);
		expect(isArchivalTombstone(pool.getByRole("scout")!)).toBe(true);
		await session.dispose();
	}, 10_000);

	it("split persistence: paneId lands under role lock before rename; superseded split closes", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-split-persist-");
		const cwd = tempDir("momo-split-persist-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const closed: string[] = [];
		let releaseSplit!: () => void;
		const splitGate = new Promise<void>((resolve) => {
			releaseSplit = resolve;
		});
		let splitEntered = false;
		let renameSawPane = false;
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					splitEntered = true;
					await splitGate;
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: { pane: { pane_id: "w1:p-split" } },
						}),
						stderr: "",
					};
				}
				if (args[0] === "pane" && args[1] === "rename") {
					renameSawPane = pool.getByRole("scout")?.paneId === "w1:p-split";
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				if (args[0] === "pane" && args[1] === "close") {
					closed.push(String(args[2]));
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				if (args[0] === "agent" && args[1] === "start") {
					const control =
						args.find((_a, i) => false) ??
						undefined;
					void control;
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: {
								pane_id: "w1:p-split",
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
			parentId: "parent-split-persist",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 2_000,
			sleep: async (ms) => {
				await new Promise((r) => setTimeout(r, Math.min(ms, 10)));
			},
		});
		const session = await factory({ cwd, role: getRole("scout") });
		const prompt = session.prompt("start");
		for (let i = 0; i < 50 && !splitEntered; i += 1) {
			await new Promise((r) => setTimeout(r, 20));
		}
		expect(splitEntered).toBe(true);
		expect(pool.getByRole("scout")?.status).toBe("starting");
		expect(pool.getByRole("scout")?.paneId).toBeUndefined();
		// Supersede before split returns so persist fencing rejects and closes the pane.
		const live = pool.getByRole("scout")!;
		pool.upsert({
			...live,
			generation: 2,
			generationTombstone: 2,
			workerId: `${live.workerId}-n2`,
			status: "starting",
			updatedAt: new Date().toISOString(),
		});
		releaseSplit();
		await expect(prompt).rejects.toThrow(/superseded/i);
		expect(closed).toContain("w1:p-split");
		expect(renameSawPane).toBe(false);
		const after = pool.getByRole("scout")!;
		expect(after.generation).toBe(2);
		expect(after.status).toBe("starting");
		expect(after.paneId).toBeUndefined();
		await session.dispose();
	}, 15_000);

	it("split persistence: paneId is visible in registry before rename runs", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-split-visible-");
		const cwd = tempDir("momo-split-visible-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		let releaseRename!: () => void;
		const renameGate = new Promise<void>((resolve) => {
			releaseRename = resolve;
		});
		let paneAtRename: string | undefined;
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: { pane: { pane_id: "w1:p-persist" } },
						}),
						stderr: "",
					};
				}
				if (args[0] === "pane" && args[1] === "rename") {
					paneAtRename = pool.getByRole("scout")?.paneId;
					await renameGate;
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				if (args[0] === "agent" && args[1] === "start") {
					const envReady = workerControlPaths(pool.poolRoot, "scout");
					mkdirSync(envReady.root, { recursive: true, mode: 0o700 });
					const workerId = pool.getByRole("scout")!.workerId;
					atomicWriteJson(envReady.ready, {
						version: 1,
						runId: `g${pool.getByRole("scout")!.generation}`,
						workerId,
						readyAt: new Date().toISOString(),
					});
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: {
								pane_id: "w1:p-persist",
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
			parentId: "parent-split-visible",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 3_000,
			sleep: async (ms) => {
				await new Promise((r) => setTimeout(r, Math.min(ms, 10)));
			},
		});
		const session = await factory({ cwd, role: getRole("scout") });
		const prompt = session.prompt("start");
		for (let i = 0; i < 50 && paneAtRename === undefined; i += 1) {
			await new Promise((r) => setTimeout(r, 20));
		}
		expect(paneAtRename).toBe("w1:p-persist");
		releaseRename();
		await prompt;
		expect(pool.getByRole("scout")?.paneId).toBe("w1:p-persist");
		await session.dispose();
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

	it("adoption of owner-bearing starting→idle strips provisioning metadata", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const cacheRoot = tempDir("momo-adopt-own-idle-");
		const cwd = tempDir("momo-adopt-own-idle-cwd-");
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
		const nowIso = new Date().toISOString();
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p-own-idle",
			agentName: "momo_scout",
			cwd,
			status: "starting",
			provisioningOwnerId: "adopt-owner-success",
			provisioningHeartbeatAt: nowIso,
			updatedAt: nowIso,
		});
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId,
			generation: 1,
			role: "scout",
			cwd,
			paneId: "w1:p-own-idle",
			agentName: "momo_scout",
			createdAt: nowIso,
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: nowIso,
			seq: 1,
		});
		const client = new HerdrClient({
			runCommand: async () => ({
				code: 0,
				stdout: JSON.stringify({
					id: "ok",
					result: {
						type: "agent_info",
						agent: {
							agent_status: "idle",
							pane_id: "w1:p-own-idle",
							name: "momo_scout",
						},
					},
				}),
				stderr: "",
			}),
		});
		await expect(
			adoptPoolWorkers(pool, client, identity.poolKey, {
				ui: { notify: () => undefined },
			}),
		).resolves.toBeUndefined();
		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("idle");
		expect(after.paneId).toBe("w1:p-own-idle");
		expect(after.provisioningOwnerId).toBeUndefined();
		expect(after.provisioningHeartbeatAt).toBeUndefined();
		// Registry must remain loadable (no starting-only ownership on idle).
		expect(() => validatePoolWorkerRecord(after)).not.toThrow();
	});

	it("adoption of owner-bearing starting failure→unhealthy strips metadata; supersession intact", async () => {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const cacheRoot = tempDir("momo-adopt-own-fail-");
		const cwd = tempDir("momo-adopt-own-fail-cwd-");
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
		const nowIso = new Date().toISOString();
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p-own-fail",
			agentName: "momo_scout",
			cwd,
			status: "starting",
			provisioningOwnerId: "adopt-owner-fail",
			provisioningHeartbeatAt: nowIso,
			updatedAt: nowIso,
		});
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId,
			generation: 1,
			role: "scout",
			cwd,
			paneId: "w1:p-own-fail",
			agentName: "momo_scout",
			createdAt: nowIso,
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: nowIso,
			seq: 1,
		});
		const notes: string[] = [];
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "agent" && args[1] === "get") {
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "g",
							result: {
								type: "agent_info",
								agent: {
									agent_status: "idle",
									pane_id: "w1:other-pane",
									name: "momo_scout",
								},
							},
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		await expect(
			adoptPoolWorkers(pool, client, identity.poolKey, {
				ui: { notify: (m: string) => notes.push(m) },
			}),
		).resolves.toBeUndefined();
		expect(notes.join("\n")).toMatch(/pane identity|Did not adopt/i);
		const failed = pool.getByRole("scout")!;
		expect(failed.status).toBe("unhealthy");
		expect(failed.paneId).toBe("w1:p-own-fail");
		expect(failed.provisioningOwnerId).toBeUndefined();
		expect(failed.provisioningHeartbeatAt).toBeUndefined();
		expect(() => validatePoolWorkerRecord(failed)).not.toThrow();

		// Exact snapshot supersession: replace ownership/status before catch would write.
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p-own-fail",
			agentName: "momo_scout",
			cwd,
			status: "starting",
			provisioningOwnerId: "adopt-owner-fail",
			provisioningHeartbeatAt: nowIso,
			updatedAt: nowIso,
		});
		const supersededClient = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "agent" && args[1] === "get") {
					pool.upsert({
						workerId,
						generation: 1,
						generationTombstone: 1,
						role: "scout",
						paneId: "w1:p-own-fail",
						agentName: "momo_scout",
						cwd,
						status: "busy",
						activeAssignmentId: "supersedeownr01",
						activeParentEpoch: "e-sup",
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
									pane_id: "w1:wrong",
									name: "momo_scout",
								},
							},
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		await adoptPoolWorkers(pool, supersededClient, identity.poolKey, {
			ui: { notify: () => undefined },
		});
		const kept = pool.getByRole("scout")!;
		expect(kept.status).toBe("busy");
		expect(kept.activeAssignmentId).toBe("supersedeownr01");
		expect(kept.activeParentEpoch).toBe("e-sup");
		expect(kept.uncertainWrite).toBeUndefined();
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

	it("same-generation restart: unhealthy preserves fence, queue, no prompt/rewrite", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-restart-unhealthy-");
		const cwd = tempDir("momo-restart-unhealthy-cwd-");
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
			role: "scout",
			cwd,
		});
		const assignmentA = "restartunhealthy01";
		const assignmentB = "restartunhealthy02";
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentA);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p1",
			agentName: "momo_scout",
			cwd: identity.canonicalRoot,
			status: "unhealthy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(pathsA.command, {
			version: 1,
			type: "prompt",
			task: "stale-a",
			issuedAt: new Date().toISOString(),
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "e1",
		});
		// Missing active pointer — must still preserve unhealthy, not idle/drain.
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			task: "successor",
		});
		const before = structuredClone(pool.getByRole("scout")!);
		await pi.emit("session_start", {}, ctx);
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).not.toHaveBeenCalled();
		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("unhealthy");
		expect(after.activeAssignmentId).toBe(assignmentA);
		expect(after.generation).toBe(before.generation);
		expect(after.workerId).toBe(before.workerId);
		expect(queueCount(pool.poolRoot, "scout")).toBe(1);
		expect(existsSync(control.active)).toBe(false);
	});

	it("same-generation restart: uncertain with lease/evidence preserved, no prompt", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-restart-uncertain-");
		const cwd = tempDir("momo-restart-uncertain-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const { WriterLeaseManager, createLeaseToken } = await import("../src/lease/writer-lease.js");
		const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_700_000_000_000 });
		const { pi, ctx, workerId, control } = installRole({
			pool,
			identity,
			role: "implementer",
			cwd,
			leaseManager: leases,
		});
		const assignmentA = "restartuncertain01";
		const assignmentB = "restartuncertain02";
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentA);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		const token = createLeaseToken();
		leases.acquire(cwd, workerId, token);
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p1",
			agentName: "momo_implementer",
			cwd: identity.canonicalRoot,
			status: "uncertain",
			uncertainWrite: true,
			activeAssignmentId: assignmentA,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(pathsA.command, {
			version: 1,
			type: "prompt",
			task: "stale-a",
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
			task: "successor",
		});
		await pi.emit("session_start", {}, ctx);
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).not.toHaveBeenCalled();
		const after = pool.getByRole("implementer")!;
		expect(after.status).toBe("uncertain");
		expect(after.uncertainWrite).toBe(true);
		expect(after.activeAssignmentId).toBe(assignmentA);
		expect(leases.stillHeldBy(cwd, workerId, token)).toBe(true);
		expect(queueCount(pool.poolRoot, "implementer")).toBe(1);
		expect(existsSync(control.active)).toBe(true);
	});

	it("same-generation restart: unhealthy + malformed active still handled, queue retained", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-restart-malformed-");
		const cwd = tempDir("momo-restart-malformed-cwd-");
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
			role: "planner",
			cwd,
		});
		const assignmentB = "restartmalformed02";
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "planner",
			paneId: "w1:p1",
			agentName: "momo_planner",
			status: "unhealthy",
			updatedAt: new Date().toISOString(),
		});
		writeFileSync(control.active, "not-json", { mode: 0o600 });
		enqueueAssignment(pool.poolRoot, "planner", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			task: "successor",
		});
		await pi.emit("session_start", {}, ctx);
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).not.toHaveBeenCalled();
		expect(pool.getByRole("planner")?.status).toBe("unhealthy");
		expect(queueCount(pool.poolRoot, "planner")).toBe(1);
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


describe("read-only adoption active/no-result", () => {
	const previousPiDir = process.env.PI_CODING_AGENT_DIR;

	afterEach(async () => {
		if (previousPiDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
		else process.env.PI_CODING_AGENT_DIR = previousPiDir;
		const { __clearAdoptionGraceRechecksForTest } = await import("../src/extensions/parent.js");
		__clearAdoptionGraceRechecksForTest();
		vi.useRealTimers();
	});

	function installFakeHerdrExtension(): void {
		const agentDir = tempDir("momo-pi-agent-");
		mkdirSync(path.join(agentDir, "extensions"), { recursive: true });
		writeFileSync(path.join(agentDir, "extensions", "herdr-agent-state.ts"), "// fake\n", "utf8");
		process.env.PI_CODING_AGENT_DIR = agentDir;
	}

	async function setupBusyScout(options: {
		label: string;
		agentStatus: string;
		/** Mutable Herdr agent_status for mid-test transitions (defaults to agentStatus). */
		agentStatusRef?: { value: string };
		queued?: boolean;
		/** When set, write a valid started.json at this age (ms before now). */
		startedAgeMs?: number;
		/** Age of active.dispatchedAt (ms before now). Defaults to recent (0). */
		dispatchAgeMs?: number;
		/** Non-date dispatchedAt string (fail-closed). */
		malformedDispatchedAt?: boolean;
		/** dispatchedAt this many ms in the future (fail-closed when beyond skew). */
		futureDispatchedAtMs?: number;
		/** Corrupt started marker for fail-closed tests. */
		corruptStarted?: boolean;
		/** Corrupt active pointer for fail-closed tests. */
		corruptActive?: boolean;
	}) {
		const { adoptPoolWorkers } = await import("../src/extensions/parent.js");
		const cacheRoot = tempDir(`momo-ro-adopt-${options.label}-`);
		const cwd = tempDir(`momo-ro-adopt-${options.label}-cwd-`);
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
		const assignmentA = "roactiveaaaaaa01";
		const assignmentB = "roqueuedbbbbbb02";
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
		if (options.corruptActive) {
			writeFileSync(control.active, "{not-json", { mode: 0o600 });
		} else {
			let dispatchedAt: string;
			if (options.malformedDispatchedAt) {
				dispatchedAt = "not-a-date";
			} else if (options.futureDispatchedAtMs !== undefined) {
				dispatchedAt = new Date(Date.now() + options.futureDispatchedAtMs).toISOString();
			} else {
				dispatchedAt = new Date(
					Date.now() - (options.dispatchAgeMs ?? 0),
				).toISOString();
			}
			atomicWriteJson(control.active, {
				version: 1,
				assignmentId: assignmentA,
				generation: 1,
				parentEpoch: "e1",
				dispatchedAt,
			});
		}
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentA);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(pathsA.command, {
			version: 1,
			type: "prompt",
			task: "active-a",
			issuedAt: new Date().toISOString(),
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "e1",
		});
		if (options.corruptStarted) {
			writeFileSync(pathsA.started, "{bad", { mode: 0o600 });
		} else if (options.startedAgeMs !== undefined) {
			atomicWriteJson(pathsA.started, {
				version: 1,
				runId: assignmentA,
				workerId,
				generation: 1,
				parentEpoch: "e1",
				startedAt: new Date(Date.now() - options.startedAgeMs).toISOString(),
			});
		}
		if (options.queued) {
			enqueueAssignment(pool.poolRoot, "scout", {
				assignmentId: assignmentB,
				workerId,
				generation: 1,
				parentEpoch: "e1",
				task: "queued-b",
			});
		}
		const statusRef = options.agentStatusRef ?? { value: options.agentStatus };
		statusRef.value = options.agentStatus;
		const client = new HerdrClient({
			runCommand: async () => ({
				code: 0,
				stdout: JSON.stringify({
					id: "ok",
					result: {
						type: "agent_info",
						agent: {
							agent_status: statusRef.value,
							pane_id: "w1:p1",
							name: "momo_scout",
						},
					},
				}),
				stderr: "",
			}),
		});
		await adoptPoolWorkers(pool, client, identity.poolKey, {
			ui: { notify: () => undefined },
		});
		return {
			pool,
			workerId,
			cwd,
			cacheRoot,
			identity,
			assignmentA,
			assignmentB,
			control,
			pathsA,
			statusRef,
		};
	}

	async function flushMicrotasks(times = 10): Promise<void> {
		for (let i = 0; i < times; i++) await Promise.resolve();
	}

	it("fresh heartbeat + working remains busy", async () => {
		const { pool, assignmentA } = await setupBusyScout({
			label: "working",
			agentStatus: "working",
		});
		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("busy");
		expect(after.activeAssignmentId).toBe(assignmentA);
		expect(selectClosablePoolWorkers([after]).closable).toHaveLength(0);
	});

	it("newly dispatched idle with no started marker retains busy", async () => {
		const { pool, assignmentA } = await setupBusyScout({
			label: "idle-nomarker",
			agentStatus: "idle",
			dispatchAgeMs: 0,
		});
		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("busy");
		expect(after.activeAssignmentId).toBe(assignmentA);
		expect(selectClosablePoolWorkers([after]).closable).toHaveLength(0);
	});

	it("post-grace recheck marks retained recent-dispatch idle/no-start unhealthy", async () => {
		vi.useFakeTimers();
		vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));
		const { ADOPTION_STARTED_GRACE_MS, __clearAdoptionGraceRechecksForTest, __adoptionGraceRecheckCountForTest } =
			await import("../src/extensions/parent.js");
		__clearAdoptionGraceRechecksForTest();
		const { pool, assignmentA } = await setupBusyScout({
			label: "grace-recheck",
			agentStatus: "idle",
			dispatchAgeMs: 0,
		});
		expect(pool.getByRole("scout")?.status).toBe("busy");
		expect(__adoptionGraceRecheckCountForTest()).toBe(1);
		await vi.advanceTimersByTimeAsync(ADOPTION_STARTED_GRACE_MS + 1);
		await vi.runAllTimersAsync();
		await flushMicrotasks();
		expect(__adoptionGraceRecheckCountForTest()).toBe(0);
		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("unhealthy");
		expect(after.activeAssignmentId).toBe(assignmentA);
	});

	it("post-grace recheck no-ops when generation advances", async () => {
		vi.useFakeTimers();
		vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));
		const { ADOPTION_STARTED_GRACE_MS, __clearAdoptionGraceRechecksForTest } = await import(
			"../src/extensions/parent.js"
		);
		__clearAdoptionGraceRechecksForTest();
		const { pool, workerId } = await setupBusyScout({
			label: "grace-gen",
			agentStatus: "idle",
			dispatchAgeMs: 0,
		});
		expect(pool.getByRole("scout")?.status).toBe("busy");
		const prior = pool.getByRole("scout")!;
		pool.upsert({
			workerId,
			generation: 2,
			generationTombstone: 2,
			role: "scout",
			paneId: "w1:p9",
			agentName: "momo_scout_n1",
			...(prior.cwd ? { cwd: prior.cwd } : {}),
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		await vi.advanceTimersByTimeAsync(ADOPTION_STARTED_GRACE_MS + 1);
		await flushMicrotasks();
		const after = pool.getByRole("scout")!;
		expect(after.generation).toBe(2);
		expect(after.status).toBe("idle");
		expect(after.paneId).toBe("w1:p9");
	});

	it("dispatch-grace then started-grace: marker mid-way schedules second; second marks unhealthy", async () => {
		vi.useFakeTimers();
		vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));
		const {
			ADOPTION_STARTED_GRACE_MS,
			__clearAdoptionGraceRechecksForTest,
			__adoptionGraceRecheckCountForTest,
		} = await import("../src/extensions/parent.js");
		__clearAdoptionGraceRechecksForTest();
		const { pool, assignmentA, pathsA, workerId, control } = await setupBusyScout({
			label: "grace-dispatch-then-started",
			agentStatus: "idle",
			dispatchAgeMs: 0,
		});
		expect(pool.getByRole("scout")?.status).toBe("busy");
		expect(__adoptionGraceRecheckCountForTest()).toBe(1);

		await vi.advanceTimersByTimeAsync(Math.floor(ADOPTION_STARTED_GRACE_MS / 2));
		atomicWriteJson(pathsA.started, {
			version: 1,
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			startedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 2,
		});

		await vi.advanceTimersByTimeAsync(Math.floor(ADOPTION_STARTED_GRACE_MS / 2) + 1);
		await flushMicrotasks();
		expect(pool.getByRole("scout")?.status).toBe("busy");
		expect(__adoptionGraceRecheckCountForTest()).toBe(1);

		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 3,
		});
		await vi.advanceTimersByTimeAsync(ADOPTION_STARTED_GRACE_MS + 1);
		await flushMicrotasks();
		expect(__adoptionGraceRecheckCountForTest()).toBe(0);
		expect(pool.getByRole("scout")?.status).toBe("unhealthy");
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe(assignmentA);
	});

	it("started-grace recheck no-ops when Herdr becomes working", async () => {
		vi.useFakeTimers();
		vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));
		const {
			ADOPTION_STARTED_GRACE_MS,
			__clearAdoptionGraceRechecksForTest,
			__adoptionGraceRecheckCountForTest,
		} = await import("../src/extensions/parent.js");
		__clearAdoptionGraceRechecksForTest();
		const statusRef = { value: "idle" };
		const { pool, assignmentA, pathsA, workerId, control } = await setupBusyScout({
			label: "grace-working-noop",
			agentStatus: "idle",
			agentStatusRef: statusRef,
			dispatchAgeMs: 0,
		});
		await vi.advanceTimersByTimeAsync(Math.floor(ADOPTION_STARTED_GRACE_MS / 2));
		atomicWriteJson(pathsA.started, {
			version: 1,
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			startedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 2,
		});
		await vi.advanceTimersByTimeAsync(Math.floor(ADOPTION_STARTED_GRACE_MS / 2) + 1);
		await flushMicrotasks();
		expect(__adoptionGraceRecheckCountForTest()).toBe(1);

		statusRef.value = "working";
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 3,
		});
		await vi.advanceTimersByTimeAsync(ADOPTION_STARTED_GRACE_MS + 1);
		await flushMicrotasks();
		expect(__adoptionGraceRecheckCountForTest()).toBe(0);
		expect(pool.getByRole("scout")?.status).toBe("busy");
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe(assignmentA);
	});

	it("started-grace recheck completes when result appears", async () => {
		vi.useFakeTimers();
		vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));
		const {
			ADOPTION_STARTED_GRACE_MS,
			__clearAdoptionGraceRechecksForTest,
			__adoptionGraceRecheckCountForTest,
		} = await import("../src/extensions/parent.js");
		__clearAdoptionGraceRechecksForTest();
		const { pool, assignmentA, pathsA, workerId, control } = await setupBusyScout({
			label: "grace-result-noop",
			agentStatus: "idle",
			dispatchAgeMs: 0,
		});
		await vi.advanceTimersByTimeAsync(Math.floor(ADOPTION_STARTED_GRACE_MS / 2));
		atomicWriteJson(pathsA.started, {
			version: 1,
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			startedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 2,
		});
		await vi.advanceTimersByTimeAsync(Math.floor(ADOPTION_STARTED_GRACE_MS / 2) + 1);
		await flushMicrotasks();
		expect(__adoptionGraceRecheckCountForTest()).toBe(1);

		atomicWriteJson(pathsA.result, {
			version: 1,
			runId: assignmentA,
			workerId,
			status: "completed",
			messages: [{ role: "assistant", content: [{ type: "text", text: "done" }] }],
			finishedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 3,
		});
		await vi.advanceTimersByTimeAsync(ADOPTION_STARTED_GRACE_MS + 1);
		await flushMicrotasks();
		expect(__adoptionGraceRecheckCountForTest()).toBe(0);
		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("idle");
		expect(after.activeAssignmentId).toBeUndefined();
	});

	it("started-grace recheck no-ops when successor generation replaces snapshot", async () => {
		vi.useFakeTimers();
		vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));
		const {
			ADOPTION_STARTED_GRACE_MS,
			__clearAdoptionGraceRechecksForTest,
			__adoptionGraceRecheckCountForTest,
		} = await import("../src/extensions/parent.js");
		__clearAdoptionGraceRechecksForTest();
		const { pool, assignmentA, pathsA, workerId, control } = await setupBusyScout({
			label: "grace-successor-noop",
			agentStatus: "idle",
			dispatchAgeMs: 0,
		});
		await vi.advanceTimersByTimeAsync(Math.floor(ADOPTION_STARTED_GRACE_MS / 2));
		atomicWriteJson(pathsA.started, {
			version: 1,
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			startedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 2,
		});
		await vi.advanceTimersByTimeAsync(Math.floor(ADOPTION_STARTED_GRACE_MS / 2) + 1);
		await flushMicrotasks();
		expect(__adoptionGraceRecheckCountForTest()).toBe(1);

		const prior = pool.getByRole("scout")!;
		pool.upsert({
			workerId,
			generation: 2,
			generationTombstone: 2,
			role: "scout",
			paneId: "w1:p9",
			agentName: "momo_scout_n1",
			...(prior.cwd ? { cwd: prior.cwd } : {}),
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		await vi.advanceTimersByTimeAsync(ADOPTION_STARTED_GRACE_MS + 1);
		await flushMicrotasks();
		expect(__adoptionGraceRecheckCountForTest()).toBe(0);
		const after = pool.getByRole("scout")!;
		expect(after.generation).toBe(2);
		expect(after.status).toBe("idle");
		expect(after.paneId).toBe("w1:p9");
	});

	it("initial started-grace timer marks unhealthy at expiry while still idle", async () => {
		vi.useFakeTimers();
		vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));
		const {
			ADOPTION_STARTED_GRACE_MS,
			__clearAdoptionGraceRechecksForTest,
			__adoptionGraceRecheckCountForTest,
		} = await import("../src/extensions/parent.js");
		__clearAdoptionGraceRechecksForTest();
		const { pool, assignmentA, workerId, control } = await setupBusyScout({
			label: "grace-started-direct",
			agentStatus: "idle",
			startedAgeMs: Math.floor(ADOPTION_STARTED_GRACE_MS / 2),
		});
		expect(pool.getByRole("scout")?.status).toBe("busy");
		expect(__adoptionGraceRecheckCountForTest()).toBe(1);
		atomicWriteJson(control.heartbeat, {
			version: 1,
			runId: "g1",
			workerId,
			at: new Date().toISOString(),
			seq: 2,
		});
		await vi.advanceTimersByTimeAsync(ADOPTION_STARTED_GRACE_MS + 1);
		await flushMicrotasks();
		expect(__adoptionGraceRecheckCountForTest()).toBe(0);
		expect(pool.getByRole("scout")?.status).toBe("unhealthy");
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe(assignmentA);
	});

	it("old dispatch + idle + no started becomes unhealthy (died before start)", async () => {
		const { ADOPTION_STARTED_GRACE_MS } = await import("../src/extensions/parent.js");
		const { pool, assignmentA } = await setupBusyScout({
			label: "idle-old-dispatch",
			agentStatus: "idle",
			dispatchAgeMs: ADOPTION_STARTED_GRACE_MS + 5_000,
		});
		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("unhealthy");
		expect(after.activeAssignmentId).toBe(assignmentA);
		expect(selectClosablePoolWorkers([after]).closable).toHaveLength(1);
	});

	it("malformed dispatchedAt fails closed to unhealthy", async () => {
		const { pool, assignmentA } = await setupBusyScout({
			label: "bad-dispatch",
			agentStatus: "idle",
			malformedDispatchedAt: true,
		});
		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("unhealthy");
		expect(after.activeAssignmentId).toBe(assignmentA);
	});

	it("implausibly future dispatchedAt fails closed to unhealthy", async () => {
		const { pool, assignmentA } = await setupBusyScout({
			label: "future-dispatch",
			agentStatus: "idle",
			futureDispatchedAtMs: 60_000,
		});
		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("unhealthy");
		expect(after.activeAssignmentId).toBe(assignmentA);
	});

	it("idle with recent started marker retains busy (settlement race)", async () => {
		const { ADOPTION_STARTED_GRACE_MS } = await import("../src/extensions/parent.js");
		const { pool, assignmentA } = await setupBusyScout({
			label: "idle-recent",
			agentStatus: "idle",
			startedAgeMs: Math.floor(ADOPTION_STARTED_GRACE_MS / 2),
		});
		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("busy");
		expect(after.activeAssignmentId).toBe(assignmentA);
	});

	it("idle with started marker older than grace becomes unhealthy", async () => {
		const { ADOPTION_STARTED_GRACE_MS } = await import("../src/extensions/parent.js");
		const { pool, assignmentA } = await setupBusyScout({
			label: "idle-old",
			agentStatus: "idle",
			startedAgeMs: ADOPTION_STARTED_GRACE_MS + 5_000,
		});
		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("unhealthy");
		expect(after.activeAssignmentId).toBe(assignmentA);
		expect(selectClosablePoolWorkers([after]).closable).toHaveLength(1);
	});

	it("done becomes unhealthy with active evidence", async () => {
		const { pool, assignmentA } = await setupBusyScout({
			label: "done",
			agentStatus: "done",
		});
		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("unhealthy");
		expect(after.activeAssignmentId).toBe(assignmentA);
	});

	it("invalid started marker fails closed to unhealthy", async () => {
		const { pool, assignmentA } = await setupBusyScout({
			label: "bad-started",
			agentStatus: "idle",
			corruptStarted: true,
		});
		const after = pool.getByRole("scout")!;
		expect(after.status).toBe("unhealthy");
		expect(after.activeAssignmentId).toBe(assignmentA);
	});

	it("cleanup terminalizes active A + queued B then N+1 can provision", async () => {
		const { ADOPTION_STARTED_GRACE_MS } = await import("../src/extensions/parent.js");
		const fixture = await setupBusyScout({
			label: "cleanup-n1",
			agentStatus: "idle",
			queued: true,
			startedAgeMs: ADOPTION_STARTED_GRACE_MS + 5_000,
		});
		const { pool, workerId, cwd, cacheRoot, identity, assignmentA, assignmentB } = fixture;
		expect(pool.getByRole("scout")?.status).toBe("unhealthy");
		expect(queueCount(pool.poolRoot, "scout")).toBe(1);

		const { installMomoParent } = await import("../src/extensions/parent.js");
		const closed: string[] = [];
		const commands = new Map<string, { handler: (args: string, ctx: unknown) => Promise<void> }>();
		const pi = {
			commands,
			registerTool: vi.fn(),
			setActiveTools: vi.fn(),
			sendUserMessage: vi.fn(),
			sendMessage: vi.fn(),
			registerCommand: vi.fn(
				(name: string, def: { handler: (args: string, ctx: unknown) => Promise<void> }) => {
					commands.set(name, def);
				},
			),
			on() {},
		};
		installMomoParent(pi as unknown as ExtensionAPI, {
			cwd,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "parent-ro-cleanup",
				HERDR_ENV: "1",
				HERDR_PANE_ID: "w1:p0",
				HERDR_WORKSPACE_ID: "ws",
				HERDR_SOCKET_PATH: "s",
			},
			client: new HerdrClient({
				runCommand: async (_file, args) => {
					if (args[0] === "pane" && args[1] === "close") {
						closed.push(String(args[2]));
						return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
					}
					if (args[0] === "agent" && args[1] === "get") {
						return {
							code: 0,
							stdout: JSON.stringify({
								id: "g",
								result: {
									type: "agent_info",
									agent: { agent_status: "idle", name: "momo_scout", pane_id: "w1:p1" },
								},
							}),
							stderr: "",
						};
					}
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				},
			}),
			poolRegistry: pool,
		});
		await commands.get("momo-cleanup")!.handler("", {
			ui: { notify: () => undefined },
		} as never);
		expect(closed).toEqual(["w1:p1"]);
		expect(isArchivalTombstone(pool.getByRole("scout")!)).toBe(true);
		expect(queueCount(pool.poolRoot, "scout")).toBe(0);
		for (const id of [assignmentA, assignmentB]) {
			const paths = assignmentSpoolPaths(pool.poolRoot, "scout", id);
			const result = validateResult(tryReadIpcJson(paths.result), {
				runId: id,
				workerId,
			});
			expect(result.status).toBe("failed");
		}

		// N+1 can provision after archive.
		installFakeHerdrExtension();
		let provisionedGen: string | undefined;
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-ro-n1",
			client: new HerdrClient({
				runCommand: async (_file, args) => {
					if (args[0] === "pane" && args[1] === "split") {
						const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
						provisionedGen = envArgs
							.find((v) => v.startsWith("MOMO_WORKER_GENERATION="))
							?.slice("MOMO_WORKER_GENERATION=".length);
						const controlDir =
							envArgs.find((v) => v.startsWith("MOMO_CONTROL_DIR="))?.slice(17) ??
							envArgs.find((v) => v.startsWith("MOMO_IPC_DIR="))?.slice(12) ??
							"";
						const wid = envArgs.find((v) => v.startsWith("MOMO_WORKER_ID="))?.slice(15) ?? "";
						const runId = envArgs.find((v) => v.startsWith("MOMO_RUN_ID="))?.slice(12) ?? "";
						atomicWriteJson(path.join(controlDir, "ready.json"), {
							version: 1,
							runId,
							workerId: wid,
							readyAt: new Date().toISOString(),
						});
						atomicWriteJson(path.join(controlDir, "heartbeat.json"), {
							version: 1,
							runId,
							workerId: wid,
							at: new Date().toISOString(),
							seq: 1,
						});
						return {
							code: 0,
							stdout: JSON.stringify({ id: "s", result: { pane: { pane_id: "w1:p2" } } }),
							stderr: "",
						};
					}
					if (args[0] === "agent" && args[1] === "start") {
						return {
							code: 0,
							stdout: JSON.stringify({
								id: "s",
								result: {
									pane_id: "w1:p2",
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
			}),
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 2_000,
			pollIntervalMs: 20,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});
		const session = await factory({ cwd, role: getRole("scout") });
		const proxy = session as unknown as {
			assignmentId: string;
			workerId: string;
			paths: { result: string };
		};
		const wait = session.prompt("n1").then(() => session.agent!.waitForIdle());
		await new Promise((r) => setTimeout(r, 80));
		atomicWriteJson(proxy.paths.result, {
			version: 1,
			runId: proxy.assignmentId,
			workerId: proxy.workerId,
			status: "completed",
			messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }] }],
			finishedAt: new Date().toISOString(),
		});
		await wait;
		expect(Number(provisionedGen)).toBeGreaterThanOrEqual(2);
		expect(pool.getByRole("scout")?.generation).toBeGreaterThanOrEqual(2);
		void identity;
	});
});

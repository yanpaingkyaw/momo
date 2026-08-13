import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readFileSync,
	renameSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
	installMomoWorker,
	__setWriteResultDurableLockHookForTest,
	__resetWriteResultDurableLockHookForTest,
	__setResultAtomicWriteForTest,
	__resetResultAtomicWriteForTest,
	__setMaxClaimRetryAttemptsForTest,
	__resetMaxClaimRetryAttemptsForTest,
	__setClaimRetryRecoveryHookForTest,
	__resetClaimRetryRecoveryHookForTest,
	__setControlHeartbeatHooksForTest,
	__resetControlHeartbeatHooksForTest,
} from "../src/extensions/worker-runtime.js";
import { installMomoParent, __setOrphanAwaitingLifecycleLockHookForTest, __resetCleanupRoleLockEnteredHookForTest } from "../src/extensions/parent.js";
import { WriterLeaseManager } from "../src/lease/writer-lease.js";
import { dispatchAssignment, setupPoolWorkerFixture } from "./helpers/pool-fixture.js";
import {
	__setActiveAtomicWriteForTest,
	__resetActiveAtomicWriteForTest,
	publishAssignmentCommandLocked,
} from "../src/herdr/dispatch-publish.js";
import {
	PaneLifecycleLock,
	paneLifecycleLockDir,
	withPaneLifecycleLock,
	withPaneLifecycleLockAsync,
	__setPaneLifecycleLockHooksForTest,
} from "../src/herdr/pane-lifecycle-lock.js";
import { LOCK_HEARTBEAT_MS, QueueLockError, readLockOwnerForTest } from "../src/herdr/role-queue.js";
import { PoolRegistry, isArchivalTombstone } from "../src/herdr/pool-registry.js";
import { resolvePoolIdentity, stableWorkerId, herdrAgentNameForWorker } from "../src/herdr/pool-identity.js";
import { workerControlPaths, assignmentSpoolPaths } from "../src/herdr/assignment-spool.js";
import {
	beginClaimHead,
	commitClaim,
	enqueueAssignment,
	listClaiming,
	listQueue,
	queueCount,
	recoverClaiming,
	withRoleLock,
} from "../src/herdr/role-queue.js";
import {
	roleHasOrphanPaneEvidence,
	writeOrphanPaneEvidence,
} from "../src/herdr/orphan-panes.js";
import { atomicWriteJson } from "../src/ipc/spool.js";
import { tryReadIpcJson, validateResult } from "../src/ipc/validate.js";
import { createTestHerdrClient } from "./helpers/herdr-mock-client.js";
import { createHerdrChildSessionFactory } from "../src/delegation/herdr-factory.js";
import { getRole } from "../src/roles.js";

const tempDirs: string[] = [];
const previousPiDir = process.env.PI_CODING_AGENT_DIR;

afterEach(() => {
	__resetWriteResultDurableLockHookForTest();
	__resetResultAtomicWriteForTest();
	__resetActiveAtomicWriteForTest();
	__resetMaxClaimRetryAttemptsForTest();
	__resetClaimRetryRecoveryHookForTest();
	__resetControlHeartbeatHooksForTest();
	__setPaneLifecycleLockHooksForTest();
	__resetCleanupRoleLockEnteredHookForTest();
	if (previousPiDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
	else process.env.PI_CODING_AGENT_DIR = previousPiDir;
	while (tempDirs.length > 0) {
		const dir = tempDirs.pop();
		if (dir) rmSync(dir, { recursive: true, force: true });
	}
	vi.restoreAllMocks();
});

function tempDir(prefix: string): string {
	const dir = mkdtempSync(path.join(tmpdir(), prefix));
	tempDirs.push(dir);
	return dir;
}

function createFakePi() {
	const handlers = new Map<string, Function[]>();
	const commands = new Map<string, { handler: (args: string, ctx: unknown) => Promise<void> }>();
	return {
		handlers,
		commands,
		on(event: string, handler: Function) {
			const list = handlers.get(event) ?? [];
			list.push(handler);
			handlers.set(event, list);
		},
		registerCommand(
			name: string,
			def: { handler: (args: string, ctx: unknown) => Promise<void> },
		) {
			commands.set(name, def);
		},
		setActiveTools: vi.fn(),
		registerTool: vi.fn(),
		sendUserMessage: vi.fn(),
		async emit(event: string, payload: unknown = {}, ctx?: ExtensionContext) {
			for (const handler of handlers.get(event) ?? []) {
				await handler(payload, ctx);
			}
		},
	};
}

function installFakeHerdrExtension(): void {
	const agentDir = tempDir("momo-pi-agent-");
	mkdirSync(path.join(agentDir, "extensions"), { recursive: true });
	writeFileSync(path.join(agentDir, "extensions", "herdr-agent-state.ts"), "// fake\n", "utf8");
	process.env.PI_CODING_AGENT_DIR = agentDir;
}

describe("P1 worker result/lease ordering", () => {
	it("keeps exact lease held until result.json is durable", async () => {
		vi.useFakeTimers();
		const cwd = tempDir("momo-lease-order-cwd-");
		const cacheRoot = tempDir("momo-lease-order-cache-");
		const lease = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });
		const fixture = setupPoolWorkerFixture({
			cacheRoot,
			cwd,
			role: "implementer",
			assignmentId: "leaseorder01",
		});
		let leaseHeldAtWrite = false;
		const originalRelease = WriterLeaseManager.prototype.release;
		const releaseSpy = vi
			.spyOn(WriterLeaseManager.prototype, "release")
			.mockImplementation(function (
				this: WriterLeaseManager,
				cwdArg: string,
				ownerId: string,
				token: string,
			) {
				expect(existsSync(fixture.paths.result)).toBe(true);
				validateResult(tryReadIpcJson(fixture.paths.result), {
					runId: fixture.assignmentId,
					workerId: fixture.workerId,
				});
				return originalRelease.call(this, cwdArg, ownerId, token);
			});
		__setWriteResultDurableLockHookForTest((phase) => {
			if (phase === "before-write") {
				leaseHeldAtWrite = lease.isLocked(cwd);
			}
		});
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: fixture.env,
			poolRegistry: fixture.pool,
			leaseManager: lease,
			now: () => 1_000,
		});
		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);
		dispatchAssignment({
			pool: fixture.pool,
			role: fixture.role,
			controlRoot: fixture.control.root,
			paths: fixture.paths,
			assignmentId: fixture.assignmentId,
			workerId: fixture.workerId,
			generation: fixture.generation,
			task: "edit",
		});
		await vi.advanceTimersByTimeAsync(150);
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
		expect(leaseHeldAtWrite).toBe(true);
		expect(releaseSpy).toHaveBeenCalled();
		expect(lease.isLocked(cwd)).toBe(false);
		expect(existsSync(fixture.paths.result)).toBe(true);
		releaseSpy.mockRestore();
		vi.useRealTimers();
	});

	it("returns false with lease still held when all result writes fail", async () => {
		vi.useFakeTimers();
		const cwd = tempDir("momo-result-fail-cwd-");
		const cacheRoot = tempDir("momo-result-fail-cache-");
		const lease = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });
		const fixture = setupPoolWorkerFixture({
			cacheRoot,
			cwd,
			role: "implementer",
			assignmentId: "resultfail01",
		});
		__setResultAtomicWriteForTest(() => {
			throw new Error("injected result write failure");
		});
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: fixture.env,
			poolRegistry: fixture.pool,
			leaseManager: lease,
			now: () => 1_000,
		});
		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);
		dispatchAssignment({
			pool: fixture.pool,
			role: fixture.role,
			controlRoot: fixture.control.root,
			paths: fixture.paths,
			assignmentId: fixture.assignmentId,
			workerId: fixture.workerId,
			generation: fixture.generation,
			task: "edit",
		});
		await vi.advanceTimersByTimeAsync(150);
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
		expect(existsSync(fixture.paths.result)).toBe(false);
		expect(lease.isLocked(cwd)).toBe(true);
		expect(fixture.pool.getByRole("implementer")?.status).toBe("uncertain");
		vi.useRealTimers();
	});
});

describe("P1 dispatch active pointer rollback", () => {
	it("rolls back command and busy registry when active write fails", () => {
		const cacheRoot = tempDir("momo-active-rollback-");
		const cwd = tempDir("momo-active-rollback-cwd-");
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
			paneId: "w1:p1",
			agentName: "momo_scout",
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		const assignmentId = "activefail000001";
		const control = workerControlPaths(pool.poolRoot, "scout");
		const rollbackRecord = pool.getByRole("scout")!;
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		__setActiveAtomicWriteForTest(() => {
			throw new Error("injected active write failure");
		});
		const outcome = withRoleLock(pool.poolRoot, "scout", () =>
			publishAssignmentCommandLocked({
				pool,
				poolRoot: pool.poolRoot,
				role: "scout",
				workerId,
				generation: 1,
				assignmentId,
				parentEpoch: "e1",
				task: "t",
				now: () => 1_700_000_000_000,
				rollbackRecord,
			}),
		);
		expect(outcome.ok).toBe(false);
		if (!outcome.ok) expect(outcome.reason).toBe("rolled_back");
		expect(existsSync(control.active)).toBe(false);
		expect(existsSync(assignmentSpoolPaths(pool.poolRoot, "scout", assignmentId).command)).toBe(
			false,
		);
		expect(pool.getByRole("scout")?.status).toBe("idle");
		expect(pool.getByRole("scout")?.activeAssignmentId).toBeUndefined();
	});

	it("worker publishClaim recovers FIFO after active failure without orphan busy", () => {
		const cacheRoot = tempDir("momo-claim-active-");
		const cwd = tempDir("momo-claim-active-cwd-");
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
			task: "retry-me",
		});
		const control = workerControlPaths(pool.poolRoot, "planner");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const claimed = withRoleLock(pool.poolRoot, "planner", () =>
			beginClaimHead(pool.poolRoot, "planner", 1),
		);
		expect(claimed?.assignmentId).toBe(entry.assignmentId);
		const rollbackRecord = pool.getByRole("planner")!;
		const publish = () =>
			publishAssignmentCommandLocked({
				pool,
				poolRoot: pool.poolRoot,
				role: "planner",
				workerId,
				generation: 1,
				assignmentId: entry.assignmentId,
				parentEpoch: "e1",
				task: entry.task,
				now: () => 1_700_000_000_000,
				rollbackRecord,
				onCommitted: () => commitClaim(pool.poolRoot, "planner", entry),
			});
		__setActiveAtomicWriteForTest(() => {
			throw new Error("injected active write failure");
		});
		const failed = withRoleLock(pool.poolRoot, "planner", publish);
		expect(failed.ok).toBe(false);
		expect(existsSync(control.active)).toBe(false);
		expect(pool.getByRole("planner")?.status).toBe("idle");
		const afterFail = withRoleLock(pool.poolRoot, "planner", () =>
			recoverClaiming(pool.poolRoot, "planner", 1),
		);
		expect(afterFail[0]?.assignmentId).toBe(entry.assignmentId);
		expect(listClaiming(pool.poolRoot, "planner")).toHaveLength(1);
		__resetActiveAtomicWriteForTest();
		const ok = withRoleLock(pool.poolRoot, "planner", publish);
		expect(ok.ok).toBe(true);
		expect(existsSync(control.active)).toBe(true);
		expect(pool.getByRole("planner")?.status).toBe("busy");
		expect(listClaiming(pool.poolRoot, "planner")).toHaveLength(0);
		const afterSuccess = withRoleLock(pool.poolRoot, "planner", () =>
			recoverClaiming(pool.poolRoot, "planner", 1),
		);
		expect(afterSuccess).toHaveLength(0);
	});
});

describe("P1 pane lifecycle global lock", () => {
	it("releases pane lifecycle lock after callback", async () => {
		const cacheRoot = tempDir("momo-pane-lock-");
		const cwd = tempDir("momo-pane-lock-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const lockDir = paneLifecycleLockDir(pool.poolRoot);
		await withPaneLifecycleLockAsync(pool.poolRoot, async () => {
			expect(existsSync(lockDir)).toBe(true);
			expect(existsSync(path.join(lockDir, "owner.json"))).toBe(true);
		});
		expect(existsSync(lockDir)).toBe(false);
	});

	it("blocks concurrent acquirers until release", async () => {
		const cacheRoot = tempDir("momo-pane-lock-contention-");
		const cwd = tempDir("momo-pane-lock-contention-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		let secondEntered = false;
		const first = withPaneLifecycleLockAsync(pool.poolRoot, async () => {
			await new Promise((r) => setTimeout(r, 80));
		});
		await new Promise((r) => setTimeout(r, 10));
		const second = withPaneLifecycleLockAsync(pool.poolRoot, async () => {
			secondEntered = true;
		});
		await new Promise((r) => setTimeout(r, 20));
		expect(secondEntered).toBe(false);
		await first;
		await second;
		expect(secondEntered).toBe(true);
	});

	it("holds pane lifecycle lock across split until persist completes", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-pane-split-lock-");
		const cwd = tempDir("momo-pane-split-lock-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		let releaseSplitGate!: () => void;
		const splitGate = new Promise<void>((resolve) => {
			releaseSplitGate = resolve;
		});
		let lockHeldDuringSplit = false;
		const client = createTestHerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					lockHeldDuringSplit = existsSync(paneLifecycleLockDir(pool.poolRoot));
					await splitGate;
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: { pane: { pane_id: "w1:p-split-lock" } },
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
			parentId: "parent-split-lock",
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
		const prompt = factory({ cwd, role: getRole("scout") }).then((s) => s.prompt("go"));
		for (let i = 0; i < 50 && !lockHeldDuringSplit; i += 1) {
			await new Promise((r) => setTimeout(r, 20));
		}
		expect(lockHeldDuringSplit).toBe(true);
		expect(existsSync(paneLifecycleLockDir(pool.poolRoot))).toBe(true);
		releaseSplitGate();
		await prompt.catch(() => undefined);
		expect(existsSync(paneLifecycleLockDir(pool.poolRoot))).toBe(false);
	}, 15_000);

	it("starting claim active-write fault preserves starting ownership and boundPolicy", () => {
		const cacheRoot = tempDir("momo-start-active-");
		const cwd = tempDir("momo-start-active-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "planner");
		const boundPolicy = {
			provider: "openai" as const,
			model: "gpt-4o",
			reasoning: "off" as const,
		};
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "planner",
			agentName: "momo_planner",
			status: "starting",
			provisioningOwnerId: "prov-owner-1",
			provisioningHeartbeatAt: new Date().toISOString(),
			boundPolicy,
			cwd,
			updatedAt: new Date().toISOString(),
		});
		const entry = enqueueAssignment(pool.poolRoot, "planner", {
			assignmentId: "startactive00001",
			workerId,
			generation: 1,
			parentEpoch: "e1",
			task: "queued-while-starting",
		});
		const control = workerControlPaths(pool.poolRoot, "planner");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		withRoleLock(pool.poolRoot, "planner", () => beginClaimHead(pool.poolRoot, "planner", 1));
		const rollbackRecord = pool.getByRole("planner")!;
		__setActiveAtomicWriteForTest(() => {
			throw new Error("injected starting active write failure");
		});
		const outcome = withRoleLock(pool.poolRoot, "planner", () =>
			publishAssignmentCommandLocked({
				pool,
				poolRoot: pool.poolRoot,
				role: "planner",
				workerId,
				generation: 1,
				assignmentId: entry.assignmentId,
				parentEpoch: "e1",
				task: entry.task,
				now: () => 1_700_000_000_000,
				rollbackRecord,
				onCommitted: () => commitClaim(pool.poolRoot, "planner", entry),
			}),
		);
		expect(outcome.ok).toBe(false);
		if (!outcome.ok) expect(outcome.reason).toBe("rolled_back");
		const after = pool.getByRole("planner")!;
		expect(after.status).toBe("starting");
		expect(after.provisioningOwnerId).toBe("prov-owner-1");
		expect(after.boundPolicy).toEqual(boundPolicy);
		expect(after.activeAssignmentId).toBeUndefined();
		expect(listClaiming(pool.poolRoot, "planner")).toHaveLength(1);
	});

	it("orphan cleanup blocked during split/persist never closePane live new pane", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-pane-race-");
		const cwd = tempDir("momo-pane-race-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const agentName = herdrAgentNameForWorker(workerId);
		const racePaneId = "w1:p-race-new";
		const closedDuringCleanup: string[] = [];
		const notifies: string[] = [];
		let cleanupActive = false;
		let releaseSplitGate!: () => void;
		const splitGate = new Promise<void>((resolve) => {
			releaseSplitGate = resolve;
		});
		let splitEntered = false;
		let resolveOrphanAwaitingLock!: () => void;
		const orphanAwaitingLock = new Promise<void>((resolve) => {
			resolveOrphanAwaitingLock = resolve;
		});
		__setOrphanAwaitingLifecycleLockHookForTest(() => {
			resolveOrphanAwaitingLock();
		});
		const client = createTestHerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					splitEntered = true;
					writeOrphanPaneEvidence(pool.poolRoot, "scout", {
						generation: 1,
						workerId,
						paneId: racePaneId,
						agentName: "momo_scout",
						reason: "superseded_close_failed",
						createdAt: new Date().toISOString(),
					});
					await splitGate;
					const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
					const control =
						envArgs
							.find((v) => v.startsWith("MOMO_CONTROL_DIR="))
							?.slice("MOMO_CONTROL_DIR=".length) ??
						envArgs.find((v) => v.startsWith("MOMO_IPC_DIR="))?.slice("MOMO_IPC_DIR=".length);
					const wid =
						envArgs
							.find((v) => v.startsWith("MOMO_WORKER_ID="))
							?.slice("MOMO_WORKER_ID=".length) ?? workerId;
					const runId =
						envArgs.find((v) => v.startsWith("MOMO_RUN_ID="))?.slice("MOMO_RUN_ID=".length) ??
						"g1";
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
							result: { pane: { pane_id: racePaneId } },
						}),
						stderr: "",
					};
				}
				if (args[0] === "pane" && args[1] === "close") {
					if (cleanupActive) closedDuringCleanup.push(String(args[2]));
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				if (args[0] === "agent" && args[1] === "start") {
					expect(args[2]).toBe(agentName);
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: {
								pane_id: racePaneId,
								name: agentName,
								agent: "pi",
								interactive_ready: true,
								agent_status: "idle",
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
			parentId: "parent-pane-race",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			readyTimeoutMs: 5_000,
			sleep: async (ms) => {
				await new Promise((r) => setTimeout(r, Math.min(ms, 10)));
			},
		});
		const pi = createFakePi();
		installMomoParent(pi as unknown as ExtensionAPI, {
			cwd,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "parent-pane-race",
				HERDR_ENV: "1",
				HERDR_PANE_ID: "w1:p0",
				HERDR_WORKSPACE_ID: "ws",
				HERDR_SOCKET_PATH: "s",
			},
			client,
			poolRegistry: pool,
		});
		const provision = factory({ cwd, role: getRole("scout") }).then((s) => s.prompt("race"));
		for (let i = 0; i < 80 && !splitEntered; i += 1) {
			await new Promise((r) => setTimeout(r, 25));
		}
		expect(splitEntered).toBe(true);
		expect(existsSync(paneLifecycleLockDir(pool.poolRoot))).toBe(true);
		expect(roleHasOrphanPaneEvidence(pool.poolRoot, "scout")).toBe(true);
		const cleanupDone = (async () => {
			cleanupActive = true;
			try {
				const workersAtStart = pool.list();
				const scoutAtStart = workersAtStart.find((w) => w.role === "scout");
				expect(scoutAtStart?.status).toBe("starting");
				expect(scoutAtStart?.paneId).toBeUndefined();
				await pi.commands.get("momo-cleanup")!.handler("", {
					ui: { notify: (m: string) => notifies.push(m) },
				} as never);
			} finally {
				cleanupActive = false;
			}
		})();
		await Promise.race([
			orphanAwaitingLock,
			new Promise((_, reject) =>
				setTimeout(() => reject(new Error("cleanup never reached lifecycle lock wait")), 5_000),
			),
		]);
		expect(existsSync(paneLifecycleLockDir(pool.poolRoot))).toBe(true);
		expect(pool.getByRole("scout")?.paneId).toBeUndefined();
		releaseSplitGate();
		await provision;
		await cleanupDone;
		expect(pool.getByRole("scout")?.paneId).toBe(racePaneId);
		expect(notifies.join(" ")).toMatch(/collides with live registry pane/i);
		expect(closedDuringCleanup).not.toContain(racePaneId);
		expect(roleHasOrphanPaneEvidence(pool.poolRoot, "scout")).toBe(true);
	}, 20_000);

	it("token-safe release quarantines by dev/ino and owner token", async () => {
		const cacheRoot = tempDir("momo-pane-lock-release-");
		const cwd = tempDir("momo-pane-lock-release-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const lockDir = paneLifecycleLockDir(pool.poolRoot);
		await withPaneLifecycleLockAsync(pool.poolRoot, async () => {
			expect(existsSync(lockDir)).toBe(true);
		});
		expect(existsSync(lockDir)).toBe(false);
	});

	it("lock release preserves callback error when release also fails", () => {
		const cacheRoot = tempDir("momo-pane-lock-agg-");
		const cwd = tempDir("momo-pane-lock-agg-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const lockDir = paneLifecycleLockDir(pool.poolRoot);
		expect(() =>
			withPaneLifecycleLock(
				pool.poolRoot,
				() => {
					const owner = JSON.parse(readFileSync(path.join(lockDir, "owner.json"), "utf8")) as {
						token: string;
					};
					mkdirSync(`${lockDir}.released.${owner.token}`, { mode: 0o700 });
					throw new QueueLockError("mutation failed");
				},
				{ timeoutMs: 1_000 },
			),
		).toThrow(AggregateError);
	});

	it("releaseStrict with wrong token leaves live lock untouched", () => {
		const cacheRoot = tempDir("momo-pane-lock-wrong-token-");
		const cwd = tempDir("momo-pane-lock-wrong-token-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const lockDir = paneLifecycleLockDir(pool.poolRoot);
		const lock = new PaneLifecycleLock(pool.poolRoot);
		lock.acquireSync(1_000);
		const owner = JSON.parse(readFileSync(path.join(lockDir, "owner.json"), "utf8")) as {
			token: string;
			version: number;
			pid: number;
			at: string;
			heartbeatAt: string;
		};
		writeFileSync(
			path.join(lockDir, "owner.json"),
			`${JSON.stringify({ ...owner, token: "wrong-token" })}\n`,
		);
		expect(() => lock.releaseStrict()).toThrow(/token mismatch/);
		expect(existsSync(lockDir)).toBe(true);
		expect(lock.getIdentityForTest()).toBeUndefined();
		expect(lock.isHeartbeatActiveForTest()).toBe(false);
	});

	it("releaseStrict with replaced inode leaves successor and retained evidence untouched", () => {
		const cacheRoot = tempDir("momo-pane-lock-inode-");
		const cwd = tempDir("momo-pane-lock-inode-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const lockDir = paneLifecycleLockDir(pool.poolRoot);
		const lock = new PaneLifecycleLock(pool.poolRoot);
		lock.acquireSync(1_000);
		const token = lock.getTokenForTest()!;
		lock.stopHeartbeatForTest();
		const preserved = `${lockDir}.preserved.${token}`;
		const preservedOwner = readLockOwnerForTest(lockDir);
		expect(preservedOwner?.token).toBe(token);
		renameSync(lockDir, preserved);
		mkdirSync(lockDir, { mode: 0o700 });
		writeFileSync(
			path.join(lockDir, "owner.json"),
			`${JSON.stringify(preservedOwner!)}\n`,
			{ mode: 0o600 },
		);
		expect(readLockOwnerForTest(lockDir)?.token).toBe(token);
		expect(() => lock.releaseStrict()).toThrow(/identity mismatch/);
		expect(existsSync(lockDir)).toBe(true);
		expect(existsSync(preserved)).toBe(true);
		const successor = JSON.parse(readFileSync(path.join(lockDir, "owner.json"), "utf8")) as {
			token: string;
		};
		expect(successor.token).toBe(token);
	});

	it("identity capture failure retains lock dir without recursive delete", () => {
		const cacheRoot = tempDir("momo-pane-lock-id-fail-");
		const cwd = tempDir("momo-pane-lock-id-fail-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const lockDir = paneLifecycleLockDir(pool.poolRoot);
		__setPaneLifecycleLockHooksForTest({
			captureLockDirIdentity: () => {
				throw new Error("identity capture failed");
			},
		});
		const lock = new PaneLifecycleLock(pool.poolRoot);
		expect(() => lock.acquireSync(500)).toThrow(/identity capture failed/);
		expect(existsSync(lockDir)).toBe(true);
		expect(existsSync(path.join(lockDir, "owner.json"))).toBe(false);
	});

	it("releaseStrict stops heartbeat timer so process can exit", () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-pane-lock-timer-");
		const cwd = tempDir("momo-pane-lock-timer-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const lockDir = paneLifecycleLockDir(pool.poolRoot);
		const ownerPath = path.join(lockDir, "owner.json");
		let nowMs = 1_700_000_000_000;
		const lock = new PaneLifecycleLock(pool.poolRoot, { now: () => nowMs });
		lock.acquireSync(1_000);
		expect(lock.isHeartbeatActiveForTest()).toBe(true);
		const before = JSON.parse(readFileSync(ownerPath, "utf8")).heartbeatAt as string;
		nowMs += LOCK_HEARTBEAT_MS + 1;
		vi.advanceTimersByTime(LOCK_HEARTBEAT_MS + 1);
		const during = JSON.parse(readFileSync(ownerPath, "utf8")).heartbeatAt as string;
		expect(during).not.toBe(before);
		lock.releaseStrict();
		expect(lock.isHeartbeatActiveForTest()).toBe(false);
		expect(lock.getIdentityForTest()).toBeUndefined();
		expect(existsSync(lockDir)).toBe(false);
		vi.advanceTimersByTime(LOCK_HEARTBEAT_MS * 5);
		vi.useRealTimers();
	});
});

describe("P1 FIFO retry after finish", () => {
	it("automatically publishes B after active rollback on A finish without manual retry", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-fifo-retry-");
		const cwd = tempDir("momo-fifo-retry-cwd-");
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
		const assignmentA = "fifoaaaa00000001";
		const assignmentB = "fifobbbb00000002";
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentA);
		const pathsB = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentB);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		mkdirSync(pathsB.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p2",
			agentName: "momo_scout",
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		let activeWriteAttempts = 0;
		__setActiveAtomicWriteForTest((targetPath, payload, maxBytes) => {
			activeWriteAttempts += 1;
			if (activeWriteAttempts === 1) {
				throw new Error("injected active write failure on B publish");
			}
			atomicWriteJson(targetPath, payload, maxBytes);
		});
		const pi = createFakePi();
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
		atomicWriteJson(pathsA.command, {
			version: 1,
			type: "prompt",
			task: "first",
			issuedAt: new Date().toISOString(),
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: assignmentA,
			generation: 1,
			parentEpoch: "epoch1",
			dispatchedAt: new Date().toISOString(),
		});
		pool.upsert({
			...pool.getByRole("scout")!,
			status: "busy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "epoch1",
			updatedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
			task: "successor",
		});
		await vi.advanceTimersByTimeAsync(150);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("first");
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
		expect(existsSync(pathsA.result)).toBe(true);
		expect(pool.getByRole("scout")?.status).toBe("idle");
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(1);
		expect(existsSync(control.active)).toBe(false);
		await vi.advanceTimersByTimeAsync(100);
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe(assignmentB);
		expect(pool.getByRole("scout")?.status).toBe("busy");
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(0);
		expect(queueCount(pool.poolRoot, "scout")).toBe(0);
		await vi.advanceTimersByTimeAsync(150);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("successor");
		void pathsB;
		vi.useRealTimers();
	});
});

describe("P1 idle direct dispatch FIFO", () => {
	it("enqueues C behind B in claiming instead of overtaking on idle scout", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-idle-fifo-enqueue-");
		const cwd = tempDir("momo-idle-fifo-enqueue-cwd-");
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
		const assignmentB = "idlebbbb00000001";
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
			task: "task-b",
		});
		withRoleLock(pool.poolRoot, "scout", () => {
			beginClaimHead(pool.poolRoot, "scout", 1);
		});
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(1);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-idle-fifo",
			client: createTestHerdrClient({
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
		const assignmentC = (session as unknown as { runId: string }).runId;
		await session.prompt("task-c");
		expect(pool.getByRole("scout")?.status).toBe("idle");
		expect(existsSync(control.active)).toBe(false);
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(1);
		expect(listClaiming(pool.poolRoot, "scout")[0]?.assignmentId).toBe(assignmentB);
		expect(listQueue(pool.poolRoot, "scout")).toHaveLength(1);
		expect(listQueue(pool.poolRoot, "scout")[0]?.assignmentId).toBe(assignmentC);
		await session.dispose();
	});

	it("publishes B before C when C arrives during claim retry backoff", async () => {
		vi.useFakeTimers();
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-idle-fifo-order-");
		const cwd = tempDir("momo-idle-fifo-order-cwd-");
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
		const assignmentA = "ordraaaa00000001";
		const assignmentB = "ordrbbbb00000002";
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentA);
		const pathsB = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentB);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		mkdirSync(pathsB.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p2",
			agentName: "momo_scout",
			cwd: identity.canonicalRoot,
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		let nowMs = 1_700_000_000_000;
		const now = () => nowMs;
		let activeWriteAttempts = 0;
		__setActiveAtomicWriteForTest((targetPath, payload, maxBytes) => {
			activeWriteAttempts += 1;
			if (activeWriteAttempts === 1) {
				throw new Error("injected active write failure on B publish");
			}
			atomicWriteJson(targetPath, payload, maxBytes);
		});
		const pi = createFakePi();
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
			now,
			sleep: async () => {},
		});
		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);
		atomicWriteJson(pathsA.command, {
			version: 1,
			type: "prompt",
			task: "first",
			issuedAt: new Date().toISOString(),
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: assignmentA,
			generation: 1,
			parentEpoch: "epoch1",
			dispatchedAt: new Date().toISOString(),
		});
		pool.upsert({
			...pool.getByRole("scout")!,
			status: "busy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "epoch1",
			updatedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
			task: "successor-b",
		});
		nowMs += 150;
		await vi.advanceTimersByTimeAsync(150);
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
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(1);
		expect(listClaiming(pool.poolRoot, "scout")[0]?.assignmentId).toBe(assignmentB);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-idle-order",
			client: createTestHerdrClient({
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
			parentEpoch: "epoch1",
			now,
			sleep: async () => {},
		});
		const sessionC = await factory({ cwd, role: getRole("scout") });
		const assignmentC = (sessionC as unknown as { runId: string }).runId;
		await sessionC.prompt("successor-c");
		expect(pool.getByRole("scout")?.status).toBe("idle");
		expect(listClaiming(pool.poolRoot, "scout")[0]?.assignmentId).toBe(assignmentB);
		expect(listQueue(pool.poolRoot, "scout")[0]?.assignmentId).toBe(assignmentC);
		nowMs += 200;
		await vi.advanceTimersByTimeAsync(200);
		await Promise.resolve();
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe(assignmentB);
		expect(pool.getByRole("scout")?.status).toBe("busy");
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(0);
		expect(listQueue(pool.poolRoot, "scout")[0]?.assignmentId).toBe(assignmentC);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("successor-b");
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
		nowMs += 150;
		await vi.advanceTimersByTimeAsync(150);
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe(assignmentC);
		expect(pool.getByRole("scout")?.status).toBe("busy");
		expect(listQueue(pool.poolRoot, "scout")).toHaveLength(0);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("successor-c");
		await sessionC.dispose();
		vi.useRealTimers();
	});
});

describe("P1 claim retry exhaustion", () => {
	it("fences implementer unhealthy (not uncertain) with claiming retained after retry exhaustion", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-claim-exhaust-");
		const cwd = tempDir("momo-claim-exhaust-cwd-");
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
		const assignmentA = "exhaaaaa00000001";
		const assignmentB = "exhabbbb00000002";
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentA);
		const pathsB = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentB);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p2",
			agentName: "momo_implementer",
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		__setMaxClaimRetryAttemptsForTest(2);
		__setActiveAtomicWriteForTest(() => {
			throw new Error("injected active write failure on claim publish");
		});
		const pi = createFakePi();
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
		atomicWriteJson(pathsA.command, {
			version: 1,
			type: "prompt",
			task: "first",
			issuedAt: new Date().toISOString(),
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: assignmentA,
			generation: 1,
			parentEpoch: "epoch1",
			dispatchedAt: new Date().toISOString(),
		});
		pool.upsert({
			...pool.getByRole("implementer")!,
			status: "busy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "epoch1",
			updatedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "implementer", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
			task: "blocked-b",
		});
		await vi.advanceTimersByTimeAsync(150);
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
		expect(listClaiming(pool.poolRoot, "implementer")).toHaveLength(1);
		await vi.advanceTimersByTimeAsync(500);
		const record = pool.getByRole("implementer");
		expect(record?.status).toBe("unhealthy");
		expect(record?.uncertainWrite).toBeUndefined();
		expect(record?.activeAssignmentId).toBe(assignmentB);
		expect(listClaiming(pool.poolRoot, "implementer")).toHaveLength(1);
		expect(listClaiming(pool.poolRoot, "implementer")[0]?.assignmentId).toBe(assignmentB);
		expect(existsSync(control.active)).toBe(false);
		expect(existsSync(pathsB.started)).toBe(false);
		const lease = new WriterLeaseManager();
		expect(lease.peekOwner(cwd)).toBeUndefined();
		vi.useRealTimers();
	});

	it("allows normal /momo-cleanup after pre-execution claim retry exhaustion (no lease)", async () => {
		installFakeHerdrExtension();
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-claim-exhaust-clean-");
		const cwd = tempDir("momo-claim-exhaust-clean-cwd-");
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
		const assignmentA = "exhclean00000001";
		const assignmentB = "exhclean00000002";
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentA);
		const pathsB = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentB);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p2",
			agentName: "momo_implementer",
			cwd: identity.canonicalRoot,
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		__setMaxClaimRetryAttemptsForTest(2);
		__setActiveAtomicWriteForTest(() => {
			throw new Error("injected active write failure on claim publish");
		});
		const workerPi = createFakePi();
		installMomoWorker(workerPi as unknown as ExtensionAPI, {
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
			now: () => 1_700_000_000_000,
			sleep: async () => {},
		});
		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;
		await workerPi.emit("session_start", {}, ctx);
		atomicWriteJson(pathsA.command, {
			version: 1,
			type: "prompt",
			task: "first",
			issuedAt: new Date().toISOString(),
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: assignmentA,
			generation: 1,
			parentEpoch: "epoch1",
			dispatchedAt: new Date().toISOString(),
		});
		pool.upsert({
			...pool.getByRole("implementer")!,
			status: "busy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "epoch1",
			updatedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "implementer", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
			task: "blocked-b",
		});
		await vi.advanceTimersByTimeAsync(150);
		await workerPi.emit(
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
		await workerPi.emit("agent_settled", {}, ctx);
		await vi.advanceTimersByTimeAsync(500);
		expect(pool.getByRole("implementer")?.status).toBe("unhealthy");
		expect(existsSync(pathsB.started)).toBe(false);
		expect(new WriterLeaseManager().peekOwner(cwd)).toBeUndefined();
		vi.useRealTimers();

		const parentPi = createFakePi();
		const notifies: string[] = [];
		installMomoParent(parentPi as unknown as ExtensionAPI, {
			cwd,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "parent-claim-exhaust-clean",
				HERDR_ENV: "1",
				HERDR_PANE_ID: "w1:p0",
				HERDR_WORKSPACE_ID: "ws",
				HERDR_SOCKET_PATH: "s",
			},
			client: createTestHerdrClient({
				runCommand: async (_file, args) => {
					if (args[0] === "pane" && args[1] === "close") {
						return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
					}
					if (args[0] === "agent" && args[1] === "get") {
						return {
							code: 0,
							stdout: JSON.stringify({
								id: "g",
								result: { type: "agent_info", agent: { agent_status: "idle" } },
							}),
							stderr: "",
						};
					}
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				},
			}),
			poolRegistry: pool,
		});
		await parentPi.commands.get("momo-cleanup")!.handler("", {
			ui: { notify: (m: string) => notifies.push(m) },
		} as never);
		expect(isArchivalTombstone(pool.getByRole("implementer")!)).toBe(true);
		expect(listClaiming(pool.poolRoot, "implementer")).toHaveLength(0);
		expect(listQueue(pool.poolRoot, "implementer")).toHaveLength(0);
		const resultB = validateResult(tryReadIpcJson(pathsB.result), {
			runId: assignmentB,
			workerId,
		});
		expect(resultB.status).toBe("failed");
		expect(resultB.errorMessage).toMatch(/cleanup_before_archive/i);
	});

	it("counts injected lock/recovery failures into retry budget then fences unhealthy", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-claim-retry-io-");
		const cwd = tempDir("momo-claim-retry-io-cwd-");
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
		const assignmentA = "retryioaa0000001";
		const assignmentB = "retryiobbb000002";
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentA);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p2",
			agentName: "momo_scout",
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		__setMaxClaimRetryAttemptsForTest(2);
		__setActiveAtomicWriteForTest(() => {
			throw new Error("injected active write failure on claim publish");
		});
		const pi = createFakePi();
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
		atomicWriteJson(pathsA.command, {
			version: 1,
			type: "prompt",
			task: "first",
			issuedAt: new Date().toISOString(),
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: assignmentA,
			generation: 1,
			parentEpoch: "epoch1",
			dispatchedAt: new Date().toISOString(),
		});
		pool.upsert({
			...pool.getByRole("scout")!,
			status: "busy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "epoch1",
			updatedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
			task: "blocked-b",
		});
		await vi.advanceTimersByTimeAsync(150);
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
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(1);
		__setClaimRetryRecoveryHookForTest(() => {
			throw new Error("injected role-lock recovery I/O failure");
		});
		const unhandled: unknown[] = [];
		const onRejection = (reason: unknown) => {
			unhandled.push(reason);
		};
		process.on("unhandledRejection", onRejection);
		try {
			await vi.advanceTimersByTimeAsync(500);
			await Promise.resolve();
		} finally {
			process.off("unhandledRejection", onRejection);
		}
		expect(unhandled).toHaveLength(0);
		const record = pool.getByRole("scout");
		expect(record?.status).toBe("unhealthy");
		expect(record?.activeAssignmentId).toBe(assignmentB);
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(1);
		vi.useRealTimers();
	});
});

describe("P2 supervised control heartbeat", () => {
	function trackUnhandledRejections(): { unhandled: unknown[]; stop: () => void } {
		const unhandled: unknown[] = [];
		const onRejection = (reason: unknown) => {
			unhandled.push(reason);
		};
		process.on("unhandledRejection", onRejection);
		return {
			unhandled,
			stop: () => process.off("unhandledRejection", onRejection),
		};
	}

	it("idle worker fences unhealthy on periodic control heartbeat fault without unhandled rejection", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-hb-idle-");
		const cwd = tempDir("momo-hb-idle-cwd-");
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
			paneId: "w1:p2",
			agentName: "momo_scout",
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		let controlWrites = 0;
		__setControlHeartbeatHooksForTest({
			onWrite: (phase) => {
				if (phase === "control" && controlWrites++ > 0) {
					throw new Error("injected control heartbeat fault");
				}
			},
		});
		const pi = createFakePi();
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
		const tracker = trackUnhandledRejections();
		try {
			await pi.emit("session_start", {}, {
				hasUI: true,
				isIdle: () => true,
				abort: vi.fn(),
				cwd,
			} as unknown as ExtensionContext);
			await vi.advanceTimersByTimeAsync(3_000);
			await Promise.resolve();
		} finally {
			tracker.stop();
		}
		expect(tracker.unhandled).toHaveLength(0);
		expect(pool.getByRole("scout")?.status).toBe("unhealthy");
		vi.useRealTimers();
	});

	it("active implementer with lease evidence fences uncertain on lease heartbeat fault", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-hb-lease-");
		const cwd = tempDir("momo-hb-lease-cwd-");
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
		const assignmentId = "hblease000000001";
		const paths = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p2",
			agentName: "momo_implementer",
			cwd,
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_700_000_000_000 });
		let leaseFaultInjected = false;
		__setControlHeartbeatHooksForTest({
			onWrite: (phase) => {
				if (phase === "lease" && leaseFaultInjected) {
					throw new Error("injected lease heartbeat fault");
				}
			},
		});
		const pi = createFakePi();
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
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "hold-lease",
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
		await vi.advanceTimersByTimeAsync(200);
		expect(leases.isLocked(cwd)).toBe(true);
		leaseFaultInjected = true;
		pool.upsert({
			...pool.getByRole("implementer")!,
			status: "busy",
			activeAssignmentId: assignmentId,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		const tracker = trackUnhandledRejections();
		try {
			await vi.advanceTimersByTimeAsync(3_000);
			await Promise.resolve();
		} finally {
			tracker.stop();
		}
		expect(tracker.unhandled).toHaveLength(0);
		const record = pool.getByRole("implementer")!;
		expect(record.status).toBe("uncertain");
		expect(record.uncertainWrite).toBe(true);
		expect(record.activeAssignmentId).toBe(assignmentId);
		expect(leases.peekOwner(cwd)?.ownerId).toBe(workerId);
		expect(existsSync(control.active)).toBe(true);
		expect(listClaiming(pool.poolRoot, "implementer")).toHaveLength(0);
		vi.useRealTimers();
	});

	it("active read-only worker fences unhealthy on assignment heartbeat fault", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-hb-no-evidence-");
		const cwd = tempDir("momo-hb-no-evidence-cwd-");
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
		const assignmentId = "hbnoevid00000001";
		const paths = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p2",
			agentName: "momo_scout",
			cwd,
			status: "busy",
			activeAssignmentId: assignmentId,
			activeParentEpoch: "e1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "pre-start",
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
		let assignmentFaultInjected = false;
		__setControlHeartbeatHooksForTest({
			onWrite: (phase) => {
				if (phase === "assignment" && assignmentFaultInjected) {
					throw new Error("injected assignment heartbeat fault");
				}
			},
		});
		const pi = createFakePi();
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
		const tracker = trackUnhandledRejections();
		try {
			await pi.emit("session_start", {}, ctx);
			await vi.advanceTimersByTimeAsync(200);
			assignmentFaultInjected = true;
			await vi.advanceTimersByTimeAsync(3_000);
			await Promise.resolve();
		} finally {
			tracker.stop();
		}
		expect(tracker.unhandled).toHaveLength(0);
		const record = pool.getByRole("scout")!;
		expect(record.status).toBe("unhealthy");
		expect(record.uncertainWrite).toBeUndefined();
		expect(record.activeAssignmentId).toBe(assignmentId);
		expect(existsSync(control.active)).toBe(true);
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(0);
		vi.useRealTimers();
	});

	it("survives fence-write failure on heartbeat fault without unhandled rejection", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-hb-fence-fault-");
		const cwd = tempDir("momo-hb-fence-fault-cwd-");
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
			paneId: "w1:p2",
			agentName: "momo_scout",
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		let controlWrites = 0;
		__setControlHeartbeatHooksForTest({
			onWrite: (phase) => {
				if (phase === "control" && controlWrites++ > 0) {
					throw new Error("injected control heartbeat fault");
				}
			},
			onFence: () => {
				throw new Error("injected fence write fault");
			},
		});
		const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
		const pi = createFakePi();
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
		const tracker = trackUnhandledRejections();
		try {
			await pi.emit("session_start", {}, {
				hasUI: true,
				isIdle: () => true,
				abort: vi.fn(),
				cwd,
			} as unknown as ExtensionContext);
			await vi.advanceTimersByTimeAsync(3_000);
			await Promise.resolve();
		} finally {
			tracker.stop();
		}
		expect(tracker.unhandled).toHaveLength(0);
		expect(
			errorSpy.mock.calls.some(([message]) =>
				String(message).includes("registry fence failed"),
			),
		).toBe(true);
		errorSpy.mockRestore();
		expect(pool.getByRole("scout")?.status).toBe("idle");
		vi.useRealTimers();
	});
});

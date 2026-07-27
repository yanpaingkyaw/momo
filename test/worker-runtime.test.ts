import { mkdtempSync, mkdirSync, rmSync, existsSync, writeFileSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { installMomoWorker, __setWriteResultDurableLockHookForTest, __resetWriteResultDurableLockHookForTest } from "../src/extensions/worker-runtime.js";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { WriterLeaseManager } from "../src/lease/writer-lease.js";
import { MAX_IPC_JSON_BYTES, MAX_EVENTS_FILE_BYTES, atomicWriteJson, readJsonFile } from "../src/ipc/spool.js";
import { sanitizeAssistantMessages } from "../src/ipc/validate.js";
import { PoolRegistry } from "../src/herdr/pool-registry.js";
import { resolvePoolIdentity, stableWorkerId } from "../src/herdr/pool-identity.js";
import { assignmentSpoolPaths, workerControlPaths } from "../src/herdr/assignment-spool.js";
import { enqueueAssignment, queueCount } from "../src/herdr/role-queue.js";

const tempDirs: string[] = [];

afterEach(() => {
	while (tempDirs.length > 0) {
		const dir = tempDirs.pop();
		if (dir) rmSync(dir, { recursive: true, force: true });
	}
	vi.useRealTimers();
	__resetWriteResultDurableLockHookForTest();
});

function tempDir(prefix: string): string {
	const dir = mkdtempSync(path.join(tmpdir(), prefix));
	tempDirs.push(dir);
	return dir;
}

const RUNTIME_NOT_READY =
	"Extension runtime not initialized. Action methods cannot be called during extension loading.";

function createFakePi() {
	const handlers = new Map<string, Function[]>();
	let runtimeReady = false;
	const assertRuntime = () => {
		if (!runtimeReady) throw new Error(RUNTIME_NOT_READY);
	};
	return {
		handlers,
		get runtimeReady() {
			return runtimeReady;
		},
		on(event: string, handler: Function) {
			const list = handlers.get(event) ?? [];
			list.push(handler);
			handlers.set(event, list);
		},
		sendUserMessage: vi.fn((..._args: unknown[]) => {
			assertRuntime();
		}),
		sendMessage: vi.fn((..._args: unknown[]) => {
			assertRuntime();
		}),
		setActiveTools: vi.fn((..._args: unknown[]) => {
			assertRuntime();
		}),
		registerTool: vi.fn(),
		async emit(event: string, payload: unknown = {}, ctx?: ExtensionContext) {
			if (event === "session_start") runtimeReady = true;
			const list = handlers.get(event) ?? [];
			const results = [];
			for (const handler of list) {
				results.push(await handler(payload, ctx));
			}
			return results[results.length - 1];
		},
	};
}

describe("worker runtime (persistent pool)", () => {
	it("active pointer -> command -> sendUserMessage -> settled -> result; returns idle", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-worker-cache-");
		const cwd = tempDir("momo-worker-cwd-");
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

		const assignmentId = "abcdef0123456789";
		const paths = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });

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
		});

		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;

		await pi.emit("session_start", {}, ctx);
		expect(existsSync(control.ready)).toBe(true);

		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "inspect auth",
			issuedAt: new Date().toISOString(),
			runId: assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId,
			generation: 1,
			parentEpoch: "epoch1",
			dispatchedAt: new Date().toISOString(),
		});

		await vi.advanceTimersByTimeAsync(150);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("inspect auth");

		await pi.emit(
			"message_end",
			{
				message: {
					role: "assistant",
					content: [{ type: "text", text: "found it" }],
					stopReason: "stop",
					usage: { input: 1, output: 1, totalTokens: 2, cost: { total: 0 } },
				},
			},
			ctx,
		);
		await pi.emit("agent_settled", {}, ctx);

		const result = readJsonFile(paths.result) as {
			status: string;
			runId: string;
			workerId: string;
		};
		expect(result.status).toBe("completed");
		expect(result.runId).toBe(assignmentId);
		expect(result.workerId).toBe(workerId);
		expect(pool.getByRole("scout")?.status).toBe("idle");
	});

	it("blocks interactive input even when idle; allows extension source", async () => {
		const cacheRoot = tempDir("momo-worker-in-");
		const cwd = tempDir("momo-worker-cwd2-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const control = workerControlPaths(pool.poolRoot, "planner");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "planner",
				MOMO_IPC_DIR: control.root,
				MOMO_CONTROL_DIR: control.root,
				MOMO_WORKER_ID: stableWorkerId(identity.poolKey, "planner"),
				MOMO_WORKER_GENERATION: "1",
				MOMO_POOL_KEY: identity.poolKey,
				MOMO_POOL_ROOT: pool.poolRoot,
				MOMO_RUN_ID: "g1",
				MOMO_CWD: cwd,
			},
			poolRegistry: pool,
		});
		const notify = vi.fn();
		await pi.emit("session_start", {}, {
			hasUI: true,
			isIdle: () => true,
			ui: { notify },
		} as unknown as ExtensionContext);
		const blocked = await pi.emit(
			"input",
			{ source: "interactive", text: "hi" },
			{ ui: { notify } } as unknown as ExtensionContext,
		);
		expect(blocked).toEqual({ action: "handled" });
		const allowed = await pi.emit(
			"input",
			{ source: "extension", text: "task" },
			{ ui: { notify } } as unknown as ExtensionContext,
		);
		expect(allowed).toEqual({ action: "continue" });
	});

	it("sanitizeAssistantMessages still bounds result payloads", () => {
		const huge = "x".repeat(MAX_IPC_JSON_BYTES);
		const out = sanitizeAssistantMessages(
			[{ role: "assistant", content: [{ type: "text", text: huge }] }],
			1024,
		);
		expect(JSON.stringify(out).length).toBeLessThan(MAX_IPC_JSON_BYTES);
	});

	it("implementer acquires distinct lease tokens across two sequential assignments", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-lease-cache-");
		const cwd = tempDir("momo-lease-cwd-");
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
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p3",
			agentName: "momo_impl",
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_700_000_000_000 });
		const acquiredTokens: string[] = [];
		const releasedTokens: string[] = [];
		const originalAcquire = leases.acquire.bind(leases);
		const originalRelease = leases.release.bind(leases);
		vi.spyOn(leases, "acquire").mockImplementation((acquireCwd, ownerId, token) => {
			acquiredTokens.push(token);
			return originalAcquire(acquireCwd, ownerId, token);
		});
		vi.spyOn(leases, "release").mockImplementation((releaseCwd, ownerId, token) => {
			releasedTokens.push(token);
			return originalRelease(releaseCwd, ownerId, token);
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

		async function runAssignment(assignmentId: string, task: string): Promise<void> {
			const paths = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentId);
			mkdirSync(paths.root, { recursive: true, mode: 0o700 });
			atomicWriteJson(paths.command, {
				version: 1,
				type: "prompt",
				task,
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
			expect(pi.sendUserMessage).toHaveBeenCalled();
			expect(leases.isLocked(cwd)).toBe(true);
			const tools = (pi.setActiveTools as ReturnType<typeof vi.fn>).mock.calls.at(-1)?.[0] as string[];
			expect(tools).toEqual(expect.arrayContaining(["bash", "edit", "write"]));
			await pi.emit(
				"message_end",
				{
					message: {
						role: "assistant",
						content: [{ type: "text", text: `done:${task}` }],
						stopReason: "stop",
					},
				},
				ctx,
			);
			await pi.emit("agent_settled", {}, ctx);
			expect(leases.isLocked(cwd)).toBe(false);
			expect(pool.getByRole("implementer")?.status).toBe("idle");
		}

		await runAssignment("fedcba9876543210", "edit-one");
		await runAssignment("0123456789abcdef", "edit-two");

		expect(acquiredTokens).toHaveLength(2);
		expect(releasedTokens).toHaveLength(2);
		expect(acquiredTokens[0]).not.toBe(acquiredTokens[1]);
		expect(releasedTokens[0]).toBe(acquiredTokens[0]);
		expect(releasedTokens[1]).toBe(acquiredTokens[1]);
	});

	it("result write failure marks unhealthy and does not advance queue", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-result-fail-");
		const cwd = tempDir("momo-result-fail-cwd-");
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
		const assignmentId = "aaaaaaaaaaaaaaaa";
		const paths = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		// Block durable result publish by making result path a directory.
		mkdirSync(paths.result, { recursive: true, mode: 0o700 });

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
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "first",
			issuedAt: new Date().toISOString(),
			runId: assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId,
			generation: 1,
			parentEpoch: "epoch1",
			dispatchedAt: new Date().toISOString(),
		});
		pool.upsert({
			...pool.getByRole("scout")!,
			status: "busy",
			activeAssignmentId: assignmentId,
			activeParentEpoch: "epoch1",
			updatedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: "bbbbbbbbbbbbbbbb",
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
			task: "should-not-run",
		});
		await vi.advanceTimersByTimeAsync(150);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("first");
		await pi.emit(
			"message_end",
			{
				message: {
					role: "assistant",
					content: [{ type: "text", text: "ok" }],
					stopReason: "stop",
				},
			},
			ctx,
		);
		await pi.emit("agent_settled", {}, ctx);
		expect(pool.getByRole("scout")?.status).toBe("unhealthy");
		expect(existsSync(control.active)).toBe(true);
		expect(queueCount(pool.poolRoot, "scout")).toBe(1);
		expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
	});

	it("cancelRequested publishes aborted even without assistant text", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-cancel-aborted-");
		const cwd = tempDir("momo-cancel-cwd-");
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
			generation: 1,
			generationTombstone: 1,
			role: "planner",
			paneId: "w1:p4",
			agentName: "momo_planner",
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		const assignmentId = "eeeeeeeeeeeeeeee";
		const paths = assignmentSpoolPaths(pool.poolRoot, "planner", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
		const pi = createFakePi();
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
		await pi.emit("session_start", {}, ctx);
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "work",
			issuedAt: new Date().toISOString(),
			runId: assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId,
			generation: 1,
			parentEpoch: "epoch1",
			dispatchedAt: new Date().toISOString(),
		});
		await vi.advanceTimersByTimeAsync(150);
		atomicWriteJson(paths.cancel, {
			version: 1,
			runId: assignmentId,
			workerId,
			reason: "parent_cancel",
			issuedAt: new Date().toISOString(),
		});
		await vi.advanceTimersByTimeAsync(150);
		expect(ctx.abort).toHaveBeenCalled();
		await pi.emit("agent_settled", {}, ctx);
		const result = readJsonFile(paths.result) as { status: string };
		expect(result.status).toBe("aborted");
	});

	it("terminal event size-cap failure still advances FIFO successor", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-evt-cap-");
		const cwd = tempDir("momo-evt-cap-cwd-");
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
		const assignmentA = "aaaaaaaaaaaaaaaa";
		const assignmentB = "bbbbbbbbbbbbbbbb";
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
		// After started events land, blow the size cap so only the terminal emit fails.
		writeFileSync(pathsA.events, "x".repeat(MAX_EVENTS_FILE_BYTES + 1), { mode: 0o600 });
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

		const resultA = readJsonFile(pathsA.result) as { status: string };
		expect(resultA.status).toBe("completed");
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe(assignmentB);
		expect(pool.getByRole("scout")?.status).toBe("busy");
		expect(queueCount(pool.poolRoot, "scout")).toBe(0);
		await vi.advanceTimersByTimeAsync(150);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("successor");
		void pathsB;
	});

	it("event capacity exhaustion drops further progress events but still completes and advances queue", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-evt-cap-progress-");
		const cwd = tempDir("momo-evt-cap-progress-cwd-");
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
		const assignmentA = "ccccccccccccccc1";
		const assignmentB = "ddddddddddddddd2";
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

		const sizeBeforePad = existsSync(pathsA.events) ? readFileSync(pathsA.events).length : 0;
		const padBytes = Math.max(0, MAX_EVENTS_FILE_BYTES - sizeBeforePad - 40);
		writeFileSync(
			pathsA.events,
			Buffer.concat([
				existsSync(pathsA.events) ? readFileSync(pathsA.events) : Buffer.alloc(0),
				Buffer.alloc(padBytes, 0x20),
			]),
			{ mode: 0o600 },
		);
		const sizeAtCap = readFileSync(pathsA.events).length;
		expect(sizeAtCap).toBeLessThanOrEqual(MAX_EVENTS_FILE_BYTES);

		await pi.emit(
			"message_update",
			{ assistantMessageEvent: { type: "text_delta", delta: "overflow-progress" } },
			ctx,
		);
		const afterOverflow = readFileSync(pathsA.events);
		expect(afterOverflow.length).toBe(sizeAtCap);
		expect(afterOverflow.length).toBeLessThanOrEqual(MAX_EVENTS_FILE_BYTES);

		await pi.emit(
			"message_update",
			{ assistantMessageEvent: { type: "text_delta", delta: "should-drop" } },
			ctx,
		);
		expect(readFileSync(pathsA.events).length).toBe(sizeAtCap);

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

		const resultA = readJsonFile(pathsA.result) as { status: string };
		expect(resultA.status).toBe("completed");
		expect(readFileSync(pathsA.events).length).toBeLessThanOrEqual(MAX_EVENTS_FILE_BYTES);
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe(assignmentB);
		expect(pool.getByRole("scout")?.status).toBe("busy");
		expect(queueCount(pool.poolRoot, "scout")).toBe(0);
		await vi.advanceTimersByTimeAsync(150);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("successor");
		void pathsB;
	});

	it("pollActive outer catch: mutation + unreadable cancel + unwritable result => uncertain, lease retained", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-poll-catch-mut-");
		const cwd = tempDir("momo-poll-catch-mut-cwd-");
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
		const assignmentId = "pollcatchmut0001";
		const paths = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
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
			task: "mutate",
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
		expect(pi.sendUserMessage).toHaveBeenCalled();
		expect(leases.isLocked(cwd)).toBe(true);
		await pi.emit("tool_call", { toolName: "bash" }, ctx);
		mkdirSync(paths.cancel, { recursive: true, mode: 0o700 });
		mkdirSync(paths.result, { recursive: true, mode: 0o700 });
		await vi.advanceTimersByTimeAsync(150);

		const record = pool.getByRole("implementer")!;
		expect(record.status).toBe("uncertain");
		expect(record.uncertainWrite).toBe(true);
		expect(record.activeAssignmentId).toBe(assignmentId);
		expect(leases.peekOwner(cwd)?.ownerId).toBe(workerId);
		const { selectClosablePoolWorkers, isArchivalTombstone } = await import(
			"../src/herdr/pool-registry.js"
		);
		expect(selectClosablePoolWorkers([record]).closable).toHaveLength(0);
		expect(selectClosablePoolWorkers([record], { force: true }).closable).toHaveLength(1);

		rmSync(paths.result, { recursive: true, force: true });
		const { installMomoParent } = await import("../src/extensions/parent.js");
		const { HerdrClient } = await import("../src/herdr/client.js");
		const closed: string[] = [];
		const commands = new Map<string, { handler: (args: string, ctx: unknown) => Promise<void> }>();
		const parentPi = {
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
		installMomoParent(parentPi as unknown as ExtensionAPI, {
			cwd,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "parent-poll-catch",
				HERDR_ENV: "1",
				HERDR_PANE_ID: "w1:p1",
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
							code: 1,
							stdout: JSON.stringify({
								id: "g",
								error: { code: "agent_not_found", message: "gone" },
							}),
							stderr: "",
						};
					}
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				},
			}),
			poolRegistry: pool,
			leaseManager: leases,
		});
		await commands.get("momo-cleanup")!.handler("--force", {
			ui: {
				confirm: async () => true,
				notify: () => undefined,
			},
		} as never);
		expect(closed).toEqual(["w1:p2"]);
		expect(isArchivalTombstone(pool.getByRole("implementer")!)).toBe(true);
		expect(leases.peekOwner(cwd)).toBeUndefined();
	});

	it("pollActive outer catch: leaseHeld without mutation still marks uncertain", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-poll-catch-lease-");
		const cwd = tempDir("momo-poll-catch-lease-cwd-");
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
		const assignmentId = "pollcatchlease001";
		const paths = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentId);
		mkdirSync(paths.root, { recursive: true, mode: 0o700 });
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
		mkdirSync(paths.cancel, { recursive: true, mode: 0o700 });
		mkdirSync(paths.result, { recursive: true, mode: 0o700 });
		await vi.advanceTimersByTimeAsync(150);
		const record = pool.getByRole("implementer")!;
		expect(record.status).toBe("uncertain");
		expect(record.uncertainWrite).toBe(true);
		expect(leases.peekOwner(cwd)?.ownerId).toBe(workerId);
	});

	it("cancel-after-shouldCancel-before-acquire-return: no started/prompt/lease; FIFO advances", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-cancel-after-acq-");
		const cwd = tempDir("momo-cancel-after-acq-cwd-");
		const foreignCwd = tempDir("momo-cancel-after-acq-foreign-");
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
		const assignmentA = "cancelafteracq001";
		const assignmentB = "cancelafteracq002";
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentA);
		const pathsB = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentB);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		mkdirSync(pathsB.root, { recursive: true, mode: 0o700 });
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

		const { createLeaseToken } = await import("../src/lease/writer-lease.js");
		const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_700_000_000_000 });
		const foreignOwner = "foreign_owner_id";
		leases.acquire(foreignCwd, foreignOwner, createLeaseToken());
		const originalWait = leases.waitAcquire.bind(leases);
		leases.waitAcquire = async (leaseCwd, ownerId, token, options) => {
			const record = await originalWait(leaseCwd, ownerId, token, {
				...options,
				shouldCancel: async () => false,
			});
			atomicWriteJson(pathsA.cancel, {
				version: 1,
				runId: assignmentA,
				workerId,
				generation: 1,
				reason: "parent_abort",
				issuedAt: new Date().toISOString(),
			});
			return record;
		};

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
			task: "successor",
		});
		await vi.advanceTimersByTimeAsync(200);

		expect(pi.sendUserMessage).not.toHaveBeenCalledWith("first");
		expect(existsSync(pathsA.started)).toBe(false);
		const resultA = readJsonFile(pathsA.result) as { status: string };
		expect(resultA.status).toBe("aborted");
		// Exact lease for A was released; successor B may re-acquire the same cwd.
		expect(leases.peekOwner(foreignCwd)?.ownerId).toBe(foreignOwner);
		expect(pool.getByRole("implementer")?.activeAssignmentId).toBe(assignmentB);
		expect(queueCount(pool.poolRoot, "implementer")).toBe(0);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("successor");
		expect(existsSync(pathsB.started)).toBe(true);
		void pathsB;
	});

	it("cancelAfterAcquire release failure => uncertain, lease retained, no successor", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-cancel-acq-relfail-");
		const cwd = tempDir("momo-cancel-acq-relfail-cwd-");
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
		const assignmentA = "cancelrelfail0001";
		const assignmentB = "cancelrelfail0002";
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentA);
		const pathsB = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentB);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		mkdirSync(pathsB.root, { recursive: true, mode: 0o700 });
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
		const originalWait = leases.waitAcquire.bind(leases);
		leases.waitAcquire = async (leaseCwd, ownerId, token, options) => {
			const record = await originalWait(leaseCwd, ownerId, token, {
				...options,
				shouldCancel: async () => false,
			});
			atomicWriteJson(pathsA.cancel, {
				version: 1,
				runId: assignmentA,
				workerId,
				generation: 1,
				reason: "parent_abort",
				issuedAt: new Date().toISOString(),
			});
			return record;
		};
		leases.release = () => {
			throw new Error("injected release failure");
		};

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
			task: "successor",
		});
		await vi.advanceTimersByTimeAsync(200);

		expect(pi.sendUserMessage).not.toHaveBeenCalled();
		expect(existsSync(pathsA.started)).toBe(false);
		expect(leases.isLocked(cwd)).toBe(true);
		expect(leases.peekOwner(cwd)?.ownerId).toBe(workerId);
		const resultA = readJsonFile(pathsA.result) as {
			status: string;
			uncertainWrite?: boolean;
			errorMessage?: string;
		};
		expect(resultA.status).toBe("failed");
		expect(resultA.uncertainWrite).toBe(true);
		expect(resultA.errorMessage).toMatch(/release failed/i);
		const record = pool.getByRole("implementer")!;
		expect(record.status).toBe("uncertain");
		expect(record.uncertainWrite).toBe(true);
		expect(record.activeAssignmentId).toBe(assignmentA);
		expect(queueCount(pool.poolRoot, "implementer")).toBe(1);
		expect(existsSync(pathsB.started)).toBe(false);
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).not.toHaveBeenCalledWith("successor");
	});

	it("cancelBeforeStart release failure => uncertain, lease retained, no prompt/start", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-cancel-start-relfail-");
		const cwd = tempDir("momo-cancel-start-relfail-cwd-");
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
		const assignmentA = "cancelstartrelf001";
		const assignmentB = "cancelstartrelf002";
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentA);
		const pathsB = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentB);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		mkdirSync(pathsB.root, { recursive: true, mode: 0o700 });
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
		leases.release = () => {
			throw new Error("injected release failure before start");
		};

		const pi = createFakePi();
		const originalSetActiveTools = pi.setActiveTools.bind(pi);
		pi.setActiveTools = ((tools: string[]) => {
			originalSetActiveTools(tools);
			if (tools.includes("bash") || tools.includes("edit") || tools.includes("write")) {
				atomicWriteJson(pathsA.cancel, {
					version: 1,
					runId: assignmentA,
					workerId,
					generation: 1,
					reason: "parent_abort",
					issuedAt: new Date().toISOString(),
				});
			}
		}) as typeof pi.setActiveTools;

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
			task: "successor",
		});
		await vi.advanceTimersByTimeAsync(200);

		expect(pi.sendUserMessage).not.toHaveBeenCalled();
		expect(existsSync(pathsA.started)).toBe(false);
		expect(leases.isLocked(cwd)).toBe(true);
		expect(leases.peekOwner(cwd)?.ownerId).toBe(workerId);
		const resultA = readJsonFile(pathsA.result) as {
			status: string;
			uncertainWrite?: boolean;
			errorMessage?: string;
		};
		expect(resultA.status).toBe("failed");
		expect(resultA.uncertainWrite).toBe(true);
		expect(resultA.errorMessage).toMatch(/release failed/i);
		const record = pool.getByRole("implementer")!;
		expect(record.status).toBe("uncertain");
		expect(record.uncertainWrite).toBe(true);
		expect(record.activeAssignmentId).toBe(assignmentA);
		expect(queueCount(pool.poolRoot, "implementer")).toBe(1);
		expect(existsSync(pathsB.started)).toBe(false);
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).not.toHaveBeenCalledWith("successor");
	});

	it("parent-first barrier: no result overwrite, no idle/busy resurrection, no successor", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-parent-first-barrier-");
		const cwd = tempDir("momo-parent-first-barrier-cwd-");
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
		const assignmentA = "parentfirst000001";
		const assignmentB = "parentfirst000002";
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
			cwd,
			status: "idle",
			updatedAt: new Date().toISOString(),
		});

		const parentEvidence = {
			version: 1 as const,
			runId: assignmentA,
			workerId,
			status: "failed" as const,
			messages: [{ role: "assistant", content: [{ type: "text", text: "parent-fence" }] }],
			finishedAt: new Date().toISOString(),
			errorMessage: "Worker heartbeat went stale",
		};

		__setWriteResultDurableLockHookForTest((phase) => {
			if (phase !== "before-write") return;
			// Parent-first commit under the same role lock: fence + evidence before
			// the worker's publish recheck/write.
			pool.upsert({
				...pool.getByRole("scout")!,
				status: "unhealthy",
				activeAssignmentId: assignmentA,
				activeParentEpoch: "epoch1",
				updatedAt: new Date().toISOString(),
			});
			atomicWriteJson(pathsA.result, parentEvidence);
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
					content: [{ type: "text", text: "worker-completed" }],
					stopReason: "stop",
				},
			},
			ctx,
		);
		await pi.emit("agent_settled", {}, ctx);

		const result = readJsonFile(pathsA.result) as {
			status: string;
			errorMessage?: string;
			messages: unknown[];
		};
		expect(result.status).toBe("failed");
		expect(result.errorMessage).toBe("Worker heartbeat went stale");
		expect(result.messages).toEqual(parentEvidence.messages);
		const record = pool.getByRole("scout")!;
		expect(record.status).toBe("unhealthy");
		expect(record.activeAssignmentId).toBe(assignmentA);
		expect(existsSync(control.active)).toBe(true);
		expect(queueCount(pool.poolRoot, "scout")).toBe(1);
		expect(existsSync(pathsB.started)).toBe(false);
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).not.toHaveBeenCalledWith("successor");
		expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
	});

	it("parent-first implementer uncertain fence retains exact lease (with started)", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-parent-first-lease-started-");
		const cwd = tempDir("momo-parent-first-lease-started-cwd-");
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
		const assignmentA = "pfleasewithstart001";
		const assignmentB = "pfleasewithstart002";
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentA);
		const pathsB = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentB);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		mkdirSync(pathsB.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p3",
			agentName: "momo_implementer",
			cwd,
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_700_000_000_000 });
		const releaseSpy = vi.spyOn(leases, "release");

		__setWriteResultDurableLockHookForTest((phase) => {
			if (phase !== "before-write") return;
			pool.upsert({
				...pool.getByRole("implementer")!,
				status: "uncertain",
				uncertainWrite: true,
				activeAssignmentId: assignmentA,
				activeParentEpoch: "epoch1",
				updatedAt: new Date().toISOString(),
			});
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
		atomicWriteJson(pathsA.command, {
			version: 1,
			type: "prompt",
			task: "edit",
			issuedAt: new Date().toISOString(),
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
		});
		pool.upsert({
			...pool.getByRole("implementer")!,
			status: "busy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "epoch1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: assignmentA,
			generation: 1,
			parentEpoch: "epoch1",
			dispatchedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "implementer", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
			task: "successor",
		});
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("edit");
		expect(existsSync(pathsA.started)).toBe(true);
		expect(leases.isLocked(cwd)).toBe(true);
		const ownerBefore = leases.peekOwner(cwd);
		expect(ownerBefore?.ownerId).toBe(workerId);
		const tokenBefore = ownerBefore!.token;
		releaseSpy.mockClear();

		await pi.emit(
			"message_end",
			{
				message: {
					role: "assistant",
					content: [{ type: "text", text: "late-worker" }],
					stopReason: "stop",
				},
			},
			ctx,
		);
		await pi.emit("agent_settled", {}, ctx);

		expect(releaseSpy).not.toHaveBeenCalled();
		expect(existsSync(pathsA.result)).toBe(false);
		expect(leases.isLocked(cwd)).toBe(true);
		expect(leases.stillHeldBy(cwd, workerId, tokenBefore)).toBe(true);
		const record = pool.getByRole("implementer")!;
		expect(record.status).toBe("uncertain");
		expect(record.uncertainWrite).toBe(true);
		expect(record.activeAssignmentId).toBe(assignmentA);
		expect(existsSync(control.active)).toBe(true);
		expect(queueCount(pool.poolRoot, "implementer")).toBe(1);
		expect(existsSync(pathsB.started)).toBe(false);
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).not.toHaveBeenCalledWith("successor");
	});

	it("parent-first implementer uncertain fence retains exact lease (without started)", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-parent-first-lease-nostart-");
		const cwd = tempDir("momo-parent-first-lease-nostart-cwd-");
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
		const assignmentA = "pfleasenostart00001";
		const assignmentB = "pfleasenostart00002";
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentA);
		const pathsB = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentB);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		mkdirSync(pathsB.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p3",
			agentName: "momo_implementer",
			cwd,
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_700_000_000_000 });
		const releaseSpy = vi.spyOn(leases, "release");

		__setWriteResultDurableLockHookForTest((phase) => {
			if (phase !== "before-write") return;
			pool.upsert({
				...pool.getByRole("implementer")!,
				status: "uncertain",
				uncertainWrite: true,
				activeAssignmentId: assignmentA,
				activeParentEpoch: "epoch1",
				updatedAt: new Date().toISOString(),
			});
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
		atomicWriteJson(pathsA.command, {
			version: 1,
			type: "prompt",
			task: "edit",
			issuedAt: new Date().toISOString(),
			runId: assignmentA,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
		});
		pool.upsert({
			...pool.getByRole("implementer")!,
			status: "busy",
			activeAssignmentId: assignmentA,
			activeParentEpoch: "epoch1",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId: assignmentA,
			generation: 1,
			parentEpoch: "epoch1",
			dispatchedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "implementer", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
			task: "successor",
		});
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("edit");
		expect(leases.isLocked(cwd)).toBe(true);
		// Drop started evidence while the same-worker lease remains held.
		expect(existsSync(pathsA.started)).toBe(true);
		rmSync(pathsA.started, { force: true });
		expect(existsSync(pathsA.started)).toBe(false);
		const ownerBefore = leases.peekOwner(cwd);
		expect(ownerBefore?.ownerId).toBe(workerId);
		const tokenBefore = ownerBefore!.token;
		releaseSpy.mockClear();

		await pi.emit(
			"message_end",
			{
				message: {
					role: "assistant",
					content: [{ type: "text", text: "late-worker" }],
					stopReason: "stop",
				},
			},
			ctx,
		);
		await pi.emit("agent_settled", {}, ctx);

		expect(releaseSpy).not.toHaveBeenCalled();
		expect(existsSync(pathsA.result)).toBe(false);
		expect(existsSync(pathsA.started)).toBe(false);
		expect(leases.isLocked(cwd)).toBe(true);
		expect(leases.stillHeldBy(cwd, workerId, tokenBefore)).toBe(true);
		const record = pool.getByRole("implementer")!;
		expect(record.status).toBe("uncertain");
		expect(record.uncertainWrite).toBe(true);
		expect(record.activeAssignmentId).toBe(assignmentA);
		expect(existsSync(control.active)).toBe(true);
		expect(queueCount(pool.poolRoot, "implementer")).toBe(1);
		expect(existsSync(pathsB.started)).toBe(false);
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).not.toHaveBeenCalledWith("successor");
	});

	it("stale generation fence rejects result publish without overwrite", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-stale-gen-fence-");
		const cwd = tempDir("momo-stale-gen-fence-cwd-");
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
		const assignmentId = "stalegenfence00001";
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
			status: "idle",
			updatedAt: new Date().toISOString(),
		});

		__setWriteResultDurableLockHookForTest((phase) => {
			if (phase !== "before-write") return;
			pool.upsert({
				...pool.getByRole("scout")!,
				generation: 2,
				generationTombstone: 2,
				workerId: `${workerId}-n1`,
				status: "busy",
				activeAssignmentId: assignmentId,
				activeParentEpoch: "epoch1",
				updatedAt: new Date().toISOString(),
			});
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
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: "first",
			issuedAt: new Date().toISOString(),
			runId: assignmentId,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
		});
		atomicWriteJson(control.active, {
			version: 1,
			assignmentId,
			generation: 1,
			parentEpoch: "epoch1",
			dispatchedAt: new Date().toISOString(),
		});
		pool.upsert({
			...pool.getByRole("scout")!,
			status: "busy",
			activeAssignmentId: assignmentId,
			activeParentEpoch: "epoch1",
			updatedAt: new Date().toISOString(),
		});
		await vi.advanceTimersByTimeAsync(150);
		await pi.emit(
			"message_end",
			{
				message: {
					role: "assistant",
					content: [{ type: "text", text: "should-not-publish" }],
					stopReason: "stop",
				},
			},
			ctx,
		);
		await pi.emit("agent_settled", {}, ctx);

		expect(existsSync(paths.result)).toBe(false);
		const record = pool.getByRole("scout")!;
		expect(record.generation).toBe(2);
		expect(record.workerId).toBe(`${workerId}-n1`);
		expect(record.status).toBe("busy");
		expect(record.activeAssignmentId).toBe(assignmentId);
		expect(existsSync(control.active)).toBe(true);
	});

	it("stale active assignment fence rejects result publish without overwrite", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-stale-asn-fence-");
		const cwd = tempDir("momo-stale-asn-fence-cwd-");
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
		const assignmentA = "staleasn0000000001";
		const assignmentB = "staleasn0000000002";
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentA);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p2",
			agentName: "momo_scout",
			cwd,
			status: "idle",
			updatedAt: new Date().toISOString(),
		});

		__setWriteResultDurableLockHookForTest((phase) => {
			if (phase !== "before-write") return;
			pool.upsert({
				...pool.getByRole("scout")!,
				status: "busy",
				activeAssignmentId: assignmentB,
				activeParentEpoch: "epoch1",
				updatedAt: new Date().toISOString(),
			});
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
		await vi.advanceTimersByTimeAsync(150);
		await pi.emit(
			"message_end",
			{
				message: {
					role: "assistant",
					content: [{ type: "text", text: "should-not-publish" }],
					stopReason: "stop",
				},
			},
			ctx,
		);
		await pi.emit("agent_settled", {}, ctx);

		expect(existsSync(pathsA.result)).toBe(false);
		const record = pool.getByRole("scout")!;
		expect(record.status).toBe("busy");
		expect(record.activeAssignmentId).toBe(assignmentB);
		expect(existsSync(control.active)).toBe(true);
	});

	it("existing terminal result is not overwritten on publish reject", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-existing-result-");
		const cwd = tempDir("momo-existing-result-cwd-");
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
		const assignmentA = "existingresult0001";
		const assignmentB = "existingresult0002";
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
			cwd,
			status: "idle",
			updatedAt: new Date().toISOString(),
		});

		const existing = {
			version: 1 as const,
			runId: assignmentA,
			workerId,
			status: "failed" as const,
			messages: [{ role: "assistant", content: [{ type: "text", text: "already-terminal" }] }],
			finishedAt: new Date().toISOString(),
			errorMessage: "preexisting",
		};
		atomicWriteJson(pathsA.result, existing);

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
		await pi.emit(
			"message_end",
			{
				message: {
					role: "assistant",
					content: [{ type: "text", text: "would-overwrite" }],
					stopReason: "stop",
				},
			},
			ctx,
		);
		await pi.emit("agent_settled", {}, ctx);

		const result = readJsonFile(pathsA.result) as {
			status: string;
			errorMessage?: string;
			messages: unknown[];
		};
		expect(result.status).toBe("failed");
		expect(result.errorMessage).toBe("preexisting");
		expect(result.messages).toEqual(existing.messages);
		// Still busy for this assignment → publish-failure fallback may mark unhealthy,
		// but must not idle/busy-resurrect a successor or overwrite the result.
		const record = pool.getByRole("scout")!;
		expect(record.status).toBe("unhealthy");
		expect(record.activeAssignmentId).toBe(assignmentA);
		expect(queueCount(pool.poolRoot, "scout")).toBe(1);
		expect(existsSync(pathsB.started)).toBe(false);
		expect(pi.sendUserMessage).not.toHaveBeenCalledWith("successor");
	});

	it("idle registry + stale active cancel does not promote busy or publish result", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-idle-stale-cancel-");
		const cwd = tempDir("momo-idle-stale-cancel-cwd-");
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
		const assignmentA = "idlestalecancel0001";
		const assignmentB = "idlestalecancel0002";
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "scout", assignmentA);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p2",
			agentName: "momo_scout",
			cwd,
			status: "idle",
			updatedAt: new Date().toISOString(),
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
		// Incomplete/stale dispatch: active+command+cancel without registry busy.
		atomicWriteJson(pathsA.command, {
			version: 1,
			type: "prompt",
			task: "stale",
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
		atomicWriteJson(pathsA.cancel, {
			version: 1,
			runId: assignmentA,
			workerId,
			generation: 1,
			reason: "parent_abort",
			issuedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
			task: "successor",
		});
		await vi.advanceTimersByTimeAsync(250);

		expect(existsSync(pathsA.result)).toBe(false);
		expect(existsSync(pathsA.started)).toBe(false);
		expect(pi.sendUserMessage).not.toHaveBeenCalled();
		const record = pool.getByRole("scout")!;
		expect(record.status).toBe("idle");
		expect(record.activeAssignmentId).toBeUndefined();
		expect(queueCount(pool.poolRoot, "scout")).toBe(1);
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).not.toHaveBeenCalledWith("successor");
	});

	it("starting registry + stale active cancel does not promote busy or publish result", async () => {
		vi.useFakeTimers();
		const cacheRoot = tempDir("momo-starting-stale-cancel-");
		const cwd = tempDir("momo-starting-stale-cancel-cwd-");
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
		const assignmentA = "startstalecancel001";
		const assignmentB = "startstalecancel002";
		const pathsA = assignmentSpoolPaths(pool.poolRoot, "planner", assignmentA);
		mkdirSync(pathsA.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "planner",
			paneId: "w1:p4",
			agentName: "momo_planner",
			cwd,
			status: "starting",
			updatedAt: new Date().toISOString(),
		});
		const pi = createFakePi();
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
		await pi.emit("session_start", {}, ctx);
		// Keep starting (do not let claimNext force idle); inject stale active+cancel.
		pool.upsert({
			...pool.getByRole("planner")!,
			status: "starting",
			updatedAt: new Date().toISOString(),
		});
		atomicWriteJson(pathsA.command, {
			version: 1,
			type: "prompt",
			task: "stale",
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
		atomicWriteJson(pathsA.cancel, {
			version: 1,
			runId: assignmentA,
			workerId,
			generation: 1,
			reason: "parent_abort",
			issuedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "planner", {
			assignmentId: assignmentB,
			workerId,
			generation: 1,
			parentEpoch: "epoch1",
			task: "successor",
		});
		await vi.advanceTimersByTimeAsync(250);

		expect(existsSync(pathsA.result)).toBe(false);
		expect(existsSync(pathsA.started)).toBe(false);
		expect(pi.sendUserMessage).not.toHaveBeenCalled();
		const record = pool.getByRole("planner")!;
		expect(record.status).toBe("starting");
		expect(record.activeAssignmentId).toBeUndefined();
		expect(queueCount(pool.poolRoot, "planner")).toBe(1);
		await vi.advanceTimersByTimeAsync(200);
		expect(pi.sendUserMessage).not.toHaveBeenCalledWith("successor");
	});
});

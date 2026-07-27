import { mkdtempSync, mkdirSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { installMomoWorker } from "../src/extensions/worker-runtime.js";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { WriterLeaseManager } from "../src/lease/writer-lease.js";
import { MAX_IPC_JSON_BYTES, atomicWriteJson, readJsonFile } from "../src/ipc/spool.js";
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
});

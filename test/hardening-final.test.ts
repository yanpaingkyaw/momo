import {
	mkdirSync,
	mkdtempSync,
	rmSync,
	writeFileSync,
	chmodSync,
} from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { installMomoWorker } from "../src/extensions/worker-runtime.js";
import { installMomoParent } from "../src/extensions/parent.js";
import { WriterLeaseManager, createLeaseToken } from "../src/lease/writer-lease.js";
import { createHerdrChildSessionFactory, createStableParentId } from "../src/delegation/herdr-factory.js";
import { HerdrClient, parseAgentGetResult, AGENT_PANE_BUSY_CODE } from "../src/herdr/client.js";
import { PaneRegistry, RegistryCorruptionError } from "../src/herdr/registry.js";
import { PoolRegistry } from "../src/herdr/pool-registry.js";
import { resolvePoolIdentity } from "../src/herdr/pool-identity.js";
import { atomicWriteJson } from "../src/ipc/spool.js";
import {
	IpcValidationError,
	parseJsonFile,
	tryReadIpcJson,
	validateHeartbeat,
	validateResult,
} from "../src/ipc/validate.js";
import { getRole } from "../src/roles.js";
import { dispatchAssignment, setupPoolWorkerFixture } from "./helpers/pool-fixture.js";

const tempDirs: string[] = [];
const previousPiDir = process.env.PI_CODING_AGENT_DIR;

afterEach(() => {
	while (tempDirs.length > 0) {
		const dir = tempDirs.pop();
		if (dir) rmSync(dir, { recursive: true, force: true });
	}
	if (previousPiDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
	else process.env.PI_CODING_AGENT_DIR = previousPiDir;
	vi.useRealTimers();
});

function tempDir(prefix: string): string {
	const dir = mkdtempSync(path.join(tmpdir(), prefix));
	tempDirs.push(dir);
	return dir;
}

function installFakeHerdrExtension(): void {
	const agentDir = tempDir("momo-pi-agent-");
	mkdirSync(path.join(agentDir, "extensions"), { recursive: true });
	writeFileSync(path.join(agentDir, "extensions", "herdr-agent-state.ts"), "// fake\n", "utf8");
	process.env.PI_CODING_AGENT_DIR = agentDir;
}

function createFakePi() {
	const handlers = new Map<string, Function[]>();
	let runtimeReady = false;
	return {
		handlers,
		registerTool: vi.fn(),
		setActiveTools: vi.fn(() => {
			if (!runtimeReady) throw new Error("runtime not ready");
		}),
		sendUserMessage: vi.fn(() => {
			if (!runtimeReady) throw new Error("runtime not ready");
		}),
		sendMessage: vi.fn(() => {
			if (!runtimeReady) throw new Error("runtime not ready");
		}),
		registerCommand: vi.fn(),
		on(event: string, handler: Function) {
			const list = handlers.get(event) ?? [];
			list.push(handler);
			handlers.set(event, list);
		},
		async emit(event: string, payload: unknown = {}, ctx?: unknown) {
			if (event === "session_start") runtimeReady = true;
			const list = handlers.get(event) ?? [];
			const results = [];
			for (const handler of list) results.push(await handler(payload, ctx));
			return results[results.length - 1];
		},
	};
}

describe("writer settlement ordering", () => {
	it("releases lease before a clean completed result", async () => {
		vi.useFakeTimers();
		const cwd = tempDir("momo-settle-cwd-");
		const cacheRoot = tempDir("momo-settle-cache-");
		const lease = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });

		async function runImplementer(workerId: string): Promise<void> {
			const fixture = setupPoolWorkerFixture({
				cacheRoot,
				cwd,
				role: "implementer",
				assignmentId: workerId.replace("_", "") + "000",
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
			const result = parseJsonFile(fixture.paths.result) as {
				status: string;
				uncertainWrite?: boolean;
			};
			expect(result.status).toBe("completed");
			expect(result.uncertainWrite).toBeUndefined();
			expect(lease.isLocked(cwd)).toBe(false);
		}

		await runImplementer("impl_a");
	});

	it("marks failed+uncertain and retains lock when release fails", async () => {
		vi.useFakeTimers();
		const cwd = tempDir("momo-release-fail-cwd-");
		const cacheRoot = tempDir("momo-release-fail-cache-");
		const lease = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });
		const token = createLeaseToken();
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "implementer", assignmentId: "release01" });
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
			controlRoot: fixture.control.root, paths: fixture.paths, assignmentId: fixture.assignmentId,
			workerId: fixture.workerId, generation: fixture.generation, task: "edit",
		});
		await vi.advanceTimersByTimeAsync(150);
		// Steal ownership identity by rewriting owner with same mkdir but different token via release+reacquire by foreigner
		const owner = lease.peekOwner(cwd);
		expect(owner).toBeTruthy();
		vi.spyOn(lease, "release").mockImplementation(() => {
			throw new Error("simulated release failure");
		});
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
		const result = parseJsonFile(fixture.paths.result) as {
			status: string;
			uncertainWrite?: boolean;
		};
		expect(result.status).toBe("failed");
		expect(result.uncertainWrite).toBe(true);
		expect(lease.isLocked(cwd)).toBe(true);
		void token;
	});

	it("abort-before-write releases cleanly; abort-after-write keeps uncertain lease", async () => {
		vi.useFakeTimers();
		const cwd = tempDir("momo-abort-cwd-");
		const cacheRoot = tempDir("momo-abort-cache-");
		const lease = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "implementer", assignmentId: "abortone" });
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
			controlRoot: fixture.control.root, paths: fixture.paths, assignmentId: fixture.assignmentId,
			workerId: fixture.workerId, generation: fixture.generation, task: "edit",
		});
		atomicWriteJson(fixture.paths.cancel, {
			version: 1,
			runId: fixture.assignmentId,
			workerId: fixture.workerId,
			reason: "parent_abort",
			issuedAt: new Date().toISOString(),
		});
		await vi.advanceTimersByTimeAsync(200);
		const before = parseJsonFile(fixture.paths.result) as {
			status: string;
			uncertainWrite?: boolean;
		};
		expect(before.status).toBe("aborted");
		expect(before.uncertainWrite).toBeUndefined();
		expect(lease.isLocked(cwd)).toBe(false);

		// After-write path: mutate then abort settle.
		const fixture2 = setupPoolWorkerFixture({ cacheRoot, cwd, role: "implementer", generation: 2, assignmentId: "aborttwo" });
		const pi2 = createFakePi();
		installMomoWorker(pi2 as unknown as ExtensionAPI, {
			env: fixture2.env,
			poolRegistry: fixture2.pool,
			leaseManager: lease,
			now: () => 1_000,
		});
		await pi2.emit("session_start", {}, ctx);
		dispatchAssignment({
			controlRoot: fixture2.control.root, paths: fixture2.paths, assignmentId: fixture2.assignmentId,
			workerId: fixture2.workerId, generation: fixture2.generation, task: "edit",
		});
		await vi.advanceTimersByTimeAsync(150);
		const block = await pi2.emit("tool_call", { toolName: "edit" }, ctx);
		expect(block).toBeUndefined();
		await pi2.emit(
			"message_end",
			{
				message: {
					role: "assistant",
					content: [{ type: "text", text: "partial" }],
					stopReason: "aborted",
				},
			},
			ctx,
		);
		await pi2.emit("agent_settled", {}, ctx);
		const after = parseJsonFile(fixture2.paths.result) as {
			status: string;
			uncertainWrite?: boolean;
		};
		expect(after.status).toBe("aborted");
		expect(after.uncertainWrite).toBe(true);
		expect(lease.isLocked(cwd)).toBe(true);
	});
});

describe("force cleanup lease ownership", () => {
	it("releases only matching ownerId/cwd and refuses mismatches", () => {
		const cwd = tempDir("momo-force-cwd-");
		const cacheRoot = tempDir("momo-force-cache-");
		const lease = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });
		const token = createLeaseToken();
		lease.acquire(cwd, "impl_match", token);
		expect(lease.forceReleaseIfOwner(cwd, "other").released).toBe(false);
		expect(lease.isLocked(cwd)).toBe(true);
		expect(lease.forceReleaseIfOwner(cwd, "impl_match").released).toBe(true);
		expect(lease.isLocked(cwd)).toBe(false);
	});
});

describe("factory rollback", () => {
	it("closes pane and removes registry when agent start fails after split", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-roll-cache-");
		const cwd = tempDir("momo-roll-cwd-");
		const closed: string[] = [];
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					return {
						code: 0,
						stdout: JSON.stringify({ id: "s", result: { pane: { pane_id: "w1:p9" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 1,
						stdout: JSON.stringify({
							id: "s",
							error: { code: "boom", message: "start failed" },
						}),
						stderr: "",
					};
				}
				if (args[0] === "pane" && args[1] === "close") {
					closed.push(args[2] ?? "");
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const identity = resolvePoolIdentity({ cwd, canonicalRoot: cwd, workspaceId: "test-ws", socketPath: "test-sock" });
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: "parent-roll",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "test-ws",
			socketPath: "test-sock",
			readyTimeoutMs: 500,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});
		const session = await factory({ cwd, role: getRole("scout") });
		await expect(session.prompt("start")).rejects.toThrow(/start failed/);
		expect(closed).toEqual(["w1:p9"]);
		const record = pool.getByRole("scout");
		expect(record?.status).toBe("unhealthy");
		expect(record?.generationTombstone).toBeGreaterThanOrEqual(1);
	});

	it("marks failed when close fails after readiness timeout", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-ready-cache-");
		const cwd = tempDir("momo-ready-cwd-");
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					return {
						code: 0,
						stdout: JSON.stringify({ id: "s", result: { pane: { pane_id: "w1:p8" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: {
								pane_id: "w1:p8",
								name: args[2],
								agent: "pi",
								interactive_ready: true,
							},
						}),
						stderr: "",
					};
				}
				if (args[0] === "pane" && args[1] === "close") {
					return { code: 1, stdout: "", stderr: "busy" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const identity = resolvePoolIdentity({ cwd, canonicalRoot: cwd, workspaceId: "test-ws", socketPath: "test-sock" });
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: "parent-ready",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "test-ws",
			socketPath: "test-sock",
			readyTimeoutMs: 80,
			pollIntervalMs: 20,
			now: () => Date.now(),
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});
		const session = await factory({ cwd, role: getRole("scout") });
		await expect(session.prompt("start")).rejects.toThrow(/did not become ready/);
		expect(pool.list()[0]?.status).toBe("unhealthy");
	});
});

describe("agent get / cancel statuses", () => {
	it("parses nested agent_info envelope", () => {
		const info = parseAgentGetResult({
			type: "agent_info",
			agent: { agent_status: "idle", name: "momo_x", pane_id: "w1:p2" },
		});
		expect(info.agentStatus).toBe("idle");
		expect(info.name).toBe("momo_x");
	});

	it("read-only unresolved cancel => failed; implementer => uncertain", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-cancel-cache-");
		const cwd = tempDir("momo-cancel-cwd-");
		let gets = 0;
		const client = new HerdrClient({
			agentStartBusyRetryMs: 0,
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					const envArgs = args.filter((a, i) => args[i - 1] === "--env");
					const ipc =
						envArgs.find((v) => v.startsWith("MOMO_IPC_DIR="))?.slice("MOMO_IPC_DIR=".length) ?? "";
					const workerId =
						envArgs.find((v) => v.startsWith("MOMO_WORKER_ID="))?.slice("MOMO_WORKER_ID=".length) ??
						"";
					const runId =
						envArgs.find((v) => v.startsWith("MOMO_RUN_ID="))?.slice("MOMO_RUN_ID=".length) ?? "";
					atomicWriteJson(path.join(ipc, "ready.json"), {
						version: 1,
						runId,
						workerId,
						readyAt: new Date().toISOString(),
					});
					return {
						code: 0,
						stdout: JSON.stringify({ id: "s", result: { pane: { pane_id: "w1:p7" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: {
								pane_id: "w1:p7",
								name: args[2],
								agent: "pi",
								interactive_ready: true,
							},
						}),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "get") {
					gets += 1;
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "g",
							result: { type: "agent_info", agent: { agent_status: "unknown" } },
						}),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "wait") {
					return {
						code: 1,
						stdout: JSON.stringify({
							id: "w",
							error: { code: "timeout", message: "timeout" },
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
			sleep: async () => {},
		});
		const identity = resolvePoolIdentity({ cwd, canonicalRoot: cwd, workspaceId: "test-ws", socketPath: "test-sock" });
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: "parent-c",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "test-ws",
			socketPath: "test-sock",
			pollIntervalMs: 20,
			readyTimeoutMs: 2_000,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});
		const scout = await factory({ cwd, role: getRole("scout") });
		const scoutProxy = scout as unknown as { paths: { cancel: string }; uncertainWrite?: boolean };
		await scout.prompt("x");
		await scout.abort();
		expect(pool.list().find((p) => p.role === "scout")?.status).toBe("busy");
		expect(pool.list().find((p) => p.role === "scout")?.uncertainWrite).toBeUndefined();
		expect(tryReadIpcJson(scoutProxy.paths.cancel)).toBeTruthy();

		const impl = await factory({ cwd, role: getRole("implementer") });
		const implProxy = impl as unknown as {
			paths: { cancel: string };
			uncertainWrite: boolean;
		};
		await impl.prompt("y");
		await impl.abort();
		expect(pool.list().find((p) => p.role === "implementer")?.status).toBe("busy");
		expect(tryReadIpcJson(implProxy.paths.cancel)).toBeTruthy();
		expect(implProxy.uncertainWrite).toBe(true);
		// No terminal-key / agentWait escalation on shared panes.
		expect(gets).toBe(0);
		void AGENT_PANE_BUSY_CODE;
	});
});

describe("parent identity and quit", () => {
	it("creates stable parent ids for same pane/workspace/cwd", () => {
		const cwd = tempDir("momo-stable-cwd-");
		const a = createStableParentId({ paneId: "w1:p1", workspaceId: "ws", cwd });
		const b = createStableParentId({ paneId: "w1:p1", workspaceId: "ws", cwd });
		const c = createStableParentId({ paneId: "w1:p2", workspaceId: "ws", cwd });
		expect(a).toBe(b);
		expect(a).not.toBe(c);
	});

	it("session_shutdown writes cancel IPC for active workers", async () => {
		const cwd = tempDir("momo-quit-cwd-");
		const cacheRoot = tempDir("momo-quit-cache-");
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "scout", assignmentId: "shutdown01" });
		const parentEpoch = "shutdown-epoch";
		fixture.pool.upsert({
			workerId: fixture.workerId,
			generation: fixture.generation,
			generationTombstone: fixture.generation,
			role: "scout",
			paneId: "w1:p2",
			agentName: "momo_scout",
			status: "busy",
			activeAssignmentId: fixture.assignmentId,
			activeParentEpoch: parentEpoch,
			updatedAt: new Date().toISOString(),
		});
		const keys: string[][] = [];
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "agent" && args[1] === "send-keys") {
					keys.push([...args]);
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const pi = createFakePi();
		installMomoParent(pi as unknown as ExtensionAPI, {
			cwd,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "stableparent",
				MOMO_PARENT_EPOCH: parentEpoch,
				HERDR_ENV: "1",
				HERDR_PANE_ID: "w1:p1",
				HERDR_WORKSPACE_ID: "test-ws",
				HERDR_SOCKET_PATH: "test-sock",
			},
			client,
			poolRegistry: fixture.pool,
			parentEpoch,
		});
		await pi.emit("session_shutdown", {});
		expect(tryReadIpcJson(fixture.paths.cancel)).toMatchObject({
			reason: "parent_session_shutdown",
			workerId: fixture.workerId,
		});
		expect(keys).toHaveLength(0);
	});

	it("session_shutdown writes no cancel/keys for missing or foreign activeParentEpoch", async () => {
		const cwd = tempDir("momo-quit-epoch-cwd-");
		const cacheRoot = tempDir("momo-quit-epoch-cache-");
		const missing = setupPoolWorkerFixture({
			cacheRoot,
			cwd,
			role: "scout",
			assignmentId: "shutmiss01",
		});
		const foreign = setupPoolWorkerFixture({
			cacheRoot,
			cwd,
			role: "planner",
			assignmentId: "shutforn01",
		});
		missing.pool.upsert({
			workerId: missing.workerId,
			generation: missing.generation,
			generationTombstone: missing.generation,
			role: "scout",
			paneId: "w1:p2",
			agentName: "momo_scout",
			status: "busy",
			activeAssignmentId: missing.assignmentId,
			updatedAt: new Date().toISOString(),
		});
		missing.pool.upsert({
			workerId: foreign.workerId,
			generation: foreign.generation,
			generationTombstone: foreign.generation,
			role: "planner",
			paneId: "w1:p3",
			agentName: "momo_planner",
			status: "busy",
			activeAssignmentId: foreign.assignmentId,
			activeParentEpoch: "other-epoch",
			updatedAt: new Date().toISOString(),
		});
		const keys: string[][] = [];
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "agent" && args[1] === "send-keys") {
					keys.push([...args]);
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const { cancelEpochAssignmentsOnQuit } = await import("../src/extensions/parent.js");
		await cancelEpochAssignmentsOnQuit(missing.pool, "shutdown-epoch", client);
		expect(tryReadIpcJson(missing.paths.cancel)).toBeUndefined();
		expect(tryReadIpcJson(foreign.paths.cancel)).toBeUndefined();
		expect(keys).toHaveLength(0);
	});
});

describe("IPC production validation", () => {
	it("wraps corrupt JSON/mode and validates heartbeat/result deeply", () => {
		const root = tempDir("momo-ipc-hard-");
		const file = path.join(root, "bad.json");
		writeFileSync(file, "{nope", { mode: 0o600 });
		expect(() => parseJsonFile(file)).toThrow(IpcValidationError);

		const loose = path.join(root, "loose.json");
		atomicWriteJson(loose, { version: 1, runId: "r", workerId: "w", at: "t", seq: 1 });
		chmodSync(loose, 0o644);
		expect(() => parseJsonFile(loose)).toThrow(/permissions/);

		expect(
			validateHeartbeat(
				{ version: 1, runId: "r", workerId: "w", at: "t", seq: 2 },
				{ runId: "r", workerId: "w" },
			).seq,
		).toBe(2);

		expect(() =>
			validateResult(
				{
					version: 1,
					runId: "r",
					workerId: "w",
					status: "completed",
					messages: [
						{
							role: "assistant",
							content: [{ type: "text", text: "ok" }],
							thinking: "nope",
						},
					],
					finishedAt: "t",
				},
				{ runId: "r", workerId: "w" },
			),
		).toThrow(/forbidden fields/);
	});

	it("fails closed on corrupt registry JSON", () => {
		const cacheRoot = tempDir("momo-reg-corrupt-");
		const registry = new PaneRegistry("p", cacheRoot);
		writeFileSync(registry.filePath, "{bad", "utf8");
		expect(() => registry.read()).toThrow(RegistryCorruptionError);
		const soft = registry.tryRead();
		expect(soft.corruption).toMatch(/corrupt/i);
		expect(soft.snapshot.panes).toEqual([]);
	});
});

describe("worker input policy", () => {
	it("rejects interactive input while queued/running; allows read-only after settlement", async () => {
		vi.useFakeTimers();
		const cwd = tempDir("momo-input-cwd-");
		const cacheRoot = tempDir("momo-input-cache-");
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "scout", assignmentId: "input001" });
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: fixture.env,
			poolRegistry: fixture.pool,
		});
		const notify = vi.fn();
		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
			ui: { notify },
		} as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);
		const rejected = await pi.emit(
			"input",
			{ type: "input", text: "hi", source: "interactive" },
			ctx,
		);
		expect(rejected).toEqual({ action: "handled" });
		expect(notify).toHaveBeenCalled();

		dispatchAssignment({
			controlRoot: fixture.control.root, paths: fixture.paths, assignmentId: fixture.assignmentId,
			workerId: fixture.workerId, generation: fixture.generation, task: "look",
		});
		await vi.advanceTimersByTimeAsync(150);
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
		const rejectedAfterSettlement = await pi.emit(
			"input",
			{ type: "input", text: "follow up", source: "interactive" },
			ctx,
		);
		expect(rejectedAfterSettlement).toEqual({ action: "handled" });
		expect(pi.setActiveTools).toHaveBeenCalledWith(["read", "grep", "find", "ls"]);
	});
});

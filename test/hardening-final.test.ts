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
import { atomicWriteJson } from "../src/ipc/spool.js";
import {
	IpcValidationError,
	parseJsonFile,
	tryReadIpcJson,
	validateHeartbeat,
	validateResult,
} from "../src/ipc/validate.js";
import { getRole } from "../src/roles.js";

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
	it("releases lease before clean completed result; consecutive implementer can acquire", async () => {
		vi.useFakeTimers();
		const cwd = tempDir("momo-settle-cwd-");
		const cacheRoot = tempDir("momo-settle-cache-");
		const lease = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });

		async function runImplementer(workerId: string): Promise<void> {
			const ipcDir = tempDir(`momo-settle-ipc-${workerId}-`);
			const pi = createFakePi();
			installMomoWorker(pi as unknown as ExtensionAPI, {
				env: {
					MOMO_WORKER: "1",
					MOMO_ROLE: "implementer",
					MOMO_IPC_DIR: ipcDir,
					MOMO_WORKER_ID: workerId,
					MOMO_RUN_ID: "run1",
					MOMO_CWD: cwd,
				},
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
			atomicWriteJson(path.join(ipcDir, "command.json"), {
				version: 1,
				type: "prompt",
				task: "edit",
				issuedAt: new Date().toISOString(),
				runId: "run1",
				workerId,
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
			const result = parseJsonFile(path.join(ipcDir, "result.json")) as {
				status: string;
				uncertainWrite?: boolean;
			};
			expect(result.status).toBe("completed");
			expect(result.uncertainWrite).toBeUndefined();
			expect(lease.isLocked(cwd)).toBe(false);
		}

		await runImplementer("impl_a");
		await runImplementer("impl_b");
	});

	it("marks failed+uncertain and retains lock when release fails", async () => {
		vi.useFakeTimers();
		const cwd = tempDir("momo-release-fail-cwd-");
		const cacheRoot = tempDir("momo-release-fail-cache-");
		const lease = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });
		const token = createLeaseToken();
		const ipcDir = tempDir("momo-release-fail-ipc-");
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "implementer",
				MOMO_IPC_DIR: ipcDir,
				MOMO_WORKER_ID: "impl_x",
				MOMO_RUN_ID: "runx",
				MOMO_CWD: cwd,
			},
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
		atomicWriteJson(path.join(ipcDir, "command.json"), {
			version: 1,
			type: "prompt",
			task: "edit",
			issuedAt: new Date().toISOString(),
			runId: "runx",
			workerId: "impl_x",
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
		const result = parseJsonFile(path.join(ipcDir, "result.json")) as {
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
		const ipcDir = tempDir("momo-abort-ipc-");
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "implementer",
				MOMO_IPC_DIR: ipcDir,
				MOMO_WORKER_ID: "impl_ab",
				MOMO_RUN_ID: "runab",
				MOMO_CWD: cwd,
			},
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
		atomicWriteJson(path.join(ipcDir, "cancel.json"), {
			version: 1,
			runId: "runab",
			workerId: "impl_ab",
			reason: "parent_abort",
			issuedAt: new Date().toISOString(),
		});
		await vi.advanceTimersByTimeAsync(200);
		const before = parseJsonFile(path.join(ipcDir, "result.json")) as {
			status: string;
			uncertainWrite?: boolean;
		};
		expect(before.status).toBe("aborted");
		expect(before.uncertainWrite).toBeUndefined();
		expect(lease.isLocked(cwd)).toBe(false);

		// After-write path: mutate then abort settle.
		const ipc2 = tempDir("momo-abort2-ipc-");
		const pi2 = createFakePi();
		installMomoWorker(pi2 as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "implementer",
				MOMO_IPC_DIR: ipc2,
				MOMO_WORKER_ID: "impl_ab2",
				MOMO_RUN_ID: "runab2",
				MOMO_CWD: cwd,
			},
			leaseManager: lease,
			now: () => 1_000,
		});
		await pi2.emit("session_start", {}, ctx);
		atomicWriteJson(path.join(ipc2, "command.json"), {
			version: 1,
			type: "prompt",
			task: "edit",
			issuedAt: new Date().toISOString(),
			runId: "runab2",
			workerId: "impl_ab2",
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
		const after = parseJsonFile(path.join(ipc2, "result.json")) as {
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
		const registry = new PaneRegistry("parent-roll", cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: "parent-roll",
			client,
			registry,
			cacheRoot,
			readyTimeoutMs: 500,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});
		await expect(factory({ cwd, role: getRole("scout") })).rejects.toThrow(/start failed/);
		expect(closed).toEqual(["w1:p9"]);
		expect(registry.list()).toHaveLength(0);
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
		const registry = new PaneRegistry("parent-ready", cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: "parent-ready",
			client,
			registry,
			cacheRoot,
			readyTimeoutMs: 80,
			pollIntervalMs: 20,
			now: () => Date.now(),
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});
		await expect(factory({ cwd, role: getRole("scout") })).rejects.toThrow(/did not become ready/);
		expect(registry.list()[0]?.status).toBe("failed");
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
		const registry = new PaneRegistry("parent-c", cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: "parent-c",
			client,
			registry,
			cacheRoot,
			pollIntervalMs: 20,
			readyTimeoutMs: 2_000,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});
		const scout = await factory({ cwd, role: getRole("scout") });
		await scout.prompt("x");
		await scout.abort();
		expect(registry.list().find((p) => p.role === "scout")?.status).toBe("failed");
		expect(registry.list().find((p) => p.role === "scout")?.uncertainWrite).toBeUndefined();

		const impl = await factory({ cwd, role: getRole("implementer") });
		await impl.prompt("y");
		await impl.abort();
		expect(registry.list().find((p) => p.role === "implementer")?.status).toBe("uncertain");
		expect(gets).toBeGreaterThan(0);
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
		const spool = tempDir("momo-quit-spool-");
		const registry = new PaneRegistry("stableparent", cacheRoot);
		registry.upsert({
			workerId: "scout_1",
			runId: "r1",
			role: "scout",
			paneId: "w1:p2",
			agentName: "momo_scout_1",
			spoolRoot: spool,
			cwd,
			status: "running",
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
				HERDR_ENV: "1",
				HERDR_PANE_ID: "w1:p1",
			},
			client,
			registry,
		});
		await pi.emit("session_shutdown", {});
		expect(tryReadIpcJson(path.join(spool, "cancel.json"))).toMatchObject({
			reason: "parent_session_shutdown",
			workerId: "scout_1",
		});
		expect(keys[0]).toEqual(expect.arrayContaining(["agent", "send-keys", "momo_scout_1", "ctrl+c"]));
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
		const ipcDir = tempDir("momo-input-ipc-");
		const cwd = tempDir("momo-input-cwd-");
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "scout",
				MOMO_IPC_DIR: ipcDir,
				MOMO_WORKER_ID: "scout_in",
				MOMO_RUN_ID: "runin",
				MOMO_CWD: cwd,
			},
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

		atomicWriteJson(path.join(ipcDir, "command.json"), {
			version: 1,
			type: "prompt",
			task: "look",
			issuedAt: new Date().toISOString(),
			runId: "runin",
			workerId: "scout_in",
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
		const allowed = await pi.emit(
			"input",
			{ type: "input", text: "follow up", source: "interactive" },
			ctx,
		);
		expect(allowed).toEqual({ action: "continue" });
		expect(pi.setActiveTools).toHaveBeenCalledWith(["read", "grep", "find", "ls"]);
	});
});

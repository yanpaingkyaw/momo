import { mkdtempSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { installMomoWorker } from "../src/extensions/worker-runtime.js";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { WriterLeaseManager } from "../src/lease/writer-lease.js";
import { MAX_IPC_JSON_BYTES, atomicWriteJson, readJsonFile } from "../src/ipc/spool.js";
import { sanitizeAssistantMessages } from "../src/ipc/validate.js";

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

describe("worker runtime", () => {
	it("command file -> sendUserMessage -> message_end -> agent_settled -> result", async () => {
		vi.useFakeTimers();
		const ipcDir = tempDir("momo-worker-ipc-");
		const cwd = tempDir("momo-worker-cwd-");
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "scout",
				MOMO_IPC_DIR: ipcDir,
				MOMO_WORKER_ID: "scout_test1",
				MOMO_RUN_ID: "runtest1",
				MOMO_CWD: cwd,
			},
			now: () => 1_700_000_000_000,
		});

		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;

		await pi.emit("session_start", {}, ctx);
		expect(existsSync(path.join(ipcDir, "ready.json"))).toBe(true);

		atomicWriteJson(path.join(ipcDir, "command.json"), {
			version: 1,
			type: "prompt",
			task: "inspect auth",
			issuedAt: new Date().toISOString(),
			runId: "runtest1",
			workerId: "scout_test1",
		});

		await vi.advanceTimersByTimeAsync(150);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("inspect auth");

		await pi.emit(
			"message_end",
			{
				message: {
					role: "assistant",
					content: [{ type: "text", text: "found login.ts" }],
					stopReason: "stop",
					thinking: "secret",
					toolCalls: [{ name: "read", arguments: { path: "/secret" } }],
				},
			},
			ctx,
		);
		await pi.emit("agent_settled", {}, ctx);

		const result = readJsonFile(path.join(ipcDir, "result.json")) as {
			status: string;
			messages: Array<{ content: unknown; thinking?: unknown; toolCalls?: unknown }>;
		};
		expect(result.status).toBe("completed");
		expect(result.messages[0]?.content).toEqual([{ type: "text", text: "found login.ts" }]);
		expect(result.messages[0]?.thinking).toBeUndefined();
		expect(result.messages[0]?.toolCalls).toBeUndefined();
	});

	it("user_bash returns Pi 0.82.1 BashResult shape with exitCode 126", async () => {
		const ipcDir = tempDir("momo-bash-ipc-");
		const cwd = tempDir("momo-bash-cwd-");
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "implementer",
				MOMO_IPC_DIR: ipcDir,
				MOMO_WORKER_ID: "impl_bash",
				MOMO_RUN_ID: "runbash",
				MOMO_CWD: cwd,
			},
			leaseManager: new WriterLeaseManager({ cacheRoot: tempDir("momo-lease-cache-") }),
		});

		const bashResult = await pi.emit("user_bash", {
			type: "user_bash",
			command: "echo hi",
			excludeFromContext: false,
			cwd,
		});
		expect(bashResult).toEqual({
			result: {
				output: "Direct user_bash is disabled in Momo workers",
				exitCode: 126,
				cancelled: false,
				truncated: false,
			},
		});
		expect(bashResult).not.toHaveProperty("block");
	});

	it("defers action methods until session_start (read-only tools only)", async () => {
		const ipcDir = tempDir("momo-tools-ipc-");
		const cwd = tempDir("momo-tools-cwd-");
		const pi = createFakePi();
		expect(() =>
			installMomoWorker(pi as unknown as ExtensionAPI, {
				env: {
					MOMO_WORKER: "1",
					MOMO_ROLE: "implementer",
					MOMO_IPC_DIR: ipcDir,
					MOMO_WORKER_ID: "impl_tools",
					MOMO_RUN_ID: "runtools",
					MOMO_CWD: cwd,
				},
				leaseManager: new WriterLeaseManager({
					cacheRoot: tempDir("momo-tools-lease-"),
					now: () => 1_000,
				}),
				now: () => 1_000,
			}),
		).not.toThrow();
		expect(pi.setActiveTools).not.toHaveBeenCalled();
		expect(pi.sendUserMessage).not.toHaveBeenCalled();
		expect(pi.runtimeReady).toBe(false);

		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);
		expect(pi.setActiveTools).toHaveBeenCalled();
		const tools = pi.setActiveTools.mock.calls[0]?.[0];
		expect(tools).toEqual(["read", "grep", "find", "ls"]);
	});

	it("sanitizes and bounds >256KiB model output before result write", () => {
		const huge = "x".repeat(MAX_IPC_JSON_BYTES + 50_000);
		const sanitized = sanitizeAssistantMessages(
			[
				{
					role: "assistant",
					content: [{ type: "text", text: huge }],
					thinking: "nope",
					stopReason: "stop",
				},
			],
			Math.floor(MAX_IPC_JSON_BYTES * 0.75),
		);
		const encoded = Buffer.byteLength(JSON.stringify(sanitized), "utf8");
		expect(encoded).toBeLessThan(MAX_IPC_JSON_BYTES);
		expect(JSON.stringify(sanitized)).not.toContain("thinking");
		expect((sanitized[0] as { content: { text: string }[] }).content[0]?.text).toContain(
			"truncated",
		);
	});

	it("cancel-before-prompt writes aborted terminal result", async () => {
		vi.useFakeTimers();
		const ipcDir = tempDir("momo-cancel-ipc-");
		const cwd = tempDir("momo-cancel-cwd-");
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "scout",
				MOMO_IPC_DIR: ipcDir,
				MOMO_WORKER_ID: "scout_cancel",
				MOMO_RUN_ID: "runcancel",
				MOMO_CWD: cwd,
			},
			now: () => Date.now(),
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
			runId: "runcancel",
			workerId: "scout_cancel",
			reason: "parent_abort",
			issuedAt: new Date().toISOString(),
		});
		await vi.advanceTimersByTimeAsync(200);
		const result = readJsonFile(path.join(ipcDir, "result.json")) as { status: string };
		expect(result.status).toBe("aborted");
		expect(pi.sendUserMessage).not.toHaveBeenCalled();
	});
});

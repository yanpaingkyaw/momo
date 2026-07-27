import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { roleLaunchToolsCsv } from "../src/delegation/herdr-factory.js";
import { installMomoWorker } from "../src/extensions/worker-runtime.js";
import { WriterLeaseManager } from "../src/lease/writer-lease.js";
import { getRole, ROLE_LIST } from "../src/roles.js";
import { atomicWriteJson } from "../src/ipc/spool.js";
import { createHerdrChildSessionFactory } from "../src/delegation/herdr-factory.js";
import { HerdrClient } from "../src/herdr/client.js";
import { PaneRegistry } from "../src/herdr/registry.js";
import { mkdirSync, writeFileSync } from "node:fs";

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

const RUNTIME_NOT_READY =
	"Extension runtime not initialized. Action methods cannot be called during extension loading.";

function createFakePi() {
	const handlers = new Map<string, Function[]>();
	const order: string[] = [];
	let runtimeReady = false;
	const assertRuntime = () => {
		if (!runtimeReady) throw new Error(RUNTIME_NOT_READY);
	};
	return {
		order,
		handlers,
		on(event: string, handler: Function) {
			order.push(`on:${event}`);
			const list = handlers.get(event) ?? [];
			list.push(handler);
			handlers.set(event, list);
		},
		registerTool: vi.fn((tool: { name?: string }) => {
			order.push(`registerTool:${tool?.name ?? "unknown"}`);
		}),
		setActiveTools: vi.fn((tools: string[]) => {
			assertRuntime();
			order.push(`setActiveTools:${tools.join(",")}`);
		}),
		sendUserMessage: vi.fn(() => {
			assertRuntime();
		}),
		async emit(event: string, payload: unknown = {}, ctx?: ExtensionContext) {
			if (event === "session_start") runtimeReady = true;
			order.push(`emit:${event}`);
			const list = handlers.get(event) ?? [];
			for (const handler of list) {
				await handler(payload, ctx);
			}
		},
	};
}

describe("role launch tool catalogs", () => {
	it("exports exact full role tool catalogs for Pi CLI --tools", () => {
		expect(roleLaunchToolsCsv(getRole("scout"))).toBe("read,grep,find,ls");
		expect(roleLaunchToolsCsv(getRole("planner"))).toBe("read,grep,find,ls");
		expect(roleLaunchToolsCsv(getRole("implementer"))).toBe("read,grep,find,ls,bash,edit,write");
		expect(roleLaunchToolsCsv(getRole("reviewer"))).toBe("read,grep,find,ls,workspace_diff");
		for (const role of ROLE_LIST) {
			expect(roleLaunchToolsCsv(role)).not.toContain("delegate");
		}
	});

	it("puts exact catalogs on worker agent start argv per role", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-catalog-cache-");
		const cwd = tempDir("momo-catalog-cwd-");
		const catalogs = new Map<string, string>();

		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					const envArgs = args.filter((arg, index) => args[index - 1] === "--env");
					const ipcDir =
						envArgs.find((v) => v.startsWith("MOMO_IPC_DIR="))?.slice("MOMO_IPC_DIR=".length) ?? "";
					const workerId =
						envArgs.find((v) => v.startsWith("MOMO_WORKER_ID="))?.slice("MOMO_WORKER_ID=".length) ??
						"";
					const runId =
						envArgs.find((v) => v.startsWith("MOMO_RUN_ID="))?.slice("MOMO_RUN_ID=".length) ?? "";
					const role =
						envArgs.find((v) => v.startsWith("MOMO_ROLE="))?.slice("MOMO_ROLE=".length) ?? "";
					const paneId = `w1:p_${role}`;
					atomicWriteJson(path.join(ipcDir, "ready.json"), {
						version: 1,
						runId,
						workerId,
						readyAt: new Date().toISOString(),
					});
					return {
						code: 0,
						stdout: JSON.stringify({ id: "split", result: { pane: { pane_id: paneId } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					const tools = args[args.indexOf("--tools") + 1] ?? "";
					const name = args[2] ?? "";
					const paneId = args[args.indexOf("--pane") + 1] ?? "";
					const role = paneId.replace("w1:p_", "");
					catalogs.set(role, tools);
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "start",
							result: {
								pane_id: paneId,
								name,
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
			parentPaneId: "w1:p1",
			parentId: "parent-catalog",
			client,
			registry: new PaneRegistry("parent-catalog", cacheRoot),
			cacheRoot,
			pollIntervalMs: 20,
			readyTimeoutMs: 2_000,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});

		for (const role of ROLE_LIST) {
			const session = await factory({ cwd, role });
			await session.dispose();
		}

		expect(catalogs.get("scout")).toBe("read,grep,find,ls");
		expect(catalogs.get("planner")).toBe("read,grep,find,ls");
		expect(catalogs.get("implementer")).toBe("read,grep,find,ls,bash,edit,write");
		expect(catalogs.get("reviewer")).toBe("read,grep,find,ls,workspace_diff");
	});
});

describe("worker active-tool posture", () => {
	it("registers workspace_diff for reviewer before session_start", async () => {
		const ipcDir = tempDir("momo-reviewer-ipc-");
		const cwd = tempDir("momo-reviewer-cwd-");
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "reviewer",
				MOMO_IPC_DIR: ipcDir,
				MOMO_WORKER_ID: "reviewer_1",
				MOMO_RUN_ID: "run1",
				MOMO_CWD: cwd,
			},
		});
		const registerIdx = pi.order.findIndex((step) => step === "registerTool:workspace_diff");
		const sessionHandlerIdx = pi.order.findIndex((step) => step === "on:session_start");
		expect(registerIdx).toBeGreaterThanOrEqual(0);
		expect(sessionHandlerIdx).toBeGreaterThan(registerIdx);
		expect(pi.setActiveTools).not.toHaveBeenCalled();

		await pi.emit("session_start", {}, {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext);
		expect(pi.setActiveTools).toHaveBeenCalledWith(["read", "grep", "find", "ls", "workspace_diff"]);
	});

	it("keeps implementer read-only until lease after assigned prompt", async () => {
		vi.useFakeTimers();
		const ipcDir = tempDir("momo-impl-ipc-");
		const cwd = tempDir("momo-impl-cwd-");
		const cacheRoot = tempDir("momo-impl-lease-");
		const pi = createFakePi();
		const lease = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "implementer",
				MOMO_IPC_DIR: ipcDir,
				MOMO_WORKER_ID: "impl_1",
				MOMO_RUN_ID: "run1",
				MOMO_CWD: cwd,
			},
			leaseManager: lease,
			now: () => 1_000,
		});
		expect(pi.registerTool).not.toHaveBeenCalled();
		expect(pi.setActiveTools).not.toHaveBeenCalled();

		const ctx = {
			hasUI: true,
			isIdle: () => true,
			abort: vi.fn(),
			cwd,
		} as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);
		expect(pi.setActiveTools).toHaveBeenCalledTimes(1);
		expect(pi.setActiveTools.mock.calls[0]?.[0]).toEqual(["read", "grep", "find", "ls"]);

		atomicWriteJson(path.join(ipcDir, "command.json"), {
			version: 1,
			type: "prompt",
			task: "edit auth",
			issuedAt: new Date().toISOString(),
			runId: "run1",
			workerId: "impl_1",
		});
		await vi.advanceTimersByTimeAsync(150);
		expect(pi.setActiveTools.mock.calls.length).toBeGreaterThanOrEqual(2);
		expect(pi.setActiveTools.mock.calls.at(-1)?.[0]).toEqual([
			"read",
			"grep",
			"find",
			"ls",
			"bash",
			"edit",
			"write",
		]);
		expect(pi.sendUserMessage).toHaveBeenCalledWith("edit auth");
		// No write tools before the assigned command / lease path.
		const beforeLease = pi.setActiveTools.mock.calls[0]?.[0] as string[];
		expect(beforeLease).toEqual(["read", "grep", "find", "ls"]);
		expect(beforeLease).not.toEqual(expect.arrayContaining(["bash", "edit", "write"]));
	});
});

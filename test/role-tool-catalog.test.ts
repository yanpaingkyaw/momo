import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
	createHerdrChildSessionFactory,
	roleLaunchToolsCsv,
} from "../src/delegation/herdr-factory.js";
import { installMomoWorker } from "../src/extensions/worker-runtime.js";
import { WriterLeaseManager } from "../src/lease/writer-lease.js";
import { getRole, ROLE_LIST } from "../src/roles.js";
import { atomicWriteJson } from "../src/ipc/spool.js";
import { HerdrClient } from "../src/herdr/client.js";
import { PoolRegistry } from "../src/herdr/pool-registry.js";
import { resolvePoolIdentity } from "../src/herdr/pool-identity.js";
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
		sendMessage: vi.fn(() => {
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
					const envArgs = args.filter((arg, index) => args[index - 1] === "--env");
					const ipcDir =
						envArgs.find((v) => v.startsWith("MOMO_CONTROL_DIR="))?.slice("MOMO_CONTROL_DIR=".length) ??
						envArgs.find((v) => v.startsWith("MOMO_IPC_DIR="))?.slice("MOMO_IPC_DIR=".length) ??
						"";
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
					const paneId = args[args.indexOf("--pane") + 1] ?? "";
					const role = paneId.replace("w1:p_", "");
					catalogs.set(role, tools);
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "start",
							result: {
								pane_id: paneId,
								name: args[2] ?? "",
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
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			pollIntervalMs: 20,
			readyTimeoutMs: 2_000,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});

		for (const role of ROLE_LIST) {
			const session = await factory({ cwd, role });
			const proxy = session as unknown as {
				assignmentId: string;
				workerId: string;
				paths: { result: string };
				prompt: (t: string) => Promise<void>;
				dispose: () => Promise<void>;
			};
			await proxy.prompt("boot");
			atomicWriteJson(proxy.paths.result, {
				version: 1,
				runId: proxy.assignmentId,
				workerId: proxy.workerId,
				status: "completed",
				messages: [
					{
						role: "assistant",
						content: [{ type: "text", text: "ok" }],
						usage: { input: 1, output: 1, totalTokens: 2, cost: { total: 0 } },
					},
				],
				finishedAt: new Date().toISOString(),
			});
			await session.agent?.waitForIdle();
			pool.upsert({
				...pool.getByRole(role.name)!,
				status: "idle",
				updatedAt: new Date().toISOString(),
			});
			await proxy.dispose();
		}

		expect(catalogs.get("scout")).toBe("read,grep,find,ls");
		expect(catalogs.get("planner")).toBe("read,grep,find,ls");
		expect(catalogs.get("implementer")).toBe("read,grep,find,ls,bash,edit,write");
		expect(catalogs.get("reviewer")).toBe("read,grep,find,ls,workspace_diff");
	});
});

describe("worker active-tool posture", () => {
	it("registers workspace_diff for reviewer before session_start", async () => {
		const cacheRoot = tempDir("momo-reviewer-cache-");
		const cwd = tempDir("momo-reviewer-cwd-");
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "reviewer" });
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: fixture.env,
			poolRegistry: fixture.pool,
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
		const cwd = tempDir("momo-impl-cwd-");
		const cacheRoot = tempDir("momo-impl-lease-");
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "implementer" });
		const pi = createFakePi();
		const lease = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: fixture.env,
			poolRegistry: fixture.pool,
			leaseManager: lease,
			now: () => 1_000,
			sleep: async () => {},
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
		expect(pi.setActiveTools).toHaveBeenCalled();
		expect(pi.setActiveTools.mock.calls[0]?.[0]).toEqual(["read", "grep", "find", "ls"]);

		dispatchAssignment({
			controlRoot: fixture.control.root,
			paths: fixture.paths,
			assignmentId: fixture.assignmentId,
			workerId: fixture.workerId,
			generation: fixture.generation,
			task: "edit auth",
		});
		await vi.advanceTimersByTimeAsync(200);
		const lastTools = pi.setActiveTools.mock.calls.at(-1)?.[0];
		expect(lastTools).toEqual(["read", "grep", "find", "ls", "bash", "edit", "write"]);
	});
});

import { mkdtempSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createDelegationRunner } from "../src/delegation/runner.js";
import { createHerdrChildSessionFactory } from "../src/delegation/herdr-factory.js";
import { HerdrClient } from "../src/herdr/client.js";
import { PaneRegistry } from "../src/herdr/registry.js";
import { ROLE_LIST, getRole } from "../src/roles.js";
import { atomicWriteJson } from "../src/ipc/spool.js";

const tempDirs: string[] = [];
const previousPiDir = process.env.PI_CODING_AGENT_DIR;

afterEach(() => {
	while (tempDirs.length > 0) {
		const dir = tempDirs.pop();
		if (dir) rmSync(dir, { recursive: true, force: true });
	}
	if (previousPiDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
	else process.env.PI_CODING_AGENT_DIR = previousPiDir;
	vi.restoreAllMocks();
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

function agentStartStdout(paneId: string, name: string): string {
	return JSON.stringify({
		id: "start",
		result: {
			pane_id: paneId,
			name,
			agent: "pi",
			interactive_ready: true,
			agent_status: "idle",
		},
	});
}

describe("herdr factory + runner integration", () => {
	it("lazily creates one pane per role for parallel cross-role tasks", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-cache-");
		const cwd = tempDir("momo-cwd-");
		const calls: string[][] = [];
		let paneCounter = 0;

		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				calls.push([...args]);
				if (args[0] === "pane" && args[1] === "split") {
					expect(args).toContain("--direction");
					paneCounter += 1;
					const paneId = `w1:p${paneCounter}`;
					const envArgs = args.filter((arg, index) => args[index - 1] === "--env");
					const controlDir =
						envArgs
							.find((value) => value.startsWith("MOMO_CONTROL_DIR="))
							?.slice("MOMO_CONTROL_DIR=".length) ??
						envArgs
							.find((value) => value.startsWith("MOMO_IPC_DIR="))
							?.slice("MOMO_IPC_DIR=".length);
					const workerId = envArgs
						.find((value) => value.startsWith("MOMO_WORKER_ID="))
						?.slice("MOMO_WORKER_ID=".length);
					const runId = envArgs
						.find((value) => value.startsWith("MOMO_RUN_ID="))
						?.slice("MOMO_RUN_ID=".length);
					if (controlDir && workerId && runId) {
						atomicWriteJson(path.join(controlDir, "ready.json"), {
							version: 1,
							runId,
							workerId,
							readyAt: new Date().toISOString(),
						});
						atomicWriteJson(path.join(controlDir, "heartbeat.json"), {
							version: 1,
							runId,
							workerId,
							at: new Date().toISOString(),
							seq: 1,
						});
					}
					return {
						code: 0,
						stdout: JSON.stringify({ id: "split", result: { pane: { pane_id: paneId } } }),
						stderr: "",
					};
				}
				if (args[0] === "pane" && args[1] === "rename") {
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				if (args[0] === "agent" && args[1] === "start") {
					const toolsIdx = args.indexOf("--tools");
					const tools = args[toolsIdx + 1] ?? "";
					expect(tools).not.toContain("delegate");
					const paneFlag = args.indexOf("--pane");
					const paneId = args[paneFlag + 1] ?? "";
					const name = args[2] ?? "";
					return { code: 0, stdout: agentStartStdout(paneId, name), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});

		const { PoolRegistry } = await import("../src/herdr/pool-registry.js");
		const { resolvePoolIdentity } = await import("../src/herdr/pool-identity.js");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: "parent1",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			pollIntervalMs: 20,
			readyTimeoutMs: 2_000,
			sleep: async (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
		});

		const created: string[] = [];
		const runner = createDelegationRunner({
			cwd,
			roles: ROLE_LIST,
			createChildSession: async (input) => {
				const session = await factory(input);
				created.push(input.role.name);
				const originalPrompt = session.prompt.bind(session);
				session.prompt = async (text: string) => {
					await originalPrompt(text);
					const proxy = session as unknown as {
						paths: { result: string };
						assignmentId: string;
						workerId: string;
					};
					atomicWriteJson(proxy.paths.result, {
						version: 1,
						runId: proxy.assignmentId,
						workerId: proxy.workerId,
						status: "completed",
						messages: [
							{
								role: "assistant",
								content: [{ type: "text", text: `${input.role.name}:${text}` }],
								usage: { input: 1, output: 1, totalTokens: 2, cost: { total: 0 } },
							},
						],
						finishedAt: new Date().toISOString(),
					});
				};
				return session;
			},
		});

		const result = await runner.run({
			mode: "parallel",
			tasks: [
				{ agent: "scout", task: "a" },
				{ agent: "planner", task: "b" },
			],
		});

		expect(created).toEqual(["scout", "planner"]);
		expect(paneCounter).toBe(2);
		expect(result.status).toBe("completed");
		expect(result.results.map((task) => task.status)).toEqual(["completed", "completed"]);
		expect(calls.some((args) => args[0] === "pane" && args[1] === "close")).toBe(false);
	});

	it("terminal-skips queued workers on parallel cancel and failed-chain tails", async () => {
		const cacheRoot = tempDir("momo-skip-cache-");
		const cwd = tempDir("momo-skip-cwd-");
		const registry = new PaneRegistry("parent-skip", cacheRoot);
		let n = 0;
		const sessions: Array<{
			skip: ReturnType<typeof vi.fn>;
			dispose: ReturnType<typeof vi.fn>;
		}> = [];

		const runner = createDelegationRunner({
			cwd,
			roles: ROLE_LIST,
			createChildSession: async ({ role }) => {
				n += 1;
				const workerId = `${role.name}_${n}`;
				const skip = vi.fn(async () => {
					registry.upsert({
						workerId,
						runId: "run",
						role: role.name,
						paneId: `w1:p${n}`,
						agentName: `momo_${workerId}`,
						spoolRoot: `/tmp/${workerId}`,
						cwd,

						status: "aborted",
						updatedAt: new Date().toISOString(),
					});
				});
				const dispose = vi.fn(async () => {});
				const session = {
					messages: [
						{
							role: "assistant",
							content: [{ type: "text", text: "done" }],
							stopReason: "stop",
						},
					],
					agent: { waitForIdle: async () => {} },
					subscribe: () => () => {},
					prompt: async () => {},
					abort: vi.fn(async () => {}),
					skip,
					dispose,
				};
				sessions.push({ skip, dispose });
				registry.upsert({
					workerId,
					runId: "run",
					role: role.name,
					paneId: `w1:p${n}`,
					agentName: `momo_${workerId}`,
					spoolRoot: `/tmp/${workerId}`,
						cwd,

					status: "ready",
					updatedAt: new Date().toISOString(),
				});
				return session;
			},
		});

		const controller = new AbortController();
		const parallelPromise = runner.run(
			{
				mode: "parallel",
				tasks: [
					{ agent: "scout", task: "a" },
					{ agent: "planner", task: "b" },
					{ agent: "reviewer", task: "c" },
				],
			},
			{ signal: controller.signal },
		);
		// Abort after preparation begins so some workers may be skipped while queued.
		controller.abort();
		const parallel = await parallelPromise;
		expect(parallel.results.some((r) => r.status === "skipped" || r.status === "aborted")).toBe(
			true,
		);
		expect(sessions.some((s) => s.skip.mock.calls.length > 0)).toBe(true);

		sessions.length = 0;
		n = 0;
		const failRunner = createDelegationRunner({
			cwd,
			roles: ROLE_LIST,
			createChildSession: async ({ role }) => {
				n += 1;
				const workerId = `${role.name}_${n}`;
				const skip = vi.fn(async () => {
					registry.upsert({
						workerId,
						runId: "run2",
						role: role.name,
						paneId: `w1:p${n}`,
						agentName: `momo_${workerId}`,
						spoolRoot: `/tmp/${workerId}`,
						cwd,

						status: "aborted",
						updatedAt: new Date().toISOString(),
					});
				});
				sessions.push({ skip, dispose: vi.fn(async () => {}) });
				return {
					messages: [],
					agent: {
						waitForIdle: async () => {
							throw Object.assign(new Error("boom"), { stopReason: "error" });
						},
					},
					subscribe: () => () => {},
					prompt: async () => {},
					abort: async () => {},
					skip,
					dispose: async () => {},
				};
			},
		});
		const chain = await failRunner.run({
			mode: "chain",
			steps: [
				{ agent: "scout", task: "first" },
				{ agent: "planner", task: "second {previous}" },
			],
		});
		expect(chain.results[0]?.status).toBe("failed");
		expect(chain.results[1]?.status).toBe("skipped");
		// Lazy chain: second step never allocated, so no skip IPC.
		expect(sessions).toHaveLength(1);
	});

	it("preserves sibling results when parallel allocation partially fails", async () => {
		const cwd = tempDir("momo-alloc-cwd-");
		const prompts: string[] = [];
		let count = 0;
		const runner = createDelegationRunner({
			cwd,
			roles: ROLE_LIST,
			createChildSession: async ({ role }) => {
				count += 1;
				if (role.name === "planner") throw new Error("split failed");
				return {
					messages: [
						{
							role: "assistant",
							content: [{ type: "text", text: `${role.name}-ok` }],
							usage: { input: 1, output: 1, totalTokens: 2, cost: { total: 0 } },
						},
					],
					agent: { waitForIdle: async () => {} },
					subscribe: () => () => {},
					prompt: async (text: string) => {
						prompts.push(`${role.name}:${text}`);
					},
					abort: async () => {},
					dispose: async () => {},
				};
			},
		});

		const result = await runner.run({
			mode: "parallel",
			tasks: [
				{ agent: "scout", task: "a" },
				{ agent: "planner", task: "b" },
				{ agent: "reviewer", task: "c" },
			],
		});
		expect(result.status).toBe("partial");
		expect(result.results.map((task) => task.status)).toEqual([
			"completed",
			"failed",
			"completed",
		]);
		expect(result.results[1]?.error?.message).toMatch(/split failed/);
		expect(prompts).toHaveLength(2);
		expect(prompts).toContain("scout:a");
		expect(prompts).toContain("reviewer:c");
		expect(count).toBe(3);
	});

	it("returns failed TaskResult for single allocation failure", async () => {
		const cwd = tempDir("momo-alloc-single-");
		const runner = createDelegationRunner({
			cwd,
			roles: ROLE_LIST,
			createChildSession: async () => {
				throw new Error("pane split failed");
			},
		});
		const result = await runner.run({
			mode: "single",
			agent: "scout",
			task: "x",
		});
		expect(result.status).toBe("failed");
		expect(result.results).toHaveLength(1);
		expect(result.results[0]?.status).toBe("failed");
		expect(result.results[0]?.error?.message).toMatch(/pane split failed/);
	});

	it("skips chain tails when a later step fails to allocate", async () => {
		const cwd = tempDir("momo-alloc-chain-");
		let count = 0;
		const runner = createDelegationRunner({
			cwd,
			roles: ROLE_LIST,
			createChildSession: async ({ role }) => {
				count += 1;
				if (role.name === "planner") throw new Error("later alloc failed");
				return {
					messages: [
						{
							role: "assistant",
							content: [{ type: "text", text: "scout-ok" }],
							usage: { input: 1, output: 1, totalTokens: 2, cost: { total: 0 } },
						},
					],
					agent: { waitForIdle: async () => {} },
					subscribe: () => () => {},
					prompt: async () => {},
					abort: async () => {},
					dispose: async () => {},
				};
			},
		});
		const result = await runner.run({
			mode: "chain",
			steps: [
				{ agent: "scout", task: "first" },
				{ agent: "planner", task: "second {previous}" },
				{ agent: "reviewer", task: "third" },
			],
		});
		expect(result.results.map((task) => task.status)).toEqual([
			"completed",
			"failed",
			"skipped",
		]);
		expect(result.results[1]?.error?.message).toMatch(/later alloc failed/);
		expect(count).toBe(2);
	});

	it("preserves assistant output and stopReason when remote wait fails", async () => {
		const cwd = tempDir("momo-preserve-cwd-");
		const runner = createDelegationRunner({
			cwd,
			roles: ROLE_LIST,
			createChildSession: async () => ({
				messages: [
					{
						role: "assistant",
						content: [{ type: "text", text: "partial findings" }],
						stopReason: "error",
						errorMessage: "model exploded",
					},
				],
				agent: {
					waitForIdle: async () => {
						throw Object.assign(new Error("remote wait failed"), { stopReason: "error" });
					},
				},
				subscribe: () => () => {},
				prompt: async () => {},
				abort: async () => {},
				dispose: async () => {},
			}),
		});

		const result = await runner.run({ mode: "single", agent: "scout", task: "look" });
		expect(result.results[0]?.status).toBe("failed");
		expect(result.results[0]?.output).toContain("partial findings");
		expect(result.results[0]?.error?.stopReason).toBe("error");
	});

	it("keeps role metadata for worker env", () => {
		expect(getRole("implementer").canWrite).toBe(true);
		expect(getRole("reviewer").tools).toContain("workspace_diff");
		expect(getRole("scout").tools).not.toContain("bash");
	});

	it("skip before prompt never creates a pane", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-term-cache-");
		const cwd = tempDir("momo-term-cwd-");
		let splits = 0;
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					splits += 1;
					return {
						code: 0,
						stdout: JSON.stringify({ id: "split", result: { pane: { pane_id: "w1:p2" } } }),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const { PoolRegistry } = await import("../src/herdr/pool-registry.js");
		const { resolvePoolIdentity } = await import("../src/herdr/pool-identity.js");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: "parent-term",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			pollIntervalMs: 20,
			readyTimeoutMs: 2_000,
			sleep: async (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
		});
		const session = await factory({ cwd, role: getRole("scout") });
		await session.skip?.("parallel_skipped");
		await session.dispose();
		expect(splits).toBe(0);
		expect(pool.list()).toHaveLength(0);
	});
});

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
			maxParallelConcurrency: 1,
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
					prompt: async () => {
						controller.abort();
					},
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
		const parallel = await runner.run(
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
		expect(parallel.results.some((r) => r.status === "skipped" || r.status === "aborted")).toBe(
			true,
		);
		// Lazy parallel + concurrency 1: abort during first prompt skips remaining allocations.
		expect(sessions.length).toBe(1);
		expect(parallel.results.filter((r) => r.status === "skipped")).toHaveLength(2);

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
			maxParallelConcurrency: 2,
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

	it("bounds concurrent createChildSession by maxParallelConcurrency", async () => {
		const cwd = tempDir("momo-bound-cwd-");
		let activeCreates = 0;
		let maxCreates = 0;
		const gates: Array<() => void> = [];
		const runner = createDelegationRunner({
			cwd,
			roles: ROLE_LIST,
			maxParallelConcurrency: 2,
			createChildSession: async ({ role }) => {
				activeCreates += 1;
				maxCreates = Math.max(maxCreates, activeCreates);
				await new Promise<void>((resolve) => gates.push(resolve));
				activeCreates -= 1;
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
					prompt: async () => {},
					abort: async () => {},
					dispose: async () => {},
				};
			},
		});
		const run = runner.run({
			mode: "parallel",
			tasks: [
				{ agent: "scout", task: "a" },
				{ agent: "planner", task: "b" },
				{ agent: "reviewer", task: "c" },
			],
		});
		const waitForGates = async (n: number) => {
			for (let i = 0; i < 100; i++) {
				if (gates.length >= n) return;
				await new Promise((r) => setTimeout(r, 0));
			}
			throw new Error(`expected ${n} gates, got ${gates.length}`);
		};
		await waitForGates(2);
		expect(maxCreates).toBe(2);
		expect(activeCreates).toBe(2);
		gates.shift()?.();
		gates.shift()?.();
		await waitForGates(1);
		expect(maxCreates).toBe(2);
		gates.shift()?.();
		await run;
		expect(maxCreates).toBe(2);
	});

	it("abort prevents tail createChildSession allocations", async () => {
		const cwd = tempDir("momo-abort-alloc-");
		const created: string[] = [];
		const controller = new AbortController();
		const runner = createDelegationRunner({
			cwd,
			roles: ROLE_LIST,
			maxParallelConcurrency: 1,
			createChildSession: async ({ role }) => {
				created.push(role.name);
				if (role.name === "scout") controller.abort();
				return {
					messages: [
						{
							role: "assistant",
							content: [{ type: "text", text: "ok" }],
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
		const result = await runner.run(
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
		expect(created).toEqual(["scout"]);
		expect(result.results.map((r) => r.status)).toEqual(["aborted", "skipped", "skipped"]);
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

	it("fails after heartbeatStaleMs when control heartbeat never appears", async () => {
		installFakeHerdrExtension();
		vi.useFakeTimers({ shouldAdvanceTime: true });
		const cacheRoot = tempDir("momo-miss-hb-cache-");
		const cwd = tempDir("momo-miss-hb-cwd-");
		let now = 1_000_000;
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
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
							readyAt: new Date(now).toISOString(),
						});
						// Intentionally no heartbeat.json
					}
					return {
						code: 0,
						stdout: JSON.stringify({ id: "split", result: { pane: { pane_id: "w1:p2" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 0,
						stdout: agentStartStdout("w1:p2", String(args[2])),
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
			parentId: "parent-miss-hb",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			heartbeatStaleMs: 200,
			pollIntervalMs: 20,
			readyTimeoutMs: 2_000,
			resultTimeoutMs: 60_000,
			now: () => now,
			sleep: async (ms) => {
				now += ms;
				await vi.advanceTimersByTimeAsync(ms);
			},
		});
		const session = await factory({ cwd, role: getRole("scout") });
		const promptPromise = session.prompt("go").then(() => session.agent!.waitForIdle());
		await vi.advanceTimersByTimeAsync(50);
		await expect(promptPromise).rejects.toThrow(/heartbeat went stale/i);
		vi.useRealTimers();
	});

	it("settles valid result even when events.ndjson is oversized", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-evt-result-cache-");
		const cwd = tempDir("momo-evt-result-cwd-");
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
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
						stdout: JSON.stringify({ id: "split", result: { pane: { pane_id: "w1:p3" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 0,
						stdout: agentStartStdout("w1:p3", String(args[2])),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const { PoolRegistry } = await import("../src/herdr/pool-registry.js");
		const { resolvePoolIdentity } = await import("../src/herdr/pool-identity.js");
		const { MAX_EVENTS_FILE_BYTES } = await import("../src/ipc/spool.js");
		const { writeFileSync } = await import("node:fs");
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
			parentId: "parent-evt-result",
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
		const proxy = session as unknown as {
			paths: { events: string; result: string; root: string };
			assignmentId: string;
			workerId: string;
		};
		const wait = session.prompt("go").then(() => session.agent!.waitForIdle());
		for (let i = 0; i < 50; i++) {
			if (proxy.paths.root) {
				const { mkdirSync, existsSync } = await import("node:fs");
				if (!existsSync(proxy.paths.root)) mkdirSync(proxy.paths.root, { recursive: true, mode: 0o700 });
				writeFileSync(proxy.paths.events, "x".repeat(MAX_EVENTS_FILE_BYTES + 8), { mode: 0o600 });
				atomicWriteJson(proxy.paths.result, {
					version: 1,
					runId: proxy.assignmentId,
					workerId: proxy.workerId,
					status: "completed",
					messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }] }],
					finishedAt: new Date().toISOString(),
				});
				break;
			}
			await new Promise((r) => setTimeout(r, 20));
		}
		await expect(wait).resolves.toBeUndefined();
	});

	it("event parse fault settles when a matching durable result races in", async () => {
		installFakeHerdrExtension();
		const {
			__resetIpcReadersForTest,
			__setIpcReadersForTest,
		} = await import("../src/delegation/herdr-factory.js");
		const { tryReadIpcJson: realTryRead } = await import("../src/ipc/validate.js");
		const cacheRoot = tempDir("momo-evt-race-cache-");
		const cwd = tempDir("momo-evt-race-cwd-");
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
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
						stdout: JSON.stringify({ id: "split", result: { pane: { pane_id: "w1:p9" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 0,
						stdout: agentStartStdout("w1:p9", String(args[2])),
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
			parentId: "parent-evt-race",
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
		const proxy = session as unknown as {
			paths: { events: string; root: string };
			assignmentId: string;
			workerId: string;
		};
		let resultReads = 0;
		const durable = {
			version: 1,
			runId: proxy.assignmentId,
			workerId: proxy.workerId,
			status: "completed" as const,
			messages: [{ role: "assistant", content: [{ type: "text", text: "raced" }] }],
			finishedAt: new Date().toISOString(),
		};
		__setIpcReadersForTest({
			readEventsIncrementally: () => {
				throw new Error("injected event parse fault");
			},
			tryReadIpcJson: (filePath: string) => {
				if (String(filePath).endsWith("result.json")) {
					resultReads += 1;
					// First poll check: absent. Catch re-check: durable result present.
					if (resultReads === 1) return undefined;
					return durable;
				}
				return realTryRead(filePath);
			},
		});
		try {
			const wait = session.prompt("go").then(() => session.agent!.waitForIdle());
			// Ensure events path exists so the injected parse fault is hit.
			const { mkdirSync, writeFileSync, existsSync } = await import("node:fs");
			for (let i = 0; i < 50; i++) {
				if (proxy.paths.root) {
					if (!existsSync(proxy.paths.root)) {
						mkdirSync(proxy.paths.root, { recursive: true, mode: 0o700 });
					}
					writeFileSync(proxy.paths.events, "{partial\n", { mode: 0o600 });
					break;
				}
				await new Promise((r) => setTimeout(r, 20));
			}
			await expect(wait).resolves.toBeUndefined();
			expect(resultReads).toBeGreaterThanOrEqual(2);
		} finally {
			__resetIpcReadersForTest();
		}
	});


	it("queued missing-heartbeat fails and removes queue entry without later execution", async () => {
		installFakeHerdrExtension();
		vi.useFakeTimers({ shouldAdvanceTime: true });
		const cacheRoot = tempDir("momo-q-hb-cache-");
		const cwd = tempDir("momo-q-hb-cwd-");
		let now = 1_000_000;
		const { PoolRegistry } = await import("../src/herdr/pool-registry.js");
		const { resolvePoolIdentity, stableWorkerId } = await import("../src/herdr/pool-identity.js");
		const { workerControlPaths } = await import("../src/herdr/assignment-spool.js");
		const { listQueue, queueCount } = await import("../src/herdr/role-queue.js");
		const { mkdirSync } = await import("node:fs");
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
		// Pre-seed a busy ready worker with no heartbeat — active proxy is not ours.
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p2",
			agentName: "momo_scout",
			cwd: identity.canonicalRoot,
			status: "busy",
			activeAssignmentId: "foreignactive001",
			activeParentEpoch: "other-epoch",
			updatedAt: new Date(now).toISOString(),
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
			parentPaneId: "w1:p1",
			parentId: "parent-q-hb",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			heartbeatStaleMs: 200,
			pollIntervalMs: 20,
			readyTimeoutMs: 2_000,
			resultTimeoutMs: 60_000,
			now: () => now,
			sleep: async (ms) => {
				now += ms;
				await vi.advanceTimersByTimeAsync(ms);
			},
		});
		const session = await factory({ cwd, role: getRole("scout") });
		const proxy = session as unknown as { assignmentId: string; paths: { result: string } };
		const wait = session.prompt("queued-behind").then(() => session.agent!.waitForIdle());
		await vi.advanceTimersByTimeAsync(30);
		expect(queueCount(pool.poolRoot, "scout")).toBe(1);
		expect(listQueue(pool.poolRoot, "scout")[0]?.assignmentId).toBe(proxy.assignmentId);
		await expect(wait).rejects.toThrow(/heartbeat went stale/i);
		expect(queueCount(pool.poolRoot, "scout")).toBe(0);
		expect(listQueue(pool.poolRoot, "scout")).toHaveLength(0);
		// Must not later become the active assignment.
		expect(pool.getByRole("scout")?.activeAssignmentId).toBe("foreignactive001");
		const { tryReadIpcJson, validateResult } = await import("../src/ipc/validate.js");
		const result = validateResult(tryReadIpcJson(proxy.paths.result), {
			runId: proxy.assignmentId,
			workerId,
		});
		expect(result.status).toBe("failed");
		vi.useRealTimers();
	});

	it("stopAndSettleFailure keeps reentrancy gate until lock+settle finish", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-reenter-cache-");
		const cwd = tempDir("momo-reenter-cwd-");
		const client = new HerdrClient({
			runCommand: async () => ({
				code: 0,
				stdout: JSON.stringify({ id: "ok", result: {} }),
				stderr: "",
			}),
		});
		const { PoolRegistry } = await import("../src/herdr/pool-registry.js");
		const { resolvePoolIdentity, stableWorkerId } = await import("../src/herdr/pool-identity.js");
		const { withRoleLockAsync } = await import("../src/herdr/role-queue.js");
		const { tryReadIpcJson } = await import("../src/ipc/validate.js");
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
			parentId: "parent-reenter",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const session = await factory({ cwd, role: getRole("scout") });
		const proxy = session as unknown as {
			stopAndSettleFailure: (message: string) => Promise<void>;
			paths: { cancel: string; result: string };
			assignmentId: string;
			workerId: string;
		};

		let releaseHold!: () => void;
		const holdGate = new Promise<void>((resolve) => {
			releaseHold = resolve;
		});
		const hold = withRoleLockAsync(pool.poolRoot, "scout", async () => {
			await holdGate;
		});
		await Promise.resolve();
		await Promise.resolve();

		const first = proxy.stopAndSettleFailure("first-failure");
		for (let i = 0; i < 30; i++) await Promise.resolve();
		const second = proxy.stopAndSettleFailure("second-failure");
		await Promise.resolve();

		releaseHold();
		await hold;
		await Promise.all([first, second]);

		const cancel = tryReadIpcJson(proxy.paths.cancel) as { reason?: string } | undefined;
		expect(cancel?.reason).toBe("first-failure");
		await expect(session.agent!.waitForIdle()).rejects.toThrow(/first-failure/);
		expect(stableWorkerId(identity.poolKey, "scout")).toBe(proxy.workerId);
	});

});

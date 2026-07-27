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

	it("stale-heartbeat race settles when matching result lands before failure synthesis", async () => {
		installFakeHerdrExtension();
		vi.useFakeTimers({ shouldAdvanceTime: true });
		const {
			__resetIpcReadersForTest,
			__setIpcReadersForTest,
		} = await import("../src/delegation/herdr-factory.js");
		const { tryReadIpcJson: realTryRead } = await import("../src/ipc/validate.js");
		const cacheRoot = tempDir("momo-stale-hb-race-cache-");
		const cwd = tempDir("momo-stale-hb-race-cwd-");
		let now = 1_000_000;
		const hbAt = now;
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
							readyAt: new Date(hbAt).toISOString(),
						});
						atomicWriteJson(path.join(controlDir, "heartbeat.json"), {
							version: 1,
							runId,
							workerId,
							at: new Date(hbAt).toISOString(),
							seq: 1,
						});
					}
					return {
						code: 0,
						stdout: JSON.stringify({ id: "split", result: { pane: { pane_id: "w1:p11" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 0,
						stdout: agentStartStdout("w1:p11", String(args[2])),
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
			parentId: "parent-stale-hb-race",
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
		const proxy = session as unknown as {
			assignmentId: string;
			workerId: string;
			paths: { result: string; cancel: string };
		};
		const durable = {
			version: 1,
			runId: proxy.assignmentId,
			workerId: proxy.workerId,
			status: "completed" as const,
			messages: [{ role: "assistant", content: [{ type: "text", text: "stale-race" }] }],
			finishedAt: new Date(hbAt).toISOString(),
		};
		let postStaleResultReads = 0;
		__setIpcReadersForTest({
			tryReadIpcJson: (filePath: string) => {
				if (String(filePath).endsWith("result.json")) {
					if (now - hbAt <= 200) return undefined;
					postStaleResultReads += 1;
					// Initial poll read after stale: absent. Pre-failure re-read: durable.
					if (postStaleResultReads === 1) return undefined;
					return durable;
				}
				return realTryRead(filePath);
			},
		});
		try {
			const wait = session.prompt("go").then(() => session.agent!.waitForIdle());
			await vi.advanceTimersByTimeAsync(50);
			now = hbAt + 250;
			await vi.advanceTimersByTimeAsync(50);
			await expect(wait).resolves.toBeUndefined();
			expect(postStaleResultReads).toBeGreaterThanOrEqual(2);
			expect(pool.getByRole("scout")?.status).not.toBe("unhealthy");
			const { existsSync } = await import("node:fs");
			expect(existsSync(proxy.paths.cancel)).toBe(false);
		} finally {
			__resetIpcReadersForTest();
		}
	});

	it("missing-heartbeat race settles when matching result lands before failure synthesis", async () => {
		installFakeHerdrExtension();
		vi.useFakeTimers({ shouldAdvanceTime: true });
		const {
			__resetIpcReadersForTest,
			__setIpcReadersForTest,
		} = await import("../src/delegation/herdr-factory.js");
		const { tryReadIpcJson: realTryRead } = await import("../src/ipc/validate.js");
		const cacheRoot = tempDir("momo-miss-hb-race-cache-");
		const cwd = tempDir("momo-miss-hb-race-cwd-");
		let now = 1_000_000;
		const start = now;
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
							readyAt: new Date(start).toISOString(),
						});
						// Intentionally no heartbeat.json
					}
					return {
						code: 0,
						stdout: JSON.stringify({ id: "split", result: { pane: { pane_id: "w1:p12" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 0,
						stdout: agentStartStdout("w1:p12", String(args[2])),
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
			parentId: "parent-miss-hb-race",
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
		const proxy = session as unknown as {
			assignmentId: string;
			workerId: string;
			paths: { result: string; cancel: string };
		};
		const durable = {
			version: 1,
			runId: proxy.assignmentId,
			workerId: proxy.workerId,
			status: "completed" as const,
			messages: [{ role: "assistant", content: [{ type: "text", text: "miss-race" }] }],
			finishedAt: new Date(start).toISOString(),
		};
		let postGraceResultReads = 0;
		__setIpcReadersForTest({
			tryReadIpcJson: (filePath: string) => {
				if (String(filePath).endsWith("result.json")) {
					if (now - start <= 200) return undefined;
					postGraceResultReads += 1;
					if (postGraceResultReads === 1) return undefined;
					return durable;
				}
				return realTryRead(filePath);
			},
		});
		try {
			const wait = session.prompt("go").then(() => session.agent!.waitForIdle());
			await vi.advanceTimersByTimeAsync(50);
			now = start + 250;
			await vi.advanceTimersByTimeAsync(50);
			await expect(wait).resolves.toBeUndefined();
			expect(postGraceResultReads).toBeGreaterThanOrEqual(2);
			const { existsSync } = await import("node:fs");
			expect(existsSync(proxy.paths.cancel)).toBe(false);
			expect(pool.getByRole("scout")?.status).not.toBe("unhealthy");
		} finally {
			__resetIpcReadersForTest();
		}
	});

	it("invalid raced result fails closed with validation error", async () => {
		installFakeHerdrExtension();
		vi.useFakeTimers({ shouldAdvanceTime: true });
		const {
			__resetIpcReadersForTest,
			__setIpcReadersForTest,
		} = await import("../src/delegation/herdr-factory.js");
		const { tryReadIpcJson: realTryRead } = await import("../src/ipc/validate.js");
		const cacheRoot = tempDir("momo-invalid-race-cache-");
		const cwd = tempDir("momo-invalid-race-cwd-");
		let now = 1_000_000;
		const start = now;
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
							readyAt: new Date(start).toISOString(),
						});
					}
					return {
						code: 0,
						stdout: JSON.stringify({ id: "split", result: { pane: { pane_id: "w1:p13" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 0,
						stdout: agentStartStdout("w1:p13", String(args[2])),
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
			parentId: "parent-invalid-race",
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
		const proxy = session as unknown as {
			assignmentId: string;
			workerId: string;
			paths: { result: string; cancel: string };
		};
		let postGraceResultReads = 0;
		__setIpcReadersForTest({
			tryReadIpcJson: (filePath: string) => {
				if (String(filePath).endsWith("result.json")) {
					if (now - start <= 200) return undefined;
					postGraceResultReads += 1;
					if (postGraceResultReads === 1) return undefined;
					return {
						version: 1,
						runId: "wrong-assignment-id",
						workerId: proxy.workerId,
						status: "completed",
						messages: [{ role: "assistant", content: [{ type: "text", text: "bad" }] }],
						finishedAt: new Date(start).toISOString(),
					};
				}
				return realTryRead(filePath);
			},
		});
		try {
			const wait = session.prompt("go").then(() => session.agent!.waitForIdle());
			await vi.advanceTimersByTimeAsync(50);
			now = start + 250;
			await vi.advanceTimersByTimeAsync(50);
			await expect(wait).rejects.toThrow(/identity mismatch|invalid/i);
			expect(postGraceResultReads).toBeGreaterThanOrEqual(2);
			const { existsSync } = await import("node:fs");
			expect(existsSync(proxy.paths.cancel)).toBe(true);
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

	async function expectAbortDuringProvisionPause(options: {
		label: string;
		pause: "split" | "agentStart" | "ready";
	}): Promise<void> {
		installFakeHerdrExtension();
		const cacheRoot = tempDir(`momo-abort-${options.label}-cache-`);
		const cwd = tempDir(`momo-abort-${options.label}-cwd-`);
		const closed: string[] = [];
		let releaseGate!: () => void;
		const gate = new Promise<void>((resolve) => {
			releaseGate = resolve;
		});
		let splitEntered = 0;
		let agentStartEntered = 0;
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					splitEntered += 1;
					const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
					const control =
						envArgs.find((v) => v.startsWith("MOMO_CONTROL_DIR="))?.slice("MOMO_CONTROL_DIR=".length) ??
						envArgs.find((v) => v.startsWith("MOMO_IPC_DIR="))?.slice("MOMO_IPC_DIR=".length);
					const workerId =
						envArgs.find((v) => v.startsWith("MOMO_WORKER_ID="))?.slice("MOMO_WORKER_ID=".length) ?? "";
					const runId =
						envArgs.find((v) => v.startsWith("MOMO_RUN_ID="))?.slice("MOMO_RUN_ID=".length) ?? "";
					if (options.pause === "split") await gate;
					if (control && options.pause !== "ready") {
						mkdirSync(control, { recursive: true, mode: 0o700 });
						atomicWriteJson(path.join(control, "ready.json"), {
							version: 1,
							runId,
							workerId,
							readyAt: new Date().toISOString(),
						});
					}
					return {
						code: 0,
						stdout: JSON.stringify({ id: "s", result: { pane: { pane_id: "w1:p-abort" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					agentStartEntered += 1;
					if (options.pause === "agentStart") await gate;
					return {
						code: 0,
						stdout: agentStartStdout("w1:p-abort", String(args[2])),
						stderr: "",
					};
				}
				if (args[0] === "pane" && args[1] === "close") {
					closed.push(String(args[2] ?? ""));
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const { PoolRegistry } = await import("../src/herdr/pool-registry.js");
		const { resolvePoolIdentity } = await import("../src/herdr/pool-identity.js");
		const { queueCount, listQueue } = await import("../src/herdr/role-queue.js");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: `parent-abort-${options.label}`,
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			pollIntervalMs: 20,
			readyTimeoutMs: 5_000,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});
		const session = await factory({ cwd, role: getRole("scout") });
		const proxy = session as unknown as { assignmentId: string };
		const promptPromise = session.prompt("task");

		for (let i = 0; i < 80; i += 1) {
			if (options.pause === "split" && splitEntered >= 1) break;
			if (options.pause === "agentStart" && agentStartEntered >= 1) break;
			if (options.pause === "ready" && pool.getByRole("scout")?.paneId) break;
			await new Promise((r) => setTimeout(r, 20));
		}

		const abortPromise = session.abort();
		await expect(abortPromise).resolves.toBeUndefined();
		await expect(promptPromise).resolves.toBeUndefined();

		expect(pool.getByRole("scout")?.activeAssignmentId).not.toBe(proxy.assignmentId);
		expect(queueCount(pool.poolRoot, "scout")).toBe(0);
		expect(listQueue(pool.poolRoot, "scout")).toHaveLength(0);

		releaseGate();
		// Supervised background continuation: eventual close/rollback, no orphan.
		for (let i = 0; i < 50; i += 1) {
			const record = pool.getByRole("scout");
			const archivedOrClosed =
				!record ||
				record.generation === 0 ||
				record.paneClosed === true ||
				(record.status === "unhealthy" && closed.includes("w1:p-abort")) ||
				(options.pause === "split" && (!record.paneId || record.generation === 0));
			if (archivedOrClosed && (options.pause === "split" || closed.includes("w1:p-abort") || record?.paneClosed)) {
				break;
			}
			await new Promise((r) => setTimeout(r, 20));
		}

		expect(queueCount(pool.poolRoot, "scout")).toBe(0);
		expect(pool.getByRole("scout")?.activeAssignmentId).not.toBe(proxy.assignmentId);
		if (options.pause !== "split") {
			expect(closed).toContain("w1:p-abort");
		}
		const final = pool.getByRole("scout");
		if (final?.activeAssignmentId === proxy.assignmentId) {
			throw new Error("canceled assignment must not remain active");
		}
	}

	it("abort during paused split returns bounded and rolls back without publish", async () => {
		await expectAbortDuringProvisionPause({ label: "split", pause: "split" });
	});

	it("abort during paused agentStart returns bounded and closes pane", async () => {
		await expectAbortDuringProvisionPause({ label: "agent-start", pause: "agentStart" });
	});

	it("abort during ready wait returns bounded and closes pane", async () => {
		await expectAbortDuringProvisionPause({ label: "ready", pause: "ready" });
	});

	describe("unresolved active abort registry fencing", () => {
		async function provisionAndAbort(options: {
			label: string;
			role: "scout" | "implementer";
			beforeAbort?: (ctx: {
				pool: import("../src/herdr/pool-registry.js").PoolRegistry;
				paths: { result: string; started: string; cancel: string };
				assignmentId: string;
				workerId: string;
				cwd: string;
				leases: import("../src/lease/writer-lease.js").WriterLeaseManager;
				controlDir: string;
				now: () => number;
			}) => void | Promise<void>;
		}) {
			installFakeHerdrExtension();
			const cacheRoot = tempDir(`momo-unresolved-abort-${options.label}-cache-`);
			const cwd = tempDir(`momo-unresolved-abort-${options.label}-cwd-`);
			let now = 1_000_000;
			let controlDir = "";
			const client = new HerdrClient({
				runCommand: async (_file, args) => {
					if (args[0] === "pane" && args[1] === "split") {
						const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
						controlDir =
							envArgs.find((v) => v.startsWith("MOMO_CONTROL_DIR="))?.slice("MOMO_CONTROL_DIR=".length) ??
							envArgs.find((v) => v.startsWith("MOMO_IPC_DIR="))?.slice("MOMO_IPC_DIR=".length) ??
							"";
						const workerId =
							envArgs.find((v) => v.startsWith("MOMO_WORKER_ID="))?.slice("MOMO_WORKER_ID=".length) ?? "";
						const runId =
							envArgs.find((v) => v.startsWith("MOMO_RUN_ID="))?.slice("MOMO_RUN_ID=".length) ?? "";
						if (controlDir) {
							mkdirSync(controlDir, { recursive: true, mode: 0o700 });
							atomicWriteJson(path.join(controlDir, "ready.json"), {
								version: 1,
								runId,
								workerId,
								readyAt: new Date(now).toISOString(),
							});
							atomicWriteJson(path.join(controlDir, "heartbeat.json"), {
								version: 1,
								runId,
								workerId,
								at: new Date(now).toISOString(),
								seq: 1,
							});
						}
						return {
							code: 0,
							stdout: JSON.stringify({ id: "s", result: { pane: { pane_id: "w1:p-ua" } } }),
							stderr: "",
						};
					}
					if (args[0] === "agent" && args[1] === "start") {
						return {
							code: 0,
							stdout: agentStartStdout("w1:p-ua", String(args[2])),
							stderr: "",
						};
					}
					if (args[0] === "agent" && args[1] === "send-keys") {
						throw new Error("terminal keys must not be used");
					}
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				},
			});
			const { PoolRegistry } = await import("../src/herdr/pool-registry.js");
			const { resolvePoolIdentity } = await import("../src/herdr/pool-identity.js");
			const { WriterLeaseManager } = await import("../src/lease/writer-lease.js");
			const identity = resolvePoolIdentity({
				cwd,
				canonicalRoot: cwd,
				workspaceId: "ws",
				socketPath: "s",
			});
			const pool = new PoolRegistry(identity.poolKey, cacheRoot);
			const leases = new WriterLeaseManager({ cacheRoot, now: () => now });
			const factory = createHerdrChildSessionFactory({
				cwd,
				parentPaneId: "w1:p0",
				parentId: `parent-ua-${options.label}`,
				client,
				poolRegistry: pool,
				leaseManager: leases,
				cacheRoot,
				canonicalRoot: cwd,
				workspaceId: "ws",
				socketPath: "s",
				pollIntervalMs: 20,
				readyTimeoutMs: 2_000,
				heartbeatStaleMs: 60_000,
				now: () => now,
				sleep: async (ms) => {
					now += ms;
				},
			});
			const session = await factory({ cwd, role: getRole(options.role) });
			const proxy = session as unknown as {
				assignmentId: string;
				workerId: string;
				generation: number;
				paths: { result: string; started: string; cancel: string };
				uncertainWrite: boolean;
			};
			await session.prompt("task");
			expect(pool.getByRole(options.role)?.activeAssignmentId).toBe(proxy.assignmentId);
			await options.beforeAbort?.({
				pool,
				paths: proxy.paths,
				assignmentId: proxy.assignmentId,
				workerId: proxy.workerId,
				cwd,
				leases,
				controlDir,
				now: () => now,
			});
			await session.abort();
			return { pool, proxy, leases, cwd };
		}

		it("unresolved read-only abort => registry unhealthy", async () => {
			const { tryReadIpcJson } = await import("../src/ipc/validate.js");
			const { pool, proxy } = await provisionAndAbort({ label: "scout", role: "scout" });
			const after = pool.getByRole("scout")!;
			expect(after.status).toBe("unhealthy");
			expect(after.uncertainWrite).toBeUndefined();
			expect(after.activeAssignmentId).toBe(proxy.assignmentId);
			expect(proxy.uncertainWrite).toBeFalsy();
			expect(tryReadIpcJson(proxy.paths.cancel)).toBeTruthy();
		});

		it("pre-lease implementer abort => unhealthy not uncertain", async () => {
			const { pool, proxy } = await provisionAndAbort({
				label: "impl-prelease",
				role: "implementer",
			});
			const after = pool.getByRole("implementer")!;
			expect(after.status).toBe("unhealthy");
			expect(after.uncertainWrite).toBeUndefined();
			expect(proxy.uncertainWrite).toBe(false);
		});

		it("same-worker lease implementer abort => uncertain", async () => {
			const { createLeaseToken } = await import("../src/lease/writer-lease.js");
			const { pool, proxy } = await provisionAndAbort({
				label: "impl-lease",
				role: "implementer",
				beforeAbort: ({ leases, cwd, workerId }) => {
					leases.acquire(cwd, workerId, createLeaseToken());
				},
			});
			const after = pool.getByRole("implementer")!;
			expect(after.status).toBe("uncertain");
			expect(after.uncertainWrite).toBe(true);
			expect(proxy.uncertainWrite).toBe(true);
		});

		it("started implementer abort => uncertain", async () => {
			const { pool, proxy } = await provisionAndAbort({
				label: "impl-started",
				role: "implementer",
				beforeAbort: ({ paths, assignmentId, workerId, pool: p }) => {
					const record = p.getByRole("implementer")!;
					atomicWriteJson(paths.started, {
						version: 1,
						runId: assignmentId,
						workerId,
						generation: record.generation,
						parentEpoch: record.activeParentEpoch ?? "epoch",
						startedAt: new Date().toISOString(),
					});
				},
			});
			const after = pool.getByRole("implementer")!;
			expect(after.status).toBe("uncertain");
			expect(after.uncertainWrite).toBe(true);
			expect(proxy.uncertainWrite).toBe(true);
		});

		it("durable result still wins over unresolved abort timeout", async () => {
			const { tryReadIpcJson } = await import("../src/ipc/validate.js");
			const { pool, proxy } = await provisionAndAbort({
				label: "result-wins",
				role: "scout",
				beforeAbort: async ({ paths, assignmentId, workerId }) => {
					atomicWriteJson(paths.result, {
						version: 1,
						runId: assignmentId,
						workerId,
						status: "completed",
						messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }] }],
						finishedAt: new Date().toISOString(),
					});
				},
			});
			expect(proxy.uncertainWrite).toBeFalsy();
			expect(tryReadIpcJson(proxy.paths.result)).toMatchObject({ status: "completed" });
			const after = pool.getByRole("scout");
			expect(after?.status).not.toBe("unhealthy");
			expect(after?.uncertainWrite).toBeUndefined();
		});
	});

	describe("implementer pre-lease heartbeat failure classification", () => {
		async function setupActiveImplementer(options: {
			label: string;
			started?: boolean;
			lease?: "same" | "foreign" | "corrupt" | "missing-owner" | "none";
		}) {
			installFakeHerdrExtension();
			const cacheRoot = tempDir(`momo-impl-hb-${options.label}-cache-`);
			const cwd = tempDir(`momo-impl-hb-${options.label}-cwd-`);
			const { PoolRegistry, selectClosablePoolWorkers } = await import(
				"../src/herdr/pool-registry.js"
			);
			const { resolvePoolIdentity, stableWorkerId } = await import(
				"../src/herdr/pool-identity.js"
			);
			const { workerControlPaths, assignmentSpoolPaths } = await import(
				"../src/herdr/assignment-spool.js"
			);
			const { WriterLeaseManager, createLeaseToken } = await import(
				"../src/lease/writer-lease.js"
			);
			const { mkdirSync, writeFileSync, rmSync } = await import("node:fs");
			const identity = resolvePoolIdentity({
				cwd,
				canonicalRoot: cwd,
				workspaceId: "ws",
				socketPath: "s",
			});
			const pool = new PoolRegistry(identity.poolKey, cacheRoot);
			const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_700_000_000_000 });
			const workerId = stableWorkerId(identity.poolKey, "implementer");
			const control = workerControlPaths(pool.poolRoot, "implementer");
			mkdirSync(control.root, { recursive: true, mode: 0o700 });

			const client = new HerdrClient({
				runCommand: async () => ({
					code: 0,
					stdout: JSON.stringify({ id: "ok", result: {} }),
					stderr: "",
				}),
			});
			const factory = createHerdrChildSessionFactory({
				cwd,
				parentPaneId: "w1:p0",
				parentId: `parent-impl-hb-${options.label}`,
				client,
				poolRegistry: pool,
				leaseManager: leases,
				cacheRoot,
				canonicalRoot: cwd,
				workspaceId: "ws",
				socketPath: "s",
			});
			const session = await factory({ cwd, role: getRole("implementer") });
			const proxy = session as unknown as {
				assignmentId: string;
				workerId: string;
				generation: number;
				promptStarted: boolean;
				physicalEnsured: boolean;
				stopAndSettleFailure: (message: string) => Promise<void>;
			};
			const assignmentId = proxy.assignmentId;
			expect(proxy.workerId).toBe(workerId);
			const paths = assignmentSpoolPaths(pool.poolRoot, "implementer", assignmentId);
			mkdirSync(paths.root, { recursive: true, mode: 0o700 });
			pool.upsert({
				workerId,
				generation: 1,
				generationTombstone: 1,
				role: "implementer",
				paneId: "w1:p-impl",
				agentName: "momo_implementer",
				cwd,
				status: "busy",
				activeAssignmentId: assignmentId,
				activeParentEpoch: "epoch1",
				updatedAt: new Date().toISOString(),
			});
			atomicWriteJson(control.active, {
				version: 1,
				assignmentId,
				generation: 1,
				parentEpoch: "epoch1",
				dispatchedAt: new Date().toISOString(),
			});
			if (options.started) {
				atomicWriteJson(paths.started, {
					version: 1,
					runId: assignmentId,
					workerId,
					generation: 1,
					parentEpoch: "epoch1",
					startedAt: new Date().toISOString(),
				});
			}
			if (options.lease === "same") {
				leases.acquire(cwd, workerId, createLeaseToken());
			} else if (options.lease === "foreign") {
				leases.acquire(cwd, "other_worker_id", createLeaseToken());
			} else if (options.lease === "corrupt") {
				leases.acquire(cwd, workerId, createLeaseToken());
				writeFileSync(path.join(leases.dirFor(cwd), "owner.json"), "{bad", { mode: 0o600 });
			} else if (options.lease === "missing-owner") {
				leases.acquire(cwd, workerId, createLeaseToken());
				rmSync(path.join(leases.dirFor(cwd), "owner.json"), { force: true });
			}

			proxy.generation = 1;
			proxy.promptStarted = true;
			proxy.physicalEnsured = true;

			return { pool, leases, cwd, workerId, assignmentId, proxy, selectClosablePoolWorkers };
		}

		it("foreign-lease waiting crash => unhealthy, cleanable, lease untouched", async () => {
			const { pool, leases, cwd, assignmentId, proxy, selectClosablePoolWorkers } =
				await setupActiveImplementer({
					label: "foreign",
					lease: "foreign",
				});
			const before = leases.peekOwner(cwd)!;
			await proxy.stopAndSettleFailure("Worker heartbeat went stale");
			const after = pool.getByRole("implementer")!;
			expect(after.status).toBe("unhealthy");
			expect(after.uncertainWrite).toBeUndefined();
			expect(after.activeAssignmentId).toBe(assignmentId);
			expect(selectClosablePoolWorkers([after]).closable).toHaveLength(1);
			expect(leases.peekOwner(cwd)?.ownerId).toBe(before.ownerId);
		});

		it("no-lease prestart crash => unhealthy and cleanable", async () => {
			const { pool, proxy, selectClosablePoolWorkers } = await setupActiveImplementer({
				label: "nolease",
				lease: "none",
			});
			await proxy.stopAndSettleFailure("Worker heartbeat went stale");
			const after = pool.getByRole("implementer")!;
			expect(after.status).toBe("unhealthy");
			expect(after.uncertainWrite).toBeUndefined();
			expect(selectClosablePoolWorkers([after]).closable).toHaveLength(1);
		});

		it("same-worker lease crash => uncertain", async () => {
			const { pool, leases, cwd, workerId, proxy } = await setupActiveImplementer({
				label: "same",
				lease: "same",
			});
			await proxy.stopAndSettleFailure("Worker heartbeat went stale");
			const after = pool.getByRole("implementer")!;
			expect(after.status).toBe("uncertain");
			expect(after.uncertainWrite).toBe(true);
			expect(leases.peekOwner(cwd)?.ownerId).toBe(workerId);
		});

		it("valid started crash => uncertain", async () => {
			const { pool, proxy } = await setupActiveImplementer({
				label: "started",
				started: true,
				lease: "none",
			});
			await proxy.stopAndSettleFailure("Worker heartbeat went stale");
			const after = pool.getByRole("implementer")!;
			expect(after.status).toBe("uncertain");
			expect(after.uncertainWrite).toBe(true);
		});

		it("corrupt lease crash => uncertain", async () => {
			const { pool, proxy } = await setupActiveImplementer({
				label: "corrupt",
				lease: "corrupt",
			});
			await proxy.stopAndSettleFailure("Worker heartbeat went stale");
			const after = pool.getByRole("implementer")!;
			expect(after.status).toBe("uncertain");
			expect(after.uncertainWrite).toBe(true);
		});

		it("missing-owner lease crash => uncertain", async () => {
			const { pool, proxy } = await setupActiveImplementer({
				label: "missing-owner",
				lease: "missing-owner",
			});
			await proxy.stopAndSettleFailure("Worker heartbeat went stale");
			const after = pool.getByRole("implementer")!;
			expect(after.status).toBe("uncertain");
			expect(after.uncertainWrite).toBe(true);
		});
	});

	describe("durable-result vs parent fencing commit-point", () => {
		afterEach(async () => {
			const { __resetIpcReadersForTest, __setStopAndSettleCommitHookForTest } = await import(
				"../src/delegation/herdr-factory.js"
			);
			__setStopAndSettleCommitHookForTest(undefined);
			__resetIpcReadersForTest();
			const { __resetWriteResultDurableLockHookForTest } = await import(
				"../src/extensions/worker-runtime.js"
			);
			__resetWriteResultDurableLockHookForTest();
		});

		async function setupActiveScoutProxy(label: string) {
			installFakeHerdrExtension();
			const cacheRoot = tempDir(`momo-fence-${label}-cache-`);
			const cwd = tempDir(`momo-fence-${label}-cwd-`);
			const { PoolRegistry } = await import("../src/herdr/pool-registry.js");
			const { resolvePoolIdentity, stableWorkerId } = await import(
				"../src/herdr/pool-identity.js"
			);
			const { workerControlPaths, assignmentSpoolPaths } = await import(
				"../src/herdr/assignment-spool.js"
			);
			const { withRoleLockAsync } = await import("../src/herdr/role-queue.js");
			const identity = resolvePoolIdentity({
				cwd,
				canonicalRoot: cwd,
				workspaceId: "ws",
				socketPath: "s",
			});
			const pool = new PoolRegistry(identity.poolKey, cacheRoot);
			const workerId = stableWorkerId(identity.poolKey, "scout");
			const client = new HerdrClient({
				runCommand: async () => ({
					code: 0,
					stdout: JSON.stringify({ id: "ok", result: {} }),
					stderr: "",
				}),
			});
			const factory = createHerdrChildSessionFactory({
				cwd,
				parentPaneId: "w1:p0",
				parentId: `parent-fence-${label}`,
				client,
				poolRegistry: pool,
				cacheRoot,
				canonicalRoot: cwd,
				workspaceId: "ws",
				socketPath: "s",
			});
			const session = await factory({ cwd, role: getRole("scout") });
			const proxy = session as unknown as {
				assignmentId: string;
				workerId: string;
				generation: number;
				promptStarted: boolean;
				physicalEnsured: boolean;
				paths: { result: string };
				stopAndSettleFailure: (message: string) => Promise<void>;
				uncertainWrite: boolean;
				messages: readonly unknown[];
			};
			const control = workerControlPaths(pool.poolRoot, "scout");
			mkdirSync(control.root, { recursive: true, mode: 0o700 });
			const paths = assignmentSpoolPaths(pool.poolRoot, "scout", proxy.assignmentId);
			mkdirSync(paths.root, { recursive: true, mode: 0o700 });
			pool.upsert({
				workerId,
				generation: 1,
				generationTombstone: 1,
				role: "scout",
				paneId: "w1:p-fence",
				agentName: "momo_scout",
				cwd,
				status: "busy",
				activeAssignmentId: proxy.assignmentId,
				activeParentEpoch: "epoch1",
				updatedAt: new Date().toISOString(),
			});
			proxy.generation = 1;
			proxy.promptStarted = true;
			proxy.physicalEnsured = true;
			return { pool, proxy, workerId, paths, withRoleLockAsync };
		}

		it("worker-first lock order: durable result under role lock wins parent fencing", async () => {
			const { pool, proxy, workerId, paths, withRoleLockAsync } = await setupActiveScoutProxy(
				"worker-first",
			);
			await withRoleLockAsync(pool.poolRoot, "scout", () => {
				atomicWriteJson(paths.result, {
					version: 1,
					runId: proxy.assignmentId,
					workerId,
					status: "completed",
					messages: [{ role: "assistant", content: [{ type: "text", text: "worker-first" }] }],
					finishedAt: new Date().toISOString(),
				});
			});
			await proxy.stopAndSettleFailure("Worker heartbeat went stale");
			expect(pool.getByRole("scout")?.status).toBe("busy");
			expect(proxy.uncertainWrite).toBeFalsy();
			expect(proxy.messages).toEqual([
				{ role: "assistant", content: [{ type: "text", text: "worker-first" }] },
			]);
			await expect(proxy.stopAndSettleFailure("again")).resolves.toBeUndefined();
		});

		it("parent-first lock order: fencing commits before later worker result", async () => {
			const { pool, proxy, workerId, paths, withRoleLockAsync } = await setupActiveScoutProxy(
				"parent-first",
			);
			await proxy.stopAndSettleFailure("Worker heartbeat went stale");
			expect(pool.getByRole("scout")?.status).toBe("unhealthy");
			await withRoleLockAsync(pool.poolRoot, "scout", () => {
				atomicWriteJson(paths.result, {
					version: 1,
					runId: proxy.assignmentId,
					workerId,
					status: "completed",
					messages: [{ role: "assistant", content: [{ type: "text", text: "too-late" }] }],
					finishedAt: new Date().toISOString(),
				});
			});
			expect(pool.getByRole("scout")?.status).toBe("unhealthy");
			expect(proxy.messages).not.toEqual([
				{ role: "assistant", content: [{ type: "text", text: "too-late" }] },
			]);
		});

		it("commit-point barrier: result written before mutation is adopted", async () => {
			const {
				__setStopAndSettleCommitHookForTest,
			} = await import("../src/delegation/herdr-factory.js");
			const { pool, proxy, workerId, paths } = await setupActiveScoutProxy("barrier-adopt");
			__setStopAndSettleCommitHookForTest(() => {
				atomicWriteJson(paths.result, {
					version: 1,
					runId: proxy.assignmentId,
					workerId,
					status: "completed",
					messages: [{ role: "assistant", content: [{ type: "text", text: "barrier" }] }],
					finishedAt: new Date().toISOString(),
				});
			});
			await proxy.stopAndSettleFailure("Worker heartbeat went stale");
			expect(pool.getByRole("scout")?.status).toBe("busy");
			expect(proxy.messages).toEqual([
				{ role: "assistant", content: [{ type: "text", text: "barrier" }] },
			]);
		});

		it("invalid raced result at commit point fails closed", async () => {
			const {
				__setStopAndSettleCommitHookForTest,
			} = await import("../src/delegation/herdr-factory.js");
			const { pool, proxy, paths } = await setupActiveScoutProxy("invalid");
			__setStopAndSettleCommitHookForTest(() => {
				atomicWriteJson(paths.result, {
					version: 1,
					runId: "wrong-id",
					workerId: proxy.workerId,
					status: "completed",
					messages: [{ role: "assistant", content: [{ type: "text", text: "bad" }] }],
					finishedAt: new Date().toISOString(),
				});
			});
			await proxy.stopAndSettleFailure("Worker heartbeat went stale");
			expect(pool.getByRole("scout")?.status).toBe("unhealthy");
			await expect(
				(async () => {
					const { tryReadIpcJson, validateResult } = await import("../src/ipc/validate.js");
					validateResult(tryReadIpcJson(paths.result), {
						runId: proxy.assignmentId,
						workerId: proxy.workerId,
					});
				})(),
			).rejects.toThrow(/identity mismatch/i);
		});

		it("lock order worker→parent: parent fencing waits for worker result lock", async () => {
			const { pool, proxy, workerId, paths, withRoleLockAsync } = await setupActiveScoutProxy(
				"lock-worker-parent",
			);
			const order: string[] = [];
			let releaseWorker!: () => void;
			const workerHold = new Promise<void>((resolve) => {
				releaseWorker = resolve;
			});
			const worker = withRoleLockAsync(pool.poolRoot, "scout", async () => {
				order.push("worker-enter");
				atomicWriteJson(paths.result, {
					version: 1,
					runId: proxy.assignmentId,
					workerId,
					status: "completed",
					messages: [{ role: "assistant", content: [{ type: "text", text: "held" }] }],
					finishedAt: new Date().toISOString(),
				});
				order.push("worker-wrote");
				await workerHold;
				order.push("worker-exit");
			});
			for (let i = 0; i < 20; i++) await Promise.resolve();
			const parent = proxy.stopAndSettleFailure("Worker heartbeat went stale").then(() => {
				order.push("parent-done");
			});
			await new Promise((r) => setTimeout(r, 40));
			expect(order).toEqual(["worker-enter", "worker-wrote"]);
			releaseWorker();
			await worker;
			await parent;
			expect(order).toEqual(["worker-enter", "worker-wrote", "worker-exit", "parent-done"]);
			expect(pool.getByRole("scout")?.status).toBe("busy");
			expect(proxy.messages).toEqual([
				{ role: "assistant", content: [{ type: "text", text: "held" }] },
			]);
		});

		it("lock order parent→worker: worker result write waits for parent fencing commit", async () => {
			const { __setStopAndSettleCommitHookForTest } = await import(
				"../src/delegation/herdr-factory.js"
			);
			const { pool, proxy, workerId, paths, withRoleLockAsync } = await setupActiveScoutProxy(
				"lock-parent-worker",
			);
			const order: string[] = [];
			let releaseParent!: () => void;
			const parentHold = new Promise<void>((resolve) => {
				releaseParent = resolve;
			});
			__setStopAndSettleCommitHookForTest(async () => {
				order.push("parent-recheck");
				await parentHold;
				order.push("parent-commit");
			});
			const parent = proxy.stopAndSettleFailure("Worker heartbeat went stale").then(() => {
				order.push("parent-done");
			});
			for (let i = 0; i < 30; i++) await Promise.resolve();
			const worker = withRoleLockAsync(pool.poolRoot, "scout", () => {
				order.push("worker-write");
				atomicWriteJson(paths.result, {
					version: 1,
					runId: proxy.assignmentId,
					workerId,
					status: "completed",
					messages: [{ role: "assistant", content: [{ type: "text", text: "after" }] }],
					finishedAt: new Date().toISOString(),
				});
			});
			await new Promise((r) => setTimeout(r, 40));
			expect(order).toEqual(["parent-recheck"]);
			releaseParent();
			await parent;
			await worker;
			expect(order[0]).toBe("parent-recheck");
			expect(order.indexOf("parent-commit")).toBeLessThan(order.indexOf("worker-write"));
			expect(order.indexOf("parent-done")).toBeLessThan(order.indexOf("worker-write"));
			expect(pool.getByRole("scout")?.status).toBe("unhealthy");
		});
	});

});

import { mkdtempSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createDelegationRunner } from "../src/delegation/runner.js";
import { createHerdrChildSessionFactory } from "../src/delegation/herdr-factory.js";
import { HerdrClient } from "../src/herdr/client.js";
import { PaneRegistry } from "../src/herdr/registry.js";
import { ROLE_LIST, getRole } from "../src/roles.js";
import { atomicWriteJson, readJsonFile, workerSpoolPaths } from "../src/ipc/spool.js";

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
	it("allocates distinct panes for every parallel task before prompting", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-cache-");
		const cwd = tempDir("momo-cwd-");
		const calls: string[][] = [];
		let paneCounter = 0;
		const readyWriters = new Map<string, () => void>();

		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				calls.push([...args]);
				if (args[0] === "pane" && args[1] === "split") {
					expect(args).toContain("--direction");
					paneCounter += 1;
					const paneId = `w1:p${paneCounter}`;
					const envArgs = args.filter((arg, index) => args[index - 1] === "--env");
					const ipcDir = envArgs
						.find((value) => value.startsWith("MOMO_IPC_DIR="))
						?.slice("MOMO_IPC_DIR=".length);
					const workerId = envArgs
						.find((value) => value.startsWith("MOMO_WORKER_ID="))
						?.slice("MOMO_WORKER_ID=".length);
					const runId = envArgs
						.find((value) => value.startsWith("MOMO_RUN_ID="))
						?.slice("MOMO_RUN_ID=".length);
					if (ipcDir && workerId && runId) {
						readyWriters.set(paneId, () => {
							atomicWriteJson(path.join(ipcDir, "ready.json"), {
								version: 1,
								runId,
								workerId,
								readyAt: new Date().toISOString(),
							});
						});
					}
					return {
						code: 0,
						stdout: JSON.stringify({ id: "split", result: { pane: { pane_id: paneId } } }),
						stderr: "",
					};
				}
				if (args[0] === "pane" && args[1] === "rename") {
					expect(args[0]).toBe("pane");
					expect(args[1]).toBe("rename");
					expect(args[2]).toMatch(/^w1:p/);
					expect(args[3]).toMatch(/^Momo /);
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				if (args[0] === "agent" && args[1] === "start") {
					const paneFlag = args.indexOf("--pane");
					const paneId = args[paneFlag + 1] ?? "";
					const name = args[2] ?? "";
					const toolsIdx = args.indexOf("--tools");
					const tools = args[toolsIdx + 1] ?? "";
					expect(tools).not.toContain("delegate");
					readyWriters.get(paneId)?.();
					return { code: 0, stdout: agentStartStdout(paneId, name), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});

		const registry = new PaneRegistry("parent1", cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: "parent1",
			client,
			registry,
			cacheRoot,
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
					const record = (session as unknown as { paths: ReturnType<typeof workerSpoolPaths> })
						.paths;
					const workerId = (session as unknown as { workerId: string }).workerId;
					const runId = (session as unknown as { runId: string }).runId;
					atomicWriteJson(record.result, {
						version: 1,
						runId,
						workerId,
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
		expect(registry.list().every((pane) => pane.status === "completed")).toBe(true);
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
		expect(sessions[1]?.skip).toHaveBeenCalledWith("chain_skipped_after_failure");
	});

	it("cleans up successfully prepared workers when allocation partially fails", async () => {
		const cwd = tempDir("momo-alloc-cwd-");
		const skips: string[] = [];
		let count = 0;
		const runner = createDelegationRunner({
			cwd,
			roles: ROLE_LIST,
			createChildSession: async ({ role }) => {
				count += 1;
				if (count === 2) throw new Error("split failed");
				return {
					messages: [],
					agent: { waitForIdle: async () => {} },
					subscribe: () => () => {},
					prompt: async () => {},
					abort: async () => {},
					skip: async (reason: string) => {
						skips.push(`${role.name}:${reason}`);
					},
					dispose: async () => {},
				};
			},
		});

		await expect(
			runner.run({
				mode: "parallel",
				tasks: [
					{ agent: "scout", task: "a" },
					{ agent: "planner", task: "b" },
				],
			}),
		).rejects.toThrow(/split failed/);
		expect(skips).toEqual(["scout:allocation_failed"]);
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

	it("skip command leaves registry terminal (not ready)", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-term-cache-");
		const cwd = tempDir("momo-term-cwd-");

		let ipcDir = "";
		let workerId = "";
		let runId = "";
		const paneId = "w1:p2";
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					const envArgs = args.filter((arg, index) => args[index - 1] === "--env");
					ipcDir =
						envArgs.find((value) => value.startsWith("MOMO_IPC_DIR="))?.slice("MOMO_IPC_DIR=".length) ??
						"";
					workerId =
						envArgs
							.find((value) => value.startsWith("MOMO_WORKER_ID="))
							?.slice("MOMO_WORKER_ID=".length) ?? "";
					runId =
						envArgs.find((value) => value.startsWith("MOMO_RUN_ID="))?.slice("MOMO_RUN_ID=".length) ??
						"";
					return {
						code: 0,
						stdout: JSON.stringify({ id: "split", result: { pane: { pane_id: paneId } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					atomicWriteJson(path.join(ipcDir, "ready.json"), {
						version: 1,
						runId,
						workerId,
						readyAt: new Date().toISOString(),
					});
					return { code: 0, stdout: agentStartStdout(paneId, args[2] ?? ""), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const registry = new PaneRegistry("parent-term", cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: "parent-term",
			client,
			registry,
			cacheRoot,
			pollIntervalMs: 20,
			readyTimeoutMs: 2_000,
			sleep: async (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
		});
		const session = await factory({ cwd, role: getRole("scout") });
		expect(registry.list()[0]?.status).toBe("ready");

		const poll = setInterval(() => {
			const command = readJsonFile(path.join(ipcDir, "command.json")) as
				| { type?: string; reason?: string }
				| undefined;
			if (command?.type === "skip") {
				atomicWriteJson(path.join(ipcDir, "result.json"), {
					version: 1,
					runId,
					workerId,
					status: "aborted",
					messages: [],
					errorMessage: command.reason,
					finishedAt: new Date().toISOString(),
				});
			}
		}, 10);
		await session.skip?.("parallel_skipped");
		clearInterval(poll);
		expect(registry.list()[0]?.status).toBe("aborted");
	});
});

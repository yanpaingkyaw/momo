import { mkdtempSync, mkdirSync, rmSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createDelegationRunner } from "../src/delegation/runner.js";
import { createHerdrChildSessionFactory } from "../src/delegation/herdr-factory.js";
import { HerdrClient } from "../src/herdr/client.js";
import { PoolRegistry } from "../src/herdr/pool-registry.js";
import { resolvePoolIdentity, stableWorkerId } from "../src/herdr/pool-identity.js";
import { listQueue, queueCount } from "../src/herdr/role-queue.js";
import { assignmentSpoolPaths, workerControlPaths } from "../src/herdr/assignment-spool.js";
import { ROLE_LIST, getRole } from "../src/roles.js";
import { atomicWriteJson } from "../src/ipc/spool.js";
import { installMomoWorker } from "../src/extensions/worker-runtime.js";
import { WriterLeaseManager } from "../src/lease/writer-lease.js";

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

function makeClient(options: {
	onSplit?: (env: Record<string, string>) => void;
	paneCounter?: { n: number };
}): { client: HerdrClient; splits: string[]; starts: string[] } {
	const splits: string[] = [];
	const starts: string[] = [];
	const counter = options.paneCounter ?? { n: 0 };
	const client = new HerdrClient({
		runCommand: async (_file, args) => {
			if (args[0] === "pane" && args[1] === "split") {
				counter.n += 1;
				const paneId = `w1:p${counter.n}`;
				splits.push(paneId);
				const envArgs = args.filter((arg, index) => args[index - 1] === "--env");
				const env: Record<string, string> = {};
				for (const value of envArgs) {
					const eq = value.indexOf("=");
					if (eq > 0) env[value.slice(0, eq)] = value.slice(eq + 1);
				}
				options.onSplit?.(env);
				const controlDir = env.MOMO_CONTROL_DIR || env.MOMO_IPC_DIR;
				const workerId = env.MOMO_WORKER_ID;
				const runId = env.MOMO_RUN_ID;
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
			if (args[0] === "agent" && args[1] === "start") {
				starts.push(String(args[2]));
				const paneFlag = args.indexOf("--pane");
				const paneId = args[paneFlag + 1] ?? "";
				const name = args[2] ?? "";
				return { code: 0, stdout: agentStartStdout(paneId, name), stderr: "" };
			}
			return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
		},
	});
	return { client, splits, starts };
}

async function completeAssignmentResult(session: {
	paths: { result: string };
	assignmentId: string;
	workerId: string;
	prompt: (text: string) => Promise<void>;
}, text: string, output: string): Promise<void> {
	await session.prompt(text);
	atomicWriteJson(session.paths.result, {
		version: 1,
		runId: session.assignmentId,
		workerId: session.workerId,
		status: "completed",
		messages: [
			{
				role: "assistant",
				content: [{ type: "text", text: output }],
				usage: { input: 1, output: 1, totalTokens: 2, cost: { total: 0 } },
			},
		],
		finishedAt: new Date().toISOString(),
	});
}

describe("persistent role pool", () => {
	it("reuses exact same pane/agent for repeated implementer and reviewer; one split/start each", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-pool-cache-");
		const cwd = tempDir("momo-pool-cwd-");
		const { client, splits, starts } = makeClient({});
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws1",
			socketPath: "/tmp/herdr.sock",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-pool",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws1",
			socketPath: "/tmp/herdr.sock",
			pollIntervalMs: 10,
			readyTimeoutMs: 2_000,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});

		const impl1 = (await factory({ cwd, role: getRole("implementer") })) as unknown as {
			paths: { result: string };
			assignmentId: string;
			workerId: string;
			prompt: (t: string) => Promise<void>;
			agent: { waitForIdle: () => Promise<void> };
			dispose: () => Promise<void>;
			paneId?: string;
			agentName?: string;
		};
		await completeAssignmentResult(impl1, "impl-a", "done-a");
		await impl1.agent.waitForIdle();
		const paneA = impl1.paneId;
		const agentA = impl1.agentName;
		await impl1.dispose();

		// Simulate worker returning to idle after first assignment.
		pool.upsert({
			...pool.getByRole("implementer")!,
			status: "idle",
			updatedAt: new Date().toISOString(),
		});

		const impl2 = (await factory({ cwd, role: getRole("implementer") })) as unknown as typeof impl1;
		await completeAssignmentResult(impl2, "impl-b", "done-b");
		await impl2.agent.waitForIdle();
		expect(impl2.paneId).toBe(paneA);
		expect(impl2.agentName).toBe(agentA);
		await impl2.dispose();

		const rev1 = (await factory({ cwd, role: getRole("reviewer") })) as unknown as typeof impl1;
		await completeAssignmentResult(rev1, "rev-a", "ok-a");
		await rev1.agent.waitForIdle();
		const paneR = rev1.paneId;
		pool.upsert({
			...pool.getByRole("reviewer")!,
			status: "idle",
			updatedAt: new Date().toISOString(),
		});
		await rev1.dispose();

		const rev2 = (await factory({ cwd, role: getRole("reviewer") })) as unknown as typeof impl1;
		await completeAssignmentResult(rev2, "rev-b", "ok-b");
		await rev2.agent.waitForIdle();
		expect(rev2.paneId).toBe(paneR);
		await rev2.dispose();

		expect(splits).toHaveLength(2);
		expect(starts).toHaveLength(2);
	});

	it("does not create panes for prepared chain tails (skip only)", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-chain-cache-");
		const cwd = tempDir("momo-chain-cwd-");
		const { client, splits } = makeClient({});
		const identity = resolvePoolIdentity({ cwd, canonicalRoot: cwd, workspaceId: "ws", socketPath: "s" });
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-chain",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			pollIntervalMs: 10,
			readyTimeoutMs: 1_000,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});

		const runner = createDelegationRunner({
			cwd,
			roles: ROLE_LIST,
			createChildSession: async (input) => {
				const session = await factory(input);
				const original = session.prompt.bind(session);
				session.prompt = async (text: string) => {
					await original(text);
					const proxy = session as unknown as {
						assignmentId: string;
						workerId: string;
						paths: { result: string };
					};
					atomicWriteJson(proxy.paths.result, {
						version: 1,
						runId: proxy.assignmentId,
						workerId: proxy.workerId,
						status: "failed",
						messages: [],
						errorMessage: "boom",
						finishedAt: new Date().toISOString(),
					});
				};
				return session;
			},
		});

		const result = await runner.run({
			mode: "chain",
			steps: [
				{ agent: "scout", task: "first" },
				{ agent: "planner", task: "tail" },
			],
		});
		expect(result.status).toBe("failed");
		expect(splits).toHaveLength(1);
		expect(pool.getByRole("planner")).toBeUndefined();
	});

	it("FIFO queues busy same-role work without overflow panes", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-fifo-cache-");
		const cwd = tempDir("momo-fifo-cwd-");
		const { client, splits } = makeClient({});
		const identity = resolvePoolIdentity({ cwd, canonicalRoot: cwd, workspaceId: "ws", socketPath: "s" });
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-fifo",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			pollIntervalMs: 10,
			readyTimeoutMs: 2_000,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});

		const a = (await factory({ cwd, role: getRole("scout") })) as unknown as {
			assignmentId: string;
			workerId: string;
			paths: { result: string };
			prompt: (t: string) => Promise<void>;
		};
		const b = (await factory({ cwd, role: getRole("scout") })) as unknown as typeof a;

		const promptA = a.prompt("task-a");
		await new Promise((r) => setTimeout(r, 50));
		expect(pool.getByRole("scout")?.status).toBe("busy");

		const promptB = b.prompt("task-b");
		await new Promise((r) => setTimeout(r, 50));
		expect(queueCount(pool.poolRoot, "scout")).toBe(1);
		expect(splits).toHaveLength(1);

		atomicWriteJson(a.paths.result, {
			version: 1,
			runId: a.assignmentId,
			workerId: a.workerId,
			status: "completed",
			messages: [
				{
					role: "assistant",
					content: [{ type: "text", text: "a" }],
					usage: { input: 1, output: 1, totalTokens: 2, cost: { total: 0 } },
				},
			],
			finishedAt: new Date().toISOString(),
		});
		await promptA;

		// Parent-side queue still holds B until worker claims; simulate claim+result.
		const queued = listQueue(pool.poolRoot, "scout");
		expect(queued[0]?.assignmentId).toBe(b.assignmentId);
		atomicWriteJson(b.paths.result, {
			version: 1,
			runId: b.assignmentId,
			workerId: b.workerId,
			status: "completed",
			messages: [
				{
					role: "assistant",
					content: [{ type: "text", text: "b" }],
					usage: { input: 1, output: 1, totalTokens: 2, cost: { total: 0 } },
				},
			],
			finishedAt: new Date().toISOString(),
		});
		// Cancel queue entry as if claimed, then wait.
		const { cancelQueuedAssignment } = await import("../src/herdr/role-queue.js");
		cancelQueuedAssignment(pool.poolRoot, "scout", b.assignmentId);
		await promptB;
		expect(splits).toHaveLength(1);
	});

	it("cancels queued assignment without interrupting active", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-qcancel-cache-");
		const cwd = tempDir("momo-qcancel-cwd-");
		const { client } = makeClient({});
		const identity = resolvePoolIdentity({ cwd, canonicalRoot: cwd, workspaceId: "ws", socketPath: "s" });
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-qc",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			pollIntervalMs: 10,
			readyTimeoutMs: 2_000,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});

		const active = (await factory({ cwd, role: getRole("planner") })) as unknown as {
			assignmentId: string;
			workerId: string;
			paths: { result: string; cancel: string };
			prompt: (t: string) => Promise<void>;
			abort: () => Promise<void>;
		};
		const queued = (await factory({ cwd, role: getRole("planner") })) as unknown as typeof active;

		void active.prompt("active-task");
		await new Promise((r) => setTimeout(r, 40));
		void queued.prompt("queued-task");
		await new Promise((r) => setTimeout(r, 40));
		expect(queueCount(pool.poolRoot, "planner")).toBe(1);

		await queued.abort();
		expect(queueCount(pool.poolRoot, "planner")).toBe(0);
		expect(existsSync(active.paths.cancel)).toBe(false);
		expect(pool.getByRole("planner")?.activeAssignmentId).toBe(active.assignmentId);
	});

	it("poolKey excludes parent pane and is stable per repo+workspace+socket", () => {
		const cwd = tempDir("momo-id-cwd-");
		const a = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "/sock",
		});
		const b = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "/sock",
		});
		const c = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "other",
			socketPath: "/sock",
		});
		expect(a.poolKey).toBe(b.poolKey);
		expect(a.poolKey).not.toBe(c.poolKey);
		expect(stableWorkerId(a.poolKey, "implementer")).toBe(stableWorkerId(b.poolKey, "implementer"));
	});

	it("uncertain workers are not reused", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-unc-cache-");
		const cwd = tempDir("momo-unc-cwd-");
		const { client, splits } = makeClient({});
		const identity = resolvePoolIdentity({ cwd, canonicalRoot: cwd, workspaceId: "ws", socketPath: "s" });
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		pool.upsert({
			workerId: stableWorkerId(identity.poolKey, "implementer"),
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p9",
			agentName: "momo_impl",
			status: "uncertain",
			uncertainWrite: true,
			updatedAt: new Date().toISOString(),
		});
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p0",
			parentId: "parent-unc",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "ws",
			socketPath: "s",
			pollIntervalMs: 10,
			readyTimeoutMs: 500,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});
		const session = await factory({ cwd, role: getRole("implementer") });
		await expect(session.prompt("x")).rejects.toThrow(/uncertain/);
		expect(splits).toHaveLength(0);
	});
});

describe("persistent worker assignment isolation", () => {
	it("resets lease token and filters context per assignment; blocks interactive input", async () => {
		const cacheRoot = tempDir("momo-wr-cache-");
		const cwd = tempDir("momo-wr-cwd-");
		const identity = resolvePoolIdentity({ cwd, canonicalRoot: cwd, workspaceId: "ws", socketPath: "s" });
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const control = workerControlPaths(pool.poolRoot, "implementer");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		const leases = new WriterLeaseManager({ cacheRoot });
		const tokens: string[] = [];

		const handlers = new Map<string, Function>();
		const pi = {
			registerTool: vi.fn(),
			setActiveTools: vi.fn(),
			sendUserMessage: vi.fn(),
			sendMessage: vi.fn(),
			on: (event: string, handler: Function) => {
				handlers.set(event, handler);
			},
		};

		installMomoWorker(pi as never, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "implementer",
				MOMO_WORKER_ID: stableWorkerId(identity.poolKey, "implementer"),
				MOMO_WORKER_GENERATION: "1",
				MOMO_POOL_KEY: identity.poolKey,
				MOMO_POOL_ROOT: pool.poolRoot,
				MOMO_CONTROL_DIR: control.root,
				MOMO_IPC_DIR: control.root,
				MOMO_RUN_ID: "g1",
				MOMO_CWD: cwd,
			},
			leaseManager: leases,
			poolRegistry: pool,
			sleep: async () => {},
		});

		await handlers.get("session_start")?.({}, { hasUI: true, isIdle: () => true, abort: vi.fn(), ui: { notify: vi.fn() } });

		const inputResult = await handlers.get("input")?.(
			{ source: "interactive", text: "hi" },
			{ ui: { notify: vi.fn() } },
		);
		expect(inputResult).toEqual({ action: "handled" });

		const extInput = await handlers.get("input")?.(
			{ source: "extension", text: "task" },
			{ ui: { notify: vi.fn() } },
		);
		expect(extInput).toEqual({ action: "continue" });

		const contextEmpty = await handlers.get("context")?.({ messages: [{ role: "assistant", content: "old" }] });
		expect(contextEmpty.messages).toEqual([]);

		const contextFiltered = await handlers.get("context")?.({
			messages: [
				{ role: "user", content: "old task" },
				{ role: "assistant", content: "old reply" },
				{ role: "user", content: "new task" },
				{ role: "assistant", content: "reply" },
			],
		});
		expect(contextFiltered.messages).toHaveLength(2);
		expect(contextFiltered.messages[0].content).toBe("new task");

		void tokens;
		expect(existsSync(control.ready)).toBe(true);
	});
});

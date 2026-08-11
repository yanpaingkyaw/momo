import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { createHerdrChildSessionFactory } from "../src/delegation/herdr-factory.js";
import { extractLastAssistantText } from "../src/delegation/results.js";
import { installMomoWorker } from "../src/extensions/worker-runtime.js";
import { HerdrClient } from "../src/herdr/client.js";
import { createTestHerdrClient } from "./helpers/herdr-mock-client.js";
import { resolvePoolIdentity } from "../src/herdr/pool-identity.js";
import { PoolRegistry } from "../src/herdr/pool-registry.js";
import { RoleTransactionLock, listQueue } from "../src/herdr/role-queue.js";
import { createLeaseToken, WriterLeaseManager } from "../src/lease/writer-lease.js";
import { getRole } from "../src/roles.js";

const temps: string[] = [];
const previousPiDir = process.env.PI_CODING_AGENT_DIR;

function tempDir(prefix: string): string {
	const dir = mkdtempSync(path.join(tmpdir(), prefix));
	temps.push(dir);
	return dir;
}

function installFakeHerdrExtension(): void {
	const agentDir = tempDir("momo-pi-agent-");
	mkdirSync(path.join(agentDir, "extensions"), { recursive: true });
	writeFileSync(path.join(agentDir, "extensions", "herdr-agent-state.ts"), "// fake\n", "utf8");
	process.env.PI_CODING_AGENT_DIR = agentDir;
}

afterEach(() => {
	vi.restoreAllMocks();
	for (const dir of temps.splice(0)) {
		try {
			rmSync(dir, { recursive: true, force: true });
		} catch {
			/* ignore */
		}
	}
	if (previousPiDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
	else process.env.PI_CODING_AGENT_DIR = previousPiDir;
});

function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

const RUNTIME_NOT_READY =
	"Extension runtime not initialized. Action methods cannot be called during extension loading.";

type FakePi = {
	handlers: Map<string, Function[]>;
	session: { messages: Array<Record<string, unknown>> };
	on(event: string, handler: Function): void;
	sendUserMessage: ReturnType<typeof vi.fn>;
	setActiveTools: ReturnType<typeof vi.fn>;
	registerTool: ReturnType<typeof vi.fn>;
	emit(event: string, payload?: unknown, ctx?: ExtensionContext): Promise<unknown>;
};

function createFakePi(): FakePi {
	const handlers = new Map<string, Function[]>();
	let runtimeReady = false;
	const assertRuntime = () => {
		if (!runtimeReady) throw new Error(RUNTIME_NOT_READY);
	};
	const session = { messages: [] as Array<Record<string, unknown>> };
	const pi: FakePi = {
		handlers,
		session,
		on(event, handler) {
			const list = handlers.get(event) ?? [];
			list.push(handler);
			handlers.set(event, list);
		},
		sendUserMessage: vi.fn(),
		setActiveTools: vi.fn((..._args: unknown[]) => {
			assertRuntime();
		}),
		registerTool: vi.fn(),
		async emit(event, payload = {}, ctx?) {
			if (event === "session_start") runtimeReady = true;
			const list = handlers.get(event) ?? [];
			let last: unknown;
			for (const handler of list) {
				last = await handler(payload, ctx);
			}
			return last;
		},
	};
	return pi;
}

type AssignmentProxy = {
	paths: { result: string; events: string };
	assignmentId: string;
	workerId: string;
	prompt: (text: string) => Promise<void>;
	agent: { waitForIdle: () => Promise<void> };
	messages: readonly unknown[];
	dispose: () => Promise<void>;
};

/**
 * Real sequential harness: one in-process worker runtime services sequential
 * and queued assignments through the same pane/generation without manual
 * idle/result/queue deletion by the test.
 */
describe("pool sequential assignment harness", () => {
	it("one split/start; FIFO outputs; isolation; lease reacquire; failure then next", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-seq-cache-");
		const cwd = tempDir("momo-seq-cwd-");
		mkdirSync(path.join(cwd, ".git"), { recursive: true });

		const splits: string[] = [];
		const starts: string[] = [];
		const leaseTokens: string[] = [];
		let workerEnv: Record<string, string> | null = null;
		let pi: FakePi | null = null;
		let workerCtx: ExtensionContext | null = null;
		let bootPromise: Promise<void> | null = null;

		const client = createTestHerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					splits.push("split");
					const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
					workerEnv = {};
					for (const v of envArgs) {
						const eq = v.indexOf("=");
						if (eq > 0) workerEnv[v.slice(0, eq)] = v.slice(eq + 1);
					}
					bootPromise = (async () => {
						const identity = resolvePoolIdentity({
							cwd,
							canonicalRoot: cwd,
							workspaceId: "test-ws",
							socketPath: "test-sock",
						});
						const pool = new PoolRegistry(identity.poolKey, cacheRoot);
						pi = createFakePi();
						workerCtx = {
							hasUI: true,
							isIdle: () => true,
							abort: vi.fn(),
							cwd,
						} as unknown as ExtensionContext;

						pi.sendUserMessage = vi.fn(async (task: string) => {
							if (!pi || !workerCtx) return;
							const fail = String(task).includes("FAIL_ME");
							const userMsg = { role: "user", content: [{ type: "text", text: String(task) }] };
							const priorAssistant = {
								role: "assistant",
								content: [{ type: "text", text: "PRIOR_SHOULD_NOT_LEAK" }],
							};
							const priorTool = {
								role: "toolResult",
								toolName: "read",
								content: [{ type: "text", text: "old" }],
							};
							pi.session.messages = [priorAssistant, priorTool, userMsg];

							const contextHandlers = pi.handlers.get("context") ?? [];
							for (const handler of contextHandlers) {
								const filtered = (await handler(
									{ messages: pi.session.messages },
									workerCtx,
								)) as { messages: unknown[] };
								expect(filtered.messages.some((m) => JSON.stringify(m).includes("PRIOR_SHOULD_NOT_LEAK"))).toBe(
									false,
								);
								expect(filtered.messages.some((m) => JSON.stringify(m).includes("old"))).toBe(false);
								expect(filtered.messages[0]).toEqual(userMsg);
							}

							if (fail) {
								await pi.emit(
									"message_end",
									{
										message: {
											role: "assistant",
											content: [{ type: "text", text: "failed" }],
											stopReason: "error",
											errorMessage: "simulated agent failure",
										},
									},
									workerCtx,
								);
								await pi.emit("agent_settled", {}, workerCtx);
								return;
							}

							const assistant = {
								role: "assistant",
								content: [{ type: "text", text: `out:${task}` }],
								stopReason: "stop",
								usage: { input: 2, output: 3, totalTokens: 5, cost: { total: 0.01 } },
							};
							pi.session.messages.push(assistant);
							await pi.emit("message_end", { message: assistant }, workerCtx);
							await pi.emit("agent_settled", {}, workerCtx);
						});

						installMomoWorker(pi as unknown as ExtensionAPI, {
							env: workerEnv!,
							poolRegistry: pool,
							leaseManager: new WriterLeaseManager({ sleep }),
							sleep,
						});
						await pi.emit("session_start", {}, workerCtx);
					})();
					return {
						code: 0,
						stdout: JSON.stringify({ id: "s", result: { pane: { pane_id: "w1:pseq" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					starts.push(String(args[2]));
					await bootPromise;
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: {
								pane_id: "w1:pseq",
								name: args[2],
								agent: "pi",
								interactive_ready: true,
							},
						}),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "send") {
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: { ok: true } }), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});

		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "test-ws",
			socketPath: "test-sock",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: "parent-seq",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "test-ws",
			socketPath: "test-sock",
			pollIntervalMs: 20,
			readyTimeoutMs: 5_000,
			sleep,
		});

		const sessionA = (await factory({ cwd, role: getRole("implementer") })) as unknown as AssignmentProxy;
		await sessionA.prompt("TASK_ONE");
		await sessionA.agent.waitForIdle();
		expect(extractLastAssistantText(sessionA.messages)).toContain("out:TASK_ONE");
		expect(sessionA.messages.some((m) => JSON.stringify(m).includes("PRIOR_SHOULD_NOT_LEAK"))).toBe(
			false,
		);

		const sessionB = (await factory({ cwd, role: getRole("implementer") })) as unknown as AssignmentProxy;
		await sessionB.prompt("TASK_TWO");
		await sessionB.agent.waitForIdle();
		expect(extractLastAssistantText(sessionB.messages)).toContain("out:TASK_TWO");
		expect(extractLastAssistantText(sessionB.messages)).not.toContain("TASK_ONE");

		expect(splits).toHaveLength(1);
		expect(starts).toHaveLength(1);
		expect(pool.getByRole("implementer")?.status).toBe("idle");
		expect(listQueue(pool.poolRoot, "implementer")).toHaveLength(0);

		// Fresh lease tokens across reacquire/release (writer role).
		const leases = new WriterLeaseManager({ sleep });
		const token1 = createLeaseToken();
		leases.acquire(cwd, "harness-owner", token1);
		leaseTokens.push(token1);
		leases.release(cwd, "harness-owner", token1);
		const token2 = createLeaseToken();
		leases.acquire(cwd, "harness-owner", token2);
		leaseTokens.push(token2);
		leases.release(cwd, "harness-owner", token2);
		expect(new Set(leaseTokens).size).toBe(2);

		const lock = new RoleTransactionLock(pool.poolRoot, "implementer");
		await lock.acquire(2_000);
		const lockToken1 = lock.getTokenForTest();
		lock.release();
		await lock.acquire(2_000);
		const lockToken2 = lock.getTokenForTest();
		lock.release();
		expect(lockToken1).toBeTruthy();
		expect(lockToken2).toBeTruthy();
		expect(lockToken1).not.toBe(lockToken2);

		const failSession = (await factory({
			cwd,
			role: getRole("implementer"),
		})) as unknown as AssignmentProxy;
		await failSession.prompt("FAIL_ME now");
		await expect(failSession.agent.waitForIdle()).rejects.toThrow();
		await sleep(50);
		expect(pool.getByRole("implementer")?.status).toBe("idle");

		const afterFail = (await factory({
			cwd,
			role: getRole("implementer"),
		})) as unknown as AssignmentProxy;
		await afterFail.prompt("TASK_THREE");
		await afterFail.agent.waitForIdle();
		expect(extractLastAssistantText(afterFail.messages)).toContain("out:TASK_THREE");
		expect(splits).toHaveLength(1);
		expect(starts).toHaveLength(1);
	}, 30_000);

	it("queued claim runs without manual pool idle/result/queue deletion", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-qharness-cache-");
		const cwd = tempDir("momo-qharness-cwd-");
		mkdirSync(path.join(cwd, ".git"), { recursive: true });

		let workerEnv: Record<string, string> | null = null;
		let pi: FakePi | null = null;
		let workerCtx: ExtensionContext | null = null;
		let bootPromise: Promise<void> | null = null;
		let gate = 0;

		const client = createTestHerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
					workerEnv = {};
					for (const v of envArgs) {
						const eq = v.indexOf("=");
						if (eq > 0) workerEnv[v.slice(0, eq)] = v.slice(eq + 1);
					}
					bootPromise = (async () => {
						const identity = resolvePoolIdentity({
							cwd,
							canonicalRoot: cwd,
							workspaceId: "test-ws",
							socketPath: "test-sock",
						});
						const pool = new PoolRegistry(identity.poolKey, cacheRoot);
						pi = createFakePi();
						workerCtx = {
							hasUI: true,
							isIdle: () => true,
							abort: vi.fn(),
							cwd,
						} as unknown as ExtensionContext;
						pi.sendUserMessage = vi.fn(async (task: string) => {
							gate += 1;
							const myGate = gate;
							if (myGate === 1) await sleep(150);
							const assistant = {
								role: "assistant",
								content: [{ type: "text", text: `done:${task}` }],
								stopReason: "stop",
								usage: { input: 1, output: 1, totalTokens: 2, cost: { total: 0 } },
							};
							await pi!.emit("message_end", { message: assistant }, workerCtx!);
							await pi!.emit("agent_settled", {}, workerCtx!);
						});
						installMomoWorker(pi as unknown as ExtensionAPI, {
							env: workerEnv!,
							poolRegistry: pool,
							sleep,
						});
						await pi.emit("session_start", {}, workerCtx);
					})();
					return {
						code: 0,
						stdout: JSON.stringify({ id: "s", result: { pane: { pane_id: "w1:pq" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					await bootPromise;
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: {
								pane_id: "w1:pq",
								name: args[2],
								agent: "pi",
								interactive_ready: true,
							},
						}),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "send") {
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: { ok: true } }), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});

		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "test-ws",
			socketPath: "test-sock",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: "parent-qh",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "test-ws",
			socketPath: "test-sock",
			pollIntervalMs: 20,
			readyTimeoutMs: 5_000,
			sleep,
		});

		const s1 = (await factory({ cwd, role: getRole("scout") })) as unknown as AssignmentProxy;
		const s2 = (await factory({ cwd, role: getRole("scout") })) as unknown as AssignmentProxy;
		const p1 = (async () => {
			await s1.prompt("FIRST");
			await s1.agent.waitForIdle();
			return extractLastAssistantText(s1.messages);
		})();
		await sleep(40);
		const p2 = (async () => {
			await s2.prompt("SECOND");
			await s2.agent.waitForIdle();
			return extractLastAssistantText(s2.messages);
		})();
		const [a, b] = await Promise.all([p1, p2]);
		expect(a).toContain("done:FIRST");
		expect(b).toContain("done:SECOND");
		expect(pool.getByRole("scout")?.status).toBe("idle");
		expect(listQueue(pool.poolRoot, "scout")).toHaveLength(0);
	}, 30_000);
});

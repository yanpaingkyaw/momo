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
import { createTestHerdrClient } from "./helpers/herdr-mock-client.js";
import { PaneRegistry, RegistryCorruptionError } from "../src/herdr/registry.js";
import {
	isArchivalTombstone,
	PoolRegistry,
	selectClosablePoolWorkers,
} from "../src/herdr/pool-registry.js";
import { resolvePoolIdentity } from "../src/herdr/pool-identity.js";
import { beginClaimHead, enqueueAssignment, listClaiming, listQueue } from "../src/herdr/role-queue.js";
import { atomicWriteJson } from "../src/ipc/spool.js";
import {
	IpcValidationError,
	parseJsonFile,
	tryReadIpcJson,
	validateHeartbeat,
	validateResult,
} from "../src/ipc/validate.js";
import { getRole } from "../src/roles.js";
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
		sendMessage: vi.fn(() => {
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
	it("releases lease after durable completed result", async () => {
		vi.useFakeTimers();
		const cwd = tempDir("momo-settle-cwd-");
		const cacheRoot = tempDir("momo-settle-cache-");
		const lease = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });

		async function runImplementer(workerId: string): Promise<void> {
			const fixture = setupPoolWorkerFixture({
				cacheRoot,
				cwd,
				role: "implementer",
				assignmentId: workerId.replace("_", "") + "000",
			});
			const pi = createFakePi();
			installMomoWorker(pi as unknown as ExtensionAPI, {
				env: fixture.env,
				poolRegistry: fixture.pool,
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
			dispatchAssignment({
				pool: fixture.pool,
				role: fixture.role, controlRoot: fixture.control.root, paths: fixture.paths,
				assignmentId: fixture.assignmentId,
				workerId: fixture.workerId,
				generation: fixture.generation,
				task: "edit",
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
			const result = parseJsonFile(fixture.paths.result) as {
				status: string;
				uncertainWrite?: boolean;
			};
			expect(result.status).toBe("completed");
			expect(result.uncertainWrite).toBeUndefined();
			expect(lease.isLocked(cwd)).toBe(false);
		}

		await runImplementer("impl_a");
	});

	it("marks failed+uncertain and retains lock when release fails", async () => {
		vi.useFakeTimers();
		const cwd = tempDir("momo-release-fail-cwd-");
		const cacheRoot = tempDir("momo-release-fail-cache-");
		const lease = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });
		const token = createLeaseToken();
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "implementer", assignmentId: "release01" });
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: fixture.env,
			poolRegistry: fixture.pool,
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
		dispatchAssignment({
			pool: fixture.pool,
			role: fixture.role, controlRoot: fixture.control.root, paths: fixture.paths, assignmentId: fixture.assignmentId,
			workerId: fixture.workerId, generation: fixture.generation, task: "edit",
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
		const result = parseJsonFile(fixture.paths.result) as {
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
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "implementer", assignmentId: "abortone" });
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: fixture.env,
			poolRegistry: fixture.pool,
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
		dispatchAssignment({
			pool: fixture.pool,
			role: fixture.role, controlRoot: fixture.control.root, paths: fixture.paths, assignmentId: fixture.assignmentId,
			workerId: fixture.workerId, generation: fixture.generation, task: "edit",
		});
		atomicWriteJson(fixture.paths.cancel, {
			version: 1,
			runId: fixture.assignmentId,
			workerId: fixture.workerId,
			reason: "parent_abort",
			issuedAt: new Date().toISOString(),
		});
		await vi.advanceTimersByTimeAsync(200);
		const before = parseJsonFile(fixture.paths.result) as {
			status: string;
			uncertainWrite?: boolean;
		};
		expect(before.status).toBe("aborted");
		expect(before.uncertainWrite).toBeUndefined();
		expect(lease.isLocked(cwd)).toBe(false);

		// After-write path: mutate then abort settle.
		const fixture2 = setupPoolWorkerFixture({ cacheRoot, cwd, role: "implementer", generation: 2, assignmentId: "aborttwo" });
		const pi2 = createFakePi();
		installMomoWorker(pi2 as unknown as ExtensionAPI, {
			env: fixture2.env,
			poolRegistry: fixture2.pool,
			leaseManager: lease,
			now: () => 1_000,
		});
		await pi2.emit("session_start", {}, ctx);
		dispatchAssignment({
			pool: fixture2.pool,
			role: fixture2.role, controlRoot: fixture2.control.root, paths: fixture2.paths, assignmentId: fixture2.assignmentId,
			workerId: fixture2.workerId, generation: fixture2.generation, task: "edit",
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
		const after = parseJsonFile(fixture2.paths.result) as {
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
	async function setupProvision(options: {
		label: string;
		runCommand: (
			file: string,
			args: readonly string[],
		) => Promise<{ code: number; stdout: string; stderr: string }>;
		readyTimeoutMs?: number;
		pool?: PoolRegistry;
		cwd?: string;
		cacheRoot?: string;
	}) {
		installFakeHerdrExtension();
		const cacheRoot = options.cacheRoot ?? tempDir(`momo-${options.label}-cache-`);
		const cwd = options.cwd ?? tempDir(`momo-${options.label}-cwd-`);
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "test-ws",
			socketPath: "test-sock",
		});
		const pool = options.pool ?? new PoolRegistry(identity.poolKey, cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: `parent-${options.label}`,
			client: createTestHerdrClient({ runCommand: options.runCommand }),
			poolRegistry: pool,
			cacheRoot: pool.cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "test-ws",
			socketPath: "test-sock",
			readyTimeoutMs: options.readyTimeoutMs ?? 500,
			pollIntervalMs: 20,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});
		const session = await factory({ cwd, role: getRole("scout") });
		return { session, pool, cwd, identity, cacheRoot };
	}

	function splitOk(paneId = "w1:p9") {
		return {
			code: 0,
			stdout: JSON.stringify({ id: "s", result: { pane: { pane_id: paneId } } }),
			stderr: "",
		};
	}

	function agentStartOk(paneId: string, name: string) {
		return {
			code: 0,
			stdout: JSON.stringify({
				id: "s",
				result: {
					pane_id: paneId,
					name,
					agent: "pi",
					interactive_ready: true,
				},
			}),
			stderr: "",
		};
	}

	it("rename failure after split retains pane identity + paneClosed when close succeeds", async () => {
		const closed: string[] = [];
		const { session, pool } = await setupProvision({
			label: "rename-fail",
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") return splitOk("w1:p9");
				if (args[0] === "pane" && args[1] === "rename") {
					return { code: 1, stdout: "", stderr: "rename failed" };
				}
				if (args[0] === "pane" && args[1] === "close") {
					closed.push(String(args[2]));
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		await expect(session.prompt("start")).rejects.toThrow(/rename failed/i);
		expect(closed).toEqual(["w1:p9"]);
		const record = pool.getByRole("scout")!;
		expect(record.status).toBe("unhealthy");
		expect(record.paneId).toBe("w1:p9");
		expect(record.agentName).toBeTruthy();
		expect(record.paneClosed).toBe(true);
		expect(isArchivalTombstone(record)).toBe(false);
		expect(selectClosablePoolWorkers([record]).closable).toHaveLength(1);
	});

	it("metadata failure after split retains pane identity when close succeeds", async () => {
		const closed: string[] = [];
		const { session, pool } = await setupProvision({
			label: "meta-fail",
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") return splitOk("w1:p9");
				if (args[0] === "pane" && args[1] === "report-metadata") {
					return { code: 1, stdout: "", stderr: "metadata failed" };
				}
				if (args[0] === "pane" && args[1] === "close") {
					closed.push(String(args[2]));
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		await expect(session.prompt("start")).rejects.toThrow(/metadata failed|report-metadata/i);
		expect(closed).toEqual(["w1:p9"]);
		const record = pool.getByRole("scout")!;
		expect(record.paneId).toBe("w1:p9");
		expect(record.paneClosed).toBe(true);
		expect(record.status).toBe("unhealthy");
	});

	it("agent-start failure after split retains paneId/agentName/paneClosed on successful close", async () => {
		const closed: string[] = [];
		const { session, pool } = await setupProvision({
			label: "agent-start-fail",
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") return splitOk("w1:p9");
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
					closed.push(String(args[2]));
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		await expect(session.prompt("start")).rejects.toThrow(/start failed/);
		expect(closed).toEqual(["w1:p9"]);
		const record = pool.getByRole("scout")!;
		expect(record.status).toBe("unhealthy");
		expect(record.paneId).toBe("w1:p9");
		expect(record.agentName).toMatch(/momo_/);
		expect(record.paneClosed).toBe(true);
		expect(record.generationTombstone).toBeGreaterThanOrEqual(1);
	});

	it("manifest write failure after agent-start retains closed-pane identity", async () => {
		const closed: string[] = [];
		const { session, pool } = await setupProvision({
			label: "manifest-fail",
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") return splitOk("w1:p9");
				if (args[0] === "agent" && args[1] === "start") {
					const { workerControlPaths } = await import("../src/herdr/assignment-spool.js");
					const control = workerControlPaths(pool.poolRoot, "scout");
					mkdirSync(control.manifest, { recursive: true, mode: 0o700 });
					return agentStartOk("w1:p9", String(args[2]));
				}
				if (args[0] === "pane" && args[1] === "close") {
					closed.push(String(args[2]));
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		await expect(session.prompt("start")).rejects.toThrow();
		expect(closed).toEqual(["w1:p9"]);
		const record = pool.getByRole("scout")!;
		expect(record.status).toBe("unhealthy");
		expect(record.paneId).toBe("w1:p9");
		expect(record.paneClosed).toBe(true);
	});

	it("close failure after provision error keeps paneClosed:false for cleanup retry", async () => {
		const closed: string[] = [];
		const { session, pool } = await setupProvision({
			label: "close-fail",
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") return splitOk("w1:p9");
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
					closed.push(String(args[2]));
					return { code: 1, stdout: "", stderr: "busy" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		await expect(session.prompt("start")).rejects.toThrow(/start failed/);
		expect(closed).toEqual(["w1:p9"]);
		const record = pool.getByRole("scout")!;
		expect(record.status).toBe("unhealthy");
		expect(record.paneId).toBe("w1:p9");
		expect(record.agentName).toBeTruthy();
		expect(record.paneClosed).toBe(false);
		expect(selectClosablePoolWorkers([record]).closable).toHaveLength(1);
	});

	it("confirmed-close unhealthy is cleanable without re-closing when agent is gone", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-confirmed-close-cache-");
		const cwd = tempDir("momo-confirmed-close-cwd-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "test-ws",
			socketPath: "test-sock",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const { stableWorkerId } = await import("../src/herdr/pool-identity.js");
		pool.upsert({
			workerId: stableWorkerId(identity.poolKey, "scout"),
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p9",
			agentName: "momo_scout_agent",
			cwd: identity.canonicalRoot,
			status: "unhealthy",
			paneClosed: true,
			updatedAt: new Date().toISOString(),
		});
		const closed: string[] = [];
		const commands = new Map<string, { handler: (args: string, ctx: unknown) => Promise<void> }>();
		const pi = {
			handlers: new Map<string, Function[]>(),
			commands,
			registerTool: vi.fn(),
			setActiveTools: vi.fn(),
			sendUserMessage: vi.fn(),
			sendMessage: vi.fn(),
			registerCommand: vi.fn(
				(name: string, def: { handler: (args: string, ctx: unknown) => Promise<void> }) => {
					commands.set(name, def);
				},
			),
			on() {},
		};
		installMomoParent(pi as unknown as ExtensionAPI, {
			cwd: identity.canonicalRoot,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "parent-confirmed-close",
				HERDR_ENV: "1",
				HERDR_PANE_ID: "w1:p1",
				HERDR_WORKSPACE_ID: "test-ws",
				HERDR_SOCKET_PATH: "test-sock",
			},
			client: createTestHerdrClient({
				runCommand: async (_file, args) => {
					if (args[0] === "pane" && args[1] === "close") {
						closed.push(String(args[2]));
						return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
					}
					if (args[0] === "agent" && args[1] === "get") {
						return {
							code: 1,
							stdout: JSON.stringify({
								id: "g",
								error: { code: "agent_not_found", message: "gone" },
							}),
							stderr: "",
						};
					}
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				},
			}),
			poolRegistry: pool,
		});
		await commands.get("momo-cleanup")!.handler("", {
			ui: { notify: () => undefined },
		} as never);
		expect(closed).toEqual([]);
		expect(isArchivalTombstone(pool.getByRole("scout")!)).toBe(true);
	});

	it("no-pane provision failure archives reservation so N+1 can reprovision", async () => {
		const cwd = tempDir("momo-nopane-cwd-");
		const cacheRoot = tempDir("momo-nopane-cache-");
		const { session, pool } = await setupProvision({
			label: "nopane",
			cwd,
			cacheRoot,
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					return { code: 1, stdout: "", stderr: "split failed" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		await expect(session.prompt("start")).rejects.toThrow(/split failed/i);
		const archived = pool.getByRole("scout")!;
		expect(isArchivalTombstone(archived)).toBe(true);
		expect(archived.generationTombstone).toBeGreaterThanOrEqual(1);
		expect(archived.paneId).toBeUndefined();
		const tombstone = archived.generationTombstone;

		let provisionedGen: string | undefined;
		const { session: next } = await setupProvision({
			label: "nopane-n1",
			cwd,
			cacheRoot,
			pool,
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
					provisionedGen = envArgs
						.find((v) => v.startsWith("MOMO_WORKER_GENERATION="))
						?.slice("MOMO_WORKER_GENERATION=".length);
					const controlDir =
						envArgs.find((v) => v.startsWith("MOMO_CONTROL_DIR="))?.slice(17) ??
						envArgs.find((v) => v.startsWith("MOMO_IPC_DIR="))?.slice(12) ??
						"";
					const wid = envArgs.find((v) => v.startsWith("MOMO_WORKER_ID="))?.slice(15) ?? "";
					const runId = envArgs.find((v) => v.startsWith("MOMO_RUN_ID="))?.slice(12) ?? "";
					atomicWriteJson(path.join(controlDir, "ready.json"), {
						version: 1,
						runId,
						workerId: wid,
						readyAt: new Date().toISOString(),
					});
					atomicWriteJson(path.join(controlDir, "heartbeat.json"), {
						version: 1,
						runId,
						workerId: wid,
						at: new Date().toISOString(),
						seq: 1,
					});
					return splitOk("w1:p2");
				}
				if (args[0] === "agent" && args[1] === "start") {
					return agentStartOk("w1:p2", String(args[2]));
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const proxy = next as unknown as {
			assignmentId: string;
			workerId: string;
			paths: { result: string };
		};
		const promptPromise = next.prompt("again").then(() => next.agent!.waitForIdle());
		await new Promise((r) => setTimeout(r, 80));
		atomicWriteJson(proxy.paths.result, {
			version: 1,
			runId: proxy.assignmentId,
			workerId: proxy.workerId,
			status: "completed",
			messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }] }],
			finishedAt: new Date().toISOString(),
		});
		await promptPromise;
		expect(Number(provisionedGen)).toBe(tombstone + 1);
		expect(pool.getByRole("scout")?.generation).toBe(tombstone + 1);
		expect(pool.getByRole("scout")?.paneId).toBe("w1:p2");
	});

	it("ready-timeout after finalized pane keeps pane/agent identity", async () => {
		const { session, pool } = await setupProvision({
			label: "ready-timeout",
			readyTimeoutMs: 80,
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") return splitOk("w1:p8");
				if (args[0] === "agent" && args[1] === "start") {
					return agentStartOk("w1:p8", String(args[2]));
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		await expect(session.prompt("start")).rejects.toThrow(/did not become ready/);
		const record = pool.getByRole("scout")!;
		expect(record.status).toBe("unhealthy");
		expect(record.paneId).toBe("w1:p8");
		expect(record.agentName).toBeTruthy();
		expect(record.paneClosed).not.toBe(true);
		expect(isArchivalTombstone(record)).toBe(false);
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

	it("read-only unresolved cancel => unhealthy; pre-lease implementer => unhealthy not uncertain", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-cancel-cache-");
		const cwd = tempDir("momo-cancel-cwd-");
		let gets = 0;
		let now = 1_000_000;
		const client = createTestHerdrClient({
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
						readyAt: new Date(now).toISOString(),
					});
					atomicWriteJson(path.join(ipc, "heartbeat.json"), {
						version: 1,
						runId,
						workerId,
						at: new Date(now).toISOString(),
						seq: 1,
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
					if (gets <= 2) {
						return {
							code: 1,
							stdout: JSON.stringify({
								id: "g",
								error: { code: "agent_not_found", message: "missing" },
							}),
							stderr: "",
						};
					}
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
		const identity = resolvePoolIdentity({ cwd, canonicalRoot: cwd, workspaceId: "test-ws", socketPath: "test-sock" });
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: "parent-c",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "test-ws",
			socketPath: "test-sock",
			pollIntervalMs: 20,
			readyTimeoutMs: 2_000,
			heartbeatStaleMs: 60_000,
			now: () => now,
			sleep: async (ms) => {
				now += ms;
			},
		});
		const scout = await factory({ cwd, role: getRole("scout") });
		const scoutProxy = scout as unknown as { paths: { cancel: string }; uncertainWrite?: boolean };
		await scout.prompt("x");
		await scout.abort();
		expect(pool.list().find((p) => p.role === "scout")?.status).toBe("unhealthy");
		expect(pool.list().find((p) => p.role === "scout")?.uncertainWrite).toBeUndefined();
		expect(scoutProxy.uncertainWrite).toBeFalsy();
		expect(tryReadIpcJson(scoutProxy.paths.cancel)).toBeTruthy();

		const impl = await factory({ cwd, role: getRole("implementer") });
		const implProxy = impl as unknown as {
			paths: { cancel: string };
			uncertainWrite: boolean;
		};
		await impl.prompt("y");
		await impl.abort();
		expect(pool.list().find((p) => p.role === "implementer")?.status).toBe("unhealthy");
		expect(pool.list().find((p) => p.role === "implementer")?.uncertainWrite).toBeUndefined();
		expect(tryReadIpcJson(implProxy.paths.cancel)).toBeTruthy();
		expect(implProxy.uncertainWrite).toBe(false);
		// No terminal-key / agentWait escalation on shared panes.
		expect(gets).toBe(2);
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
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "scout", assignmentId: "shutdown01" });
		const parentEpoch = "shutdown-epoch";
		fixture.pool.upsert({
			workerId: fixture.workerId,
			generation: fixture.generation,
			generationTombstone: fixture.generation,
			role: "scout",
			paneId: "w1:p2",
			agentName: "momo_scout",
			status: "busy",
			activeAssignmentId: fixture.assignmentId,
			activeParentEpoch: parentEpoch,
			updatedAt: new Date().toISOString(),
		});
		const keys: string[][] = [];
		const client = createTestHerdrClient({
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
				MOMO_PARENT_EPOCH: parentEpoch,
				HERDR_ENV: "1",
				HERDR_PANE_ID: "w1:p1",
				HERDR_WORKSPACE_ID: "test-ws",
				HERDR_SOCKET_PATH: "test-sock",
			},
			client,
			poolRegistry: fixture.pool,
			parentEpoch,
		});
		await pi.emit("session_shutdown", {});
		expect(tryReadIpcJson(fixture.paths.cancel)).toMatchObject({
			reason: "parent_session_shutdown",
			workerId: fixture.workerId,
		});
		expect(keys).toHaveLength(0);
	});

	it("session_shutdown writes no cancel/keys for missing or foreign activeParentEpoch", async () => {
		const cwd = tempDir("momo-quit-epoch-cwd-");
		const cacheRoot = tempDir("momo-quit-epoch-cache-");
		const missing = setupPoolWorkerFixture({
			cacheRoot,
			cwd,
			role: "scout",
			assignmentId: "shutmiss01",
		});
		const foreign = setupPoolWorkerFixture({
			cacheRoot,
			cwd,
			role: "planner",
			assignmentId: "shutforn01",
		});
		missing.pool.upsert({
			workerId: missing.workerId,
			generation: missing.generation,
			generationTombstone: missing.generation,
			role: "scout",
			paneId: "w1:p2",
			agentName: "momo_scout",
			status: "busy",
			activeAssignmentId: missing.assignmentId,
			updatedAt: new Date().toISOString(),
		});
		missing.pool.upsert({
			workerId: foreign.workerId,
			generation: foreign.generation,
			generationTombstone: foreign.generation,
			role: "planner",
			paneId: "w1:p3",
			agentName: "momo_planner",
			status: "busy",
			activeAssignmentId: foreign.assignmentId,
			activeParentEpoch: "other-epoch",
			updatedAt: new Date().toISOString(),
		});
		const keys: string[][] = [];
		const client = createTestHerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "agent" && args[1] === "send-keys") {
					keys.push([...args]);
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const { cancelEpochAssignmentsOnQuit } = await import("../src/extensions/parent.js");
		await cancelEpochAssignmentsOnQuit(missing.pool, "shutdown-epoch", client);
		expect(tryReadIpcJson(missing.paths.cancel)).toBeUndefined();
		expect(tryReadIpcJson(foreign.paths.cancel)).toBeUndefined();
		expect(keys).toHaveLength(0);
	});

	it("removes matching-epoch queue/claiming; leaves foreign FIFO; cancels active epoch", async () => {
		const cwd = tempDir("momo-quit-queue-cwd-");
		const cacheRoot = tempDir("momo-quit-queue-cache-");
		const fixture = setupPoolWorkerFixture({
			cacheRoot,
			cwd,
			role: "implementer",
			assignmentId: "activeepoch001",
		});
		const exitingEpoch = "shutdown-epoch";
		const foreignEpoch = "other-parent-epoch";
		fixture.pool.upsert({
			workerId: fixture.workerId,
			generation: fixture.generation,
			generationTombstone: fixture.generation,
			role: "implementer",
			paneId: "w1:p2",
			agentName: "momo_implementer",
			status: "busy",
			activeAssignmentId: fixture.assignmentId,
			activeParentEpoch: foreignEpoch,
			updatedAt: new Date().toISOString(),
		});

		// Non-active claiming entry owned by exiting epoch — must be removed.
		enqueueAssignment(fixture.pool.poolRoot, "implementer", {
			assignmentId: "claimours000001",
			workerId: fixture.workerId,
			generation: fixture.generation,
			parentEpoch: exitingEpoch,
			task: "ours-claiming-not-active",
		});
		beginClaimHead(fixture.pool.poolRoot, "implementer", fixture.generation);

		// Mixed FIFO: ours / foreign / ours / foreign — only ours removed.
		enqueueAssignment(fixture.pool.poolRoot, "implementer", {
			assignmentId: "queuedours00001",
			workerId: fixture.workerId,
			generation: fixture.generation,
			parentEpoch: exitingEpoch,
			task: "ours-queued",
		});
		enqueueAssignment(fixture.pool.poolRoot, "implementer", {
			assignmentId: "queuedforeign01",
			workerId: fixture.workerId,
			generation: fixture.generation,
			parentEpoch: foreignEpoch,
			task: "foreign-queued",
		});
		enqueueAssignment(fixture.pool.poolRoot, "implementer", {
			assignmentId: "queuedours00002",
			workerId: fixture.workerId,
			generation: fixture.generation,
			parentEpoch: exitingEpoch,
			task: "ours-queued-tail",
		});
		enqueueAssignment(fixture.pool.poolRoot, "implementer", {
			assignmentId: "claimforeign001",
			workerId: fixture.workerId,
			generation: fixture.generation,
			parentEpoch: foreignEpoch,
			task: "foreign-still-queued",
		});

		const client = createTestHerdrClient({
			runCommand: async () => ({
				code: 0,
				stdout: JSON.stringify({ id: "ok", result: {} }),
				stderr: "",
			}),
		});
		const { cancelEpochAssignmentsOnQuit } = await import("../src/extensions/parent.js");
		await cancelEpochAssignmentsOnQuit(fixture.pool, exitingEpoch, client);

		expect(listQueue(fixture.pool.poolRoot, "implementer").map((e) => e.assignmentId)).toEqual([
			"queuedforeign01",
			"claimforeign001",
		]);
		expect(listClaiming(fixture.pool.poolRoot, "implementer")).toHaveLength(0);
		// Active belongs to foreign epoch — no cancel written for it.
		expect(tryReadIpcJson(fixture.paths.cancel)).toBeUndefined();
	});

	it("claiming entry that is the active epoch assignment receives cancel IPC", async () => {
		const cwd = tempDir("momo-quit-claim-cwd-");
		const cacheRoot = tempDir("momo-quit-claim-cache-");
		const fixture = setupPoolWorkerFixture({
			cacheRoot,
			cwd,
			role: "implementer",
			assignmentId: "claimactive0001",
		});
		const exitingEpoch = "shutdown-epoch";
		enqueueAssignment(fixture.pool.poolRoot, "implementer", {
			assignmentId: fixture.assignmentId,
			workerId: fixture.workerId,
			generation: fixture.generation,
			parentEpoch: exitingEpoch,
			task: "active-claiming",
		});
		beginClaimHead(fixture.pool.poolRoot, "implementer", fixture.generation);
		fixture.pool.upsert({
			workerId: fixture.workerId,
			generation: fixture.generation,
			generationTombstone: fixture.generation,
			role: "implementer",
			paneId: "w1:p2",
			agentName: "momo_implementer",
			status: "busy",
			activeAssignmentId: fixture.assignmentId,
			activeParentEpoch: exitingEpoch,
			updatedAt: new Date().toISOString(),
		});
		const client = createTestHerdrClient({
			runCommand: async () => ({
				code: 0,
				stdout: JSON.stringify({ id: "ok", result: {} }),
				stderr: "",
			}),
		});
		const { cancelEpochAssignmentsOnQuit } = await import("../src/extensions/parent.js");
		await cancelEpochAssignmentsOnQuit(fixture.pool, exitingEpoch, client);
		expect(tryReadIpcJson(fixture.paths.cancel)).toMatchObject({
			reason: "parent_session_shutdown",
			runId: fixture.assignmentId,
		});
		// Active claiming entry is canceled, not removed (recovery evidence).
		expect(listClaiming(fixture.pool.poolRoot, "implementer")).toHaveLength(1);
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
		const cwd = tempDir("momo-input-cwd-");
		const cacheRoot = tempDir("momo-input-cache-");
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "scout", assignmentId: "input001" });
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: fixture.env,
			poolRegistry: fixture.pool,
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

		dispatchAssignment({
			pool: fixture.pool,
			role: fixture.role, controlRoot: fixture.control.root, paths: fixture.paths, assignmentId: fixture.assignmentId,
			workerId: fixture.workerId, generation: fixture.generation, task: "look",
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
		const rejectedAfterSettlement = await pi.emit(
			"input",
			{ type: "input", text: "follow up", source: "interactive" },
			ctx,
		);
		expect(rejectedAfterSettlement).toEqual({ action: "handled" });
		expect(pi.setActiveTools).toHaveBeenCalledWith(["read", "grep", "find", "ls"]);
	});
});

import {
	chmodSync,
	mkdirSync,
	mkdtempSync,
	rmSync,
	symlinkSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { installMomoParent, reconcileRegistry } from "../src/extensions/parent.js";
import { installMomoWorker } from "../src/extensions/worker-runtime.js";
import { HerdrClient } from "../src/herdr/client.js";
import { createTestHerdrClient } from "./helpers/herdr-mock-client.js";
import { PaneRegistry, selectClosablePanes } from "../src/herdr/registry.js";
import {
	isArchivalTombstone,
	PoolRegistry,
	selectClosablePoolWorkers,
} from "../src/herdr/pool-registry.js";
import { resolvePoolIdentity } from "../src/herdr/pool-identity.js";
import { assignmentSpoolPaths } from "../src/herdr/assignment-spool.js";
import {
	appendEvent,
	atomicWriteJson,
	readEventsIncrementally,
	type IpcEvent,
} from "../src/ipc/spool.js";
import { IpcValidationError, parseJsonFile, tryReadIpcJson } from "../src/ipc/validate.js";
import {
	createLeaseToken,
	WriterLeaseManager,
	LeaseWaitCancelledError,
	LeaseWaitTimeoutError,
} from "../src/lease/writer-lease.js";
import { createHerdrChildSessionFactory } from "../src/delegation/herdr-factory.js";
import { getRole } from "../src/roles.js";
import { dispatchAssignment, setupPoolWorkerFixture } from "./helpers/pool-fixture.js";

const tempDirs: string[] = [];
const previousPiDir = process.env.PI_CODING_AGENT_DIR;

afterEach(() => {
	vi.useRealTimers();
	while (tempDirs.length > 0) {
		const dir = tempDirs.pop();
		if (dir) rmSync(dir, { recursive: true, force: true });
	}
	if (previousPiDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
	else process.env.PI_CODING_AGENT_DIR = previousPiDir;
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
	const commands = new Map<string, { handler: (args: string, ctx: unknown) => Promise<void> }>();
	let runtimeReady = false;
	return {
		handlers,
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
		on(event: string, handler: Function) {
			const list = handlers.get(event) ?? [];
			list.push(handler);
			handlers.set(event, list);
		},
		async emit(event: string, payload: unknown = {}, ctx?: unknown) {
			if (event === "session_start") runtimeReady = true;
			void runtimeReady;
			for (const handler of handlers.get(event) ?? []) {
				await handler(payload, ctx);
			}
		},
	};
}

function baseResult(overrides: Record<string, unknown> = {}) {
	return {
		version: 1,
		runId: "r1",
		workerId: "scout_1",
		status: "completed",
		messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }] }],
		finishedAt: new Date().toISOString(),
		...overrides,
	};
}

describe("parent reconciliation from result.json", () => {
	async function setupPane(status: "completed" | "aborted" | "failed", opts: {
		role?: "scout" | "implementer";
		uncertainWrite?: boolean;
		corrupt?: boolean;
		omitResult?: boolean;
		herdrStatus?: string;
	} = {}) {
		const cwd = tempDir("momo-recon-cwd-");
		const cacheRoot = tempDir("momo-recon-cache-");
		const spool = tempDir("momo-recon-spool-");
		const role = opts.role ?? "scout";
		const workerId = role === "implementer" ? "impl_1" : "scout_1";
		const registry = new PaneRegistry("parent-recon", cacheRoot);
		registry.upsert({
			workerId,
			runId: "r1",
			role,
			paneId: "w1:p2",
			agentName: `momo_${workerId}`,
			spoolRoot: spool,
			cwd,
			status: "running",
			updatedAt: new Date().toISOString(),
		});
		if (!opts.omitResult) {
			if (opts.corrupt) {
				writeFileSync(path.join(spool, "result.json"), "{not-json", { mode: 0o600 });
			} else {
				atomicWriteJson(
					path.join(spool, "result.json"),
					baseResult({
						workerId,
						status,
						...(opts.uncertainWrite ? { uncertainWrite: true } : {}),
					}),
				);
			}
		}
		const notifies: string[] = [];
		const client = createTestHerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "agent" && args[1] === "get") {
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "g",
							result: {
								type: "agent_info",
								agent: { agent_status: opts.herdrStatus ?? "idle" },
							},
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		await reconcileRegistry(registry, client, {
			ui: { notify: (m) => notifies.push(m) },
		});
		return { registry, notifies, workerId };
	}

	it("maps completed/aborted/failed(+uncertain) from result after relaunch", async () => {
		const completed = await setupPane("completed");
		expect(completed.registry.list()[0]?.status).toBe("completed");

		const aborted = await setupPane("aborted");
		expect(aborted.registry.list()[0]?.status).toBe("aborted");

		const failed = await setupPane("failed");
		expect(failed.registry.list()[0]?.status).toBe("failed");

		const uncertain = await setupPane("failed", {
			role: "implementer",
			uncertainWrite: true,
		});
		expect(uncertain.registry.list()[0]?.status).toBe("uncertain");
		expect(uncertain.registry.list()[0]?.uncertainWrite).toBe(true);
	});

	it("idle/done with missing result: read-only failed, implementer uncertain", async () => {
		const scout = await setupPane("completed", { omitResult: true, herdrStatus: "idle" });
		expect(scout.registry.list()[0]?.status).toBe("failed");

		const impl = await setupPane("completed", {
			omitResult: true,
			herdrStatus: "done",
			role: "implementer",
		});
		expect(impl.registry.list()[0]?.status).toBe("uncertain");
		expect(impl.registry.list()[0]?.uncertainWrite).toBe(true);
	});

	it("working/blocked with no result leave active; corrupt result is terminal with notify", async () => {
		const working = await setupPane("completed", {
			omitResult: true,
			herdrStatus: "working",
		});
		expect(working.registry.list()[0]?.status).toBe("running");
		const blocked = await setupPane("completed", {
			omitResult: true,
			herdrStatus: "blocked",
		});
		expect(blocked.registry.list()[0]?.status).toBe("running");

		const corrupt = await setupPane("completed", { corrupt: true, role: "implementer" });
		expect(corrupt.registry.list()[0]?.status).toBe("uncertain");
		expect(corrupt.notifies.some((n) => /Corrupt|invalid result/i.test(n))).toBe(true);
	});

	it("shutdown -> result -> relaunch discovers prior registry statuses", async () => {
		const cwd = tempDir("momo-relaunch-cwd-");
		const cacheRoot = tempDir("momo-relaunch-cache-");
		const spool = tempDir("momo-relaunch-spool-");
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "planner", assignmentId: "relaunch01" });
		const parentEpoch = "relaunch-epoch";
		fixture.pool.upsert({
			...fixture.pool.getByRole("planner")!,
			status: "busy",
			activeAssignmentId: fixture.assignmentId,
			activeParentEpoch: parentEpoch,
			updatedAt: new Date().toISOString(),
		});
		const registry = new PaneRegistry("stable-relaunch", cacheRoot);
		registry.upsert({
			workerId: "planner_1",
			runId: "r1",
			role: "planner",
			paneId: "w1:p3",
			agentName: "momo_planner_1",
			spoolRoot: spool,
			cwd,
			status: "running",
			updatedAt: new Date().toISOString(),
		});
		const pi = createFakePi();
		const client = createTestHerdrClient({
			runCommand: async () => ({
				code: 0,
				stdout: JSON.stringify({ id: "ok", result: {} }),
				stderr: "",
			}),
		});
		installMomoParent(pi as unknown as ExtensionAPI, {
			cwd,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "stable-relaunch",
				MOMO_PARENT_EPOCH: parentEpoch,
				HERDR_ENV: "1",
				HERDR_PANE_ID: "w1:p1",
				HERDR_WORKSPACE_ID: "test-ws",
				HERDR_SOCKET_PATH: "test-sock",
			},
			client,
			registry,
			poolRegistry: fixture.pool,
			parentEpoch,
		});
		await pi.emit("session_shutdown", {});
		expect(tryReadIpcJson(fixture.paths.cancel)).toBeTruthy();

		atomicWriteJson(path.join(spool, "result.json"), baseResult({
			workerId: "planner_1",
			status: "aborted",
			stopReason: "aborted",
		}));
		await reconcileRegistry(registry, client, {});

		const notifies: string[] = [];
		await pi.emit("session_start", {}, {
			hasUI: true,
			ui: { notify: (m: string) => notifies.push(m) },
		});
		expect(registry.list()[0]?.status).toBe("aborted");
	});
});

describe("NDJSON fail-closed", () => {
	it("rejects symlink, directory, and group-readable 0644 events files", () => {
		const root = tempDir("momo-ndjson-");
		const target = path.join(root, "real.ndjson");
		writeFileSync(target, "", { mode: 0o600 });
		const link = path.join(root, "events.ndjson");
		symlinkSync(target, link);
		expect(() => readEventsIncrementally(link, 0)).toThrow(IpcValidationError);

		const dir = path.join(root, "as-dir");
		mkdirSync(dir);
		expect(() => readEventsIncrementally(dir, 0)).toThrow(/regular file/);

		const open = path.join(root, "open.ndjson");
		writeFileSync(open, `${JSON.stringify({ version: 1 })}\n`, { mode: 0o644 });
		chmodSync(open, 0o644);
		expect(() => readEventsIncrementally(open, 0)).toThrow(/group\/other permissions/);
	});

	it("rejects malformed lines", () => {
		const root = tempDir("momo-ndjson-bad-");
		const file = path.join(root, "events.ndjson");
		writeFileSync(file, "{bad\n", { mode: 0o600 });
		expect(() => readEventsIncrementally(file, 0)).toThrow(/Malformed IPC event/);
	});

	it("factory terminal-fails on corrupted events even when result is later valid", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-evt-cache-");
		const cwd = tempDir("momo-evt-cwd-");
		let controlDir = "";
		let workerId = "";
		let runId = "";
		const calls: string[][] = [];
		const client = createTestHerdrClient({
			runCommand: async (_file, args) => {
				calls.push([...args]);
				if (args[0] === "pane" && args[1] === "split") {
					const envArgs = args.filter((a, i) => args[i - 1] === "--env");
					controlDir =
						envArgs.find((v) => v.startsWith("MOMO_CONTROL_DIR="))?.slice("MOMO_CONTROL_DIR=".length) ??
						"";
					workerId =
						envArgs
							.find((v) => v.startsWith("MOMO_WORKER_ID="))
							?.slice("MOMO_WORKER_ID=".length) ?? "";
					runId =
						envArgs.find((v) => v.startsWith("MOMO_RUN_ID="))?.slice("MOMO_RUN_ID=".length) ??
						"";
					atomicWriteJson(path.join(controlDir, "ready.json"), {
						version: 1,
						runId,
						workerId,
						readyAt: new Date().toISOString(),
					});
					return {
						code: 0,
						stdout: JSON.stringify({ id: "s", result: { pane: { pane_id: "w1:p9" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: {
								pane_id: "w1:p9",
								name: args[2],
								agent: "pi",
								interactive_ready: true,
							},
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const identity = resolvePoolIdentity({ cwd, canonicalRoot: cwd, workspaceId: "test-ws", socketPath: "test-sock" });
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: "parent-evt",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "test-ws",
			socketPath: "test-sock",
			pollIntervalMs: 20,
			readyTimeoutMs: 2_000,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});
		const session = await factory({ cwd, role: getRole("scout") });
		const proxy = session as unknown as { paths: { events: string; result: string; cancel: string }; assignmentId: string; workerId: string };
		await session.prompt("x");
		// Pre-result event corruption remains fail-closed.
		writeFileSync(proxy.paths.events, "{broken\n", { mode: 0o600 });
		await expect(session.agent!.waitForIdle()).rejects.toThrow(/Malformed|event/i);
		expect(pool.list()[0]?.status).toBe("unhealthy");
		expect(tryReadIpcJson(proxy.paths.cancel)).toBeTruthy();
		expect(calls.some((args) => args[0] === "agent" && args[1] === "send-keys")).toBe(false);
	});

	it("accepts a valid result even when events.ndjson is corrupt", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-evt-ok-cache-");
		const cwd = tempDir("momo-evt-ok-cwd-");
		const client = createTestHerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "pane" && args[1] === "split") {
					const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
					const ipc =
						envArgs.find((v) => v.startsWith("MOMO_CONTROL_DIR="))?.slice("MOMO_CONTROL_DIR=".length) ??
						envArgs.find((v) => v.startsWith("MOMO_IPC_DIR="))?.slice("MOMO_IPC_DIR=".length) ??
						"";
					const workerId =
						envArgs.find((v) => v.startsWith("MOMO_WORKER_ID="))?.slice("MOMO_WORKER_ID=".length) ?? "";
					const runId =
						envArgs.find((v) => v.startsWith("MOMO_RUN_ID="))?.slice("MOMO_RUN_ID=".length) ?? "";
					atomicWriteJson(path.join(ipc, "ready.json"), {
						version: 1,
						runId,
						workerId,
						readyAt: new Date().toISOString(),
					});
					atomicWriteJson(path.join(ipc, "heartbeat.json"), {
						version: 1,
						runId,
						workerId,
						at: new Date().toISOString(),
						seq: 1,
					});
					return {
						code: 0,
						stdout: JSON.stringify({ id: "s", result: { pane: { pane_id: "w1:p5" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: {
								pane_id: "w1:p5",
								name: args[2],
								agent: "pi",
								interactive_ready: true,
							},
						}),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const identity = resolvePoolIdentity({ cwd, canonicalRoot: cwd, workspaceId: "test-ws", socketPath: "test-sock" });
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: "parent-evt-ok",
			client,
			poolRegistry: pool,
			cacheRoot,
			canonicalRoot: cwd,
			workspaceId: "test-ws",
			socketPath: "test-sock",
			pollIntervalMs: 20,
			readyTimeoutMs: 2_000,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});
		const session = await factory({ cwd, role: getRole("scout") });
		const proxy = session as unknown as {
			paths: { events: string; result: string };
			assignmentId: string;
			workerId: string;
		};
		const wait = session.prompt("x").then(() => session.agent!.waitForIdle());
		// Write durable result first, then corrupt events — result remains authoritative.
		atomicWriteJson(proxy.paths.result, baseResult({ workerId: proxy.workerId, runId: proxy.assignmentId }));
		writeFileSync(proxy.paths.events, "{broken\n", { mode: 0o600 });
		await expect(wait).resolves.toBeUndefined();
	});

	it("supervises and stops a worker before a result-timeout returns", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-timeout-cache-");
		const cwd = tempDir("momo-timeout-cwd-");
		let controlDir = "";
		let workerId = "";
		let runId = "";
		const calls: string[][] = [];
		const client = createTestHerdrClient({
			runCommand: async (_file, args) => {
				calls.push([...args]);
				if (args[0] === "pane" && args[1] === "split") {
					const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
					controlDir = envArgs.find((v) => v.startsWith("MOMO_CONTROL_DIR="))?.slice("MOMO_CONTROL_DIR=".length) ?? "";
					workerId = envArgs.find((v) => v.startsWith("MOMO_WORKER_ID="))?.slice(15) ?? "";
					runId = envArgs.find((v) => v.startsWith("MOMO_RUN_ID="))?.slice(12) ?? "";
					atomicWriteJson(path.join(controlDir, "ready.json"), {
						version: 1, runId, workerId, readyAt: new Date().toISOString(),
					});
					return { code: 0, stdout: JSON.stringify({ id: "s", result: { pane: { pane_id: "w1:p8" } } }), stderr: "" };
				}
				if (args[0] === "agent" && args[1] === "start") {
					return { code: 0, stdout: JSON.stringify({ id: "a", result: { pane_id: "w1:p8", name: args[2], agent: "pi", interactive_ready: true } }), stderr: "" };
				}
				if (args[0] === "agent" && args[1] === "wait") {
					return { code: 0, stdout: JSON.stringify({ id: "w", result: { type: "agent_info", agent: { agent_status: "done" } } }), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});
		const identity = resolvePoolIdentity({ cwd, canonicalRoot: cwd, workspaceId: "test-ws", socketPath: "test-sock" });
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd, parentPaneId: "w1:p1", parentId: "parent-timeout", client,
			poolRegistry: pool, cacheRoot, canonicalRoot: cwd, workspaceId: "test-ws", socketPath: "test-sock",
			pollIntervalMs: 10, readyTimeoutMs: 1_000, resultTimeoutMs: 40,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});
		const session = await factory({ cwd, role: getRole("implementer") });
		const proxy = session as unknown as { paths: { cancel: string }; uncertainWrite: boolean };
		await session.prompt("wait then edit");
		await expect(session.agent!.waitForIdle()).rejects.toMatchObject({
			message: expect.stringMatching(/Timed out waiting for worker result/i),
			stopReason: "error",
		});
		// Pre-lease / no started: heartbeat/result timeout is unhealthy (not uncertain).
		expect(proxy.uncertainWrite).toBe(false);
		expect(pool.getByRole("implementer")?.status).toBe("unhealthy");
		expect(tryReadIpcJson(proxy.paths.cancel)).toBeTruthy();
		expect(calls.some((args) => args[0] === "agent" && args[1] === "send-keys")).toBe(false);
	});
});

describe("lease release race with foreign acquirer", () => {
	it("keeps clean success when foreign owner acquires immediately after release", async () => {
		vi.useFakeTimers();
		const cwd = tempDir("momo-race-cwd-");
		const cacheRoot = tempDir("momo-race-cache-");
		const lease = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "implementer", assignmentId: "race0001" });
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: fixture.env,
			poolRegistry: fixture.pool,
			leaseManager: lease,
			now: () => 1_000,
			sleep: async () => {},
			leaseWaitMs: 1_000,
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

		const originalRelease = lease.release.bind(lease);
		vi.spyOn(lease, "release").mockImplementation((releaseCwd, ownerId, token) => {
			originalRelease(releaseCwd, ownerId, token);
			// Foreign owner grabs the lease before result publication.
			lease.acquire(releaseCwd, "impl_foreign", createLeaseToken());
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
		expect(result.status).toBe("completed");
		expect(result.uncertainWrite).toBeUndefined();
		expect(lease.isLocked(cwd)).toBe(true);
		expect(lease.peekOwner(cwd)?.ownerId).toBe("impl_foreign");
	});
});

describe("cross-parent implementer wait/serialize", () => {
	it("serializes two independent waitAcquire callers and supports cancel/timeout", async () => {
		const cwd = tempDir("momo-wait-cwd-");
		const cacheRoot = tempDir("momo-wait-cache-");
		let now = 1_000;
		const sleepCalls: number[] = [];
		const leasesA = new WriterLeaseManager({
			cacheRoot,
			now: () => now,
			sleep: async (ms) => {
				sleepCalls.push(ms);
				now += ms;
			},
		});
		const leasesB = new WriterLeaseManager({
			cacheRoot,
			now: () => now,
			sleep: async (ms) => {
				sleepCalls.push(ms);
				now += ms;
			},
		});
		const tokenA = createLeaseToken();
		const tokenB = createLeaseToken();
		leasesA.acquire(cwd, "owner-a", tokenA);

		const waiting: string[] = [];
		const waiter = leasesB.waitAcquire(cwd, "owner-b", tokenB, {
			deadlineMs: 5_000,
			intervalMs: 100,
			onWaiting: (info) => waiting.push(info.holder ?? "?"),
		});

		// Release A after waiter has started looping.
		await Promise.resolve();
		leasesA.release(cwd, "owner-a", tokenA);
		const acquired = await waiter;
		expect(acquired.ownerId).toBe("owner-b");
		expect(waiting.length).toBeGreaterThan(0);

		leasesB.release(cwd, "owner-b", tokenB);
		leasesA.acquire(cwd, "owner-a", tokenA);
		await expect(
			leasesB.waitAcquire(cwd, "owner-b", createLeaseToken(), {
				deadlineMs: 300,
				intervalMs: 50,
			}),
		).rejects.toBeInstanceOf(LeaseWaitTimeoutError);

		await expect(
			leasesB.waitAcquire(cwd, "owner-b", createLeaseToken(), {
				deadlineMs: 5_000,
				intervalMs: 50,
				shouldCancel: async () => true,
			}),
		).rejects.toBeInstanceOf(LeaseWaitCancelledError);
		expect(sleepCalls.length).toBeGreaterThan(0);
	});

	it("worker waits read-only, then acquires and enables mutation tools", async () => {
		const cwd = tempDir("momo-wsuccess-cwd-");
		const cacheRoot = tempDir("momo-wsuccess-cache-");
		const lease = new WriterLeaseManager({ cacheRoot });
		const holderToken = createLeaseToken();
		lease.acquire(cwd, "holder", holderToken);
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "implementer", assignmentId: "success01" });
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: fixture.env,
			poolRegistry: fixture.pool,
			leaseManager: lease, leaseWaitMs: 2_000,
		});
		const ctx = { hasUI: true, isIdle: () => true, abort: vi.fn(), cwd } as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);
		dispatchAssignment({
			pool: fixture.pool,
			role: fixture.role, controlRoot: fixture.control.root, paths: fixture.paths, assignmentId: fixture.assignmentId,
			workerId: fixture.workerId, generation: fixture.generation, task: "edit",
		});
		await new Promise((r) => setTimeout(r, 150));
		const before = (pi.setActiveTools as ReturnType<typeof vi.fn>).mock.calls.at(-1)?.[0] as string[];
		expect(before.includes("bash")).toBe(false);
		lease.release(cwd, "holder", holderToken);
		await new Promise((r) => setTimeout(r, 250));
		expect(pi.sendUserMessage).toHaveBeenCalledWith("edit");
		const after = (pi.setActiveTools as ReturnType<typeof vi.fn>).mock.calls.at(-1)?.[0] as string[];
		expect(after).toContain("bash");
		await pi.emit("message_end", { message: { role: "assistant", content: [{ type: "text", text: "done" }], stopReason: "stop" } }, ctx);
		await pi.emit("agent_settled", {}, ctx);
		expect((parseJsonFile(fixture.paths.result) as { status: string }).status).toBe("completed");
		expect(lease.isLocked(cwd)).toBe(false);
	});

	it("worker waits read-only then enables mutation; cancel while waiting aborts cleanly", async () => {
		const cwd = tempDir("momo-wwait-cwd-");
		const cacheRoot = tempDir("momo-wwait-cache-");
		let now = 1_000;
		let allowCancel = false;
		const lease = new WriterLeaseManager({
			cacheRoot,
			now: () => now,
			sleep: async (ms) => {
				// Yield so the test can flip cancel mid-wait without burning the deadline.
				await new Promise((r) => setImmediate(r));
				now += Math.min(ms, 50);
			},
		});
		const holderToken = createLeaseToken();
		lease.acquire(cwd, "holder", holderToken);

		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "implementer", assignmentId: "waiting01" });
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: fixture.env,
			poolRegistry: fixture.pool,
			leaseManager: lease,
			now: () => now,
			sleep: async (ms) => {
				await new Promise((r) => setImmediate(r));
				if (allowCancel) {
					atomicWriteJson(fixture.paths.cancel, {
						version: 1,
						runId: fixture.assignmentId,
						workerId: fixture.workerId,
						reason: "parent_abort",
						issuedAt: new Date().toISOString(),
					});
				}
				now += Math.min(ms, 50);
			},
			leaseWaitMs: 60_000,
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

		await new Promise((r) => setTimeout(r, 120));
		allowCancel = true;
		await new Promise((r) => setTimeout(r, 200));
		expect(pi.setActiveTools).toHaveBeenCalled();
		const lastTools = (pi.setActiveTools as ReturnType<typeof vi.fn>).mock.calls.at(-1)?.[0] as
			| string[]
			| undefined;
		expect(lastTools?.includes("bash")).toBeFalsy();
		const result = parseJsonFile(fixture.paths.result) as {
			status: string;
			uncertainWrite?: boolean;
		};
		expect(result.status).toBe("aborted");
		expect(result.uncertainWrite).toBeUndefined();
		expect(lease.peekOwner(cwd)?.ownerId).toBe("holder");
	});
});

describe("worker cancellation IPC fail-closed", () => {
	it("settles failed instead of ignoring corrupt cancellation IPC", async () => {
		const cwd = tempDir("momo-badcancel-cwd-");
		const cacheRoot = tempDir("momo-badcancel-cache-");
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "scout", assignmentId: "badcan01" });
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: fixture.env,
			poolRegistry: fixture.pool,
		});
		const ctx = { hasUI: true, isIdle: () => true, abort: vi.fn(), cwd } as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);
		dispatchAssignment({
			pool: fixture.pool,
			role: fixture.role, controlRoot: fixture.control.root, paths: fixture.paths, assignmentId: fixture.assignmentId,
			workerId: fixture.workerId, generation: fixture.generation, task: "look",
		});
		atomicWriteJson(fixture.paths.cancel, { version: 1, bad: true });
		await new Promise((r) => setTimeout(r, 180));
		const result = parseJsonFile(fixture.paths.result) as { status: string; errorMessage?: string };
		expect(result.status).toBe("failed");
		expect(result.errorMessage).toMatch(/cancel/i);
		expect(pi.sendUserMessage).not.toHaveBeenCalled();
	});
});

describe("force cleanup recovery retention", () => {
	it("retains registry with paneClosed/recoveryRequired when lease release fails", async () => {
		const cwd = tempDir("momo-force-cwd-");
		const cacheRoot = tempDir("momo-force-cache-");
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "implementer" });
		const pool = fixture.pool;
		pool.upsert({
			workerId: fixture.workerId,
			generation: fixture.generation,
			generationTombstone: fixture.generation,
			role: "implementer",
			paneId: "w1:p9",
			agentName: "momo_implementer",
			status: "uncertain",
			uncertainWrite: true,
			updatedAt: new Date().toISOString(),
		});
		const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });
		leases.acquire(cwd, "someone_else", createLeaseToken());
		const closed: string[] = [];
		const pi = createFakePi();
		const notifies: string[] = [];
		installMomoParent(pi as unknown as ExtensionAPI, {
			cwd,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "parent-force",
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
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				},
			}),
			poolRegistry: pool,
			leaseManager: leases,
		});
		const command = pi.commands.get("momo-cleanup");
		expect(command).toBeTruthy();
		expect(pool.list()).toHaveLength(1);
		// Pi command handlers receive the args string after the command name.
		await command!.handler("--force", {
			ui: {
				confirm: async () => true,
				notify: (m: string) => notifies.push(m),
			},
		} as never);
		// Lease-first: refuse before close when owner is missing/mismatched.
		expect(notifies.join("\n"), `notifies=${JSON.stringify(notifies)}`).toMatch(
			/refused uncertain|lease owner unavailable|mismatched/i,
		);
		expect(closed).toEqual([]);
		const retained = pool.getByRole("implementer");
		expect(retained?.status).toBe("uncertain");
		expect(retained?.paneClosed).not.toBe(true);
	});

	it("force cleanup: transient agentGet refuses; agent_not_found/idle proceed", async () => {
		async function runCleanup(options: {
			agentGet: () => Promise<{ code: number; stdout: string; stderr: string }>;
			label: string;
		}): Promise<{
			notifies: string[];
			closed: string[];
			pool: PoolRegistry;
			leases: WriterLeaseManager;
			workerId: string;
			cwd: string;
		}> {
			const cwd = tempDir(`momo-force-${options.label}-cwd-`);
			const cacheRoot = tempDir(`momo-force-${options.label}-cache-`);
			const fixture = setupPoolWorkerFixture({
				cacheRoot,
				cwd,
				role: "implementer",
			});
			const pool = fixture.pool;
			pool.upsert({
				workerId: fixture.workerId,
				generation: fixture.generation,
				generationTombstone: fixture.generation,
				role: "implementer",
				paneId: "w1:p9",
				agentName: "momo_implementer",
				cwd: fixture.identity.canonicalRoot,
				status: "uncertain",
				uncertainWrite: true,
				updatedAt: new Date().toISOString(),
			});
			const leases = new WriterLeaseManager({
				cacheRoot,
				now: () => 1_000,
			});
			leases.acquire(fixture.identity.canonicalRoot, fixture.workerId, createLeaseToken());
			const closed: string[] = [];
			const notifies: string[] = [];
			const pi = createFakePi();
			installMomoParent(pi as unknown as ExtensionAPI, {
				cwd: fixture.identity.canonicalRoot,
				env: {
					MOMO_PARENT: "1",
					MOMO_PARENT_ID: `parent-${options.label}`,
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
							return options.agentGet();
						}
						return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
					},
				}),
				poolRegistry: pool,
				leaseManager: leases,
			});
			const command = pi.commands.get("momo-cleanup");
			await command!.handler("--force", {
				ui: {
					confirm: async () => true,
					notify: (m: string) => notifies.push(m),
				},
			} as never);
			return {
				notifies,
				closed,
				pool,
				leases,
				workerId: fixture.workerId,
				cwd: fixture.identity.canonicalRoot,
			};
		}

		const transient = await runCleanup({
			label: "transient",
			agentGet: async () => ({
				code: 1,
				stdout: JSON.stringify({
					id: "g",
					error: { code: "timeout", message: "agent get timed out" },
				}),
				stderr: "",
			}),
		});
		expect(transient.closed).toEqual(["w1:p9"]);
		expect(transient.notifies.join("\n")).toMatch(/refused .*agent lookup failed|timeout/i);
		expect(transient.pool.getByRole("implementer")?.status).toBe("uncertain");
		expect(transient.pool.getByRole("implementer")?.paneClosed).toBe(true);
		expect(transient.pool.getByRole("implementer")?.recoveryRequired).not.toBe(true);
		expect(transient.leases.peekOwner(transient.cwd)?.ownerId).toBe(transient.workerId);

		const notFound = await runCleanup({
			label: "notfound",
			agentGet: async () => ({
				code: 1,
				stdout: JSON.stringify({
					id: "g",
					error: { code: "agent_not_found", message: "no such agent" },
				}),
				stderr: "",
			}),
		});
		expect(notFound.closed).toEqual(["w1:p9"]);
		expect(isArchivalTombstone(notFound.pool.getByRole("implementer")!)).toBe(true);
		expect(notFound.leases.peekOwner(notFound.cwd)).toBeUndefined();

		const idle = await runCleanup({
			label: "idle",
			agentGet: async () => ({
				code: 0,
				stdout: JSON.stringify({
					id: "g",
					result: {
						type: "agent_info",
						agent: { agent_status: "idle", name: "momo_implementer" },
					},
				}),
				stderr: "",
			}),
		});
		expect(idle.closed).toEqual(["w1:p9"]);
		expect(isArchivalTombstone(idle.pool.getByRole("implementer")!)).toBe(true);
	});

	it("retries confirmation after paneClosed without re-closing (transient then not-found)", async () => {
		const cwd = tempDir("momo-force-retry-cwd-");
		const cacheRoot = tempDir("momo-force-retry-cache-");
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "implementer" });
		const pool = fixture.pool;
		pool.upsert({
			workerId: fixture.workerId,
			generation: fixture.generation,
			generationTombstone: fixture.generation,
			role: "implementer",
			paneId: "w1:p9",
			agentName: "momo_implementer",
			cwd: fixture.identity.canonicalRoot,
			status: "uncertain",
			uncertainWrite: true,
			updatedAt: new Date().toISOString(),
		});
		const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });
		leases.acquire(fixture.identity.canonicalRoot, fixture.workerId, createLeaseToken());
		const closed: string[] = [];
		let agentGets = 0;
		const notifies: string[] = [];
		const pi = createFakePi();
		installMomoParent(pi as unknown as ExtensionAPI, {
			cwd: fixture.identity.canonicalRoot,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "parent-retry",
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
						agentGets += 1;
						if (agentGets === 1) {
							return {
								code: 1,
								stdout: JSON.stringify({
									id: "g",
									error: { code: "timeout", message: "transient" },
								}),
								stderr: "",
							};
						}
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
			leaseManager: leases,
		});
		const command = pi.commands.get("momo-cleanup");
		const ui = {
			confirm: async () => true,
			notify: (m: string) => notifies.push(m),
		};
		await command!.handler("--force", { ui } as never);
		expect(closed).toEqual(["w1:p9"]);
		expect(pool.getByRole("implementer")?.paneClosed).toBe(true);
		expect(pool.getByRole("implementer")?.status).toBe("uncertain");
		expect(leases.peekOwner(fixture.identity.canonicalRoot)?.ownerId).toBe(fixture.workerId);

		await command!.handler("--force", { ui } as never);
		expect(closed).toEqual(["w1:p9"]); // no second close
		expect(agentGets).toBe(2);
		expect(isArchivalTombstone(pool.getByRole("implementer")!)).toBe(true);
		expect(leases.peekOwner(fixture.identity.canonicalRoot)).toBeUndefined();
	});

	it("retains paneClosed+recoveryRequired when force-release fails after confirmed close", async () => {
		const cwd = tempDir("momo-force-ev-cwd-");
		const cacheRoot = tempDir("momo-force-ev-cache-");
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "implementer" });
		const pool = fixture.pool;
		pool.upsert({
			workerId: fixture.workerId,
			generation: fixture.generation,
			generationTombstone: fixture.generation,
			role: "implementer",
			paneId: "w1:p9",
			agentName: "momo_implementer",
			cwd: fixture.identity.canonicalRoot,
			status: "uncertain",
			uncertainWrite: true,
			updatedAt: new Date().toISOString(),
		});
		const base = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });
		base.acquire(fixture.identity.canonicalRoot, fixture.workerId, createLeaseToken());
		const leases = Object.create(base) as WriterLeaseManager;
		leases.forceReleaseIfOwner = () => ({
			released: false,
			reason: "injected_release_failure",
		});
		leases.peekOwner = (c: string) => base.peekOwner(c);
		const closed: string[] = [];
		const notifies: string[] = [];
		const pi = createFakePi();
		installMomoParent(pi as unknown as ExtensionAPI, {
			cwd: fixture.identity.canonicalRoot,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "parent-ev",
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
							code: 0,
							stdout: JSON.stringify({
								id: "g",
								result: {
									type: "agent_info",
									agent: { agent_status: "done", name: "momo_implementer" },
								},
							}),
							stderr: "",
						};
					}
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				},
			}),
			poolRegistry: pool,
			leaseManager: leases,
		});
		const command = pi.commands.get("momo-cleanup");
		await command!.handler("--force", {
			ui: {
				confirm: async () => true,
				notify: (m: string) => notifies.push(m),
			},
		} as never);
		expect(closed).toEqual(["w1:p9"]);
		expect(notifies.join("\n")).toMatch(/lease not released|injected_release_failure/i);
		const retained = pool.getByRole("implementer");
		expect(retained?.status).toBe("uncertain");
		expect(retained?.paneClosed).toBe(true);
		expect(retained?.recoveryRequired).toBe(true);
		expect(isArchivalTombstone(retained!)).toBe(false);
	});

	it("retains paneClosed + exact lease when terminalization fails after confirmed stop", async () => {
		const cwd = tempDir("momo-force-termfail-cwd-");
		const cacheRoot = tempDir("momo-force-termfail-cache-");
		const fixture = setupPoolWorkerFixture({
			cacheRoot,
			cwd,
			role: "implementer",
			assignmentId: "termfailactive01",
		});
		const pool = fixture.pool;
		pool.upsert({
			workerId: fixture.workerId,
			generation: fixture.generation,
			generationTombstone: fixture.generation,
			role: "implementer",
			paneId: "w1:p9",
			agentName: "momo_implementer",
			cwd: fixture.identity.canonicalRoot,
			status: "uncertain",
			uncertainWrite: true,
			activeAssignmentId: fixture.assignmentId,
			updatedAt: new Date().toISOString(),
		});
		const paths = assignmentSpoolPaths(
			pool.poolRoot,
			"implementer",
			fixture.assignmentId,
		);
		mkdirSync(paths.result, { recursive: true, mode: 0o700 }); // blocks durable terminal result
		const leaseToken = createLeaseToken();
		const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });
		leases.acquire(fixture.identity.canonicalRoot, fixture.workerId, leaseToken);
		const closed: string[] = [];
		const notifies: string[] = [];
		const pi = createFakePi();
		installMomoParent(pi as unknown as ExtensionAPI, {
			cwd: fixture.identity.canonicalRoot,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "parent-termfail",
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
			leaseManager: leases,
		});
		const command = pi.commands.get("momo-cleanup");
		const ui = {
			confirm: async () => true,
			notify: (m: string) => notifies.push(m),
		};
		await command!.handler("--force", { ui } as never);

		expect(closed).toEqual(["w1:p9"]);
		expect(notifies.join("\n")).toMatch(/could not terminalize prior assignments/i);
		const retained = pool.getByRole("implementer");
		expect(retained?.status).toBe("uncertain");
		expect(retained?.paneClosed).toBe(true);
		expect(retained?.recoveryRequired).not.toBe(true);
		expect(isArchivalTombstone(retained!)).toBe(false);
		const owner = leases.peekOwner(fixture.identity.canonicalRoot);
		expect(owner?.ownerId).toBe(fixture.workerId);
		expect(owner?.token).toBe(leaseToken);
		expect(selectClosablePoolWorkers([retained!], { force: true }).closable).toHaveLength(1);

		// After repairing the blocked result path, --force retry can proceed to release+archive.
		rmSync(paths.result, { recursive: true, force: true });
		await command!.handler("--force", { ui } as never);
		expect(closed).toEqual(["w1:p9"]); // no second pane close
		expect(isArchivalTombstone(pool.getByRole("implementer")!)).toBe(true);
		expect(leases.peekOwner(fixture.identity.canonicalRoot)).toBeUndefined();
	});

	it("structured pane_not_found close treats as already closed; agent missing/done archives", async () => {
		async function run(options: {
			label: string;
			closeCode: string;
			agentGet: () => Promise<{ code: number; stdout: string; stderr: string }>;
		}) {
			const cwd = tempDir(`momo-pane-nf-${options.label}-cwd-`);
			const cacheRoot = tempDir(`momo-pane-nf-${options.label}-cache-`);
			const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "scout" });
			const pool = fixture.pool;
			pool.upsert({
				workerId: fixture.workerId,
				generation: fixture.generation,
				generationTombstone: fixture.generation,
				role: "scout",
				paneId: "w1:p-missing",
				agentName: "momo_scout",
				cwd: fixture.identity.canonicalRoot,
				status: "unhealthy",
				updatedAt: new Date().toISOString(),
			});
			const closed: string[] = [];
			const notifies: string[] = [];
			const pi = createFakePi();
			installMomoParent(pi as unknown as ExtensionAPI, {
				cwd,
				env: {
					MOMO_PARENT: "1",
					MOMO_PARENT_ID: `parent-pane-nf-${options.label}`,
					HERDR_ENV: "1",
					HERDR_PANE_ID: "w1:p1",
					HERDR_WORKSPACE_ID: "test-ws",
					HERDR_SOCKET_PATH: "test-sock",
				},
				client: createTestHerdrClient({
					runCommand: async (_file, args) => {
						if (args[0] === "pane" && args[1] === "close") {
							closed.push(String(args[2]));
							return {
								code: 1,
								stdout: JSON.stringify({
									id: "c",
									error: { code: options.closeCode, message: "gone" },
								}),
								stderr: "",
							};
						}
						if (args[0] === "agent" && args[1] === "get") {
							return options.agentGet();
						}
						return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
					},
				}),
				poolRegistry: pool,
			});
			await pi.commands.get("momo-cleanup")!.handler("", {
				ui: { notify: (m: string) => notifies.push(m) },
			} as never);
			return { pool, closed, notifies };
		}

		const missing = await run({
			label: "agent-missing",
			closeCode: "pane_not_found",
			agentGet: async () => ({
				code: 1,
				stdout: JSON.stringify({
					id: "g",
					error: { code: "agent_not_found", message: "no such agent" },
				}),
				stderr: "",
			}),
		});
		expect(missing.closed).toEqual(["w1:p-missing"]);
		expect(isArchivalTombstone(missing.pool.getByRole("scout")!)).toBe(true);

		const done = await run({
			label: "agent-done",
			closeCode: "not_found",
			agentGet: async () => ({
				code: 0,
				stdout: JSON.stringify({
					id: "g",
					result: {
						type: "agent_info",
						agent: { agent_status: "done", name: "momo_scout" },
					},
				}),
				stderr: "",
			}),
		});
		expect(done.closed).toEqual(["w1:p-missing"]);
		expect(isArchivalTombstone(done.pool.getByRole("scout")!)).toBe(true);
	});

	it("transient/unstructured pane close refusal retains row; generation supersession refuses", async () => {
		const cwd = tempDir("momo-pane-close-refuse-cwd-");
		const cacheRoot = tempDir("momo-pane-close-refuse-cache-");
		const fixture = setupPoolWorkerFixture({ cacheRoot, cwd, role: "scout" });
		const pool = fixture.pool;
		pool.upsert({
			workerId: fixture.workerId,
			generation: fixture.generation,
			generationTombstone: fixture.generation,
			role: "scout",
			paneId: "w1:p9",
			agentName: "momo_scout",
			cwd: fixture.identity.canonicalRoot,
			status: "unhealthy",
			updatedAt: new Date().toISOString(),
		});
		const notifies: string[] = [];
		const pi = createFakePi();
		installMomoParent(pi as unknown as ExtensionAPI, {
			cwd,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "parent-pane-refuse",
				HERDR_ENV: "1",
				HERDR_PANE_ID: "w1:p1",
				HERDR_WORKSPACE_ID: "test-ws",
				HERDR_SOCKET_PATH: "test-sock",
			},
			client: createTestHerdrClient({
				runCommand: async (_file, args) => {
					if (args[0] === "pane" && args[1] === "close") {
						return {
							code: 1,
							stdout: JSON.stringify({
								id: "c",
								error: { code: "timeout", message: "pane close timed out" },
							}),
							stderr: "",
						};
					}
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				},
			}),
			poolRegistry: pool,
		});
		await pi.commands.get("momo-cleanup")!.handler("", {
			ui: { notify: (m: string) => notifies.push(m) },
		} as never);
		expect(notifies.join("\n")).toMatch(/Failed to close|timed out/i);
		expect(pool.getByRole("scout")?.status).toBe("unhealthy");
		expect(pool.getByRole("scout")?.paneClosed).not.toBe(true);
		expect(isArchivalTombstone(pool.getByRole("scout")!)).toBe(false);

		// Generation supersession after structured missing close: refuse archive.
		const cwd2 = tempDir("momo-pane-close-super-cwd-");
		const cacheRoot2 = tempDir("momo-pane-close-super-cache-");
		const fixture2 = setupPoolWorkerFixture({ cacheRoot: cacheRoot2, cwd: cwd2, role: "scout" });
		const pool2 = fixture2.pool;
		pool2.upsert({
			workerId: fixture2.workerId,
			generation: fixture2.generation,
			generationTombstone: fixture2.generation,
			role: "scout",
			paneId: "w1:p9",
			agentName: "momo_scout",
			cwd: fixture2.identity.canonicalRoot,
			status: "unhealthy",
			updatedAt: new Date().toISOString(),
		});
		const notifies2: string[] = [];
		const pi2 = createFakePi();
		installMomoParent(pi2 as unknown as ExtensionAPI, {
			cwd: cwd2,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "parent-pane-super",
				HERDR_ENV: "1",
				HERDR_PANE_ID: "w1:p1",
				HERDR_WORKSPACE_ID: "test-ws",
				HERDR_SOCKET_PATH: "test-sock",
			},
			client: createTestHerdrClient({
				runCommand: async (_file, args) => {
					if (args[0] === "pane" && args[1] === "close") {
						pool2.upsert({
							...pool2.getByRole("scout")!,
							generation: fixture2.generation + 1,
							generationTombstone: fixture2.generation + 1,
							workerId: `${fixture2.workerId}-n1`,
							status: "idle",
							paneId: "w1:p-new",
							updatedAt: new Date().toISOString(),
						});
						return {
							code: 1,
							stdout: JSON.stringify({
								id: "c",
								error: { code: "pane_not_found", message: "gone" },
							}),
							stderr: "",
						};
					}
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				},
			}),
			poolRegistry: pool2,
		});
		await pi2.commands.get("momo-cleanup")!.handler("", {
			ui: { notify: (m: string) => notifies2.push(m) },
		} as never);
		expect(notifies2.join("\n")).toMatch(/generation\/worker changed after pane close/i);
		expect(pool2.getByRole("scout")?.generation).toBe(fixture2.generation + 1);
		expect(pool2.getByRole("scout")?.status).toBe("idle");
	});
});

describe("appendEvent still works for private files", () => {
	it("reads valid incremental events", () => {
		const root = tempDir("momo-ok-evt-");
		const file = path.join(root, "events.ndjson");
		const event: IpcEvent = {
			version: 1,
			type: "text",
			at: "t",
			runId: "r",
			workerId: "w",
			seq: 1,
			message: "hi",
		};
		appendEvent(file, event);
		const chunk = readEventsIncrementally(file, 0);
		expect(chunk.events).toHaveLength(1);
	});
});

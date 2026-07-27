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
import { PaneRegistry, selectClosablePanes } from "../src/herdr/registry.js";
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
		const client = new HerdrClient({
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
		const client = new HerdrClient({
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
				HERDR_ENV: "1",
				HERDR_PANE_ID: "w1:p1",
			},
			client,
			registry,
		});
		await pi.emit("session_shutdown", {});
		expect(tryReadIpcJson(path.join(spool, "cancel.json"))).toBeTruthy();

		atomicWriteJson(path.join(spool, "result.json"), baseResult({
			workerId: "planner_1",
			status: "aborted",
			stopReason: "aborted",
		}));

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
		let ipcDir = "";
		let workerId = "";
		let runId = "";
		const calls: string[][] = [];
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				calls.push([...args]);
				if (args[0] === "pane" && args[1] === "split") {
					const envArgs = args.filter((a, i) => args[i - 1] === "--env");
					ipcDir =
						envArgs.find((v) => v.startsWith("MOMO_IPC_DIR="))?.slice("MOMO_IPC_DIR=".length) ??
						"";
					workerId =
						envArgs
							.find((v) => v.startsWith("MOMO_WORKER_ID="))
							?.slice("MOMO_WORKER_ID=".length) ?? "";
					runId =
						envArgs.find((v) => v.startsWith("MOMO_RUN_ID="))?.slice("MOMO_RUN_ID=".length) ??
						"";
					atomicWriteJson(path.join(ipcDir, "ready.json"), {
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
		const registry = new PaneRegistry("parent-evt", cacheRoot);
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: "parent-evt",
			client,
			registry,
			cacheRoot,
			pollIntervalMs: 20,
			readyTimeoutMs: 2_000,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});
		const session = await factory({ cwd, role: getRole("scout") });
		writeFileSync(path.join(ipcDir, "events.ndjson"), "{broken\n", { mode: 0o600 });
		await session.prompt("x");
		// Valid result must not override prior event corruption settlement.
		atomicWriteJson(path.join(ipcDir, "result.json"), baseResult({ workerId, runId }));
		await expect(session.agent!.waitForIdle()).rejects.toThrow(/Malformed|event/i);
		expect(registry.list()[0]?.status).toBe("failed");
		expect(tryReadIpcJson(path.join(ipcDir, "cancel.json"))).toBeTruthy();
		expect(calls.some((args) => args[0] === "agent" && args[1] === "send-keys")).toBe(true);
	});

	it("supervises and stops a worker before a result-timeout returns", async () => {
		installFakeHerdrExtension();
		const cacheRoot = tempDir("momo-timeout-cache-");
		const cwd = tempDir("momo-timeout-cwd-");
		let ipcDir = "";
		let workerId = "";
		let runId = "";
		const calls: string[][] = [];
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				calls.push([...args]);
				if (args[0] === "pane" && args[1] === "split") {
					const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
					ipcDir = envArgs.find((v) => v.startsWith("MOMO_IPC_DIR="))?.slice(13) ?? "";
					workerId = envArgs.find((v) => v.startsWith("MOMO_WORKER_ID="))?.slice(15) ?? "";
					runId = envArgs.find((v) => v.startsWith("MOMO_RUN_ID="))?.slice(12) ?? "";
					atomicWriteJson(path.join(ipcDir, "ready.json"), {
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
		const factory = createHerdrChildSessionFactory({
			cwd, parentPaneId: "w1:p1", parentId: "parent-timeout", client,
			cacheRoot, pollIntervalMs: 10, readyTimeoutMs: 1_000, resultTimeoutMs: 40,
			sleep: async (ms) => new Promise((r) => setTimeout(r, ms)),
		});
		const session = await factory({ cwd, role: getRole("implementer") });
		await session.prompt("wait then edit");
		await expect(session.agent!.waitForIdle()).rejects.toMatchObject({ uncertainWrite: true });
		expect(tryReadIpcJson(path.join(ipcDir, "cancel.json"))).toBeTruthy();
		expect(calls.some((args) => args[0] === "agent" && args[1] === "send-keys")).toBe(true);
		expect((session as unknown as { uncertainWrite: boolean }).uncertainWrite).toBe(true);
	});
});

describe("lease release race with foreign acquirer", () => {
	it("keeps clean success when foreign owner acquires immediately after release", async () => {
		vi.useFakeTimers();
		const cwd = tempDir("momo-race-cwd-");
		const cacheRoot = tempDir("momo-race-cache-");
		const lease = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });
		const ipcDir = tempDir("momo-race-ipc-");
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "implementer",
				MOMO_IPC_DIR: ipcDir,
				MOMO_WORKER_ID: "impl_first",
				MOMO_RUN_ID: "run1",
				MOMO_CWD: cwd,
			},
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
		atomicWriteJson(path.join(ipcDir, "command.json"), {
			version: 1,
			type: "prompt",
			task: "edit",
			issuedAt: new Date().toISOString(),
			runId: "run1",
			workerId: "impl_first",
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
		const result = parseJsonFile(path.join(ipcDir, "result.json")) as {
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
		const ipcDir = tempDir("momo-wsuccess-ipc-");
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: { MOMO_WORKER: "1", MOMO_ROLE: "implementer", MOMO_IPC_DIR: ipcDir,
				MOMO_WORKER_ID: "impl_success", MOMO_RUN_ID: "runs", MOMO_CWD: cwd },
			leaseManager: lease, leaseWaitMs: 2_000,
		});
		const ctx = { hasUI: true, isIdle: () => true, abort: vi.fn(), cwd } as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);
		atomicWriteJson(path.join(ipcDir, "command.json"), {
			version: 1, type: "prompt", task: "edit", issuedAt: new Date().toISOString(),
			runId: "runs", workerId: "impl_success",
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
		expect((parseJsonFile(path.join(ipcDir, "result.json")) as { status: string }).status).toBe("completed");
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

		const ipcDir = tempDir("momo-wwait-ipc-");
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: {
				MOMO_WORKER: "1",
				MOMO_ROLE: "implementer",
				MOMO_IPC_DIR: ipcDir,
				MOMO_WORKER_ID: "impl_wait",
				MOMO_RUN_ID: "runw",
				MOMO_CWD: cwd,
			},
			leaseManager: lease,
			now: () => now,
			sleep: async (ms) => {
				await new Promise((r) => setImmediate(r));
				if (allowCancel) {
					atomicWriteJson(path.join(ipcDir, "cancel.json"), {
						version: 1,
						runId: "runw",
						workerId: "impl_wait",
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
		atomicWriteJson(path.join(ipcDir, "command.json"), {
			version: 1,
			type: "prompt",
			task: "edit",
			issuedAt: new Date().toISOString(),
			runId: "runw",
			workerId: "impl_wait",
		});

		await new Promise((r) => setTimeout(r, 120));
		allowCancel = true;
		await new Promise((r) => setTimeout(r, 200));
		expect(pi.setActiveTools).toHaveBeenCalled();
		const lastTools = (pi.setActiveTools as ReturnType<typeof vi.fn>).mock.calls.at(-1)?.[0] as
			| string[]
			| undefined;
		expect(lastTools?.includes("bash")).toBeFalsy();
		const result = parseJsonFile(path.join(ipcDir, "result.json")) as {
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
		const ipcDir = tempDir("momo-badcancel-ipc-");
		const pi = createFakePi();
		installMomoWorker(pi as unknown as ExtensionAPI, {
			env: { MOMO_WORKER: "1", MOMO_ROLE: "scout", MOMO_IPC_DIR: ipcDir,
				MOMO_WORKER_ID: "scout_badcancel", MOMO_RUN_ID: "runbad", MOMO_CWD: cwd },
		});
		const ctx = { hasUI: true, isIdle: () => true, abort: vi.fn(), cwd } as unknown as ExtensionContext;
		await pi.emit("session_start", {}, ctx);
		atomicWriteJson(path.join(ipcDir, "cancel.json"), { version: 1, bad: true });
		await new Promise((r) => setTimeout(r, 180));
		const result = parseJsonFile(path.join(ipcDir, "result.json")) as { status: string; errorMessage?: string };
		expect(result.status).toBe("failed");
		expect(result.errorMessage).toMatch(/cancel/i);
		expect(pi.sendUserMessage).not.toHaveBeenCalled();
	});
});

describe("force cleanup recovery retention", () => {
	it("retains registry with paneClosed/recoveryRequired when lease release fails", async () => {
		const cwd = tempDir("momo-force-cwd-");
		const cacheRoot = tempDir("momo-force-cache-");
		const registry = new PaneRegistry("parent-force", cacheRoot);
		registry.upsert({
			workerId: "impl_u",
			runId: "r1",
			role: "implementer",
			paneId: "w1:p9",
			agentName: "momo_impl_u",
			spoolRoot: tempDir("momo-force-spool-"),
			cwd,
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
			},
			client: new HerdrClient({
				runCommand: async (_file, args) => {
					if (args[0] === "pane" && args[1] === "close") {
						closed.push(String(args[2]));
						return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
					}
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				},
			}),
			registry,
			leaseManager: leases,
		});
		const command = pi.commands.get("momo-cleanup");
		expect(command).toBeTruthy();
		expect(selectClosablePanes(registry.list(), { force: true }).closable).toHaveLength(1);
		// Pi command handlers receive the args string after the command name.
		await command!.handler("--force", {
			ui: {
				confirm: async () => true,
				notify: (m: string) => notifies.push(m),
			},
		} as never);
		expect(notifies.join("\n"), `notifies=${JSON.stringify(notifies)}`).toMatch(
			/lease not released|registry retained|recoveryRequired/i,
		);
		expect(closed, `closed=${JSON.stringify(closed)}; notifies=${JSON.stringify(notifies)}`).toEqual([
			"w1:p9",
		]);
		const retained = registry.list()[0];
		expect(retained?.paneClosed).toBe(true);
		expect(retained?.recoveryRequired).toBe(true);
		expect(retained?.status).toBe("uncertain");
		const again = selectClosablePanes(registry.list(), { force: true });
		expect(again.closable).toHaveLength(0);
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

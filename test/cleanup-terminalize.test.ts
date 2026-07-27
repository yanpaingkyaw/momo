import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { installMomoParent, terminalizeAndClearRoleAssignmentsLocked } from "../src/extensions/parent.js";
import { HerdrClient } from "../src/herdr/client.js";
import { PoolRegistry, isArchivalTombstone } from "../src/herdr/pool-registry.js";
import { resolvePoolIdentity, stableWorkerId } from "../src/herdr/pool-identity.js";
import {
	beginClaimHead,
	enqueueAssignment,
	listClaiming,
	listQueue,
	queueCount,
} from "../src/herdr/role-queue.js";
import { assignmentSpoolPaths, workerControlPaths } from "../src/herdr/assignment-spool.js";
import { createHerdrChildSessionFactory } from "../src/delegation/herdr-factory.js";
import { atomicWriteJson } from "../src/ipc/spool.js";
import { tryReadIpcJson, validateResult } from "../src/ipc/validate.js";
import { getRole } from "../src/roles.js";

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
	const commands = new Map<string, { handler: (args: string, ctx: unknown) => Promise<void> }>();
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
			for (const handler of handlers.get(event) ?? []) {
				await handler(payload, ctx);
			}
		},
	};
}

describe("cleanup terminalizes old-generation queue/claim", () => {
	it("unhealthy queue+claim get failed results then N+1 progresses", async () => {
		const cwd = tempDir("momo-clean-q-cwd-");
		const cacheRoot = tempDir("momo-clean-q-cache-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "test-ws",
			socketPath: "test-sock",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p2",
			agentName: "momo_scout",
			cwd: identity.canonicalRoot,
			status: "unhealthy",
			updatedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: "queuedoldgen0001",
			workerId,
			generation: 1,
			parentEpoch: "epoch-a",
			task: "queued-old",
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: "claimoldgen00001",
			workerId,
			generation: 1,
			parentEpoch: "epoch-a",
			task: "claim-old",
		});
		beginClaimHead(pool.poolRoot, "scout", 1);
		expect(queueCount(pool.poolRoot, "scout")).toBe(2);

		const pi = createFakePi();
		const notifies: string[] = [];
		installMomoParent(pi as unknown as ExtensionAPI, {
			cwd,
			env: {
				MOMO_PARENT: "1",
				MOMO_PARENT_ID: "parent-clean-q",
				HERDR_ENV: "1",
				HERDR_PANE_ID: "w1:p1",
				HERDR_WORKSPACE_ID: "test-ws",
				HERDR_SOCKET_PATH: "test-sock",
			},
			client: new HerdrClient({
				runCommand: async (_file, args) => {
					if (args[0] === "pane" && args[1] === "close") {
						return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
					}
					if (args[0] === "agent" && args[1] === "get") {
						return {
							code: 0,
							stdout: JSON.stringify({
								id: "g",
								result: { type: "agent_info", agent: { agent_status: "idle" } },
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

		expect(listQueue(pool.poolRoot, "scout")).toHaveLength(0);
		expect(listClaiming(pool.poolRoot, "scout")).toHaveLength(0);
		expect(isArchivalTombstone(pool.getByRole("scout")!)).toBe(true);
		for (const id of ["queuedoldgen0001", "claimoldgen00001"]) {
			const paths = assignmentSpoolPaths(pool.poolRoot, "scout", id);
			const result = validateResult(tryReadIpcJson(paths.result), {
				runId: id,
				workerId,
			});
			expect(result.status).toBe("failed");
			expect(result.errorMessage).toMatch(/cleanup_before_archive/i);
		}

		// N+1 can progress without generation-fence mismatch from leftover FIFO.
		installFakeHerdrExtension();
		let splits = 0;
		const factory = createHerdrChildSessionFactory({
			cwd,
			parentPaneId: "w1:p1",
			parentId: "parent-clean-q",
			client: new HerdrClient({
				runCommand: async (_file, args) => {
					if (args[0] === "pane" && args[1] === "split") {
						splits += 1;
						const envArgs = args.filter((_a, i) => args[i - 1] === "--env");
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
			}),
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
			assignmentId: string;
			workerId: string;
			paths: { result: string };
		};
		const wait = session.prompt("next-gen").then(() => session.agent!.waitForIdle());
		for (let i = 0; i < 50; i++) {
			if (pool.getByRole("scout")?.generation === 2) break;
			await new Promise((r) => setTimeout(r, 20));
		}
		expect(pool.getByRole("scout")?.generation).toBe(2);
		atomicWriteJson(proxy.paths.result, {
			version: 1,
			runId: proxy.assignmentId,
			workerId: proxy.workerId,
			status: "completed",
			messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }] }],
			finishedAt: new Date().toISOString(),
		});
		await expect(wait).resolves.toBeUndefined();
		expect(splits).toBe(1);
		expect(notifies.join("\n")).toMatch(/Closed 1/i);
	});

	it("refuses archive when a terminal result cannot be made durable", () => {
		const cwd = tempDir("momo-clean-refuse-cwd-");
		const cacheRoot = tempDir("momo-clean-refuse-cache-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "test-ws",
			socketPath: "test-sock",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "w1:p2",
			agentName: "momo_scout",
			cwd: identity.canonicalRoot,
			status: "unhealthy",
			updatedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: "blockedresult001",
			workerId,
			generation: 1,
			parentEpoch: "epoch-a",
			task: "blocked",
		});
		const paths = assignmentSpoolPaths(pool.poolRoot, "scout", "blockedresult001");
		mkdirSync(paths.result, { recursive: true, mode: 0o700 }); // result path is a directory → write fails

		const worker = pool.getByRole("scout")!;
		const cleared = terminalizeAndClearRoleAssignmentsLocked({
			pool,
			role: "scout",
			worker,
			reason: "role_worker_cleanup_before_archive",
		});
		expect(cleared.ok).toBe(false);
		expect(listQueue(pool.poolRoot, "scout")).toHaveLength(1);
		expect(pool.getByRole("scout")?.status).toBe("unhealthy");
		expect(pool.getByRole("scout")?.generation).toBe(1);
	});

	it("preserves uncertainWrite on active implementer and writes clean failed for queued", () => {
		const cwd = tempDir("momo-clean-unc-cwd-");
		const cacheRoot = tempDir("momo-clean-unc-cache-");
		const identity = resolvePoolIdentity({
			cwd,
			canonicalRoot: cwd,
			workspaceId: "test-ws",
			socketPath: "test-sock",
		});
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "implementer");
		const activeId = "activeuncert0001";
		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "implementer",
			paneId: "w1:p2",
			agentName: "momo_implementer",
			cwd: identity.canonicalRoot,
			status: "uncertain",
			uncertainWrite: true,
			activeAssignmentId: activeId,
			updatedAt: new Date().toISOString(),
		});
		enqueueAssignment(pool.poolRoot, "implementer", {
			assignmentId: "queuedclean00001",
			workerId,
			generation: 1,
			parentEpoch: "epoch-a",
			task: "queued",
		});
		const cleared = terminalizeAndClearRoleAssignmentsLocked({
			pool,
			role: "implementer",
			worker: pool.getByRole("implementer")!,
			reason: "role_worker_cleanup_before_archive",
		});
		expect(cleared.ok).toBe(true);
		const active = validateResult(
			tryReadIpcJson(assignmentSpoolPaths(pool.poolRoot, "implementer", activeId).result),
			{ runId: activeId, workerId },
		);
		expect(active.uncertainWrite).toBe(true);
		const queued = validateResult(
			tryReadIpcJson(
				assignmentSpoolPaths(pool.poolRoot, "implementer", "queuedclean00001").result,
			),
			{ runId: "queuedclean00001", workerId },
		);
		expect(queued.status).toBe("failed");
		expect(queued.uncertainWrite).toBeUndefined();
		expect(queueCount(pool.poolRoot, "implementer")).toBe(0);
	});
});

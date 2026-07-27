import { createHash, randomUUID } from "node:crypto";
import { realpathSync } from "node:fs";
import path from "node:path";
import type { AgentRole } from "../roles.js";
import { HerdrClient } from "../herdr/client.js";
import { PaneRegistry, type PaneRecord } from "../herdr/registry.js";
import {
	atomicWriteJson,
	createRunId,
	createWorkerId,
	DEFAULT_HEARTBEAT_STALE_MS,
	ensurePrivateDir,
	momoCacheRoot,
	readEventsIncrementally,
	workerSpoolPaths,
	type IpcEvent,
	type IpcManifest,
	type IpcResult,
} from "../ipc/spool.js";
import {
	IpcValidationError,
	tryReadIpcJson,
	validateEvent,
	validateHeartbeat,
	validateReady,
	validateResult,
} from "../ipc/validate.js";
import {
	getHerdrPiExtensionPath,
	getWorkerExtensionPath,
	resolveCanonicalPiPath,
} from "../paths.js";
import type {
	ChildSession,
	ChildSessionFactory,
	ChildSessionFactoryInput,
	DelegationProgress,
} from "./runner.js";

export interface HerdrFactoryOptions {
	cwd: string;
	parentPaneId: string;
	parentId: string;
	client?: HerdrClient;
	registry?: PaneRegistry;
	cacheRoot?: string;
	runId?: string;
	heartbeatStaleMs?: number;
	pollIntervalMs?: number;
	readyTimeoutMs?: number;
	resultTimeoutMs?: number;
	now?: () => number;
	sleep?: (ms: number) => Promise<void>;
	splitDirection?: "right" | "down";
}

/**
 * Pi CLI `--tools` catalog for workers. Must include every tool the role may
 * later activate via setActiveTools (catalog is a hard ceiling). Never includes
 * `delegate`. Active-tool narrowing (implementer writes after lease) happens
 * inside the worker extension.
 */
export function roleLaunchToolsCsv(role: AgentRole): string {
	const tools = role.tools.filter((tool) => tool !== "delegate");
	return tools.join(",");
}

function herdrAgentName(workerId: string): string {
	const compact = `momo_${workerId}`.replace(/[^a-z0-9_-]/gi, "_").toLowerCase();
	return compact.slice(0, 32);
}

class HerdrChildSession implements ChildSession {
	private messagesInternal: unknown[] = [];
	private readonly listeners = new Set<(event: unknown) => void>();
	private eventOffset = 0;
	private lastEventSeq = 0;
	private settled: IpcResult | undefined;
	private disposed = false;
	private promptStarted = false;
	private terminalCommandIssued = false;
	private diagnosticFailure: string | undefined;
	private stoppingAfterFailure = false;
	uncertainWrite = false;

	constructor(
		readonly role: AgentRole,
		readonly paths: ReturnType<typeof workerSpoolPaths>,
		readonly paneId: string,
		readonly agentName: string,
		readonly runId: string,
		readonly workerId: string,
		readonly cwd: string,
		private readonly client: HerdrClient,
		private readonly registry: PaneRegistry,
		private readonly options: {
			heartbeatStaleMs: number;
			pollIntervalMs: number;
			resultTimeoutMs: number;
			now: () => number;
			sleep: (ms: number) => Promise<void>;
		},
	) {
		this.startPolling();
	}

	get messages(): readonly unknown[] {
		return this.messagesInternal;
	}

	get agent() {
		return {
			waitForIdle: async () => {
				await this.waitForResult();
			},
		};
	}

	subscribe(listener: (event: unknown) => void): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}

	private emit(event: unknown): void {
		for (const listener of this.listeners) {
			try {
				listener(event);
			} catch {
				// ignore listener failures
			}
		}
	}

	private identity() {
		return { runId: this.runId, workerId: this.workerId };
	}

	private updateRegistry(status: PaneRecord["status"], uncertainWrite?: boolean): void {
		const record: PaneRecord = {
			workerId: this.workerId,
			runId: this.runId,
			role: this.role.name,
			paneId: this.paneId,
			agentName: this.agentName,
			spoolRoot: this.paths.root,
			cwd: this.cwd,
			status,
			updatedAt: new Date(this.options.now()).toISOString(),
		};
		if (uncertainWrite !== undefined) record.uncertainWrite = uncertainWrite;
		this.registry.upsert(record);
	}

	private pollTimer: ReturnType<typeof setInterval> | undefined;

	private startPolling(): void {
		this.pollTimer = setInterval(() => {
			void this.poll();
		}, this.options.pollIntervalMs);
		this.pollTimer.unref?.();
	}

	private stopPolling(): void {
		if (this.pollTimer !== undefined) {
			clearInterval(this.pollTimer);
			this.pollTimer = undefined;
		}
	}

	private async poll(): Promise<void> {
		if (this.disposed || this.settled || this.stoppingAfterFailure) return;

		try {
			const chunk = readEventsIncrementally(this.paths.events, this.eventOffset);
			this.eventOffset = chunk.nextOffset;
			for (const parsed of chunk.events) {
				const event = validateEvent(parsed, this.identity(), this.lastEventSeq);
				this.lastEventSeq = event.seq;
				this.forwardEvent(event);
			}

			const resultRaw = tryReadIpcJson(this.paths.result);
			if (resultRaw && !this.settled) {
				this.settle(validateResult(resultRaw, this.identity()));
				return;
			}

			if ((this.promptStarted || this.terminalCommandIssued) && !this.settled) {
				const heartbeatRaw = tryReadIpcJson(this.paths.heartbeat);
				if (heartbeatRaw) {
					const heartbeat = validateHeartbeat(heartbeatRaw, this.identity());
					const age = this.options.now() - Date.parse(heartbeat.at);
					if (Number.isFinite(age) && age > this.options.heartbeatStaleMs) {
						await this.stopAndSettleFailure("Worker heartbeat went stale");
					}
				}
			}
		} catch (error) {
			// IPC/filesystem corruption is terminal, but the worker must be stopped
			// before the parent returns so it cannot continue or later mutate.
			const message =
				error instanceof IpcValidationError || error instanceof Error
					? error.message
					: String(error);
			this.diagnosticFailure = message;
			await this.stopAndSettleFailure(message);
		}
	}

	private async stopAndSettleFailure(message: string): Promise<void> {
		if (this.settled || this.stoppingAfterFailure) return;
		this.stoppingAfterFailure = true;
		this.stopPolling();
		try {
			atomicWriteJson(this.paths.cancel, {
				version: 1,
				runId: this.runId,
				workerId: this.workerId,
				reason: message.slice(0, 1024),
				issuedAt: new Date(this.options.now()).toISOString(),
			});
		} catch {
			// The spool itself may be corrupt; continue with terminal escalation.
		}
		try {
			await this.client.agentSendKeys(this.agentName, ["esc"]);
		} catch {
			// best effort
		}
		try {
			await this.client.agentWait(this.agentName, {
				until: ["idle", "done", "unknown"],
				timeoutMs: 3_000,
			});
		} catch {
			// bounded wait; uncertainty below is conservative for implementers
		}
		this.stoppingAfterFailure = false;
		this.settle({
			version: 1,
			runId: this.runId,
			workerId: this.workerId,
			status: "failed",
			messages: this.messagesInternal,
			errorMessage: message,
			uncertainWrite: this.role.canWrite,
			finishedAt: new Date(this.options.now()).toISOString(),
		});
	}

	private forwardEvent(event: IpcEvent): void {
		if (event.type === "text" && event.message) {
			this.emit({
				type: "message_update",
				assistantMessageEvent: { type: "text_delta", delta: event.message },
			});
		} else if (event.type === "tool_started") {
			this.emit({ type: "tool_execution_start", toolName: event.toolName ?? "tool" });
		} else if (event.type === "tool_finished") {
			this.emit({
				type: "tool_execution_end",
				toolName: event.toolName ?? "tool",
				isError: event.state === "failed",
			});
		}
	}

	private settle(result: IpcResult): void {
		if (this.settled) return;
		this.settled = result;
		this.messagesInternal = Array.isArray(result.messages) ? result.messages : [];
		this.uncertainWrite = result.uncertainWrite === true;
		const status: PaneRecord["status"] =
			result.uncertainWrite
				? "uncertain"
				: result.status === "completed"
					? "completed"
					: result.status === "aborted"
						? "aborted"
						: "failed";
		this.updateRegistry(status, result.uncertainWrite);
		this.stopPolling();
	}

	async waitForReady(timeoutMs: number): Promise<void> {
		const deadline = this.options.now() + timeoutMs;
		while (this.options.now() < deadline) {
			try {
				const readyRaw = tryReadIpcJson(this.paths.ready);
				if (readyRaw) {
					validateReady(readyRaw, this.identity());
					this.updateRegistry("ready");
					return;
				}
			} catch (error) {
				if (error instanceof IpcValidationError) {
					throw new Error(`Worker ${this.workerId} ready IPC invalid: ${error.message}`);
				}
				throw error;
			}
			await this.options.sleep(50);
		}
		throw new Error(`Worker ${this.workerId} did not become ready in time`);
	}

	async prompt(text: string): Promise<void> {
		this.promptStarted = true;
		this.updateRegistry("running");
		atomicWriteJson(this.paths.command, {
			version: 1,
			type: "prompt",
			task: text,
			issuedAt: new Date(this.options.now()).toISOString(),
			runId: this.runId,
			workerId: this.workerId,
		});
	}

	/** Terminal skip/cancel for prepared but never-active workers. */
	async skip(reason: string): Promise<void> {
		if (this.settled) return;
		this.terminalCommandIssued = true;
		atomicWriteJson(this.paths.command, {
			version: 1,
			type: "skip",
			reason,
			issuedAt: new Date(this.options.now()).toISOString(),
			runId: this.runId,
			workerId: this.workerId,
		});
		const deadline = this.options.now() + 5_000;
		while (this.options.now() < deadline) {
			await this.poll();
			if (this.settled) return;
			await this.options.sleep(50);
		}
		// If worker never consumed skip, write parent-side terminal result so registry leaves ready.
		this.settle({
			version: 1,
			runId: this.runId,
			workerId: this.workerId,
			status: "aborted",
			messages: [],
			errorMessage: reason,
			finishedAt: new Date(this.options.now()).toISOString(),
		});
	}

	private async waitForResult(): Promise<IpcResult> {
		const deadline = this.options.now() + this.options.resultTimeoutMs;
		while (this.options.now() < deadline) {
			await this.poll();
			if (this.settled) {
				if (this.settled.uncertainWrite) {
					throw Object.assign(new Error(this.settled.errorMessage ?? "Uncertain implementer write"), {
						uncertainWrite: true,
						stopReason: this.settled.stopReason ?? "error",
					});
				}
				if (this.settled.status === "failed") {
					throw Object.assign(new Error(this.settled.errorMessage ?? "Worker failed"), {
						stopReason: this.settled.stopReason ?? "error",
					});
				}
				if (this.settled.status === "aborted") {
					throw Object.assign(new Error(this.settled.errorMessage ?? "Worker aborted"), {
						stopReason: "aborted",
					});
				}
				return this.settled;
			}
			await this.options.sleep(this.options.pollIntervalMs);
		}
		await this.stopAndSettleFailure("Timed out waiting for worker result");
		// Re-enter once to translate the supervised terminal state through the
		// normal failed/uncertain result path.
		return this.waitForResult();
	}

	async abort(): Promise<void> {
		if (this.settled) return;
		this.terminalCommandIssued = true;
		atomicWriteJson(this.paths.cancel, {
			version: 1,
			runId: this.runId,
			workerId: this.workerId,
			reason: "parent_abort",
			issuedAt: new Date(this.options.now()).toISOString(),
		});
		if (!this.promptStarted) {
			// Cancel-before-prompt: also issue skip so idle ready workers terminate via command poll.
			atomicWriteJson(this.paths.command, {
				version: 1,
				type: "cancel",
				reason: "cancelled_before_prompt",
				issuedAt: new Date(this.options.now()).toISOString(),
				runId: this.runId,
				workerId: this.workerId,
			});
		}

		const waitUntil = this.options.now() + 2_000;
		while (this.options.now() < waitUntil) {
			await this.poll();
			if (this.settled) return;
			await this.options.sleep(50);
		}

		try {
			await this.client.agentSendKeys(this.agentName, ["ctrl+c"]);
		} catch {
			// best effort escalation
		}

		try {
			await this.client.agentWait(this.agentName, {
				until: ["idle", "done"],
				timeoutMs: 3_000,
			});
		} catch {
			// wait may timeout; inspect readiness next
		}

		await this.poll();
		if (this.settled) return;

		let agentStatus: string | undefined;
		try {
			const agent = await this.client.agentGet(this.agentName);
			agentStatus = agent.agentStatus;
		} catch {
			agentStatus = undefined;
		}

		if (agentStatus === "idle" || agentStatus === "done") {
			this.settle({
				version: 1,
				runId: this.runId,
				workerId: this.workerId,
				status: "aborted",
				messages: this.messagesInternal,
				errorMessage: "Worker aborted after escalation",
				stopReason: "aborted",
				finishedAt: new Date(this.options.now()).toISOString(),
			});
			return;
		}

		// Unresolved (including unknown): keep pane/lease; do not fabricate a clean abort.
		if (this.role.canWrite) {
			this.settle({
				version: 1,
				runId: this.runId,
				workerId: this.workerId,
				status: "failed",
				messages: this.messagesInternal,
				errorMessage: "Worker cancel unresolved after Herdr escalation",
				uncertainWrite: true,
				finishedAt: new Date(this.options.now()).toISOString(),
			});
			return;
		}
		this.settle({
			version: 1,
			runId: this.runId,
			workerId: this.workerId,
			status: "failed",
			messages: this.messagesInternal,
			errorMessage: "Worker cancel unresolved after Herdr escalation",
			finishedAt: new Date(this.options.now()).toISOString(),
		});
	}

	async dispose(): Promise<void> {
		this.disposed = true;
		this.stopPolling();
		// Retain panes until explicit cleanup.
	}
}

export function createHerdrChildSessionFactory(options: HerdrFactoryOptions): ChildSessionFactory {
	const client = options.client ?? new HerdrClient();
	const cacheRoot = options.cacheRoot ?? momoCacheRoot();
	const registry = options.registry ?? new PaneRegistry(options.parentId, cacheRoot);
	const runId = options.runId ?? createRunId();
	const runRoot = path.join(cacheRoot, "runs", runId);
	ensurePrivateDir(runRoot);
	const now = options.now ?? Date.now;
	const sleep =
		options.sleep ??
		((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
	const direction = options.splitDirection ?? "right";

	return async ({ cwd, role }: ChildSessionFactoryInput) => {
		if (cwd !== options.cwd) {
			throw new Error("Child working directory must match the parent working directory");
		}

		const workerId = createWorkerId(role.name);
		const paths = workerSpoolPaths(runRoot, workerId);
		ensurePrivateDir(paths.root);
		const agentName = herdrAgentName(workerId);
		const workerExtension = getWorkerExtensionPath();
		const herdrExtension = getHerdrPiExtensionPath();
		if (!herdrExtension) {
			throw new Error(
				"Official Herdr Pi lifecycle extension is required (herdr integration install pi)",
			);
		}
		// Canonical Pi path is resolved so workers use the same PATH-tested binary as parent.
		void resolveCanonicalPiPath();

		const workerEnv: Record<string, string> = {
			MOMO_WORKER: "1",
			MOMO_ROLE: role.name,
			MOMO_RUN_ID: runId,
			MOMO_WORKER_ID: workerId,
			MOMO_IPC_DIR: paths.root,
			MOMO_CWD: cwd,
			MOMO_PARENT_ID: options.parentId,
		};

		let paneId: string | undefined;
		try {
			const split = await client.splitPane({
				pane: options.parentPaneId,
				direction,
				cwd,
				noFocus: true,
				env: workerEnv,
			});
			paneId = split.paneId;

			const manifest: IpcManifest = {
				version: 1,
				runId,
				workerId,
				role: role.name,
				cwd,
				paneId,
				agentName,
				createdAt: new Date(now()).toISOString(),
			};
			atomicWriteJson(paths.manifest, manifest);

			registry.upsert({
				workerId,
				runId,
				role: role.name,
				paneId,
				agentName,
				spoolRoot: paths.root,
				cwd,
				status: "starting",
				updatedAt: new Date(now()).toISOString(),
			});

			await client.renamePane(paneId, `Momo ${role.name}`);
			await client.reportMetadata(paneId, {
				source: "momo:parent",
				displayAgent: `Momo ${role.name}`,
				title: `Momo ${role.name}`,
				agent: "pi",
			});

			const agentArgs = [
				"--name",
				`Momo ${role.name}`,
				"--no-session",
				"--no-extensions",
				"--no-skills",
				"--no-prompt-templates",
				"--no-themes",
				"--tools",
				roleLaunchToolsCsv(role),
				"-e",
				workerExtension,
				"-e",
				herdrExtension,
			];

			await client.agentStart({
				name: agentName,
				paneId,
				kind: "pi",
				timeoutMs: 60_000,
				agentArgs,
			});

			const session = new HerdrChildSession(
				role,
				paths,
				paneId,
				agentName,
				runId,
				workerId,
				cwd,
				client,
				registry,
				{
					heartbeatStaleMs: options.heartbeatStaleMs ?? DEFAULT_HEARTBEAT_STALE_MS,
					pollIntervalMs: options.pollIntervalMs ?? 100,
					resultTimeoutMs: options.resultTimeoutMs ?? 60 * 60 * 1000,
					now,
					sleep,
				},
			);
			await session.waitForReady(options.readyTimeoutMs ?? 60_000);
			return session;
		} catch (error) {
			if (paneId) {
				try {
					await client.closePane(paneId);
					try {
						registry.remove(workerId);
					} catch {
						// registry write best-effort after close
					}
				} catch {
					// Close failed: mark terminal failed (not starting) so cleanup can retry.
					try {
						registry.upsert({
							workerId,
							runId,
							role: role.name,
							paneId,
							agentName,
							spoolRoot: paths.root,
							cwd,
							status: "failed",
							updatedAt: new Date(now()).toISOString(),
						});
					} catch {
						// ignore
					}
				}
			}
			throw error;
		}
	};
}

/** Stable parent identity for the same Herdr pane + workspace + real cwd. */
export function createStableParentId(options: {
	paneId: string;
	workspaceId?: string;
	cwd: string;
}): string {
	const resolved = realpathSync(options.cwd);
	const material = `${options.paneId}\0${options.workspaceId ?? ""}\0${resolved}`;
	return createHash("sha256").update(material).digest("hex").slice(0, 16);
}

/** @deprecated Prefer createStableParentId for Herdr parents. */
export function createParentId(): string {
	return randomUUID().replace(/-/g, "").slice(0, 12);
}

export type { DelegationProgress };

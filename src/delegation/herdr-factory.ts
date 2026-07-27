import { createHash, randomUUID } from "node:crypto";
import { existsSync, realpathSync } from "node:fs";
import path from "node:path";
import type { AgentName, AgentRole } from "../roles.js";
import { HerdrClient } from "../herdr/client.js";
import {
	assignmentSpoolPaths,
	ensureAssignmentSpool,
	workerControlPaths,
	type WorkerActivePointer,
	type WorkerManifest,
} from "../herdr/assignment-spool.js";
import {
	createAssignmentId,
	createParentEpoch,
	ensurePoolLayout,
	herdrAgentNameForWorker,
	poolRootPath,
	resolvePoolIdentity,
	stableWorkerId,
	type PoolIdentity,
} from "../herdr/pool-identity.js";
import {
	clearControlEphemerals,
	formatWorkerStatusLine,
	isArchivalTombstone,
	isNonReusableLiveWorker,
	PoolRegistry,
	type PoolWorkerRecord,
	type PoolWorkerState,
} from "../herdr/pool-registry.js";
import {
	cancelQueuedAssignment,
	commitClaim,
	enqueueAssignment,
	listClaiming,
	withRoleLock,
	withRoleLockAsync,
} from "../herdr/role-queue.js";
import {
	atomicWriteJson,
	DEFAULT_HEARTBEAT_STALE_MS,
	ensurePrivateDir,
	momoCacheRoot,
	readEventsIncrementally as readEventsIncrementallyImpl,
	type IpcEvent,
	type IpcResult,
} from "../ipc/spool.js";
import {
	IpcValidationError,
	assertHeartbeatFreshness,
	tryReadIpcJson as tryReadIpcJsonImpl,
	validateEvent,
	validateHeartbeat,
	validateReady,
	validateResult,
} from "../ipc/validate.js";

/** Namespace handles so tests can inject result/event races deterministically. */
const ipcSpool = {
	readEventsIncrementally: readEventsIncrementallyImpl,
};
const ipcValidate = {
	tryReadIpcJson: tryReadIpcJsonImpl,
};

/** @internal test-only: restore default IPC readers. */
export function __resetIpcReadersForTest(): void {
	ipcSpool.readEventsIncrementally = readEventsIncrementallyImpl;
	ipcValidate.tryReadIpcJson = tryReadIpcJsonImpl;
}

/** @internal test-only: override IPC readers for race/fault injection. */
export function __setIpcReadersForTest(options: {
	readEventsIncrementally?: typeof readEventsIncrementallyImpl;
	tryReadIpcJson?: typeof tryReadIpcJsonImpl;
}): void {
	if (options.readEventsIncrementally) {
		ipcSpool.readEventsIncrementally = options.readEventsIncrementally;
	}
	if (options.tryReadIpcJson) {
		ipcValidate.tryReadIpcJson = options.tryReadIpcJson;
	}
}
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

/**
 * Failure during physical pane provisioning (split → agent start → manifest).
 * Carries enough identity for generation-fenced reservation rollback:
 * - no paneId: reservation may be archived (tombstone retained)
 * - paneId + closePaneSucceeded false: unhealthy keeps pane for /momo-cleanup retry
 * - paneId + closePaneSucceeded true: unhealthy keeps paneId with paneClosed so cleanup
 *   can confirm agent stop without re-closing
 */
export class ProvisioningFailure extends Error {
	readonly paneId?: string;
	readonly agentName: string;
	readonly closePaneSucceeded?: boolean;

	constructor(options: {
		message: string;
		agentName: string;
		paneId?: string;
		closePaneSucceeded?: boolean;
		cause?: unknown;
	}) {
		super(options.message, options.cause !== undefined ? { cause: options.cause } : undefined);
		this.name = "ProvisioningFailure";
		this.agentName = options.agentName;
		if (options.paneId !== undefined) this.paneId = options.paneId;
		if (options.closePaneSucceeded !== undefined) {
			this.closePaneSucceeded = options.closePaneSucceeded;
		}
	}
}

export interface HerdrFactoryOptions {
	cwd: string;
	parentPaneId: string;
	parentId: string;
	parentEpoch?: string;
	client?: HerdrClient;
	poolRegistry?: PoolRegistry;
	cacheRoot?: string;
	workspaceId?: string;
	socketPath?: string;
	canonicalRoot?: string;
	heartbeatStaleMs?: number;
	pollIntervalMs?: number;
	readyTimeoutMs?: number;
	resultTimeoutMs?: number;
	now?: () => number;
	sleep?: (ms: number) => Promise<void>;
	splitDirection?: "right" | "down";
	/** @deprecated Legacy pane registry ignored by pool factory. */
	registry?: unknown;
	/** @deprecated Per-run id unused; pool uses stable workers. */
	runId?: string;
}

/**
 * Pi CLI `--tools` catalog for workers. Must include every tool the role may
 * later activate via setActiveTools (catalog is a hard ceiling). Never includes
 * `delegate`.
 */
export function roleLaunchToolsCsv(role: AgentRole): string {
	const tools = role.tools.filter((tool) => tool !== "delegate");
	return tools.join(",");
}

interface FactoryRuntime {
	cwd: string;
	parentPaneId: string;
	parentId: string;
	parentEpoch: string;
	client: HerdrClient;
	pool: PoolRegistry;
	identity: PoolIdentity;
	cacheRoot: string;
	heartbeatStaleMs: number;
	pollIntervalMs: number;
	readyTimeoutMs: number;
	resultTimeoutMs: number;
	now: () => number;
	sleep: (ms: number) => Promise<void>;
	direction: "right" | "down";
}

function controlRunId(generation: number): string {
	return `g${generation}`;
}

/**
 * Non-archival live pool workers must already execute at the pool canonical root.
 * Missing (pre-fix) or subdirectory cwd is incompatible — fail closed for cleanup.
 */
function assertLiveWorkerCanonicalCwd(
	record: PoolWorkerRecord,
	canonicalRoot: string,
	roleName: string,
): void {
	if (!record.cwd) {
		throw new Error(
			`Role ${roleName} live worker missing cwd (incompatible with canonical pool); run /momo-cleanup`,
		);
	}
	if (record.cwd !== canonicalRoot) {
		throw new Error(
			`Role ${roleName} live worker cwd is not the pool canonical root; run /momo-cleanup`,
		);
	}
}

function assignmentIdentity(assignmentId: string, workerId: string): { runId: string; workerId: string } {
	return { runId: assignmentId, workerId };
}

class AssignmentProxy implements ChildSession {
	private messagesInternal: unknown[] = [];
	private readonly listeners = new Set<(event: unknown) => void>();
	private eventOffset = 0;
	private lastEventSeq = 0;
	private settled: IpcResult | undefined;
	private disposed = false;
	private promptStarted = false;
	private physicalEnsured = false;
	/** Set when this assignment is dispatched and should expect control heartbeats. */
	private heartbeatExpectedAt: number | undefined;
	private diagnosticFailure: string | undefined;
	private stoppingAfterFailure = false;
	private pollTimer: ReturnType<typeof setInterval> | undefined;
	uncertainWrite = false;

	readonly assignmentId: string;
	readonly workerId: string;
	generation = 0;
	paneId: string | undefined;
	agentName: string | undefined;

	constructor(
		readonly role: AgentRole,
		private readonly runtime: FactoryRuntime,
	) {
		this.assignmentId = createAssignmentId();
		this.workerId = stableWorkerId(runtime.identity.poolKey, role.name);
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

	/** Test/debug: derived assignment spool under pool root. */
	get paths() {
		return assignmentSpoolPaths(this.runtime.pool.poolRoot, this.role.name, this.assignmentId);
	}

	get runId(): string {
		return this.assignmentId;
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
				// ignore
			}
		}
	}

	private startPolling(): void {
		if (this.pollTimer) return;
		this.pollTimer = setInterval(() => {
			void this.poll();
		}, this.runtime.pollIntervalMs);
		this.pollTimer.unref?.();
	}

	private stopPolling(): void {
		if (this.pollTimer !== undefined) {
			clearInterval(this.pollTimer);
			this.pollTimer = undefined;
		}
	}

	private async poll(): Promise<void> {
		if (this.disposed || this.settled || this.stoppingAfterFailure || !this.promptStarted) return;
		const paths = this.paths;
		try {
			// Durable result is authoritative: validate/settle result BEFORE events so
			// corrupt/oversized event logs cannot reject an already-valid terminal result.
			const resultRaw = ipcValidate.tryReadIpcJson(paths.result);
			if (resultRaw && !this.settled) {
				this.settle(validateResult(resultRaw, assignmentIdentity(this.assignmentId, this.workerId)));
				return;
			}

			// Pre-result event corruption remains fail-closed — but if a durable
			// result appears while event parsing fails, prefer the result.
			if (existsSync(paths.events)) {
				const chunk = ipcSpool.readEventsIncrementally(paths.events, this.eventOffset);
				this.eventOffset = chunk.nextOffset;
				for (const parsed of chunk.events) {
					const event = validateEvent(parsed, assignmentIdentity(this.assignmentId, this.workerId), this.lastEventSeq);
					this.lastEventSeq = event.seq;
					this.forwardEvent(event);
				}
			}

			if (this.physicalEnsured && !this.settled && this.generation > 0) {
				const control = workerControlPaths(this.runtime.pool.poolRoot, this.role.name);
				const heartbeatRaw = ipcValidate.tryReadIpcJson(control.heartbeat);
				if (heartbeatRaw) {
					const heartbeat = validateHeartbeat(heartbeatRaw, {
						runId: controlRunId(this.generation),
						workerId: this.workerId,
					});
					assertHeartbeatFreshness(heartbeat.at, {
						now: this.runtime.now(),
						staleMs: this.runtime.heartbeatStaleMs,
					});
				} else if (
					this.heartbeatExpectedAt !== undefined &&
					this.runtime.now() - this.heartbeatExpectedAt > this.runtime.heartbeatStaleMs
				) {
					// Missing heartbeat after dispatch/readiness fails like a stale one.
					await this.stopAndSettleFailure("Worker heartbeat went stale");
				}
			}
		} catch (error) {
			// Close the check/read race: a result may have landed after the first
			// result check and before/during event parse failure.
			try {
				const raced = ipcValidate.tryReadIpcJson(paths.result);
				if (raced && !this.settled) {
					this.settle(validateResult(raced, assignmentIdentity(this.assignmentId, this.workerId)));
					return;
				}
			} catch (resultError) {
				const message =
					resultError instanceof IpcValidationError || resultError instanceof Error
						? resultError.message
						: String(resultError);
				this.diagnosticFailure = message;
				await this.stopAndSettleFailure(message);
				return;
			}
			const message =
				error instanceof IpcValidationError || error instanceof Error
					? error.message
					: String(error);
			this.diagnosticFailure = message;
			await this.stopAndSettleFailure(message);
		}
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
		this.stopPolling();
	}

	private async stopAndSettleFailure(message: string): Promise<void> {
		if (this.settled || this.stoppingAfterFailure) return;
		this.stoppingAfterFailure = true;
		this.stopPolling();
		let activeFailure = false;
		try {
			// Assignment-specific cancel IPC only — never send terminal keys on a shared pane.
			try {
				atomicWriteJson(this.paths.cancel, {
					version: 1,
					runId: this.assignmentId,
					workerId: this.workerId,
					generation: this.generation,
					reason: message.slice(0, 1024),
					issuedAt: new Date(this.runtime.now()).toISOString(),
				});
			} catch {
				// continue
			}
			await withRoleLockAsync(this.runtime.pool.poolRoot, this.role.name, () => {
				const record = this.runtime.pool.getByRole(this.role.name);
				const isActive =
					record?.generation === this.generation &&
					record.workerId === this.workerId &&
					record.activeAssignmentId === this.assignmentId;
				if (isActive && record) {
					activeFailure = true;
					this.runtime.pool.upsert({
						...record,
						status: this.role.canWrite ? "uncertain" : "unhealthy",
						...(this.role.canWrite ? { uncertainWrite: true } : {}),
						generationTombstone: Math.max(record.generationTombstone, this.generation),
						updatedAt: new Date(this.runtime.now()).toISOString(),
					});
					return;
				}
				// Queued / non-active claiming: remove exact entry so it cannot later run.
				cancelQueuedAssignment(
					this.runtime.pool.poolRoot,
					this.role.name,
					this.assignmentId,
				);
				const claiming = listClaiming(this.runtime.pool.poolRoot, this.role.name).find(
					(entry) => entry.assignmentId === this.assignmentId,
				);
				if (
					claiming &&
					record?.activeAssignmentId !== claiming.assignmentId
				) {
					commitClaim(this.runtime.pool.poolRoot, this.role.name, claiming);
				}
				// Durable failed result for waiting proxies / cleanup observers.
				try {
					const paths = ensureAssignmentSpool(
						this.runtime.pool.poolRoot,
						this.role.name,
						this.assignmentId,
					);
					if (!ipcValidate.tryReadIpcJson(paths.result)) {
						atomicWriteJson(paths.result, {
							version: 1,
							runId: this.assignmentId,
							workerId: this.workerId,
							status: "failed",
							messages: this.messagesInternal,
							errorMessage: message.slice(0, 1024),
							finishedAt: new Date(this.runtime.now()).toISOString(),
						});
					}
				} catch {
					// settle in-memory regardless
				}
			}, { now: this.runtime.now, sleep: this.runtime.sleep });
			this.settle({
				version: 1,
				runId: this.assignmentId,
				workerId: this.workerId,
				status: "failed",
				messages: this.messagesInternal,
				errorMessage: message,
				uncertainWrite: this.role.canWrite && activeFailure,
				finishedAt: new Date(this.runtime.now()).toISOString(),
			});
		} finally {
			// Keep the reentrancy gate until lock cleanup + settle complete.
			this.stoppingAfterFailure = false;
		}
	}

	async prompt(text: string): Promise<void> {
		if (this.disposed) throw new Error("Assignment proxy disposed");
		if (this.promptStarted) throw new Error("Assignment already prompted");
		this.promptStarted = true;
		ensureAssignmentSpool(this.runtime.pool.poolRoot, this.role.name, this.assignmentId);
		await this.dispatchOrEnqueue(text);
		this.startPolling();
	}

	private async dispatchOrEnqueue(task: string): Promise<void> {
		type Plan =
			| { kind: "provision"; generation: number; agentName: string }
			| {
					kind: "wait-ready";
					generation: number;
					workerId: string;
					paneId?: string;
					agentName?: string;
			  }
			| { kind: "done" };

		const plan = await withRoleLockAsync(
			this.runtime.pool.poolRoot,
			this.role.name,
			async (): Promise<Plan> => {
				const existing = this.runtime.pool.getByRole(this.role.name);
				// Genuinely unhealthy/uncertain live workers refuse reuse.
				if (existing && isNonReusableLiveWorker(existing)) {
					throw new Error(
						`Role ${this.role.name} worker is ${existing.status} and cannot be reused; run /momo-cleanup`,
					);
				}

				// Live starting reservation (even before paneId lands) must WAIT —
				// never treat !paneId as a signal to reserve generation N+1.
				if (existing && !isArchivalTombstone(existing) && existing.status === "starting") {
					assertLiveWorkerCanonicalCwd(
						existing,
						this.runtime.cwd,
						this.role.name,
					);
					if (existing.workerId !== this.workerId) {
						throw new Error(
							`Role ${this.role.name} starting worker ${existing.workerId} mismatches ${this.workerId}`,
						);
					}
					this.generation = existing.generation;
					if (existing.paneId) {
						this.paneId = existing.paneId;
						this.physicalEnsured = true;
					}
					if (existing.agentName) this.agentName = existing.agentName;
					return {
						kind: "wait-ready",
						generation: existing.generation,
						workerId: existing.workerId,
						...(existing.paneId ? { paneId: existing.paneId } : {}),
						...(existing.agentName ? { agentName: existing.agentName } : {}),
					};
				}

				// Only absent records or true generation-0 archival tombstones may provision.
				if (!existing || isArchivalTombstone(existing)) {
					const generation = this.runtime.pool.nextGeneration(this.role.name);
					const agentName = herdrAgentNameForWorker(this.workerId);
					clearControlEphemerals(this.runtime.pool.poolRoot, this.role.name);
					this.runtime.pool.upsert({
						workerId: this.workerId,
						generation,
						generationTombstone: Math.max(existing?.generationTombstone ?? 0, generation),
						role: this.role.name,
						agentName,
						status: "starting",
						cwd: this.runtime.cwd,
						updatedAt: new Date(this.runtime.now()).toISOString(),
					});
					return { kind: "provision", generation, agentName };
				}

				// Every other non-archival live record must already be at canonical root.
				assertLiveWorkerCanonicalCwd(existing, this.runtime.cwd, this.role.name);

				if (!existing.paneId || !existing.agentName) {
					throw new Error(
						`Role ${this.role.name} live worker gen=${existing.generation} missing pane/agent`,
					);
				}

				this.generation = existing.generation;
				this.paneId = existing.paneId;
				this.agentName = existing.agentName;
				this.physicalEnsured = true;

				if (existing.status === "idle") {
					this.dispatchActiveLocked(task, existing);
					return { kind: "done" };
				}

				if (existing.status === "busy" || existing.status === "blocked") {
					enqueueAssignment(this.runtime.pool.poolRoot, this.role.name, {
						assignmentId: this.assignmentId,
						workerId: this.workerId,
						generation: existing.generation,
						parentEpoch: this.runtime.parentEpoch,
						task,
					});
					// Queued behind a ready physical worker still expects heartbeats.
					this.heartbeatExpectedAt = this.runtime.now();
					return { kind: "done" };
				}

				throw new Error(`Role ${this.role.name} worker in unexpected state ${existing.status}`);
			},
			{ now: this.runtime.now, sleep: this.runtime.sleep },
		);

		if (plan.kind === "done") return;

		if (plan.kind === "provision") {
			this.generation = plan.generation;
			this.agentName = plan.agentName;
			try {
				const paneId = await this.provisionPhysicalWorker(plan.generation, plan.agentName);
				const finalized = await withRoleLockAsync(
					this.runtime.pool.poolRoot,
					this.role.name,
					() => {
						const record = this.runtime.pool.getByRole(this.role.name);
						if (
							!record ||
							record.generation !== plan.generation ||
							record.workerId !== this.workerId ||
							record.status !== "starting"
						) {
							return false;
						}
						this.runtime.pool.upsert({
							...record,
							paneId,
							agentName: plan.agentName,
							status: "starting",
							cwd: this.runtime.cwd,
							generationTombstone: Math.max(record.generationTombstone, plan.generation),
							updatedAt: new Date(this.runtime.now()).toISOString(),
						});
						return true;
					},
					{ now: this.runtime.now, sleep: this.runtime.sleep },
				);
				if (!finalized) {
					await this.runtime.client.closePane(paneId).catch(() => undefined);
					throw new Error("Worker provision superseded by a newer generation");
				}
				this.paneId = paneId;
				this.physicalEnsured = true;
				await this.waitForWorkerReady();
			} catch (error) {
				await withRoleLockAsync(
					this.runtime.pool.poolRoot,
					this.role.name,
					() => {
						const record = this.runtime.pool.getByRole(this.role.name);
						if (
							record?.generation !== plan.generation ||
							record.workerId !== this.workerId
						) {
							return;
						}
						const nowIso = new Date(this.runtime.now()).toISOString();
						const provisionFail =
							error instanceof ProvisioningFailure ? error : undefined;
						const paneId = provisionFail?.paneId ?? record.paneId;
						const agentName =
							provisionFail?.agentName ?? record.agentName ?? plan.agentName;

						// Never leave a pane-less unhealthy live reservation — archive so
						// generation N+1 can provision while retaining the monotonic tombstone.
						if (!paneId) {
							this.runtime.pool.archiveRoleKeepingTombstone(this.role.name, nowIso);
							const archived = this.runtime.pool.getByRole(this.role.name);
							if (
								archived &&
								archived.generationTombstone < plan.generation
							) {
								this.runtime.pool.upsert({
									...archived,
									generationTombstone: plan.generation,
									updatedAt: nowIso,
								});
							}
							return;
						}

						if (provisionFail?.paneId) {
							const closeSucceeded = provisionFail.closePaneSucceeded === true;
							this.runtime.pool.upsert({
								...record,
								paneId,
								agentName,
								status: "unhealthy",
								paneClosed: closeSucceeded,
								generationTombstone: Math.max(
									record.generationTombstone,
									plan.generation,
								),
								updatedAt: nowIso,
							});
							return;
						}

						// Ready-timeout / post-finalization: keep pane/agent identity.
						this.runtime.pool.upsert({
							...record,
							status: "unhealthy",
							generationTombstone: Math.max(
								record.generationTombstone,
								plan.generation,
							),
							updatedAt: nowIso,
						});
					},
					{ now: this.runtime.now, sleep: this.runtime.sleep },
				);
				throw error;
			}
			await this.dispatchOrEnqueue(task);
			return;
		}

		// wait-ready: join the in-flight starting reservation, then dispatch/enqueue once.
		this.generation = plan.generation;
		if (plan.paneId) this.paneId = plan.paneId;
		if (plan.agentName) this.agentName = plan.agentName;
		await this.waitForWorkerReady();
		await withRoleLockAsync(
			this.runtime.pool.poolRoot,
			this.role.name,
			async () => {
				const record = this.runtime.pool.getByRole(this.role.name);
				if (
					!record ||
					record.generation !== plan.generation ||
					record.workerId !== plan.workerId
				) {
					throw new Error(`Role ${this.role.name} worker superseded after ready`);
				}
				assertLiveWorkerCanonicalCwd(record, this.runtime.cwd, this.role.name);
				if (!record.paneId || !record.agentName) {
					throw new Error(`Role ${this.role.name} worker missing pane/agent after ready`);
				}
				this.paneId = record.paneId;
				this.agentName = record.agentName;
				this.physicalEnsured = true;
				if (record.status === "idle") {
					this.dispatchActiveLocked(task, record);
					return;
				}
				if (record.status === "busy" || record.status === "blocked") {
					enqueueAssignment(this.runtime.pool.poolRoot, this.role.name, {
						assignmentId: this.assignmentId,
						workerId: this.workerId,
						generation: record.generation,
						parentEpoch: this.runtime.parentEpoch,
						task,
					});
					this.heartbeatExpectedAt = this.runtime.now();
					return;
				}
				if (record.status === "starting") {
					throw new Error(`Role ${this.role.name} worker still starting after ready`);
				}
				throw new Error(
					`Role ${this.role.name} worker not dispatchable after ready (${record.status})`,
				);
			},
			{ now: this.runtime.now, sleep: this.runtime.sleep },
		);
	}

	/**
	 * Durable dispatch order under the role lock:
	 * 1) command.json  2) registry busy  3) active.json (commit point).
	 * Registry failure must not publish active.json.
	 */
	private dispatchActiveLocked(task: string, record: PoolWorkerRecord): void {
		if (record.generation !== this.generation || record.workerId !== this.workerId) {
			throw new Error("dispatch generation/worker fence mismatch");
		}
		assertLiveWorkerCanonicalCwd(record, this.runtime.cwd, this.role.name);
		if (!record.paneId || !record.agentName) {
			throw new Error("dispatch requires live pane/agent identity");
		}
		const paths = ensureAssignmentSpool(
			this.runtime.pool.poolRoot,
			this.role.name,
			this.assignmentId,
		);
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task,
			issuedAt: new Date(this.runtime.now()).toISOString(),
			runId: this.assignmentId,
			workerId: this.workerId,
			generation: this.generation,
			parentEpoch: this.runtime.parentEpoch,
		});
		this.runtime.pool.upsert({
			...record,
			status: "busy",
			activeAssignmentId: this.assignmentId,
			activeParentEpoch: this.runtime.parentEpoch,
			updatedAt: new Date(this.runtime.now()).toISOString(),
		});
		const control = workerControlPaths(this.runtime.pool.poolRoot, this.role.name);
		const active: WorkerActivePointer = {
			version: 1,
			assignmentId: this.assignmentId,
			generation: this.generation,
			parentEpoch: this.runtime.parentEpoch,
			dispatchedAt: new Date(this.runtime.now()).toISOString(),
		};
		atomicWriteJson(control.active, active);
		// After durable dispatch, this prompted assignment should expect heartbeats.
		this.heartbeatExpectedAt = this.runtime.now();
	}

	private async provisionPhysicalWorker(generation: number, agentName: string): Promise<string> {
		ensurePoolLayout(this.runtime.pool.poolRoot, this.role.name);
		const control = workerControlPaths(this.runtime.pool.poolRoot, this.role.name);
		ensurePrivateDir(control.root);

		const workerExtension = getWorkerExtensionPath();
		const herdrExtension = getHerdrPiExtensionPath();
		if (!herdrExtension) {
			throw new Error(
				"Official Herdr Pi lifecycle extension is required (herdr integration install pi)",
			);
		}
		void resolveCanonicalPiPath();

		const workerEnv: Record<string, string> = {
			MOMO_WORKER: "1",
			MOMO_ROLE: this.role.name,
			MOMO_WORKER_ID: this.workerId,
			MOMO_WORKER_GENERATION: String(generation),
			MOMO_POOL_KEY: this.runtime.identity.poolKey,
			MOMO_POOL_ROOT: this.runtime.pool.poolRoot,
			MOMO_CONTROL_DIR: control.root,
			MOMO_CWD: this.runtime.cwd,
			MOMO_PARENT_ID: this.runtime.parentId,
			MOMO_RUN_ID: controlRunId(generation),
			MOMO_IPC_DIR: control.root,
		};

		let paneId: string | undefined;
		try {
			const split = await this.runtime.client.splitPane({
				pane: this.runtime.parentPaneId,
				direction: this.runtime.direction,
				cwd: this.runtime.cwd,
				noFocus: true,
				env: workerEnv,
			});
			paneId = split.paneId;
			this.paneId = paneId;

			await this.runtime.client.renamePane(paneId, `Momo ${this.role.name}`);
			await this.runtime.client.reportMetadata(paneId, {
				source: "momo:parent",
				displayAgent: `Momo ${this.role.name}`,
				title: `Momo ${this.role.name}`,
				agent: "pi",
			});

			const agentArgs = [
				"--name",
				`Momo ${this.role.name}`,
				"--no-session",
				"--no-extensions",
				"--no-skills",
				"--no-prompt-templates",
				"--no-themes",
				"--tools",
				roleLaunchToolsCsv(this.role),
				"-e",
				workerExtension,
				"-e",
				herdrExtension,
			];

			await this.runtime.client.agentStart({
				name: agentName,
				paneId,
				kind: "pi",
				timeoutMs: 60_000,
				agentArgs,
			});
			const manifest: WorkerManifest = {
				version: 2, poolKey: this.runtime.identity.poolKey, workerId: this.workerId,
				generation, role: this.role.name, cwd: this.runtime.cwd, paneId, agentName,
				createdAt: new Date(this.runtime.now()).toISOString(),
			};
			atomicWriteJson(control.manifest, manifest);
			return paneId;
		} catch (error) {
			let closePaneSucceeded: boolean | undefined;
			if (paneId) {
				try {
					await this.runtime.client.closePane(paneId);
					closePaneSucceeded = true;
				} catch {
					closePaneSucceeded = false;
				}
			}
			const message = error instanceof Error ? error.message : String(error);
			throw new ProvisioningFailure({
				message,
				agentName,
				...(paneId !== undefined ? { paneId } : {}),
				...(closePaneSucceeded !== undefined ? { closePaneSucceeded } : {}),
				cause: error,
			});
		}
	}

	private async waitForWorkerReady(): Promise<void> {
		const control = workerControlPaths(this.runtime.pool.poolRoot, this.role.name);
		const deadline = this.runtime.now() + this.runtime.readyTimeoutMs;
		while (this.runtime.now() < deadline) {
			try {
				const readyRaw = ipcValidate.tryReadIpcJson(control.ready);
				if (readyRaw) {
					validateReady(readyRaw, {
						runId: controlRunId(this.generation),
						workerId: this.workerId,
					});
					const promoted = withRoleLock(this.runtime.pool.poolRoot, this.role.name, () => {
						const record = this.runtime.pool.getByRole(this.role.name);
						if (
							!record ||
							record.generation !== this.generation ||
							record.workerId !== this.workerId
						) {
							return "superseded" as const;
						}
						// Live starting→idle requires pane/agent identity (pane may land after split).
						if (!record.paneId || !record.agentName) {
							return "wait" as const;
						}
						if (record.status === "starting") {
							this.runtime.pool.upsert({
								...record,
								status: "idle",
								updatedAt: new Date(this.runtime.now()).toISOString(),
							});
						}
						this.paneId = record.paneId;
						this.agentName = record.agentName;
						return "ready" as const;
					});
					if (promoted === "superseded") {
						throw new Error(`Worker ${this.workerId} superseded while waiting for ready`);
					}
					if (promoted === "wait") {
						await this.runtime.sleep(50);
						continue;
					}
					this.physicalEnsured = true;
					return;
				}
			} catch (error) {
				if (error instanceof IpcValidationError) {
					throw new Error(`Worker ${this.workerId} ready IPC invalid: ${error.message}`);
				}
				throw error;
			}
			await this.runtime.sleep(50);
		}
		throw new Error(`Worker ${this.workerId} did not become ready in time`);
	}

	/** Terminal skip for prepared but never-prompted proxies (chain tails / cancel-before-prompt). */
	async skip(reason: string): Promise<void> {
		if (this.settled) return;
		if (this.promptStarted) {
			await this.abort();
			return;
		}
		this.settle({
			version: 1,
			runId: this.assignmentId,
			workerId: this.workerId,
			status: "aborted",
			messages: [],
			errorMessage: reason,
			finishedAt: new Date(this.runtime.now()).toISOString(),
		});
	}

	private async waitForResult(): Promise<IpcResult> {
		const deadline = this.runtime.now() + this.runtime.resultTimeoutMs;
		while (this.runtime.now() < deadline) {
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
			await this.runtime.sleep(this.runtime.pollIntervalMs);
		}
		await this.stopAndSettleFailure("Timed out waiting for worker result");
		return this.waitForResult();
	}

	async abort(): Promise<void> {
		if (this.settled) return;
		if (!this.promptStarted) {
			await this.skip("cancelled_before_prompt");
			return;
		}

		const cancelledQueued = withRoleLock(this.runtime.pool.poolRoot, this.role.name, () => {
			const record = this.runtime.pool.getByRole(this.role.name);
			if (record?.activeAssignmentId === this.assignmentId) {
				return false;
			}
			return cancelQueuedAssignment(this.runtime.pool.poolRoot, this.role.name, this.assignmentId);
		});

		if (cancelledQueued) {
			ensureAssignmentSpool(this.runtime.pool.poolRoot, this.role.name, this.assignmentId);
			this.settle({
				version: 1,
				runId: this.assignmentId,
				workerId: this.workerId,
				status: "aborted",
				messages: [],
				errorMessage: "cancelled_while_queued",
				finishedAt: new Date(this.runtime.now()).toISOString(),
			});
			return;
		}

		// Assignment-specific cancel IPC only — never esc/ctrl+c on a shared pane.
		atomicWriteJson(this.paths.cancel, {
			version: 1,
			runId: this.assignmentId,
			workerId: this.workerId,
			generation: this.generation,
			reason: "parent_abort",
			issuedAt: new Date(this.runtime.now()).toISOString(),
		});

		const waitUntil = this.runtime.now() + 2_000;
		while (this.runtime.now() < waitUntil) {
			await this.poll();
			if (this.settled) return;
			await this.runtime.sleep(50);
		}

		await this.poll();
		if (this.settled) return;

		if (this.role.canWrite) {
			this.settle({
				version: 1,
				runId: this.assignmentId,
				workerId: this.workerId,
				status: "failed",
				messages: this.messagesInternal,
				errorMessage: "Worker cancel unresolved after cancel IPC",
				uncertainWrite: true,
				finishedAt: new Date(this.runtime.now()).toISOString(),
			});
			return;
		}
		this.settle({
			version: 1,
			runId: this.assignmentId,
			workerId: this.workerId,
			status: "aborted",
			messages: this.messagesInternal,
			errorMessage: "Worker cancel unresolved after cancel IPC",
			stopReason: "aborted",
			finishedAt: new Date(this.runtime.now()).toISOString(),
		});
	}

	async dispose(): Promise<void> {
		this.disposed = true;
		this.stopPolling();
		// Persistent panes are retained; proxies do not close them.
	}
}

export function createHerdrChildSessionFactory(options: HerdrFactoryOptions): ChildSessionFactory {
	const client = options.client ?? new HerdrClient();
	const cacheRoot = options.cacheRoot ?? momoCacheRoot();
	const identity = resolvePoolIdentity({
		cwd: options.cwd,
		...(options.workspaceId !== undefined ? { workspaceId: options.workspaceId } : {}),
		...(options.socketPath !== undefined ? { socketPath: options.socketPath } : {}),
		...(options.canonicalRoot !== undefined ? { canonicalRoot: options.canonicalRoot } : {}),
		...(options.workspaceId === undefined && process.env.HERDR_WORKSPACE_ID
			? { workspaceId: process.env.HERDR_WORKSPACE_ID }
			: {}),
		...(options.socketPath === undefined && process.env.HERDR_SOCKET_PATH
			? { socketPath: process.env.HERDR_SOCKET_PATH }
			: {}),
	});
	const pool =
		options.poolRegistry ?? new PoolRegistry(identity.poolKey, cacheRoot);
	const parentEpoch = options.parentEpoch ?? createParentEpoch();
	const now = options.now ?? Date.now;
	const sleep =
		options.sleep ??
		((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));

	const runtime: FactoryRuntime = {
		// Persistent workers always execute from the pool canonical root so
		// parents in different subdirectories of the same repo share one pane.
		cwd: identity.canonicalRoot,
		parentPaneId: options.parentPaneId,
		parentId: options.parentId,
		parentEpoch,
		client,
		pool,
		identity,
		cacheRoot,
		heartbeatStaleMs: options.heartbeatStaleMs ?? DEFAULT_HEARTBEAT_STALE_MS,
		pollIntervalMs: options.pollIntervalMs ?? 100,
		readyTimeoutMs: options.readyTimeoutMs ?? 60_000,
		resultTimeoutMs: options.resultTimeoutMs ?? 60 * 60 * 1000,
		now,
		sleep,
		direction: options.splitDirection ?? "right",
	};

	return async ({ cwd, role }: ChildSessionFactoryInput) => {
		// Factory still accepts the parent request cwd (must match parent options).
		if (cwd !== options.cwd) {
			throw new Error("Child working directory must match the parent working directory");
		}
		// Logical preallocation only — no pane/queue until prompt().
		return new AssignmentProxy(role, runtime);
	};
}

export function getFactoryPoolInfo(factoryOptions: {
	cwd: string;
	cacheRoot?: string;
	workspaceId?: string;
	socketPath?: string;
	canonicalRoot?: string;
}): { identity: PoolIdentity; pool: PoolRegistry; statusLines: string[] } {
	const identity = resolvePoolIdentity({
		cwd: factoryOptions.cwd,
		...(factoryOptions.workspaceId !== undefined
			? { workspaceId: factoryOptions.workspaceId }
			: {}),
		...(factoryOptions.socketPath !== undefined ? { socketPath: factoryOptions.socketPath } : {}),
		...(factoryOptions.canonicalRoot !== undefined
			? { canonicalRoot: factoryOptions.canonicalRoot }
			: {}),
	});
	const cacheRoot = factoryOptions.cacheRoot ?? momoCacheRoot();
	const pool = new PoolRegistry(identity.poolKey, cacheRoot);
	const statusLines = pool.list().map((worker) => formatWorkerStatusLine(worker, pool.poolRoot));
	return { identity, pool, statusLines };
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

export { createParentEpoch, poolRootPath, stableWorkerId };
export type { DelegationProgress, PoolWorkerState };

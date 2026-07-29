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
	withoutProvisioningOwnership,
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
import { completeCleanAssignmentLocked } from "../herdr/terminal-transition.js";
import {
	assertNoOrphanPaneEvidence,
	OrphanPaneEvidencePersistenceError,
	writeOrphanPaneEvidence,
} from "../herdr/orphan-panes.js";
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
	validateActivePointer,
	validateEvent,
	validateHeartbeat,
	validateReady,
	validateResult,
	validateStarted,
} from "../ipc/validate.js";
import {
	LeaseCorruptionError,
	WriterLeaseManager,
} from "../lease/writer-lease.js";

/** Namespace handles so tests can inject result/event races deterministically. */
const ipcSpool = {
	readEventsIncrementally: readEventsIncrementallyImpl,
};
const ipcValidate = {
	tryReadIpcJson: tryReadIpcJsonImpl,
};

/** @internal test-only: runs inside stopAndSettleFailure lock after result recheck. */
let stopAndSettleCommitHook: (() => void | Promise<void>) | undefined;

/** @internal test-only: restore default IPC readers. */
export function __resetIpcReadersForTest(): void {
	ipcSpool.readEventsIncrementally = readEventsIncrementallyImpl;
	ipcValidate.tryReadIpcJson = tryReadIpcJsonImpl;
	stopAndSettleCommitHook = undefined;
	joinerReadyTimeoutLockHook = undefined;
	provisioningHeartbeatBeforeLockHook = undefined;
	postPlanPreBranchHook = undefined;
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

/** @internal test-only: barrier inside stopAndSettleFailure after result recheck, before mutation. */
export function __setStopAndSettleCommitHookForTest(
	hook?: () => void | Promise<void>,
): void {
	stopAndSettleCommitHook = hook;
}

/** @internal test-only: runs under the joiner ready-timeout role lock before liveness+fence. */
let joinerReadyTimeoutLockHook: (() => void | Promise<void>) | undefined;

/** @internal test-only: runs inside provisioning heartbeat before the role-lock acquire. */
let provisioningHeartbeatBeforeLockHook: (() => void) | undefined;

/**
 * @internal test-only: after provision plan owner+heartbeat installed, before try body
 * (post-plan / pre-branch barrier for cancel-strand coverage).
 */
let postPlanPreBranchHook:
	| ((plan: {
			kind: "provision";
			generation: number;
			provisioningOwnerId: string;
	  }) => void | Promise<void>)
	| undefined;

/** @internal test-only: barrier at start of atomic joiner ready-timeout lock callback. */
export function __setJoinerReadyTimeoutLockHookForTest(
	hook?: () => void | Promise<void>,
): void {
	joinerReadyTimeoutLockHook = hook;
}

/** @internal test-only: inject faults before provisioning heartbeat lock acquire. */
export function __setProvisioningHeartbeatBeforeLockHookForTest(hook?: () => void): void {
	provisioningHeartbeatBeforeLockHook = hook;
}

/** @internal test-only: barrier after provision owner install, before try/cancel throw. */
export function __setPostPlanPreBranchHookForTest(
	hook?: (plan: {
		kind: "provision";
		generation: number;
		provisioningOwnerId: string;
	}) => void | Promise<void>,
): void {
	postPlanPreBranchHook = hook;
}

/** @internal test-only: clear provisioning ownership test hooks. */
export function __resetProvisioningOwnershipHooksForTest(): void {
	joinerReadyTimeoutLockHook = undefined;
	provisioningHeartbeatBeforeLockHook = undefined;
	postPlanPreBranchHook = undefined;
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

/** Local wait-ready deadline expired — distinct from invalid ready IPC (fail-closed). */
export class WorkerReadyTimeoutError extends Error {
	constructor(workerId: string) {
		super(`Worker ${workerId} did not become ready in time`);
		this.name = "WorkerReadyTimeoutError";
	}
}

/** Detached background protocol/cleanup failure after prompt/abort already returned. */
export type HerdrDiagnosticReport = {
	kind: "detached_protocol_failure";
	message: string;
	error: unknown;
	paneId?: string;
	reason?: string;
	assignmentId: string;
	workerId: string;
	role: string;
};

export type HerdrDiagnosticReporter = (report: HerdrDiagnosticReport) => void;

function defaultDiagnosticReporter(report: HerdrDiagnosticReport): void {
	const pane = report.paneId ? ` pane=${report.paneId}` : "";
	const reason = report.reason ? ` reason=${report.reason}` : "";
	console.error(
		`[momo] Detached protocol cleanup failure (${report.role}/${report.workerId}): ${report.message}.${pane}${reason}. Run /momo-cleanup if orphan panes remain.`,
	);
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
	leaseManager?: WriterLeaseManager;
	/**
	 * Surfaces non-cancellation protocol/cleanup failures from detached background
	 * dispatch after cancel won the prompt race. Defaults to console.error.
	 */
	reportDiagnostic?: HerdrDiagnosticReporter;
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
	leases: WriterLeaseManager;
	heartbeatStaleMs: number;
	pollIntervalMs: number;
	readyTimeoutMs: number;
	resultTimeoutMs: number;
	now: () => number;
	sleep: (ms: number) => Promise<void>;
	direction: "right" | "down";
	reportDiagnostic: HerdrDiagnosticReporter;
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
	/** Bounded owner heartbeat while this proxy owns a `starting` reservation. */
	private provisioningHeartbeatTimer: ReturnType<typeof setInterval> | undefined;
	/** Unique token matching registry `provisioningOwnerId` for this provision owner. */
	private provisioningOwnerId: string | undefined;
	/** Set by abort(); prompt/dispatch check this and stop without publishing. */
	private cancelRequested = false;
	private readonly cancelWaiters: Array<() => void> = [];
	/** Supervised background dispatch/provision; must not become unhandled. */
	private backgroundDispatch: Promise<void> | undefined;
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

	private provisioningHeartbeatIntervalMs(): number {
		return Math.max(25, Math.min(1_000, Math.floor(this.runtime.readyTimeoutMs / 3)));
	}

	/** Owner heartbeat older than this is treated as stale for joiner fence decisions. */
	private provisioningHeartbeatStaleMs(): number {
		return this.provisioningHeartbeatIntervalMs() * 3;
	}

	private startProvisioningHeartbeat(generation: number): void {
		this.stopProvisioningHeartbeat();
		const intervalMs = this.provisioningHeartbeatIntervalMs();
		this.provisioningHeartbeatTimer = setInterval(() => {
			this.beatProvisioningHeartbeat(generation);
		}, intervalMs);
		this.provisioningHeartbeatTimer.unref?.();
	}

	private stopProvisioningHeartbeat(): void {
		if (this.provisioningHeartbeatTimer !== undefined) {
			clearInterval(this.provisioningHeartbeatTimer);
			this.provisioningHeartbeatTimer = undefined;
		}
	}

	/** True when this proxy still owns the in-flight starting reservation. */
	private matchesProvisioningOwnerLocked(
		record: PoolWorkerRecord,
		generation: number,
	): boolean {
		return (
			record.generation === generation &&
			record.workerId === this.workerId &&
			record.status === "starting" &&
			!!this.provisioningOwnerId &&
			record.provisioningOwnerId === this.provisioningOwnerId
		);
	}

	/**
	 * Durable orphan-pane evidence when close fails after supersession / ownership
	 * loss. Never mutates the current registry successor. Persistence failure is a
	 * protocol error (never silent).
	 */
	private recordOrphanPaneCloseFailure(options: {
		generation: number;
		paneId: string;
		agentName: string;
		reason: string;
	}): void {
		try {
			writeOrphanPaneEvidence(this.runtime.pool.poolRoot, this.role.name, {
				generation: options.generation,
				workerId: this.workerId,
				paneId: options.paneId,
				agentName: options.agentName,
				reason: options.reason,
				createdAt: new Date(this.runtime.now()).toISOString(),
			});
		} catch (evidenceError) {
			throw new OrphanPaneEvidencePersistenceError({
				paneId: options.paneId,
				reason: options.reason,
				evidenceError,
			});
		}
	}

	/**
	 * Generation+worker+starting+owner fenced heartbeat. Stops the timer only when
	 * ownership is definitively lost. Lock/registry errors are swallowed so the
	 * interval cannot become an uncaught exception; the timer is retained to retry.
	 */
	private beatProvisioningHeartbeat(generation: number): void {
		try {
			if (this.disposed || !this.provisioningOwnerId) {
				this.stopProvisioningHeartbeat();
				return;
			}
			provisioningHeartbeatBeforeLockHook?.();
			const ownerId = this.provisioningOwnerId;
			const stillOwner = withRoleLock(this.runtime.pool.poolRoot, this.role.name, () => {
				const record = this.runtime.pool.getByRole(this.role.name);
				if (
					!record ||
					record.generation !== generation ||
					record.workerId !== this.workerId ||
					record.status !== "starting" ||
					record.provisioningOwnerId !== ownerId
				) {
					return false;
				}
				const nowIso = new Date(this.runtime.now()).toISOString();
				this.runtime.pool.upsert({
					...record,
					provisioningHeartbeatAt: nowIso,
					updatedAt: nowIso,
				});
				return true;
			});
			if (!stillOwner) {
				this.stopProvisioningHeartbeat();
			}
		} catch {
			// Retain timer and retry on the next interval — do not declare ownership dead.
		}
	}

	/**
	 * Under role lock: classify provision-owner liveness for a still-starting reservation.
	 * Missing legacy ownership metadata is treated as stale (recoverable via fence).
	 * Future timestamps are never "fresh" (maxFutureSkewMs=0), even though the shared
	 * heartbeat assert otherwise permits a small clock-skew grace.
	 */
	private classifyProvisioningOwnerLivenessLocked(
		generation: number,
		workerId: string,
	): "fresh" | "stale" | "gone" {
		const record = this.runtime.pool.getByRole(this.role.name);
		if (
			!record ||
			record.generation !== generation ||
			record.workerId !== workerId ||
			record.status !== "starting"
		) {
			return "gone";
		}
		if (!record.provisioningOwnerId || !record.provisioningHeartbeatAt) {
			return "stale";
		}
		try {
			assertHeartbeatFreshness(record.provisioningHeartbeatAt, {
				now: this.runtime.now(),
				staleMs: this.provisioningHeartbeatStaleMs(),
				maxFutureSkewMs: 0,
			});
			return "fresh";
		} catch {
			return "stale";
		}
	}

	/**
	 * Atomic under the caller-held role lock: classify owner liveness and, when stale,
	 * fence/archive in the same critical section so a refresh cannot interleave.
	 */
	private resolveJoinerReadyTimeoutLocked(
		generation: number,
		workerId: string,
	): "renew" | "fenced" | "gone" {
		const liveness = this.classifyProvisioningOwnerLivenessLocked(generation, workerId);
		if (liveness === "fresh") return "renew";
		if (liveness === "gone") return "gone";
		this.fenceStartingReservationOnJoinerFailureLocked(generation, workerId);
		return "fenced";
	}

	private notifyCancelled(): void {
		for (const waiter of this.cancelWaiters.splice(0)) {
			try {
				waiter();
			} catch {
				// ignore
			}
		}
	}

	private whenCancelled(): Promise<void> {
		if (this.cancelRequested) return Promise.resolve();
		return new Promise<void>((resolve) => {
			this.cancelWaiters.push(resolve);
		});
	}

	private throwIfCancelled(): void {
		if (!this.cancelRequested) return;
		throw Object.assign(new Error("Assignment cancelled"), {
			stopReason: "aborted" as const,
			cancelled: true as const,
		});
	}

	private isCancelledError(error: unknown): boolean {
		return (
			typeof error === "object" &&
			error !== null &&
			"cancelled" in error &&
			(error as { cancelled?: unknown }).cancelled === true
		);
	}

	/**
	 * Finalize a validated durable result under the role lock, then settle.
	 * Clean completed/failed/aborted advances FIFO/idle via
	 * completeCleanAssignmentLocked. Uncertain results and existing
	 * unhealthy/uncertain registry fences retain evidence (no advance).
	 * Must run under the role lock.
	 */
	private finalizeAuthoritativeResultLocked(result: IpcResult): void {
		if (this.settled) return;
		const record = this.runtime.pool.getByRole(this.role.name);
		const owns =
			!!record &&
			record.generation === this.generation &&
			record.workerId === this.workerId;

		if (owns && (record.status === "unhealthy" || record.status === "uncertain")) {
			// Protocol/parent fence wins: settle in-memory only.
			this.settle(result);
			return;
		}

		if (result.uncertainWrite === true) {
			if (
				owns &&
				record.activeAssignmentId === this.assignmentId &&
				(record.status === "busy" || record.status === "blocked")
			) {
				this.runtime.pool.upsert({
					...record,
					status: "uncertain",
					uncertainWrite: true,
					activeAssignmentId: this.assignmentId,
					...(record.activeParentEpoch !== undefined
						? { activeParentEpoch: record.activeParentEpoch }
						: {}),
					generationTombstone: Math.max(record.generationTombstone, this.generation),
					updatedAt: new Date(this.runtime.now()).toISOString(),
				});
			}
			this.settle(result);
			return;
		}

		// Clean durable result: advance A→B / idle under lock before settle so
		// worker death after result-before-claim still drains FIFO.
		if (owns && record.activeAssignmentId === this.assignmentId) {
			completeCleanAssignmentLocked({
				pool: this.runtime.pool,
				role: this.role.name,
				workerId: this.workerId,
				generation: this.generation,
				finishedAssignmentId: this.assignmentId,
				now: this.runtime.now,
			});
		}
		this.settle(result);
	}

	/**
	 * Authoritative result.json settle helper shared by the initial poll read,
	 * event-parse race recovery, and heartbeat pre-failure checks.
	 * Acquires the role lock. Prefer trySettleAuthoritativeResultLocked when
	 * already holding the lock (e.g. stopAndSettleFailure commit path).
	 * @returns true when a valid matching result was settled
	 * @returns false when no result is present
	 * @throws when result.json exists but fails validation (fail closed)
	 */
	private trySettleAuthoritativeResult(): boolean {
		if (this.settled) return true;
		return withRoleLock(
			this.runtime.pool.poolRoot,
			this.role.name,
			() => this.trySettleAuthoritativeResultLocked(),
			{ now: this.runtime.now, sleep: this.runtime.sleep },
		);
	}

	/** Must run under the role lock. */
	private trySettleAuthoritativeResultLocked(): boolean {
		if (this.settled) return true;
		const resultRaw = ipcValidate.tryReadIpcJson(this.paths.result);
		if (!resultRaw) return false;
		const result = validateResult(
			resultRaw,
			assignmentIdentity(this.assignmentId, this.workerId),
		);
		this.finalizeAuthoritativeResultLocked(result);
		return true;
	}

	/**
	 * Active implementer failure: started valid OR same-worker lease OR
	 * corrupt/missing-owner lease ambiguity ⇒ uncertain. No started + no lease,
	 * or no started + foreign lease ⇒ unhealthy. Never touches leases.
	 */
	private classifyImplementerActiveFailure(record: PoolWorkerRecord): {
		status: "uncertain" | "unhealthy";
		uncertainWrite?: true;
	} {
		const assignmentId = record.activeAssignmentId ?? this.assignmentId;
		const paths = assignmentSpoolPaths(
			this.runtime.pool.poolRoot,
			this.role.name,
			assignmentId,
		);
		const control = workerControlPaths(this.runtime.pool.poolRoot, this.role.name);
		let parentEpoch = record.activeParentEpoch ?? this.runtime.parentEpoch;
		try {
			const activeRaw = ipcValidate.tryReadIpcJson(control.active);
			if (activeRaw) {
				const ptr = validateActivePointer(activeRaw, {
					generation: record.generation,
					assignmentId,
				});
				parentEpoch = ptr.parentEpoch;
			}
		} catch {
			// Ambiguous active pointer — treat as uncertain (may have started).
			return { status: "uncertain", uncertainWrite: true };
		}

		let hasValidStarted = false;
		if (existsSync(paths.started)) {
			try {
				const startedRaw = ipcValidate.tryReadIpcJson(paths.started);
				validateStarted(startedRaw, {
					runId: assignmentId,
					workerId: this.workerId,
					generation: record.generation,
					parentEpoch,
				});
				hasValidStarted = true;
			} catch {
				return { status: "uncertain", uncertainWrite: true };
			}
		}

		const cwd = record.cwd ?? this.runtime.cwd;
		const lockDir = this.runtime.leases.dirFor(cwd);
		if (existsSync(lockDir)) {
			try {
				const owner = this.runtime.leases.peekOwner(cwd);
				if (!owner) {
					// Lock dir without readable owner metadata ⇒ ambiguity.
					return { status: "uncertain", uncertainWrite: true };
				}
				if (owner.ownerId === this.workerId) {
					return { status: "uncertain", uncertainWrite: true };
				}
				// Foreign lease + no valid started ⇒ provably no mutation capability.
				if (!hasValidStarted) {
					return { status: "unhealthy" };
				}
				return { status: "uncertain", uncertainWrite: true };
			} catch (error) {
				if (error instanceof LeaseCorruptionError) {
					return { status: "uncertain", uncertainWrite: true };
				}
				throw error;
			}
		}

		if (hasValidStarted) {
			return { status: "uncertain", uncertainWrite: true };
		}
		// No started + no lease ⇒ unhealthy (pre-start, no mutation capability).
		return { status: "unhealthy" };
	}

	private async poll(): Promise<void> {
		if (this.disposed || this.settled || this.stoppingAfterFailure || !this.promptStarted) return;
		const paths = this.paths;
		try {
			// Durable result is authoritative: validate/settle result BEFORE events so
			// corrupt/oversized event logs cannot reject an already-valid terminal result.
			if (this.trySettleAuthoritativeResult()) return;

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
					try {
						assertHeartbeatFreshness(heartbeat.at, {
							now: this.runtime.now(),
							staleMs: this.runtime.heartbeatStaleMs,
						});
					} catch (heartbeatError) {
						// Result may have landed after the initial poll read.
						if (this.trySettleAuthoritativeResult()) return;
						throw heartbeatError;
					}
				} else if (
					this.heartbeatExpectedAt !== undefined &&
					this.runtime.now() - this.heartbeatExpectedAt > this.runtime.heartbeatStaleMs
				) {
					// Missing heartbeat after dispatch/readiness fails like a stale one.
					// Close the result-vs-heartbeat race before synthesizing failure.
					if (this.trySettleAuthoritativeResult()) return;
					await this.stopAndSettleFailure("Worker heartbeat went stale");
				}
			}
		} catch (error) {
			// Close the check/read race: a result may have landed after the first
			// result check and before/during event parse or heartbeat failure.
			try {
				if (this.trySettleAuthoritativeResult()) return;
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
		let invalidResultMessage: string | undefined;
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
			await withRoleLockAsync(
				this.runtime.pool.poolRoot,
				this.role.name,
				async () => {
					// Commit-point recheck: durable result wins over failure fencing.
					// Finalize under this lock (no reentrant withRoleLock).
					try {
						if (this.trySettleAuthoritativeResultLocked()) {
							return;
						}
					} catch (resultError) {
						invalidResultMessage =
							resultError instanceof IpcValidationError || resultError instanceof Error
								? resultError.message
								: String(resultError);
						// Invalid result fails closed — continue to registry failure mutation.
					}

					if (stopAndSettleCommitHook) {
						await stopAndSettleCommitHook();
						// Worker may have published result while we yielded — recheck.
						try {
							if (this.trySettleAuthoritativeResultLocked()) {
								return;
							}
						} catch (resultError) {
							invalidResultMessage =
								resultError instanceof IpcValidationError || resultError instanceof Error
									? resultError.message
									: String(resultError);
						}
					}

					const record = this.runtime.pool.getByRole(this.role.name);
					const isActive =
						record?.generation === this.generation &&
						record.workerId === this.workerId &&
						record.activeAssignmentId === this.assignmentId;
					if (isActive && record) {
						activeFailure = true;
						if (this.role.canWrite) {
							const classified = this.classifyImplementerActiveFailure(record);
							this.runtime.pool.upsert({
								...record,
								status: classified.status,
								...(classified.uncertainWrite ? { uncertainWrite: true } : {}),
								generationTombstone: Math.max(record.generationTombstone, this.generation),
								updatedAt: new Date(this.runtime.now()).toISOString(),
							});
							activeFailure = classified.status === "uncertain";
						} else {
							this.runtime.pool.upsert({
								...record,
								status: "unhealthy",
								generationTombstone: Math.max(record.generationTombstone, this.generation),
								updatedAt: new Date(this.runtime.now()).toISOString(),
							});
							activeFailure = false;
						}
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
								errorMessage: (invalidResultMessage ?? message).slice(0, 1024),
								finishedAt: new Date(this.runtime.now()).toISOString(),
							});
						}
					} catch {
						// settle in-memory regardless
					}
				},
				{ now: this.runtime.now, sleep: this.runtime.sleep },
			);
			if (this.settled) return;
			this.settle({
				version: 1,
				runId: this.assignmentId,
				workerId: this.workerId,
				status: "failed",
				messages: this.messagesInternal,
				errorMessage: invalidResultMessage ?? message,
				...(activeFailure ? { uncertainWrite: true } : {}),
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

		const work = this.runDispatchSupervised(text);
		this.backgroundDispatch = work;
		await Promise.race([work, this.whenCancelled()]);
		if (this.cancelRequested) {
			// Abort already settled; keep background work supervised.
			// Protocol cleanup failures must not be silently dropped.
			void work.catch((error: unknown) => {
				this.handleDetachedBackgroundFailure(error);
			});
			return;
		}
		await work;
		if (!this.settled && !this.cancelRequested) {
			this.startPolling();
		}
	}

	private async runDispatchSupervised(task: string): Promise<void> {
		try {
			await this.dispatchOrEnqueue(task);
		} catch (error) {
			// Orphan evidence persistence (and similar protocol cleanup failures) must
			// bypass cancellation swallowing so detached work can surface them.
			if (error instanceof OrphanPaneEvidencePersistenceError) {
				throw error;
			}
			if (this.cancelRequested || this.isCancelledError(error)) {
				return;
			}
			throw error;
		}
	}

	/**
	 * After cancel won the prompt race, background dispatch must not unhandled-reject.
	 * Ordinary cancellation stays quiet; protocol/cleanup failures are reported.
	 */
	private handleDetachedBackgroundFailure(error: unknown): void {
		if (this.isCancelledError(error)) return;
		this.reportDetachedProtocolFailure(error);
	}

	private reportDetachedProtocolFailure(error: unknown): void {
		const message = error instanceof Error ? error.message : String(error);
		this.diagnosticFailure = message;
		const report: HerdrDiagnosticReport = {
			kind: "detached_protocol_failure",
			message,
			error,
			assignmentId: this.assignmentId,
			workerId: this.workerId,
			role: this.role.name,
			...(error instanceof OrphanPaneEvidencePersistenceError
				? { paneId: error.paneId, reason: error.reason }
				: {}),
		};
		try {
			this.runtime.reportDiagnostic(report);
		} catch {
			// Reporter must not break supervision; fall back to stderr.
			defaultDiagnosticReporter(report);
		}
	}

	/** @internal test-only */
	getDiagnosticFailureForTest(): string | undefined {
		return this.diagnosticFailure;
	}

	private async dispatchOrEnqueue(task: string): Promise<void> {
		this.throwIfCancelled();
		type Plan =
			| {
					kind: "provision";
					generation: number;
					agentName: string;
					provisioningOwnerId: string;
			  }
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
				this.throwIfCancelled();
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
				this.throwIfCancelled();
				if (!existing || isArchivalTombstone(existing)) {
					assertNoOrphanPaneEvidence(this.runtime.pool.poolRoot, this.role.name);
					const generation = this.runtime.pool.nextGeneration(this.role.name);
					const agentName = herdrAgentNameForWorker(this.workerId);
					const provisioningOwnerId = randomUUID();
					const nowIso = new Date(this.runtime.now()).toISOString();
					clearControlEphemerals(this.runtime.pool.poolRoot, this.role.name);
					this.runtime.pool.upsert({
						workerId: this.workerId,
						generation,
						generationTombstone: Math.max(existing?.generationTombstone ?? 0, generation),
						role: this.role.name,
						agentName,
						status: "starting",
						cwd: this.runtime.cwd,
						provisioningOwnerId,
						provisioningHeartbeatAt: nowIso,
						updatedAt: nowIso,
					});
					return { kind: "provision", generation, agentName, provisioningOwnerId };
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

				this.throwIfCancelled();
				if (existing.status === "idle") {
					this.dispatchActiveLocked(task, existing);
					return { kind: "done" };
				}

				if (existing.status === "busy" || existing.status === "blocked") {
					this.throwIfCancelled();
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
			// Do not throwIfCancelled between reservation and this branch: that would
			// strand a pane-less starting owner with no catch/rollback. Install owner
			// + heartbeat, enter try, then throw so the fenced catch archives.
			this.generation = plan.generation;
			this.agentName = plan.agentName;
			this.provisioningOwnerId = plan.provisioningOwnerId;
			this.startProvisioningHeartbeat(plan.generation);
			if (postPlanPreBranchHook) {
				await postPlanPreBranchHook({
					kind: "provision",
					generation: plan.generation,
					provisioningOwnerId: plan.provisioningOwnerId,
				});
			}
			try {
				this.throwIfCancelled();
				const paneId = await this.provisionPhysicalWorker(plan.generation, plan.agentName);
				if (this.cancelRequested) {
					let closePaneSucceeded = false;
					try {
						await this.runtime.client.closePane(paneId);
						closePaneSucceeded = true;
					} catch {
						closePaneSucceeded = false;
					}
					throw new ProvisioningFailure({
						message: "Assignment cancelled during provisioning",
						agentName: plan.agentName,
						paneId,
						closePaneSucceeded,
					});
				}
				const finalized = await withRoleLockAsync(
					this.runtime.pool.poolRoot,
					this.role.name,
					() => {
						this.throwIfCancelled();
						const record = this.runtime.pool.getByRole(this.role.name);
						if (!record || !this.matchesProvisioningOwnerLocked(record, plan.generation)) {
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
					this.stopProvisioningHeartbeat();
					let closePaneSucceeded = false;
					try {
						await this.runtime.client.closePane(paneId);
						closePaneSucceeded = true;
					} catch {
						closePaneSucceeded = false;
					}
					if (!closePaneSucceeded) {
						this.recordOrphanPaneCloseFailure({
							generation: plan.generation,
							paneId,
							agentName: plan.agentName,
							reason: "provision_superseded_after_finalize_close_failed",
						});
					}
					throw new Error("Worker provision superseded by a newer generation");
				}
				this.paneId = paneId;
				this.physicalEnsured = true;
				this.throwIfCancelled();
				await this.waitForWorkerReady();
				this.throwIfCancelled();
			} catch (error) {
				this.stopProvisioningHeartbeat();
				let failError: unknown = error;
				if (error instanceof OrphanPaneEvidencePersistenceError) {
					// Already a surfaced protocol failure from close+evidence write.
					failError = error;
				} else if (
					this.cancelRequested &&
					!(error instanceof ProvisioningFailure) &&
					this.paneId
				) {
					let closePaneSucceeded = false;
					try {
						await this.runtime.client.closePane(this.paneId);
						closePaneSucceeded = true;
					} catch {
						closePaneSucceeded = false;
					}
					failError = new ProvisioningFailure({
						message: "Assignment cancelled during provisioning",
						agentName: plan.agentName,
						paneId: this.paneId,
						closePaneSucceeded,
					});
				}
				let evidencePersistenceError: OrphanPaneEvidencePersistenceError | undefined;
				try {
					await withRoleLockAsync(
						this.runtime.pool.poolRoot,
						this.role.name,
						() => {
							const record = this.runtime.pool.getByRole(this.role.name);
							const provisionFail =
								failError instanceof ProvisioningFailure ? failError : undefined;
							// Same-generation replacement owners must not be mutated by a stale owner.
							// If close already failed for a superseded pane, persist orphan evidence.
							if (
								!record ||
								!this.matchesProvisioningOwnerLocked(record, plan.generation)
							) {
								// Skip a second evidence write when the inner path already
								// surfaced OrphanPaneEvidencePersistenceError (preserve first reason).
								if (
									!(failError instanceof OrphanPaneEvidencePersistenceError) &&
									provisionFail?.paneId &&
									provisionFail.closePaneSucceeded === false
								) {
									this.recordOrphanPaneCloseFailure({
										generation: plan.generation,
										paneId: provisionFail.paneId,
										agentName: provisionFail.agentName,
										reason: "provision_ownership_superseded_close_failed",
									});
								}
								return;
							}
							const nowIso = new Date(this.runtime.now()).toISOString();
							const paneId =
								provisionFail?.paneId ?? this.paneId ?? record.paneId;
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
								this.runtime.pool.upsert(
									withoutProvisioningOwnership(record, {
										paneId,
										agentName,
										status: "unhealthy",
										paneClosed: closeSucceeded,
										generationTombstone: Math.max(
											record.generationTombstone,
											plan.generation,
										),
										updatedAt: nowIso,
									}),
								);
								return;
							}

							// Ready-timeout / post-finalization: keep pane/agent identity.
							this.runtime.pool.upsert(
								withoutProvisioningOwnership(record, {
									status: "unhealthy",
									generationTombstone: Math.max(
										record.generationTombstone,
										plan.generation,
									),
									updatedAt: nowIso,
								}),
							);
						},
						{ now: this.runtime.now, sleep: this.runtime.sleep },
					);
				} catch (lockError) {
					if (lockError instanceof OrphanPaneEvidencePersistenceError) {
						evidencePersistenceError = lockError;
					} else {
						throw lockError;
					}
				}
				// Evidence persistence failure is a non-cancellation protocol error —
				// never swallow even when cancel settles the assignment. Prefer the
				// earliest persistence error when both inner and lock paths failed.
				if (failError instanceof OrphanPaneEvidencePersistenceError) {
					throw failError;
				}
				if (evidencePersistenceError) {
					throw evidencePersistenceError;
				}
				if (this.cancelRequested || this.isCancelledError(error)) {
					return;
				}
				throw failError;
			} finally {
				this.stopProvisioningHeartbeat();
			}
			this.throwIfCancelled();
			await this.dispatchOrEnqueue(task);
			return;
		}

		// wait-ready: join the in-flight starting reservation, then dispatch/enqueue once.
		// Cancellation must not fence. Invalid ready IPC is fail-closed (not a local timeout).
		// Stale ready-timeout fencing happens inside waitForWorkerReady({ asJoiner: true }).
		this.generation = plan.generation;
		if (plan.paneId) this.paneId = plan.paneId;
		if (plan.agentName) this.agentName = plan.agentName;
		this.throwIfCancelled();
		await this.waitForWorkerReady({ asJoiner: true });
		this.throwIfCancelled();
		await withRoleLockAsync(
			this.runtime.pool.poolRoot,
			this.role.name,
			async () => {
				this.throwIfCancelled();
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
				this.throwIfCancelled();
				if (record.status === "idle") {
					this.dispatchActiveLocked(task, record);
					return;
				}
				if (record.status === "busy" || record.status === "blocked") {
					this.throwIfCancelled();
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
	 * Fence a still-starting reservation. Caller must hold the role lock.
	 * Paneful → unhealthy (retain pane/agent for cleanup). Pane-less → archive with
	 * monotonic tombstone so N+1 can reprovision. Never overwrite a newer generation
	 * or a non-starting status.
	 */
	private fenceStartingReservationOnJoinerFailureLocked(
		generation: number,
		workerId: string,
	): void {
		const record = this.runtime.pool.getByRole(this.role.name);
		if (
			!record ||
			record.generation !== generation ||
			record.workerId !== workerId ||
			record.status !== "starting"
		) {
			return;
		}
		const nowIso = new Date(this.runtime.now()).toISOString();
		if (record.paneId) {
			this.runtime.pool.upsert(
				withoutProvisioningOwnership(record, {
					status: "unhealthy",
					generationTombstone: Math.max(record.generationTombstone, generation),
					updatedAt: nowIso,
				}),
			);
			return;
		}
		this.runtime.pool.archiveRoleKeepingTombstone(this.role.name, nowIso);
		const archived = this.runtime.pool.getByRole(this.role.name);
		if (archived && archived.generationTombstone < generation) {
			this.runtime.pool.upsert({
				...archived,
				generationTombstone: generation,
				updatedAt: nowIso,
			});
		}
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
			const splitPaneId = split.paneId;
			this.throwIfCancelled();

			// Persist paneId into the starting reservation immediately after split
			// (before rename/start) so parent death leaves closable evidence.
			const persisted = withRoleLock(this.runtime.pool.poolRoot, this.role.name, () => {
				const record = this.runtime.pool.getByRole(this.role.name);
				if (!record || !this.matchesProvisioningOwnerLocked(record, generation)) {
					return false;
				}
				this.runtime.pool.upsert({
					...record,
					paneId: splitPaneId,
					agentName,
					cwd: this.runtime.cwd,
					generationTombstone: Math.max(record.generationTombstone, generation),
					updatedAt: new Date(this.runtime.now()).toISOString(),
				});
				return true;
			});
			if (!persisted) {
				// Stale/superseded split: close the orphan pane; do not mutate foreign gen.
				let closePaneSucceeded = false;
				try {
					await this.runtime.client.closePane(splitPaneId);
					closePaneSucceeded = true;
				} catch {
					closePaneSucceeded = false;
				}
				if (!closePaneSucceeded) {
					this.recordOrphanPaneCloseFailure({
						generation,
						paneId: splitPaneId,
						agentName,
						reason: "provision_superseded_after_split_close_failed",
					});
				}
				throw new ProvisioningFailure({
					message: "Worker provision superseded after pane split",
					agentName,
					paneId: splitPaneId,
					closePaneSucceeded,
				});
			}

			await this.runtime.client.renamePane(splitPaneId, `Momo ${this.role.name}`);
			await this.runtime.client.reportMetadata(splitPaneId, {
				source: "momo:parent",
				displayAgent: `Momo ${this.role.name}`,
				title: `Momo ${this.role.name}`,
				agent: "pi",
			});
			this.throwIfCancelled();

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
				paneId: splitPaneId,
				kind: "pi",
				timeoutMs: 60_000,
				agentArgs,
			});
			this.throwIfCancelled();
			const manifest: WorkerManifest = {
				version: 2, poolKey: this.runtime.identity.poolKey, workerId: this.workerId,
				generation, role: this.role.name, cwd: this.runtime.cwd, paneId: splitPaneId, agentName,
				createdAt: new Date(this.runtime.now()).toISOString(),
			};
			atomicWriteJson(control.manifest, manifest);
			return splitPaneId;
		} catch (error) {
			if (error instanceof ProvisioningFailure) {
				// Superseded path already closed (or attempted close). Other
				// ProvisioningFailures should not appear here yet.
				throw error;
			}
			if (error instanceof OrphanPaneEvidencePersistenceError) {
				// Close failed and evidence could not be persisted — surface as-is.
				throw error;
			}
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

	/**
	 * Wait until control ready.json is valid and the starting reservation is promoted.
	 * Joiners renew their local deadline while the provision owner's heartbeat is fresh;
	 * only a stale/missing owner heartbeat may fence+archive the reservation.
	 */
	private async waitForWorkerReady(options?: { asJoiner?: boolean }): Promise<void> {
		for (;;) {
			try {
				await this.waitForWorkerReadyOnce();
				this.stopProvisioningHeartbeat();
				return;
			} catch (error) {
				if (!(error instanceof WorkerReadyTimeoutError)) {
					throw error;
				}
				if (!options?.asJoiner) {
					throw error;
				}
				// Liveness classify + stale fence/archive must be one role-lock critical
				// section so an owner heartbeat cannot refresh between check and fence.
				const decision = await withRoleLockAsync(
					this.runtime.pool.poolRoot,
					this.role.name,
					async () => {
						if (joinerReadyTimeoutLockHook) {
							await joinerReadyTimeoutLockHook();
						}
						return this.resolveJoinerReadyTimeoutLocked(this.generation, this.workerId);
					},
					{ now: this.runtime.now, sleep: this.runtime.sleep },
				);
				if (decision === "renew") {
					continue;
				}
				// "fenced" or "gone": fail (gone leaves reservation untouched).
				throw error;
			}
		}
	}

	private async waitForWorkerReadyOnce(): Promise<void> {
		const control = workerControlPaths(this.runtime.pool.poolRoot, this.role.name);
		const deadline = this.runtime.now() + this.runtime.readyTimeoutMs;
		while (this.runtime.now() < deadline) {
			this.throwIfCancelled();
			try {
				const readyRaw = ipcValidate.tryReadIpcJson(control.ready);
				if (readyRaw) {
					validateReady(readyRaw, {
						runId: controlRunId(this.generation),
						workerId: this.workerId,
					});
					const promoted = withRoleLock(this.runtime.pool.poolRoot, this.role.name, () => {
						this.throwIfCancelled();
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
							this.runtime.pool.upsert(
								withoutProvisioningOwnership(record, {
									status: "idle",
									updatedAt: new Date(this.runtime.now()).toISOString(),
								}),
							);
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
					// Fail closed: invalid ready is not a local timeout and must not fence.
					throw new Error(`Worker ${this.workerId} ready IPC invalid: ${error.message}`);
				}
				throw error;
			}
			await this.runtime.sleep(50);
		}
		throw new WorkerReadyTimeoutError(this.workerId);
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
		if (this.settled && !this.cancelRequested) return;
		if (!this.promptStarted) {
			await this.skip("cancelled_before_prompt");
			return;
		}

		this.cancelRequested = true;
		this.notifyCancelled();

		const cancelledQueued = withRoleLock(this.runtime.pool.poolRoot, this.role.name, () => {
			const record = this.runtime.pool.getByRole(this.role.name);
			if (record?.activeAssignmentId === this.assignmentId) {
				return false;
			}
			return cancelQueuedAssignment(this.runtime.pool.poolRoot, this.role.name, this.assignmentId);
		});

		if (cancelledQueued) {
			ensureAssignmentSpool(this.runtime.pool.poolRoot, this.role.name, this.assignmentId);
			if (!this.settled) {
				this.settle({
					version: 1,
					runId: this.assignmentId,
					workerId: this.workerId,
					status: "aborted",
					messages: [],
					errorMessage: "cancelled_while_queued",
					finishedAt: new Date(this.runtime.now()).toISOString(),
				});
			}
			return;
		}

		const record = this.runtime.pool.getByRole(this.role.name);
		const isActive =
			record?.generation === this.generation &&
			record.workerId === this.workerId &&
			record.activeAssignmentId === this.assignmentId;

		// Mid-provision / wait-ready / pre-dispatch: settle aborted immediately so
		// prompt() unblocks; background work rolls back without publishing.
		if (!isActive) {
			const midProvision =
				!this.physicalEnsured && this.heartbeatExpectedAt === undefined;
			if (!midProvision) {
				// Previously dispatched (or superseded after A→B): assignment-specific
				// cancel IPC only — never terminal keys on the shared pane.
				try {
					atomicWriteJson(this.paths.cancel, {
						version: 1,
						runId: this.assignmentId,
						workerId: this.workerId,
						generation: this.generation,
						reason: "parent_abort",
						issuedAt: new Date(this.runtime.now()).toISOString(),
					});
				} catch {
					// best effort
				}
			}
			if (!this.settled) {
				this.settle({
					version: 1,
					runId: this.assignmentId,
					workerId: this.workerId,
					status: "aborted",
					messages: [],
					errorMessage: "parent_abort",
					finishedAt: new Date(this.runtime.now()).toISOString(),
				});
			}
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

		// Final durable-result poll before synthesizing unresolved-cancel failure.
		await this.poll();
		if (this.settled) return;

		// Fenced registry transition: unhealthy/uncertain via classifyImplementerActiveFailure,
		// queue/claim cleanup for non-active, parent settlement matches classification.
		await this.stopAndSettleFailure("Worker cancel unresolved after cancel IPC");
	}

	async dispose(): Promise<void> {
		this.disposed = true;
		this.stopPolling();
		this.stopProvisioningHeartbeat();
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
		leases:
			options.leaseManager ??
			new WriterLeaseManager({
				cacheRoot,
				now,
				sleep,
			}),
		heartbeatStaleMs: options.heartbeatStaleMs ?? DEFAULT_HEARTBEAT_STALE_MS,
		pollIntervalMs: options.pollIntervalMs ?? 100,
		readyTimeoutMs: options.readyTimeoutMs ?? 60_000,
		resultTimeoutMs: options.resultTimeoutMs ?? 60 * 60 * 1000,
		now,
		sleep,
		direction: options.splitDirection ?? "right",
		reportDiagnostic: options.reportDiagnostic ?? defaultDiagnosticReporter,
	};

	return async ({ cwd, role }: ChildSessionFactoryInput) => {
		// Factory still accepts the parent request cwd (must match parent options).
		if (cwd !== options.cwd) {
			throw new Error("Child working directory must match the parent working directory");
		}
		// Assignment proxy only — no pane/queue until prompt().
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

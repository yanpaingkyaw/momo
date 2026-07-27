import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { existsSync, rmSync } from "node:fs";
import path from "node:path";
import { createWorkspaceDiffTool } from "../delegation/workspace-diff.js";
import { assignmentSpoolPaths } from "../herdr/assignment-spool.js";
import {
	beginClaimHead,
	commitClaim,
	listClaiming,
	recoverClaiming,
	withRoleLock,
	type QueueEntry,
} from "../herdr/role-queue.js";
import type { PoolWorkerRecord } from "../herdr/pool-registry.js";
import { PoolRegistry } from "../herdr/pool-registry.js";
import { completeCleanAssignmentLocked } from "../herdr/terminal-transition.js";
import { getRole, isAgentName, type AgentName } from "../roles.js";
import {
	IMPLEMENTER_SYSTEM_PROMPT,
	PLANNER_SYSTEM_PROMPT,
	REVIEWER_SYSTEM_PROMPT,
	SCOUT_SYSTEM_PROMPT,
} from "../prompts.js";
import {
	IpcValidationError,
	sanitizeAssistantMessages,
	tryReadIpcJson,
	validateActivePointer,
	validateCancel,
	validateCommand,
	validateResult,
	validateStarted,
} from "../ipc/validate.js";
import {
	appendEvent,
	atomicWriteJson,
	EventsFileCapacityError,
	MAX_IPC_JSON_BYTES,
	type IpcActivePointer,
	type IpcCommand,
	type IpcEvent,
	type IpcResult,
	type IpcStarted,
} from "../ipc/spool.js";
import {
	DEFAULT_LEASE_WAIT_MS,
	LeaseCorruptionError,
	LeaseWaitCancelledError,
	LeaseWaitTimeoutError,
	WriterLeaseManager,
	createLeaseToken,
} from "../lease/writer-lease.js";

const ROLE_PROMPTS: Record<AgentName, string> = {
	scout: SCOUT_SYSTEM_PROMPT,
	planner: PLANNER_SYSTEM_PROMPT,
	implementer: IMPLEMENTER_SYSTEM_PROMPT,
	reviewer: REVIEWER_SYSTEM_PROMPT,
};

const MUTATION_TOOLS = new Set(["bash", "edit", "write"]);

/** @deprecated Context isolation now uses the latest user message. */
export const ASSIGNMENT_BOUNDARY_TYPE = "momo-assignment-boundary";

export interface WorkerRuntimeOptions {
	env?: NodeJS.ProcessEnv;
	leaseManager?: WriterLeaseManager;
	now?: () => number;
	sleep?: (ms: number) => Promise<void>;
	leaseWaitMs?: number;
	poolRegistry?: PoolRegistry;
}

interface ActiveAssignment {
	assignmentId: string;
	paths: ReturnType<typeof assignmentSpoolPaths>;
	assistantMessages: unknown[];
	eventSeq: number;
	mutationAttempted: boolean;
	mutationToolsEnabled: boolean;
	leaseHeld: boolean;
	leaseToken: string;
	resultWritten: boolean;
	acquiringLease: boolean;
	parentEpoch?: string;
	/** A cancellation is terminal even when Pi settles without an assistant message. */
	cancelRequested?: string;
	/** Progress event log hit the byte cap; further non-authoritative events are dropped. */
	eventsCapacityExhausted?: boolean;
}

export function installMomoWorker(pi: ExtensionAPI, options: WorkerRuntimeOptions = {}): void {
	const env = options.env ?? process.env;
	if (env.MOMO_WORKER !== "1") return;

	const roleName = env.MOMO_ROLE;
	const controlDir = env.MOMO_CONTROL_DIR || env.MOMO_IPC_DIR;
	const workerIdRaw = env.MOMO_WORKER_ID;
	const generationRaw = env.MOMO_WORKER_GENERATION;
	const poolKeyRaw = env.MOMO_POOL_KEY;
	const poolRootRaw = env.MOMO_POOL_ROOT;
	const runIdRaw = env.MOMO_RUN_ID;
	const cwd = env.MOMO_CWD || process.cwd();
	if (
		!roleName ||
		!isAgentName(roleName) ||
		!controlDir ||
		!workerIdRaw ||
		!runIdRaw ||
		!poolKeyRaw ||
		!poolRootRaw ||
		!generationRaw
	) {
		throw new Error(
			"Momo worker missing MOMO_ROLE / MOMO_CONTROL_DIR|MOMO_IPC_DIR / MOMO_WORKER_ID / MOMO_RUN_ID / MOMO_POOL_KEY / MOMO_POOL_ROOT / MOMO_WORKER_GENERATION",
		);
	}
	const workerId: string = workerIdRaw;
	const runId: string = runIdRaw;
	const poolKey: string = poolKeyRaw;
	const poolRoot: string = poolRootRaw;
	const generation = Number(generationRaw);
	if (!Number.isInteger(generation) || generation < 1) {
		throw new Error("MOMO_WORKER_GENERATION must be a positive integer");
	}

	const role = getRole(roleName);
	const sleep =
		options.sleep ?? ((ms: number) => new Promise((resolve) => setTimeout(resolve, ms)));
	const lease =
		options.leaseManager ??
		new WriterLeaseManager({
			sleep,
			...(options.now ? { now: options.now } : {}),
		});
	const leaseOwner = workerId;
	const now = options.now ?? Date.now;
	const leaseWaitMs = options.leaseWaitMs ?? DEFAULT_LEASE_WAIT_MS;
	const pool = options.poolRegistry ?? new PoolRegistry(poolKey, path.dirname(path.dirname(poolRoot)));
	// PoolRegistry derives poolRoot from cacheRoot+poolKey; when MOMO_POOL_ROOT is set,
	// prefer constructing with the parent cache that contains pools/<key>.
	const controlPaths = {
		ready: path.join(controlDir, "ready.json"),
		heartbeat: path.join(controlDir, "heartbeat.json"),
		manifest: path.join(controlDir, "manifest.json"),
		active: path.join(controlDir, "active.json"),
	};

	let active: ActiveAssignment | undefined;
	let controlHeartbeatSeq = 0;
	let heartbeatTimer: ReturnType<typeof setInterval> | undefined;
	let pollTimer: ReturnType<typeof setInterval> | undefined;
	let protocolUnhealthy = false;

	const readOnlyTools = role.tools.filter((tool) => !MUTATION_TOOLS.has(tool) && tool !== "workspace_diff");

	if (role.name === "reviewer") {
		pi.registerTool(createWorkspaceDiffTool(cwd));
	}

	function activeReadOnlyTools(): string[] {
		const tools = [...readOnlyTools];
		if (role.name === "reviewer") tools.push("workspace_diff");
		return tools;
	}

	function disableMutationTools(): void {
		if (active) active.mutationToolsEnabled = false;
		pi.setActiveTools(activeReadOnlyTools());
	}

	function enableMutationTools(assignment: ActiveAssignment): void {
		if (!role.canWrite) return;
		if (!lease.validate(cwd, leaseOwner, assignment.leaseToken)) {
			throw new Error("Writer lease invalid");
		}
		assignment.mutationToolsEnabled = true;
		pi.setActiveTools([...readOnlyTools, "bash", "edit", "write"]);
	}

	function emitAssignment(
		assignment: ActiveAssignment,
		type: IpcEvent["type"],
		message?: string,
		extra: Partial<IpcEvent> = {},
	): void {
		if (assignment.eventsCapacityExhausted) return;
		assignment.eventSeq += 1;
		const event: IpcEvent = {
			version: 1,
			type,
			at: new Date(now()).toISOString(),
			runId: assignment.assignmentId,
			workerId,
			seq: assignment.eventSeq,
			...extra,
		};
		if (message !== undefined) event.message = message.slice(0, 8192);
		try {
			appendEvent(assignment.paths.events, event);
		} catch (error) {
			if (error instanceof EventsFileCapacityError) {
				assignment.eventsCapacityExhausted = true;
				return;
			}
			throw error;
		}
	}

	function writeControlHeartbeat(): void {
		controlHeartbeatSeq += 1;
		atomicWriteJson(controlPaths.heartbeat, {
			version: 1,
			runId,
			workerId,
			at: new Date(now()).toISOString(),
			seq: controlHeartbeatSeq,
		});
		if (active?.leaseHeld) {
			try {
				lease.heartbeat(cwd, leaseOwner, active.leaseToken);
			} catch {
				disableMutationTools();
			}
		}
		if (active && !active.resultWritten) {
			atomicWriteJson(active.paths.heartbeat, {
				version: 1,
				runId: active.assignmentId,
				workerId,
				at: new Date(now()).toISOString(),
				seq: controlHeartbeatSeq,
			});
		}
	}

	type RegistryExtra = Partial<{
		activeAssignmentId?: string;
		activeParentEpoch?: string;
		uncertainWrite?: boolean;
	}>;

	function ownsCurrentGeneration(
		current: ReturnType<PoolRegistry["getByRole"]>,
	): current is NonNullable<ReturnType<PoolRegistry["getByRole"]>> {
		return (
			!!current &&
			current.generation === generation &&
			current.workerId === workerId
		);
	}

	/**
	 * Fence failure: no registry write, no queue claim/commit, no control removal.
	 * Disable mutation and stop further queue advancement.
	 */
	function failGenerationFence(): void {
		disableMutationTools();
		protocolUnhealthy = true;
	}

	/**
	 * Must run under the per-role lock. Used by claimNextOrIdleLocked / session_start.
	 * Never call withRoleLock from here — RoleTransactionLock is not reentrant.
	 * Returns false when generation/worker fence fails (no upsert performed).
	 */
	function markRegistryLocked(
		status: "idle" | "busy" | "blocked" | "unhealthy" | "uncertain",
		extra: RegistryExtra = {},
	): boolean {
		const current = pool.getByRole(role.name);
		if (!ownsCurrentGeneration(current)) {
			failGenerationFence();
			return false;
		}
		if (status === "idle") {
			pool.upsert({
				workerId,
				generation,
				generationTombstone: Math.max(current.generationTombstone, generation),
				role: role.name,
				...(current.paneId ? { paneId: current.paneId } : {}),
				...(current.agentName ? { agentName: current.agentName } : {}),
				...(current.cwd ? { cwd: current.cwd } : cwd ? { cwd } : {}),
				status: "idle",
				updatedAt: new Date(now()).toISOString(),
			});
			return true;
		}
		pool.upsert({
			workerId,
			generation,
			generationTombstone: Math.max(current.generationTombstone, generation),
			role: role.name,
			...(current.paneId ? { paneId: current.paneId } : {}),
			...(current.agentName ? { agentName: current.agentName } : {}),
			...(current.cwd ? { cwd: current.cwd } : cwd ? { cwd } : {}),
			status,
			updatedAt: new Date(now()).toISOString(),
			...(extra.activeAssignmentId !== undefined
				? { activeAssignmentId: extra.activeAssignmentId }
				: {}),
			...(extra.activeParentEpoch !== undefined
				? { activeParentEpoch: extra.activeParentEpoch }
				: {}),
			...(extra.uncertainWrite !== undefined ? { uncertainWrite: extra.uncertainWrite } : {}),
		});
		return true;
	}

	/** Outside the role lock — acquires the lock. Never call from inside withRoleLock. */
	function markRegistry(
		status: "idle" | "busy" | "blocked" | "unhealthy" | "uncertain",
		extra: RegistryExtra = {},
	): void {
		withRoleLock(poolRoot, role.name, () => {
			markRegistryLocked(status, extra);
		});
	}

	function writeResultDurable(
		assignment: ActiveAssignment,
		partial: Omit<IpcResult, "version" | "runId" | "workerId" | "finishedAt" | "messages"> & {
			messages?: unknown[];
		},
	): boolean {
		disableMutationTools();

		let status = partial.status;
		let uncertainWrite = partial.uncertainWrite === true;
		let errorMessage = partial.errorMessage;

		if (assignment.leaseHeld && !uncertainWrite) {
			try {
				lease.release(cwd, leaseOwner, assignment.leaseToken);
				assignment.leaseHeld = false;
				if (lease.stillHeldBy(cwd, leaseOwner, assignment.leaseToken)) {
					throw new Error("Writer lease still held by this worker after release");
				}
			} catch (error) {
				status = "failed";
				uncertainWrite = true;
				errorMessage = `Writer lease release failed: ${
					error instanceof Error ? error.message : String(error)
				}`;
			}
		}

		const messages = sanitizeAssistantMessages(
			partial.messages ?? assignment.assistantMessages,
			Math.floor(MAX_IPC_JSON_BYTES * 0.75),
		);
		const payload: IpcResult = {
			version: 1,
			runId: assignment.assignmentId,
			workerId,
			status,
			messages,
			finishedAt: new Date(now()).toISOString(),
		};
		if (partial.stopReason !== undefined) payload.stopReason = partial.stopReason;
		if (errorMessage !== undefined) payload.errorMessage = errorMessage;
		if (uncertainWrite) payload.uncertainWrite = true;
		if (partial.usage !== undefined) payload.usage = partial.usage;

		if (uncertainWrite && payload.status === "completed") {
			payload.status = "failed";
		}

		let written = false;
		for (let attempt = 0; attempt < 3; attempt += 1) {
			try {
				atomicWriteJson(assignment.paths.result, payload, MAX_IPC_JSON_BYTES);
				written = true;
				break;
			} catch {
				// retry
			}
		}
		if (!written) return false;
		assignment.resultWritten = true;

		if (uncertainWrite) {
			markRegistry("uncertain", {
				activeAssignmentId: assignment.assignmentId,
				uncertainWrite: true,
			});
			protocolUnhealthy = true;
			return true;
		}

		return true;
	}

	function resetForNextAssignment(): void {
		active = undefined;
		disableMutationTools();
		try {
			if (existsSync(controlPaths.active)) {
				rmSync(controlPaths.active, { force: true });
			}
		} catch {
			// ignore
		}
	}

	/** Fail closed under the role lock: stop advancement; do not touch queue/claiming. */
	function markProtocolCorruptLocked(_reason: string): void {
		disableMutationTools();
		protocolUnhealthy = true;
		const current = pool.getByRole(role.name);
		if (!ownsCurrentGeneration(current)) return;
		pool.upsert({
			...current,
			status: role.canWrite ? "uncertain" : "unhealthy",
			...(role.canWrite ? { uncertainWrite: true } : {}),
			updatedAt: new Date(now()).toISOString(),
		});
	}

	function agreeParentEpochs(
		...values: Array<string | undefined>
	): string {
		const present = values.filter((value): value is string => typeof value === "string" && value.length > 0);
		if (present.length === 0) {
			throw new Error("parentEpoch missing");
		}
		const first = present[0]!;
		for (const value of present) {
			if (value !== first) throw new Error("parentEpoch mismatch");
		}
		return first;
	}

	function clearActivePointerLocked(): void {
		try {
			if (existsSync(controlPaths.active)) {
				rmSync(controlPaths.active, { force: true });
			}
		} catch {
			// ignore
		}
	}

	/**
	 * At-most-once startup reconcile under the role lock.
	 * - `in_flight`: publication recovered / ready to prompt once (no started/result)
	 * - `handled`: terminal or fail-closed; do not drain further
	 * - `idle`: no in-flight assignment; caller may drain queue
	 */
	function reconcileInFlightLocked(
		current: PoolWorkerRecord,
	): "in_flight" | "handled" | "idle" {
		const claiming = listClaiming(poolRoot, role.name);
		for (const entry of claiming) {
			if (entry.generation !== generation || entry.workerId !== workerId) {
				markProtocolCorruptLocked("claiming identity mismatch");
				return "handled";
			}
		}

		let ptr: IpcActivePointer | undefined;
		if (existsSync(controlPaths.active)) {
			try {
				const raw = tryReadIpcJson(controlPaths.active);
				ptr = validateActivePointer(raw, { generation });
			} catch {
				// Malformed/primitive/null/array active must fail closed — never treat as missing.
				markProtocolCorruptLocked("invalid active pointer");
				return "handled";
			}
		}

		const registryBusy = current.status === "busy" || current.status === "blocked";
		if (!ptr && !registryBusy) {
			return "idle";
		}

		const assignmentId = ptr?.assignmentId ?? current.activeAssignmentId;
		if (!assignmentId) {
			markProtocolCorruptLocked("busy without activeAssignmentId");
			return "handled";
		}

		if (ptr && current.activeAssignmentId !== ptr.assignmentId) {
			markProtocolCorruptLocked("active/registry assignment mismatch");
			return "handled";
		}
		if (ptr && !registryBusy) {
			markProtocolCorruptLocked("active pointer without busy registry");
			return "handled";
		}

		const paths = assignmentSpoolPaths(poolRoot, role.name, assignmentId);

		let result: IpcResult | undefined;
		if (existsSync(paths.result)) {
			try {
				const raw = tryReadIpcJson(paths.result);
				result = validateResult(raw, { runId: assignmentId, workerId });
			} catch {
				markProtocolCorruptLocked("invalid result");
				return "handled";
			}
		}

		let command: IpcCommand | undefined;
		try {
			const commandRaw = tryReadIpcJson(paths.command);
			if (!commandRaw) {
				markProtocolCorruptLocked("missing command for in-flight assignment");
				return "handled";
			}
			command = validateCommand(commandRaw, {
				runId: assignmentId,
				workerId,
				generation,
			});
		} catch {
			markProtocolCorruptLocked("invalid command");
			return "handled";
		}

		if (claiming.length > 1) {
			markProtocolCorruptLocked("multiple claiming entries");
			return "handled";
		}
		if (claiming.length === 1 && claiming[0]!.assignmentId !== assignmentId) {
			markProtocolCorruptLocked("claiming/active assignment mismatch");
			return "handled";
		}

		let parentEpoch: string;
		try {
			parentEpoch = agreeParentEpochs(
				current.activeParentEpoch,
				ptr?.parentEpoch,
				command.parentEpoch,
				claiming[0]?.parentEpoch,
			);
		} catch {
			markProtocolCorruptLocked("parentEpoch disagreement");
			return "handled";
		}

		let started: IpcStarted | undefined;
		if (existsSync(paths.started)) {
			try {
				const raw = tryReadIpcJson(paths.started);
				started = validateStarted(raw, {
					runId: assignmentId,
					workerId,
					generation,
					parentEpoch,
				});
			} catch {
				markProtocolCorruptLocked("invalid started marker");
				return "handled";
			}
		}

		// 1) Valid result => never rerun.
		if (result) {
			if (result.uncertainWrite === true) {
				disableMutationTools();
				protocolUnhealthy = true;
				pool.upsert({
					...current,
					status: "uncertain",
					uncertainWrite: true,
					activeAssignmentId: assignmentId,
					activeParentEpoch: parentEpoch,
					updatedAt: new Date(now()).toISOString(),
				});
				return "handled";
			}
			// Shared idempotent A→B (or idle) terminal transition.
			completeCleanAssignmentLocked({
				pool,
				role: role.name,
				workerId,
				generation,
				finishedAssignmentId: assignmentId,
				now,
			});
			return "handled";
		}

		// 2) No result + started marker => never rerun (includes marker-before-send crash).
		if (started) {
			disableMutationTools();
			protocolUnhealthy = true;
			pool.upsert({
				...current,
				status: role.canWrite ? "uncertain" : "unhealthy",
				...(role.canWrite ? { uncertainWrite: true } : {}),
				activeAssignmentId: assignmentId,
				activeParentEpoch: parentEpoch,
				updatedAt: new Date(now()).toISOString(),
			});
			// Retain active/claiming/queue evidence; do not commit or advance.
			return "handled";
		}

		// 2b) Implementer: lease held by this worker before started marker ⇒
		// crash after acquire; token unknowable ⇒ uncertain, never replay.
		if (role.canWrite) {
			try {
				const owner = lease.peekOwner(cwd);
				if (owner && owner.ownerId === workerId) {
					disableMutationTools();
					protocolUnhealthy = true;
					pool.upsert({
						...current,
						status: "uncertain",
						uncertainWrite: true,
						activeAssignmentId: assignmentId,
						activeParentEpoch: parentEpoch,
						updatedAt: new Date(now()).toISOString(),
					});
					return "handled";
				}
				// Foreign owner: leave untouched; safe wait if we later prompt.
			} catch (error) {
				if (error instanceof LeaseCorruptionError) {
					disableMutationTools();
					protocolUnhealthy = true;
					pool.upsert({
						...current,
						status: "uncertain",
						uncertainWrite: true,
						activeAssignmentId: assignmentId,
						activeParentEpoch: parentEpoch,
						updatedAt: new Date(now()).toISOString(),
					});
					return "handled";
				}
				throw error;
			}
		}

		// 3) No result / no started => safe publication recovery only; prompt once.
		if (!ptr && registryBusy) {
			atomicWriteJson(controlPaths.active, {
				version: 1,
				assignmentId,
				generation,
				parentEpoch,
				dispatchedAt: new Date(now()).toISOString(),
			});
			if (claiming.length === 1) {
				commitClaim(poolRoot, role.name, claiming[0]!);
			}
			return "in_flight";
		}

		if (ptr) {
			if (claiming.length === 1) {
				commitClaim(poolRoot, role.name, claiming[0]!);
			}
			return "in_flight";
		}

		markProtocolCorruptLocked("unreachable in-flight state");
		return "handled";
	}

	/**
	 * Must run under the role lock. `afterFinish` clears the completed active
	 * pointer; startup must never delete a parent-dispatched active (race).
	 */
	function claimNextOrIdleLocked(
		mode: "startup" | "after-finish",
		finishedAssignmentId?: string,
	): void {
		if (protocolUnhealthy) return;
		const current = pool.getByRole(role.name);
		// Fence before any queue claim, registry write, or control removal.
		if (!ownsCurrentGeneration(current)) {
			failGenerationFence();
			return;
		}

		if (mode === "startup") {
			const outcome = reconcileInFlightLocked(current);
			if (protocolUnhealthy) return;
			if (outcome === "in_flight" || outcome === "handled") return;

			if (current.status === "starting") {
				// Ready is published; parent owns starting→idle. Recover an
				// incomplete claim only — do not force idle or dequeue while starting.
				const recovering = recoverClaiming(poolRoot, role.name, generation);
				if (!recovering[0]) return;
				publishClaimLocked(recovering[0]);
				return;
			}
		} else {
			if (!finishedAssignmentId) {
				resetForNextAssignment();
			} else {
				// Stale finisher must no-op if registry no longer owns A.
				const outcome = completeCleanAssignmentLocked({
					pool,
					role: role.name,
					workerId,
					generation,
					finishedAssignmentId,
					now,
				});
				if (outcome.kind === "noop_superseded") {
					// B (or another owner) already holds the role — do not touch active.
					active = undefined;
					disableMutationTools();
					return;
				}
				active = undefined;
				disableMutationTools();
				return;
			}
		}

		// Drain queue when idle (or after finish without finished id). Never leave idle-with-queue.
		const claimed = recoverClaiming(poolRoot, role.name, generation);
		const next = claimed[0] ?? beginClaimHead(poolRoot, role.name, generation);
		if (!next) {
			if (!markRegistryLocked("idle")) return;
			resetForNextAssignment();
			return;
		}
		publishClaimLocked(next);
	}

	/**
	 * Durable claim publication under the role lock:
	 * 1) command  2) registry busy  3) active.json  4) commitClaim.
	 * Registry/fence failure must not expose active.json or drop claiming recovery.
	 */
	function publishClaimLocked(next: QueueEntry): boolean {
		if (protocolUnhealthy) return false;
		const current = pool.getByRole(role.name);
		if (!ownsCurrentGeneration(current)) {
			failGenerationFence();
			return false;
		}
		const paths = assignmentSpoolPaths(poolRoot, role.name, next.assignmentId);
		atomicWriteJson(paths.command, {
			version: 1,
			type: "prompt",
			task: next.task,
			issuedAt: new Date(now()).toISOString(),
			runId: next.assignmentId,
			workerId,
			generation,
			parentEpoch: next.parentEpoch,
		});
		if (
			!markRegistryLocked("busy", {
				activeAssignmentId: next.assignmentId,
				activeParentEpoch: next.parentEpoch,
			})
		) {
			// Command may exist; active.json and commitClaim must not run.
			return false;
		}
		atomicWriteJson(controlPaths.active, {
			version: 1,
			assignmentId: next.assignmentId,
			generation,
			parentEpoch: next.parentEpoch,
			dispatchedAt: new Date(now()).toISOString(),
		});
		commitClaim(poolRoot, role.name, next);
		return true;
	}

	function claimNextOrIdle(
		mode: "startup" | "after-finish" = "after-finish",
		finishedAssignmentId?: string,
	): void {
		if (protocolUnhealthy) return;
		withRoleLock(poolRoot, role.name, () => {
			claimNextOrIdleLocked(mode, finishedAssignmentId);
		});
	}

	async function finishAssignment(
		assignment: ActiveAssignment,
		partial: Omit<IpcResult, "version" | "runId" | "workerId" | "finishedAt" | "messages"> & {
			messages?: unknown[];
		},
		eventType: "completed" | "failed" | "aborted",
	): Promise<void> {
		if (assignment.resultWritten) return;
		const ok = writeResultDurable(assignment, partial);
		if (!ok) {
			// The active pointer and claiming entry are recovery evidence. Do
			// not advance the queue or announce a completion that was not durable.
			protocolUnhealthy = true;
			markRegistry(role.canWrite && (assignment.mutationAttempted || assignment.leaseHeld) ? "uncertain" : "unhealthy", {
				activeAssignmentId: assignment.assignmentId,
				...(role.canWrite ? { uncertainWrite: true } : {}),
			});
			return;
		}
		// Durable result is authoritative. Terminal event append is best-effort
		// and must never prevent completeCleanAssignment / queue advancement.
		// Progress emitters rethrow non-capacity faults; capacity is dropped in-place.
		try {
			emitAssignment(assignment, eventType, eventType);
		} catch {
			// ignore event-log I/O failures after durable result
		}
		if (protocolUnhealthy || partial.uncertainWrite) return;
		// Task failure returns idle (or next queued); protocol failure stays unhealthy.
		claimNextOrIdle("after-finish", assignment.assignmentId);
	}

	function beginAssignment(assignmentId: string, parentEpoch?: string): ActiveAssignment {
		const paths = assignmentSpoolPaths(poolRoot, role.name, assignmentId);
		const assignment: ActiveAssignment = {
			assignmentId,
			paths,
			assistantMessages: [],
			eventSeq: 0,
			mutationAttempted: false,
			mutationToolsEnabled: false,
			leaseHeld: false,
			leaseToken: createLeaseToken(),
			resultWritten: false,
			acquiringLease: false,
			...(parentEpoch !== undefined ? { parentEpoch } : {}),
		};
		active = assignment;
		disableMutationTools();
		return assignment;
	}

	function cancelPending(assignment: ActiveAssignment): string | undefined {
		const cancelRaw = tryReadIpcJson(assignment.paths.cancel);
		if (!cancelRaw) return undefined;
		return validateCancel(cancelRaw, {
			runId: assignment.assignmentId,
			workerId,
			generation,
		}).reason || "cancelled";
	}

	pi.on("session_start", async (_event, ctx) => {
		if (ctx.hasUI !== true) return;
		pi.setActiveTools(activeReadOnlyTools());

		// Ready + startup claim under one lock so parent cannot dispatch between
		// ready publish and an active-wiping claimNext.
		withRoleLock(poolRoot, role.name, () => {
			const record = pool.getByRole(role.name);
			if (record && record.generation !== generation && record.generation !== 0) {
				failGenerationFence();
				throw new Error("Worker generation no longer owns this role");
			}
			if (record && record.generation === generation && record.workerId !== workerId) {
				failGenerationFence();
				throw new Error("Worker identity no longer owns this role");
			}
			if (!record || record.generation === 0) {
				const tombstone = record?.generationTombstone ?? 0;
				// Stale gen N must not overwrite an archival tombstone (or any
				// tombstone at/above N). Only a strictly newer generation may adopt.
				if (generation <= tombstone) {
					failGenerationFence();
					throw new Error("Worker generation superseded by archival tombstone");
				}
				pool.upsert({
					workerId,
					generation,
					generationTombstone: Math.max(tombstone, generation),
					role: role.name,
					status: "idle",
					updatedAt: new Date(now()).toISOString(),
					...(cwd ? { cwd } : {}),
				});
			}
			atomicWriteJson(controlPaths.ready, {
				version: 1,
				runId,
				workerId,
				readyAt: new Date(now()).toISOString(),
			});
			claimNextOrIdleLocked("startup");
		});

		heartbeatTimer = setInterval(writeControlHeartbeat, 3_000);
		heartbeatTimer.unref?.();
		writeControlHeartbeat();

		pollTimer = setInterval(() => {
			void pollActive(ctx);
		}, 100);
		pollTimer.unref?.();
	});

	async function pollActive(ctx: ExtensionContext): Promise<void> {
		if (protocolUnhealthy) return;
		try {
			// Discover dispatched assignment via control active pointer.
			if (!active) {
				if (!existsSync(controlPaths.active)) return;
				let pointer: IpcActivePointer;
				try {
					const activeRaw = tryReadIpcJson(controlPaths.active);
					pointer = validateActivePointer(activeRaw, { generation });
				} catch (error) {
					protocolUnhealthy = true;
					markRegistry(role.canWrite ? "uncertain" : "unhealthy", {
						...(role.canWrite ? { uncertainWrite: true } : {}),
					});
					void error;
					return;
				}
				const assignment = beginAssignment(pointer.assignmentId, pointer.parentEpoch);
				await runAssignment(ctx, assignment);
				return;
			}

			if (active.resultWritten || active.acquiringLease) return;
			const cancelReason = cancelPending(active);
			if (cancelReason) {
				active.cancelRequested = cancelReason;
				ctx.abort();
			}
		} catch (error) {
			const message =
				error instanceof IpcValidationError || error instanceof Error
					? error.message
					: String(error);
			protocolUnhealthy = true;
			if (heartbeatTimer) clearInterval(heartbeatTimer);
			if (pollTimer) clearInterval(pollTimer);

			if (active && !active.resultWritten) {
				// Do not preemptively mark unhealthy: ambiguous writer state must
				// remain/become uncertain so leases and active evidence stay recoverable.
				const ambiguous =
					role.canWrite && (active.mutationAttempted || active.leaseHeld);
				const persisted = writeResultDurable(active, {
					status: "failed",
					messages: active.assistantMessages,
					errorMessage: message,
					uncertainWrite: ambiguous,
				});
				try {
					emitAssignment(active, "failed", message);
				} catch {
					// Terminal event append is best-effort after the durable attempt.
				}
				if (!persisted) {
					markRegistry(ambiguous ? "uncertain" : "unhealthy", {
						...(ambiguous
							? {
									activeAssignmentId: active.assignmentId,
									...(active.parentEpoch
										? { activeParentEpoch: active.parentEpoch }
										: {}),
									uncertainWrite: true,
								}
							: {}),
					});
				} else if (!ambiguous) {
					markRegistry("unhealthy");
				}
				// persisted + ambiguous: writeResultDurable already marked uncertain.
			} else {
				markRegistry("unhealthy");
			}
		}
	}

	async function runAssignment(ctx: ExtensionContext, assignment: ActiveAssignment): Promise<void> {
		try {
			const cancelReason = cancelPending(assignment);
			if (cancelReason) {
				await finishAssignment(
					assignment,
					{
						status: "aborted",
						messages: [],
						errorMessage: cancelReason,
						uncertainWrite: false,
					},
					"aborted",
				);
				return;
			}

			const commandRaw = tryReadIpcJson(assignment.paths.command);
			if (!commandRaw) {
				// Active pointer without command yet — wait.
				active = undefined;
				return;
			}
			const command = validateCommand(commandRaw, {
				runId: assignment.assignmentId,
				workerId,
				generation,
				...(assignment.parentEpoch ? { parentEpoch: assignment.parentEpoch } : {}),
			});
			if (command.type === "skip" || command.type === "cancel") {
				await finishAssignment(
					assignment,
					{
						status: "aborted",
						messages: [],
						errorMessage: command.reason,
						uncertainWrite: false,
					},
					"aborted",
				);
				return;
			}

			const parentEpoch = command.parentEpoch ?? assignment.parentEpoch;
			if (!parentEpoch) {
				throw new Error("Persistent assignment command missing parentEpoch");
			}
			assignment.parentEpoch = parentEpoch;

			markRegistry("busy", {
				activeAssignmentId: assignment.assignmentId,
				activeParentEpoch: parentEpoch,
			});

			if (role.canWrite) {
				assignment.acquiringLease = true;
				pi.setActiveTools(activeReadOnlyTools());
				emitAssignment(assignment, "state", "waiting_for_writer_lease", {
					state: "waiting_for_writer_lease",
				});
				try {
					await lease.waitAcquire(cwd, leaseOwner, assignment.leaseToken, {
						deadlineMs: leaseWaitMs,
						now,
						sleep,
						shouldCancel: async () => cancelPending(assignment) !== undefined,
						onWaiting: (info) => {
							emitAssignment(
								assignment,
								"state",
								`waiting_for_writer_lease${info.holder ? ` holder=${info.holder}` : ""}`,
								{ state: "waiting_for_writer_lease" },
							);
						},
					});
					assignment.leaseHeld = true;
					enableMutationTools(assignment);
				} catch (error) {
					if (error instanceof LeaseWaitCancelledError) {
						const reason = cancelPending(assignment) || "cancelled_while_waiting_for_lease";
						await finishAssignment(
							assignment,
							{
								status: "aborted",
								messages: assignment.assistantMessages,
								errorMessage: reason,
								uncertainWrite: false,
							},
							"aborted",
						);
						return;
					}
					if (error instanceof LeaseWaitTimeoutError) {
						await finishAssignment(
							assignment,
							{
								status: "failed",
								messages: assignment.assistantMessages,
								errorMessage: error.message,
								uncertainWrite: false,
							},
							"failed",
						);
						return;
					}
					throw error;
				} finally {
					assignment.acquiringLease = false;
				}
			}

			emitAssignment(assignment, "started", `${role.name} started`);
			if (command.type !== "prompt") {
				throw new Error("Expected prompt command after skip/cancel filter");
			}

			// At-most-once: if started marker already exists, never re-prompt.
			if (existsSync(assignment.paths.started)) {
				throw new Error("Assignment already started; refusing duplicate prompt");
			}
			// Durable start fence immediately BEFORE sendUserMessage.
			atomicWriteJson(assignment.paths.started, {
				version: 1,
				runId: assignment.assignmentId,
				workerId,
				generation,
				parentEpoch,
				startedAt: new Date(now()).toISOString(),
			});
			pi.sendUserMessage(command.task);
		} catch (error) {
			const message =
				error instanceof IpcValidationError || error instanceof Error
					? error.message
					: String(error);
			protocolUnhealthy = true;
			markRegistry("unhealthy");
			await finishAssignment(
				assignment,
				{
					status: "failed",
					messages: assignment.assistantMessages,
					errorMessage: message,
					uncertainWrite: role.canWrite && assignment.mutationAttempted,
				},
				"failed",
			);
		}
	}

	// Interactive / RPC input always blocked; extension-injected assignment prompts continue.
	pi.on("input", async (event, ctx) => {
		if (event.source === "extension") {
			return { action: "continue" as const };
		}
		ctx.ui?.notify?.(
			"Momo persistent worker accepts only assigned tasks; interactive input is disabled.",
		);
		return { action: "handled" as const };
	});

	pi.on("before_agent_start", async () => ({
		systemPrompt: ROLE_PROMPTS[role.name],
	}));

	// Pi documents context filtering as returning the desired transcript. Keep
	// the task user message and every subsequent model/tool message.
	pi.on("context", async (event) => {
		let start = -1;
		for (let i = 0; i < event.messages.length; i += 1) {
			const msg = event.messages[i] as { role?: string };
			if (msg?.role === "user") start = i;
		}
		if (start < 0) {
			return { messages: [] };
		}
		return { messages: event.messages.slice(start) };
	});

	pi.on("message_update", (event) => {
		if (!active || active.resultWritten) return;
		const delta = (event as { assistantMessageEvent?: { type?: string; delta?: string } })
			.assistantMessageEvent;
		if (delta?.type === "text_delta" && delta.delta) {
			emitAssignment(active, "text", String(delta.delta));
		}
	});

	pi.on("message_end", (event) => {
		if (!active || active.resultWritten) return;
		const message = (event as { message?: { role?: string } }).message;
		if (message?.role === "assistant") {
			active.assistantMessages.push(message);
		}
	});

	pi.on("tool_call", async (event) => {
		const toolName = String((event as { toolName?: string }).toolName ?? "");
		if (toolName === "delegate") {
			return { block: true, reason: "Workers cannot use delegate" };
		}
		if (MUTATION_TOOLS.has(toolName)) {
			if (
				!active ||
				!role.canWrite ||
				!active.mutationToolsEnabled ||
				!lease.validate(cwd, leaseOwner, active.leaseToken)
			) {
				return { block: true, reason: "Mutation tools require a valid writer lease" };
			}
			active.mutationAttempted = true;
		}
		if (active && !active.resultWritten) {
			emitAssignment(active, "tool_started", `Running ${toolName}`, { toolName });
		}
		return undefined;
	});

	pi.on("tool_result", (event) => {
		if (!active || active.resultWritten) return;
		const toolName = String((event as { toolName?: string }).toolName ?? "tool");
		emitAssignment(active, "tool_finished", `${toolName} finished`, {
			toolName,
			state: (event as { isError?: boolean }).isError ? "failed" : "ok",
		});
	});

	pi.on("user_bash", async () => ({
		result: {
			output: "Direct user_bash is disabled in Momo workers",
			exitCode: 126,
			cancelled: false,
			truncated: false,
		},
	}));

	pi.on("agent_settled", async (_event, ctx) => {
		if (!active || active.resultWritten) return;
		if (ctx.isIdle() !== true) return;
		const assignment = active;

		const last = assignment.assistantMessages[assignment.assistantMessages.length - 1] as
			| { stopReason?: string; errorMessage?: string }
			| undefined;
		const stopReason = last?.stopReason;
		const allowedStop =
			typeof stopReason === "string" &&
			["stop", "length", "toolUse", "error", "aborted", "cancelled"].includes(stopReason)
				? stopReason
				: undefined;
		if (assignment.cancelRequested || stopReason === "aborted" || stopReason === "cancelled") {
			await finishAssignment(
				assignment,
				{
					status: "aborted",
					messages: assignment.assistantMessages,
					stopReason: "aborted",
					errorMessage: assignment.cancelRequested ?? last?.errorMessage ?? "aborted",
					uncertainWrite: role.canWrite && assignment.mutationAttempted,
				},
				"aborted",
			);
			return;
		}
		if (stopReason === "error") {
			await finishAssignment(
				assignment,
				{
					status: "failed",
					messages: assignment.assistantMessages,
					stopReason: "error",
					errorMessage: last?.errorMessage ?? "error",
					uncertainWrite: role.canWrite && assignment.mutationAttempted,
				},
				"failed",
			);
			return;
		}
		await finishAssignment(
			assignment,
			{
				status: "completed",
				messages: assignment.assistantMessages,
				...(allowedStop !== undefined ? { stopReason: allowedStop } : {}),
			},
			"completed",
		);
	});
}

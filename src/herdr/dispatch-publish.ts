import { existsSync, rmSync } from "node:fs";
import type { AgentName } from "../roles.js";
import { atomicWriteJson } from "../ipc/spool.js";
import { tryReadIpcJson, validateActivePointer } from "../ipc/validate.js";
import type { IpcModelPolicy } from "../ipc/spool.js";
import {
	assignmentSpoolPaths,
	workerControlPaths,
} from "./assignment-spool.js";
import type { PoolRegistry, PoolWorkerRecord } from "./pool-registry.js";

/** @internal test-only */
let activeAtomicWriteForTest: typeof atomicWriteJson | undefined;

/** @internal test-only */
export function __setActiveAtomicWriteForTest(
	impl?: typeof atomicWriteJson,
): void {
	activeAtomicWriteForTest = impl;
}

/** @internal test-only */
export function __resetActiveAtomicWriteForTest(): void {
	activeAtomicWriteForTest = undefined;
}

function writeActivePointer(controlActivePath: string, payload: Record<string, unknown>): void {
	const write = activeAtomicWriteForTest ?? atomicWriteJson;
	write(controlActivePath, payload);
}

export type PublishAssignmentCommandOutcome =
	| { ok: true }
	| { ok: false; reason: "rolled_back" | "fenced" };

export interface PublishAssignmentCommandInput {
	pool: PoolRegistry;
	poolRoot: string;
	role: AgentName;
	workerId: string;
	generation: number;
	assignmentId: string;
	parentEpoch: string;
	task: string;
	now: () => number;
	/** Exact registry row to restore on active-write rollback (caller-owned shape). */
	rollbackRecord: PoolWorkerRecord;
	commandExtras?: {
		capability?: number;
		modelPolicy?: IpcModelPolicy;
	};
	/** Called after durable active.json when publication fully commits (e.g. commitClaim). */
	onCommitted?: () => void;
}

function validateRollbackRecord(
	current: PoolWorkerRecord,
	rollbackRecord: PoolWorkerRecord,
	role: AgentName,
	workerId: string,
	generation: number,
): void {
	if (
		rollbackRecord.role !== role ||
		rollbackRecord.workerId !== workerId ||
		rollbackRecord.generation !== generation ||
		rollbackRecord.role !== current.role ||
		rollbackRecord.workerId !== current.workerId ||
		rollbackRecord.generation !== current.generation
	) {
		throw new Error("publishAssignmentCommandLocked rollbackRecord fence mismatch");
	}
}

function fenceDispatchPublishFailure(
	pool: PoolRegistry,
	record: PoolWorkerRecord,
	assignmentId: string,
	parentEpoch: string,
): void {
	pool.upsert({
		...record,
		status: record.role === "implementer" ? "uncertain" : "unhealthy",
		activeAssignmentId: assignmentId,
		activeParentEpoch: parentEpoch,
		...(record.role === "implementer" ? { uncertainWrite: true } : {}),
		updatedAt: new Date().toISOString(),
	});
}

/**
 * Durable dispatch/claim publication under the role lock:
 * 1) command.json  2) registry busy  3) active.json (commit point).
 * Active write failure re-reads for crash-after-commit; otherwise rolls back
 * command + busy registry to the caller-supplied rollbackRecord without advancing
 * queue. Rollback failure fences worker.
 */
export function publishAssignmentCommandLocked(
	input: PublishAssignmentCommandInput,
): PublishAssignmentCommandOutcome {
	const {
		pool,
		poolRoot,
		role,
		workerId,
		generation,
		assignmentId,
		parentEpoch,
		task,
		now,
		rollbackRecord,
		commandExtras,
		onCommitted,
	} = input;

	const current = pool.getByRole(role);
	if (
		!current ||
		current.generation !== generation ||
		current.workerId !== workerId
	) {
		throw new Error("publishAssignmentCommandLocked generation/worker fence mismatch");
	}
	validateRollbackRecord(current, rollbackRecord, role, workerId, generation);

	const paths = assignmentSpoolPaths(poolRoot, role, assignmentId);
	const control = workerControlPaths(poolRoot, role);

	atomicWriteJson(paths.command, {
		version: 1,
		type: "prompt",
		task,
		issuedAt: new Date(now()).toISOString(),
		runId: assignmentId,
		workerId,
		generation,
		parentEpoch,
		...(commandExtras?.capability !== undefined ? { capability: commandExtras.capability } : {}),
		...(commandExtras?.modelPolicy !== undefined ? { modelPolicy: commandExtras.modelPolicy } : {}),
	});

	const {
		provisioningOwnerId: _provisioningOwnerId,
		provisioningHeartbeatAt: _provisioningHeartbeatAt,
		...currentWithoutProvisioning
	} = current;
	const busyRecord: PoolWorkerRecord = {
		...currentWithoutProvisioning,
		status: "busy",
		activeAssignmentId: assignmentId,
		activeParentEpoch: parentEpoch,
		updatedAt: new Date(now()).toISOString(),
	};
	pool.upsert(busyRecord);

	let activeWriteError: unknown;
	try {
		writeActivePointer(control.active, {
			version: 1,
			assignmentId,
			generation,
			parentEpoch,
			dispatchedAt: new Date(now()).toISOString(),
		});
	} catch (error) {
		activeWriteError = error;
	}

	if (!activeWriteError) {
		onCommitted?.();
		return { ok: true };
	}

	try {
		const activeRaw = tryReadIpcJson(control.active);
		validateActivePointer(activeRaw, { generation, assignmentId });
		onCommitted?.();
		return { ok: true };
	} catch {
		// absent or invalid — roll back
	}

	try {
		if (existsSync(paths.command)) {
			rmSync(paths.command, { force: true });
		}
	} catch {
		fenceDispatchPublishFailure(pool, busyRecord, assignmentId, parentEpoch);
		return { ok: false, reason: "fenced" };
	}

	try {
		pool.upsert(rollbackRecord);
	} catch {
		fenceDispatchPublishFailure(pool, busyRecord, assignmentId, parentEpoch);
		return { ok: false, reason: "fenced" };
	}

	return { ok: false, reason: "rolled_back" };
}

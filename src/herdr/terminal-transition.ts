import { existsSync, rmSync } from "node:fs";
import type { AgentName } from "../roles.js";
import { assertIpcPoliciesEqualRecords, tryReadIpcJson, validateActivePointer } from "../ipc/validate.js";
import { workerControlPaths } from "./assignment-spool.js";
import type { PoolRegistry, PoolWorkerRecord } from "./pool-registry.js";
import {
	beginClaimHead,
	commitClaim,
	listClaiming,
	recoverClaiming,
	type QueueEntry,
} from "./role-queue.js";
import { publishAssignmentCommandLocked } from "./dispatch-publish.js";

export type CleanTerminalOutcome =
	| { kind: "advanced"; nextAssignmentId: string }
	| { kind: "idle" }
	| { kind: "noop_superseded" }
	| { kind: "retry_pending"; nextAssignmentId: string }
	| { kind: "publish_fenced"; nextAssignmentId: string };

function idleRollbackRecord(still: PoolWorkerRecord, now: () => number): PoolWorkerRecord {
	return {
		workerId: still.workerId,
		generation: still.generation,
		generationTombstone: Math.max(still.generationTombstone, still.generation),
		role: still.role,
		status: "idle",
		updatedAt: new Date(now()).toISOString(),
		...(still.paneId ? { paneId: still.paneId } : {}),
		...(still.agentName ? { agentName: still.agentName } : {}),
		...(still.cwd ? { cwd: still.cwd } : {}),
		...(still.boundPolicy ? { boundPolicy: still.boundPolicy } : {}),
	};
}

/**
 * Idempotent clean-result terminal transition under the per-role lock.
 *
 * Requires exact generation + worker. If registry already names a different
 * active assignment B, no-ops without touching B. Commits only A's claiming
 * entry; clears control active only if the pointer still names A; then
 * atomically publishes FIFO B (command → registry busy → active → commit)
 * or idle if no B.
 *
 * Idle registry (cancel-before-busy) still clears A when the pointer names A.
 *
 * Stale finishers (registry already advanced to B) no-op without touching B.
 */
export function completeCleanAssignmentLocked(options: {
	pool: PoolRegistry;
	role: AgentName;
	workerId: string;
	generation: number;
	finishedAssignmentId: string;
	now?: () => number;
}): CleanTerminalOutcome {
	const now = options.now ?? Date.now;
	const { pool, role, workerId, generation, finishedAssignmentId } = options;
	const current = pool.getByRole(role);
	if (
		!current ||
		current.generation !== generation ||
		current.workerId !== workerId
	) {
		return { kind: "noop_superseded" };
	}
	// Stale finisher: registry already advanced to a different assignment B.
	// Idle / missing activeAssignmentId is OK (cancel-before-busy) — still clear A.
	if (
		current.activeAssignmentId !== undefined &&
		current.activeAssignmentId !== finishedAssignmentId
	) {
		return { kind: "noop_superseded" };
	}

	// Commit only A's leftover claiming entry.
	const leftover = listClaiming(pool.poolRoot, role).find(
		(entry) =>
			entry.assignmentId === finishedAssignmentId &&
			entry.generation === generation &&
			entry.workerId === workerId,
	);
	if (leftover) {
		commitClaim(pool.poolRoot, role, leftover);
	}

	// Clear control active only if the strict pointer still names A.
	const control = workerControlPaths(pool.poolRoot, role);
	if (existsSync(control.active)) {
		try {
			const raw = tryReadIpcJson(control.active);
			const ptr = validateActivePointer(raw, { generation });
			if (ptr.assignmentId === finishedAssignmentId) {
				rmSync(control.active, { force: true });
			}
		} catch {
			// Invalid active while finishing A — remove only if registry does not
			// already name a successor.
			const latest = pool.getByRole(role);
			if (
				!latest?.activeAssignmentId ||
				latest.activeAssignmentId === finishedAssignmentId
			) {
				try {
					rmSync(control.active, { force: true });
				} catch {
					// ignore
				}
			}
		}
	}

	// Re-fence before publishing successor (another writer may have raced).
	const still = pool.getByRole(role);
	if (
		!still ||
		still.generation !== generation ||
		still.workerId !== workerId ||
		(still.activeAssignmentId !== undefined &&
			still.activeAssignmentId !== finishedAssignmentId)
	) {
		return { kind: "noop_superseded" };
	}

	const claimed = recoverClaiming(pool.poolRoot, role, generation);
	const next = claimed[0] ?? beginClaimHead(pool.poolRoot, role, generation);
	if (!next) {
		pool.upsert(idleRollbackRecord(still, now));
		return { kind: "idle" };
	}

	const rollbackRecord = idleRollbackRecord(still, now);
	const outcome = publishNextAssignmentLocked(
		pool,
		role,
		workerId,
		generation,
		next,
		rollbackRecord,
		now,
	);
	if (!outcome.ok) {
		if (outcome.reason === "rolled_back") {
			return { kind: "retry_pending", nextAssignmentId: next.assignmentId };
		}
		return { kind: "publish_fenced", nextAssignmentId: next.assignmentId };
	}
	return { kind: "advanced", nextAssignmentId: next.assignmentId };
}

/**
 * Durable next-assignment publication under the role lock:
 * 1) command  2) registry busy  3) active.json  4) commitClaim.
 */
function publishNextAssignmentLocked(
	pool: PoolRegistry,
	role: AgentName,
	workerId: string,
	generation: number,
	next: QueueEntry,
	rollbackRecord: PoolWorkerRecord,
	now: () => number,
): { ok: true } | { ok: false; reason: "rolled_back" | "fenced" } {
	if (next.capability !== undefined && next.modelPolicy !== undefined) {
		const record = pool.getByRole(role);
		if (!record?.boundPolicy) {
			throw new Error("publishNextAssignmentLocked missing registry boundPolicy for v3 queue entry");
		}
		assertIpcPoliciesEqualRecords(next.modelPolicy, record.boundPolicy, "queue→registry");
	}
	return publishAssignmentCommandLocked({
		pool,
		poolRoot: pool.poolRoot,
		role,
		workerId,
		generation,
		assignmentId: next.assignmentId,
		parentEpoch: next.parentEpoch,
		task: next.task,
		now,
		rollbackRecord,
		...(next.capability !== undefined || next.modelPolicy !== undefined
			? {
					commandExtras: {
						...(next.capability !== undefined ? { capability: next.capability } : {}),
						...(next.modelPolicy !== undefined ? { modelPolicy: next.modelPolicy } : {}),
					},
				}
			: {}),
		onCommitted: () => commitClaim(pool.poolRoot, role, next),
	});
}

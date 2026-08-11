import { existsSync, rmSync } from "node:fs";
import type { AgentName } from "../roles.js";
import { atomicWriteJson } from "../ipc/spool.js";
import { assertIpcPoliciesEqualRecords, tryReadIpcJson, validateActivePointer } from "../ipc/validate.js";
import {
	assignmentSpoolPaths,
	workerControlPaths,
} from "./assignment-spool.js";
import type { PoolRegistry } from "./pool-registry.js";
import {
	beginClaimHead,
	commitClaim,
	listClaiming,
	recoverClaiming,
	type QueueEntry,
} from "./role-queue.js";

export type CleanTerminalOutcome =
	| { kind: "advanced"; nextAssignmentId: string }
	| { kind: "idle" }
	| { kind: "noop_superseded" };

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
		pool.upsert({
			workerId,
			generation,
			generationTombstone: Math.max(still.generationTombstone, generation),
			role,
			status: "idle",
			updatedAt: new Date(now()).toISOString(),
			...(still.paneId ? { paneId: still.paneId } : {}),
			...(still.agentName ? { agentName: still.agentName } : {}),
			...(still.cwd ? { cwd: still.cwd } : {}),
			...(still.boundPolicy ? { boundPolicy: still.boundPolicy } : {}),
		});
		return { kind: "idle" };
	}

	publishNextAssignmentLocked(pool, role, workerId, generation, next, now);
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
	now: () => number,
): void {
	const current = pool.getByRole(role);
	if (
		!current ||
		current.generation !== generation ||
		current.workerId !== workerId
	) {
		throw new Error("publishNextAssignmentLocked generation/worker fence mismatch");
	}
	const paths = assignmentSpoolPaths(pool.poolRoot, role, next.assignmentId);
	const ipcFields = {
		...(next.capability !== undefined ? { capability: next.capability } : {}),
		...(next.modelPolicy !== undefined ? { modelPolicy: next.modelPolicy } : {}),
	};
	if (next.capability !== undefined && next.modelPolicy !== undefined) {
		const record = pool.getByRole(role);
		if (!record?.boundPolicy) {
			throw new Error("publishNextAssignmentLocked missing registry boundPolicy for v3 queue entry");
		}
		assertIpcPoliciesEqualRecords(next.modelPolicy, record.boundPolicy, "queue→registry");
	}
	atomicWriteJson(paths.command, {
		version: 1,
		type: "prompt",
		task: next.task,
		issuedAt: new Date(now()).toISOString(),
		runId: next.assignmentId,
		workerId,
		generation,
		parentEpoch: next.parentEpoch,
		...ipcFields,
	});
	pool.upsert({
		...current,
		status: "busy",
		activeAssignmentId: next.assignmentId,
		activeParentEpoch: next.parentEpoch,
		updatedAt: new Date(now()).toISOString(),
	});
	const control = workerControlPaths(pool.poolRoot, role);
	atomicWriteJson(control.active, {
		version: 1,
		assignmentId: next.assignmentId,
		generation,
		parentEpoch: next.parentEpoch,
		dispatchedAt: new Date(now()).toISOString(),
	});
	commitClaim(pool.poolRoot, role, next);
}

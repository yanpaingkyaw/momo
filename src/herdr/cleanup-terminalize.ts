/**
 * Before archiving a role worker, terminalize every assignment that would
 * otherwise survive into generation N+1 (queue, claiming, and active).
 */
import type { AgentName } from "../roles.js";
import { atomicWriteJson, type IpcResult } from "../ipc/spool.js";
import { tryReadIpcJson, validateResult } from "../ipc/validate.js";
import { ensureAssignmentSpool } from "./assignment-spool.js";
import type { PoolRegistry, PoolWorkerRecord } from "./pool-registry.js";
import {
	cancelQueuedAssignment,
	commitClaim,
	listClaiming,
	listQueue,
} from "./role-queue.js";

export type TerminalizeCleanupResult =
	| { ok: true; terminalized: number }
	| { ok: false; reason: string };

/**
 * Must run under the per-role lock. Writes identity-valid failed results for
 * every queued/claiming/active assignment that lacks a valid durable result,
 * then removes queue + claiming entries. Refuses when a terminal result cannot
 * be made durable/validated — caller must retain registry state.
 */
export function terminalizeAndClearRoleAssignmentsLocked(options: {
	pool: PoolRegistry;
	role: AgentName;
	worker: PoolWorkerRecord;
	reason: string;
	now?: () => number;
}): TerminalizeCleanupResult {
	const now = options.now ?? Date.now;
	const { pool, role, worker, reason } = options;
	const queued = listQueue(pool.poolRoot, role);
	const claiming = listClaiming(pool.poolRoot, role);
	const seen = new Set<string>();
	const targets: Array<{ assignmentId: string; workerId: string; isActive: boolean }> = [];

	for (const entry of [...queued, ...claiming]) {
		if (seen.has(entry.assignmentId)) continue;
		seen.add(entry.assignmentId);
		targets.push({
			assignmentId: entry.assignmentId,
			workerId: entry.workerId,
			isActive: worker.activeAssignmentId === entry.assignmentId,
		});
	}

	if (worker.activeAssignmentId && !seen.has(worker.activeAssignmentId)) {
		targets.push({
			assignmentId: worker.activeAssignmentId,
			workerId: worker.workerId,
			isActive: true,
		});
	}

	let terminalized = 0;
	for (const target of targets) {
		const paths = ensureAssignmentSpool(pool.poolRoot, role, target.assignmentId);
		let existingRaw: unknown;
		try {
			existingRaw = tryReadIpcJson(paths.result);
		} catch (error) {
			return {
				ok: false,
				reason: `unreadable existing result for ${target.assignmentId}: ${
					error instanceof Error ? error.message : String(error)
				}`,
			};
		}
		if (existingRaw !== undefined) {
			try {
				validateResult(existingRaw, {
					runId: target.assignmentId,
					workerId: target.workerId,
				});
				// Preserve existing durable evidence (including uncertainWrite).
				continue;
			} catch (error) {
				return {
					ok: false,
					reason: `invalid existing result for ${target.assignmentId}: ${
						error instanceof Error ? error.message : String(error)
					}`,
				};
			}
		}

		const uncertainActiveImplementer =
			target.isActive &&
			role === "implementer" &&
			(worker.status === "uncertain" || worker.uncertainWrite === true);

		const payload: IpcResult = {
			version: 1,
			runId: target.assignmentId,
			workerId: target.workerId,
			status: "failed",
			messages: [],
			errorMessage: reason.slice(0, 1024),
			finishedAt: new Date(now()).toISOString(),
			...(uncertainActiveImplementer ? { uncertainWrite: true } : {}),
		};

		try {
			atomicWriteJson(paths.result, payload);
			const written = tryReadIpcJson(paths.result);
			if (written === undefined) {
				return {
					ok: false,
					reason: `failed to persist terminal result for ${target.assignmentId}`,
				};
			}
			validateResult(written, {
				runId: target.assignmentId,
				workerId: target.workerId,
			});
			terminalized += 1;
		} catch (error) {
			return {
				ok: false,
				reason: `could not terminalize ${target.assignmentId}: ${
					error instanceof Error ? error.message : String(error)
				}`,
			};
		}
	}

	for (const entry of listQueue(pool.poolRoot, role)) {
		cancelQueuedAssignment(pool.poolRoot, role, entry.assignmentId);
	}
	for (const entry of listClaiming(pool.poolRoot, role)) {
		commitClaim(pool.poolRoot, role, entry);
	}

	return { ok: true, terminalized };
}

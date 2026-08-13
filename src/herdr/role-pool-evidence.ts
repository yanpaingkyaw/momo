import { existsSync, readdirSync } from "node:fs";
import path from "node:path";
import type { AgentName } from "../roles.js";
import { workerControlPaths } from "./assignment-spool.js";
import { roleHasOrphanPaneEvidence } from "./orphan-panes.js";
import { rolePoolRoot } from "./pool-identity.js";
import { listClaiming, listQueue } from "./role-queue.js";

export class RolePoolEvidenceError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "RolePoolEvidenceError";
	}
}

function roleAssignmentsDir(poolRoot: string, role: AgentName): string {
	return path.join(rolePoolRoot(poolRoot, role), "assignments");
}

function hasAssignmentEvidence(
	poolRoot: string,
	role: AgentName,
	ignoreAssignmentIds?: ReadonlySet<string>,
): boolean {
	const dir = roleAssignmentsDir(poolRoot, role);
	if (!existsSync(dir)) return false;
	try {
		const names = readdirSync(dir).filter((name) => !ignoreAssignmentIds?.has(name));
		return names.length > 0;
	} catch {
		return true;
	}
}

/** True when durable role-pool evidence blocks pristine provisioning. */
export function roleHasProvisionBlockingEvidence(
	poolRoot: string,
	role: AgentName,
	options: { ignoreAssignmentIds?: ReadonlySet<string> } = {},
): boolean {
	const control = workerControlPaths(poolRoot, role);
	if (
		existsSync(control.manifest) ||
		existsSync(control.ready) ||
		existsSync(control.heartbeat) ||
		existsSync(control.active)
	) {
		return true;
	}
	if (listQueue(poolRoot, role).length > 0) return true;
	if (listClaiming(poolRoot, role).length > 0) return true;
	if (hasAssignmentEvidence(poolRoot, role, options.ignoreAssignmentIds)) return true;
	if (roleHasOrphanPaneEvidence(poolRoot, role)) return true;
	return false;
}

/** Fail closed when absent registry would provision over durable evidence. */
export function assertRolePoolPristineForProvision(
	poolRoot: string,
	role: AgentName,
	options: { ignoreAssignmentIds?: ReadonlySet<string> } = {},
): void {
	if (!roleHasProvisionBlockingEvidence(poolRoot, role, options)) return;
	throw new RolePoolEvidenceError(
		`Role ${role} pool has durable evidence; run /momo-cleanup before provisioning a new worker`,
	);
}

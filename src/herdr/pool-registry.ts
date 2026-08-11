import { existsSync, readdirSync, rmSync } from "node:fs";
import path from "node:path";
import { isAgentName, ROLE_LIST, type AgentName } from "../roles.js";
import {
	atomicWriteJson,
	DEFAULT_HEARTBEAT_CLOCK_SKEW_MS,
	ensurePrivateDir,
	momoCacheRoot,
} from "../ipc/spool.js";
import { parseJsonFile, assertExactKeys } from "../ipc/validate.js";
import { validateModelPolicySnapshot } from "../config/model-policy.js";
import type { IpcModelPolicy } from "../ipc/spool.js";
import {
	POOL_REGISTRY_VERSION,
	poolRootPath,
	rolePoolRoot,
} from "./pool-identity.js";
import { queueCount } from "./role-queue.js";
import { workerControlPaths } from "./assignment-spool.js";

export type PoolWorkerState =
	| "starting"
	| "idle"
	| "busy"
	| "blocked"
	| "unhealthy"
	| "uncertain";

const STATES = new Set<PoolWorkerState>([
	"starting",
	"idle",
	"busy",
	"blocked",
	"unhealthy",
	"uncertain",
]);

export interface PoolWorkerRecord {
	workerId: string;
	generation: number;
	/** Monotonic high-water mark retained across cleanup. */
	generationTombstone: number;
	role: AgentName;
	paneId?: string;
	agentName?: string;
	status: PoolWorkerState;
	activeAssignmentId?: string;
	activeParentEpoch?: string;
	uncertainWrite?: boolean;
	paneClosed?: boolean;
	recoveryRequired?: boolean;
	cwd?: string;
	/**
	 * Unique token for the assignment proxy that owns an in-flight `starting`
	 * reservation. Cleared when leaving starting.
	 */
	provisioningOwnerId?: string;
	/** ISO timestamp of the latest generation-fenced provisioning owner heartbeat. */
	provisioningHeartbeatAt?: string;
	/** Generation-bound concrete model policy (v3). Omitted on legacy v2 rows. */
	boundPolicy?: import("../ipc/spool.js").IpcModelPolicy;
	updatedAt: string;
}

export interface PoolRegistrySnapshot {
	version: typeof POOL_REGISTRY_VERSION;
	poolKey: string;
	workers: PoolWorkerRecord[];
}

export class PoolRegistryCorruptionError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "PoolRegistryCorruptionError";
	}
}

function requireNonEmptyString(value: unknown, label: string, max = 4096): string {
	if (typeof value !== "string" || value.length === 0 || value.length > max || value.includes("\0")) {
		throw new PoolRegistryCorruptionError(`Pool registry ${label} invalid`);
	}
	return value;
}

function parseRegistryBoundPolicy(value: unknown, label: string): IpcModelPolicy {
	const snapshot = validateModelPolicySnapshot(value, label);
	return {
		provider: snapshot.provider,
		model: snapshot.model,
		reasoning: snapshot.reasoning,
	};
}

export function validatePoolWorkerRecord(value: unknown, label = "worker"): PoolWorkerRecord {
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		throw new PoolRegistryCorruptionError(`Pool registry ${label} must be an object`);
	}
	const record = value as Record<string, unknown>;
	const optionalRegistryKeys = [
		"generationTombstone",
		"paneId",
		"agentName",
		"activeAssignmentId",
		"activeParentEpoch",
		"uncertainWrite",
		"paneClosed",
		"recoveryRequired",
		"cwd",
		"boundPolicy",
		"provisioningOwnerId",
		"provisioningHeartbeatAt",
	] as const;
	assertExactKeys(
		record,
		[
			"workerId",
			"generation",
			"role",
			"status",
			"updatedAt",
			...optionalRegistryKeys.filter((key) => key in record),
		],
		label,
	);
	const roleRaw = requireNonEmptyString(record.role, `${label}.role`, 32);
	if (!isAgentName(roleRaw)) {
		throw new PoolRegistryCorruptionError(`Pool registry ${label}.role invalid`);
	}
	const statusRaw = requireNonEmptyString(record.status, `${label}.status`, 32);
	if (!STATES.has(statusRaw as PoolWorkerState)) {
		throw new PoolRegistryCorruptionError(`Pool registry ${label}.status invalid`);
	}
	if (typeof record.generation !== "number" || !Number.isInteger(record.generation) || record.generation < 0) {
		throw new PoolRegistryCorruptionError(`Pool registry ${label}.generation invalid`);
	}
	const tombstone =
		typeof record.generationTombstone === "number" && Number.isInteger(record.generationTombstone)
			? record.generationTombstone
			: record.generation;
	if (tombstone < record.generation) {
		throw new PoolRegistryCorruptionError(`Pool registry ${label}.generationTombstone behind generation`);
	}
	const out: PoolWorkerRecord = {
		workerId: requireNonEmptyString(record.workerId, `${label}.workerId`, 64),
		generation: record.generation,
		generationTombstone: tombstone,
		role: roleRaw,
		status: statusRaw as PoolWorkerState,
		updatedAt: requireNonEmptyString(record.updatedAt, `${label}.updatedAt`, 64),
	};
	if (typeof record.paneId === "string" && record.paneId) out.paneId = record.paneId;
	if (typeof record.agentName === "string" && record.agentName) out.agentName = record.agentName;
	if (typeof record.activeAssignmentId === "string") out.activeAssignmentId = record.activeAssignmentId;
	if (typeof record.activeParentEpoch === "string") out.activeParentEpoch = record.activeParentEpoch;
	if (typeof record.uncertainWrite === "boolean") out.uncertainWrite = record.uncertainWrite;
	if (typeof record.paneClosed === "boolean") out.paneClosed = record.paneClosed;
	if (typeof record.recoveryRequired === "boolean") out.recoveryRequired = record.recoveryRequired;
	if (typeof record.cwd === "string" && record.cwd) out.cwd = record.cwd;
	if ("boundPolicy" in record) {
		if (record.boundPolicy === null || record.boundPolicy === undefined) {
			throw new PoolRegistryCorruptionError(
				`${label}.boundPolicy null/undefined forbidden; omit key for legacy rows`,
			);
		}
		out.boundPolicy = parseRegistryBoundPolicy(record.boundPolicy, `${label}.boundPolicy`);
	}

	const ownerRaw = record.provisioningOwnerId;
	const heartbeatRaw = record.provisioningHeartbeatAt;
	const ownerPresent = ownerRaw !== undefined && ownerRaw !== null;
	const heartbeatPresent = heartbeatRaw !== undefined && heartbeatRaw !== null;
	if (ownerPresent || heartbeatPresent) {
		if (statusRaw !== "starting") {
			throw new PoolRegistryCorruptionError(
				`Pool registry ${label} provisioning ownership only allowed on starting`,
			);
		}
		if (!ownerPresent || !heartbeatPresent) {
			throw new PoolRegistryCorruptionError(
				`Pool registry ${label} provisioning ownership fields must be a pair`,
			);
		}
		const ownerId = requireNonEmptyString(ownerRaw, `${label}.provisioningOwnerId`, 128);
		const at = requireNonEmptyString(heartbeatRaw, `${label}.provisioningHeartbeatAt`, 64);
		const parsed = Date.parse(at);
		if (!Number.isFinite(parsed)) {
			throw new PoolRegistryCorruptionError(
				`Pool registry ${label}.provisioningHeartbeatAt invalid`,
			);
		}
		const ageMs = Date.now() - parsed;
		// Fail closed on implausibly future timestamps (beyond shared clock-skew grace).
		if (ageMs < -DEFAULT_HEARTBEAT_CLOCK_SKEW_MS) {
			throw new PoolRegistryCorruptionError(
				`Pool registry ${label}.provisioningHeartbeatAt is in the future`,
			);
		}
		out.provisioningOwnerId = ownerId;
		out.provisioningHeartbeatAt = at;
	}
	return out;
}

/**
 * Copy a registry row without provisioning-ownership fields. Use when leaving
 * `starting` so joiners cannot treat stale ownership metadata as live.
 */
export function withoutProvisioningOwnership(
	record: PoolWorkerRecord,
	patch: Partial<PoolWorkerRecord> = {},
): PoolWorkerRecord {
	const merged: PoolWorkerRecord = {
		workerId: patch.workerId ?? record.workerId,
		generation: patch.generation ?? record.generation,
		generationTombstone: patch.generationTombstone ?? record.generationTombstone,
		role: patch.role ?? record.role,
		status: patch.status ?? record.status,
		updatedAt: patch.updatedAt ?? record.updatedAt,
	};
	const paneId = patch.paneId !== undefined ? patch.paneId : record.paneId;
	const agentName = patch.agentName !== undefined ? patch.agentName : record.agentName;
	const activeAssignmentId =
		patch.activeAssignmentId !== undefined
			? patch.activeAssignmentId
			: record.activeAssignmentId;
	const activeParentEpoch =
		patch.activeParentEpoch !== undefined
			? patch.activeParentEpoch
			: record.activeParentEpoch;
	const uncertainWrite =
		patch.uncertainWrite !== undefined ? patch.uncertainWrite : record.uncertainWrite;
	const paneClosed = patch.paneClosed !== undefined ? patch.paneClosed : record.paneClosed;
	const recoveryRequired =
		patch.recoveryRequired !== undefined ? patch.recoveryRequired : record.recoveryRequired;
	const cwd = patch.cwd !== undefined ? patch.cwd : record.cwd;
	const boundPolicy =
		patch.boundPolicy !== undefined ? patch.boundPolicy : record.boundPolicy;
	if (paneId !== undefined) merged.paneId = paneId;
	if (agentName !== undefined) merged.agentName = agentName;
	if (activeAssignmentId !== undefined) merged.activeAssignmentId = activeAssignmentId;
	if (activeParentEpoch !== undefined) merged.activeParentEpoch = activeParentEpoch;
	if (uncertainWrite !== undefined) merged.uncertainWrite = uncertainWrite;
	if (paneClosed !== undefined) merged.paneClosed = paneClosed;
	if (recoveryRequired !== undefined) merged.recoveryRequired = recoveryRequired;
	if (cwd !== undefined) merged.cwd = cwd;
	if (boundPolicy !== undefined) merged.boundPolicy = boundPolicy;
	return merged;
}

/**
 * Per-role atomic registry files under roles/{role}/registry.json.
 * Role lock protects all mutations for that role (no global cross-role RMW).
 */
export class PoolRegistry {
	readonly poolKey: string;
	readonly poolRoot: string;
	readonly cacheRoot: string;

	constructor(poolKey: string, cacheRoot = momoCacheRoot()) {
		this.poolKey = poolKey;
		this.cacheRoot = cacheRoot;
		this.poolRoot = poolRootPath(poolKey, cacheRoot);
		ensurePrivateDir(this.poolRoot);
	}

	roleFile(role: AgentName): string {
		return path.join(rolePoolRoot(this.poolRoot, role), "registry.json");
	}

	readRole(role: AgentName): PoolWorkerRecord | undefined {
		const filePath = this.roleFile(role);
		if (!existsSync(filePath)) return undefined;
		try {
			const parsed = parseJsonFile(filePath);
			const record = validatePoolWorkerRecord(parsed, `roles.${role}`);
			if (record.role !== role) {
				throw new PoolRegistryCorruptionError(`Role file role mismatch for ${role}`);
			}
			return record;
		} catch (error) {
			if (error instanceof PoolRegistryCorruptionError) throw error;
			throw new PoolRegistryCorruptionError(
				`Pool role registry corrupt at ${filePath}: ${
					error instanceof Error ? error.message : String(error)
				}`,
			);
		}
	}

	/** Fail-closed read used by callers that must not continue on corruption. */
	getByRole(role: AgentName): PoolWorkerRecord | undefined {
		return this.readRole(role);
	}

	upsert(record: PoolWorkerRecord): void {
		const validated = validatePoolWorkerRecord(record);
		ensurePrivateDir(rolePoolRoot(this.poolRoot, validated.role));
		const tombstone = Math.max(validated.generationTombstone, validated.generation);
		atomicWriteJson(this.roleFile(validated.role), {
			...validated,
			generationTombstone: tombstone,
		});
	}

	list(): PoolWorkerRecord[] {
		const workers: PoolWorkerRecord[] = [];
		for (const role of ROLE_LIST) {
			const record = this.readRole(role.name);
			if (record) workers.push(record);
		}
		return workers;
	}

	read(): PoolRegistrySnapshot {
		return {
			version: POOL_REGISTRY_VERSION,
			poolKey: this.poolKey,
			workers: this.list(),
		};
	}

	tryRead(): { snapshot: PoolRegistrySnapshot; corruption?: string } {
		try {
			return { snapshot: this.read() };
		} catch (error) {
			if (error instanceof PoolRegistryCorruptionError) {
				return {
					snapshot: { version: POOL_REGISTRY_VERSION, poolKey: this.poolKey, workers: [] },
					corruption: error.message,
				};
			}
			throw error;
		}
	}

	/**
	 * Cleanup remove: clear live worker fields but retain generation tombstone.
	 * Clears control ready/heartbeat/active (not tombstone file).
	 * Result is a pane-less archival tombstone (generation 0, no paneId) that
	 * remains recreatable via nextGeneration while preserving the high-water mark.
	 */
	archiveRoleKeepingTombstone(role: AgentName, nowIso: string): PoolWorkerRecord | undefined {
		const existing = this.readRole(role);
		if (!existing) return undefined;
		const tombstone = Math.max(existing.generationTombstone, existing.generation);
		clearControlEphemerals(this.poolRoot, role);
		const archived: PoolWorkerRecord = {
			workerId: existing.workerId,
			generation: 0,
			generationTombstone: tombstone,
			role,
			status: "unhealthy",
			updatedAt: nowIso,
			...(existing.cwd ? { cwd: existing.cwd } : {}),
		};
		this.upsert(archived);
		return archived;
	}

	removeRole(role: AgentName): void {
		this.archiveRoleKeepingTombstone(role, new Date().toISOString());
	}

	nextGeneration(role: AgentName): number {
		const existing = this.readRole(role);
		const tombstone = existing ? Math.max(existing.generationTombstone, existing.generation) : 0;
		return tombstone + 1;
	}
}

/** Clear ready/heartbeat/active/manifest before a new generation launch. */
export function clearControlEphemerals(poolRoot: string, role: AgentName): void {
	const control = workerControlPaths(poolRoot, role);
	for (const file of [control.ready, control.heartbeat, control.active, control.manifest]) {
		try {
			if (existsSync(file)) rmSync(file, { force: true });
		} catch {
			// ignore
		}
	}
	// Also clear any stray files in worker dir except we keep the directory.
	if (existsSync(control.root)) {
		for (const name of readdirSync(control.root)) {
			if (name.endsWith(".tmp")) {
				try {
					rmSync(path.join(control.root, name), { force: true });
				} catch {
					// ignore
				}
			}
		}
	}
}

/**
 * Pane-less archival tombstone left by cleanup: generation 0, no pane/agent.
 * Recreatable by provisioning generation tombstone+1. Distinct from a live
 * unhealthy/uncertain worker that still owns a generation (and usually a pane).
 */
export function isArchivalTombstone(record: PoolWorkerRecord): boolean {
	return record.generation === 0 && !record.paneId;
}

export type LivePaneIdCollision =
	| {
			kind: "collision";
			role: AgentName;
			status: PoolWorkerRecord["status"];
			generation: number;
	  }
	| { kind: "clear" }
	| { kind: "error"; reason: string };

/**
 * Fail-closed scan of every live pool role for a registered paneId match.
 * Archival tombstones are ignored. Registry read/parse failures return `error`
 * so callers retain orphan evidence instead of closing/removing.
 */
export function findLivePaneIdCollision(
	pool: PoolRegistry,
	paneId: string,
): LivePaneIdCollision {
	if (typeof paneId !== "string" || paneId.length === 0) {
		return { kind: "error", reason: "paneId invalid for live collision scan" };
	}
	try {
		for (const worker of pool.list()) {
			if (isArchivalTombstone(worker)) continue;
			if (worker.paneId && worker.paneId === paneId) {
				return {
					kind: "collision",
					role: worker.role,
					status: worker.status,
					generation: worker.generation,
				};
			}
		}
		return { kind: "clear" };
	} catch (error) {
		return {
			kind: "error",
			reason: error instanceof Error ? error.message : String(error),
		};
	}
}

/** Live workers that must not be reused or auto-reprovisioned without cleanup. */
export function isNonReusableLiveWorker(record: PoolWorkerRecord): boolean {
	if (isArchivalTombstone(record)) return false;
	return record.status === "uncertain" || record.status === "unhealthy";
}

export function selectClosablePoolWorkers(
	workers: readonly PoolWorkerRecord[],
	options: { force?: boolean } = {},
): { closable: PoolWorkerRecord[]; refused: PoolWorkerRecord[] } {
	const force = options.force === true;
	const closable = workers.filter((worker) => {
		if (worker.generation === 0 && !worker.paneId) return false; // tombstone-only
		if (worker.status === "busy" || worker.status === "blocked" || worker.status === "starting") {
			return false;
		}
		if (worker.status === "uncertain") {
			if (!force) return false;
			if (worker.paneClosed === true && worker.recoveryRequired === true) return false;
			return true;
		}
		if (worker.status === "idle" || worker.status === "unhealthy") {
			if (worker.paneClosed === true && worker.recoveryRequired === true) return false;
			return Boolean(worker.paneId);
		}
		return false;
	});
	const closableIds = new Set(closable.map((worker) => worker.workerId + ":" + worker.generation));
	const refused = workers.filter(
		(worker) => !closableIds.has(worker.workerId + ":" + worker.generation),
	);
	return { closable, refused };
}

export function formatWorkerStatusLine(worker: PoolWorkerRecord, poolRoot: string): string {
	const queued = queueCount(poolRoot, worker.role);
	const assignment = worker.activeAssignmentId ? ` assignment=${worker.activeAssignmentId}` : "";
	return `- ${worker.role} state=${worker.status} worker=${worker.workerId} gen=${worker.generation} tombstone=${worker.generationTombstone}${
		worker.paneId ? ` pane=${worker.paneId}` : ""
	}${assignment} queued=${queued}${worker.uncertainWrite ? " uncertainWrite" : ""}${
		worker.recoveryRequired ? " recoveryRequired" : ""
	}${worker.paneClosed ? " paneClosed" : ""}`;
}

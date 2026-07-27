import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { atomicWriteJson, ensurePrivateDir, momoCacheRoot } from "../ipc/spool.js";
import { isAgentName, type AgentName } from "../roles.js";

export type PaneLifecycle =
	| "starting"
	| "ready"
	| "running"
	| "completed"
	| "failed"
	| "aborted"
	| "uncertain";

const LIFECYCLES = new Set<PaneLifecycle>([
	"starting",
	"ready",
	"running",
	"completed",
	"failed",
	"aborted",
	"uncertain",
]);

export interface PaneRecord {
	workerId: string;
	runId: string;
	role: AgentName;
	paneId: string;
	agentName: string;
	spoolRoot: string;
	cwd: string;
	status: PaneLifecycle;
	uncertainWrite?: boolean;
	/** Pane was closed during force cleanup but lease recovery failed. */
	paneClosed?: boolean;
	/** Operator must recover lease/registry manually; avoid re-close loops. */
	recoveryRequired?: boolean;
	updatedAt: string;
}

export interface PaneRegistrySnapshot {
	version: 1;
	parentId: string;
	panes: PaneRecord[];
}

export class RegistryCorruptionError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "RegistryCorruptionError";
	}
}

function requireNonEmptyString(value: unknown, label: string, max = 4096): string {
	if (typeof value !== "string" || value.length === 0 || value.length > max || value.includes("\0")) {
		throw new RegistryCorruptionError(`Pane registry ${label} invalid`);
	}
	return value;
}

export function validatePaneRecord(value: unknown, index: number): PaneRecord {
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		throw new RegistryCorruptionError(`Pane registry panes[${index}] must be an object`);
	}
	const record = value as Record<string, unknown>;
	const workerId = requireNonEmptyString(record.workerId, `panes[${index}].workerId`, 64);
	const runId = requireNonEmptyString(record.runId, `panes[${index}].runId`, 64);
	const roleRaw = requireNonEmptyString(record.role, `panes[${index}].role`, 32);
	if (!isAgentName(roleRaw)) {
		throw new RegistryCorruptionError(`Pane registry panes[${index}].role invalid`);
	}
	const statusRaw = requireNonEmptyString(record.status, `panes[${index}].status`, 32);
	if (!LIFECYCLES.has(statusRaw as PaneLifecycle)) {
		throw new RegistryCorruptionError(`Pane registry panes[${index}].status invalid`);
	}
	const spoolRoot = requireNonEmptyString(record.spoolRoot, `panes[${index}].spoolRoot`, 4096);
	if (!path.isAbsolute(spoolRoot)) {
		throw new RegistryCorruptionError(`Pane registry panes[${index}].spoolRoot must be absolute`);
	}
	const cwd = requireNonEmptyString(record.cwd, `panes[${index}].cwd`, 4096);
	const out: PaneRecord = {
		workerId,
		runId,
		role: roleRaw,
		paneId: requireNonEmptyString(record.paneId, `panes[${index}].paneId`, 128),
		agentName: requireNonEmptyString(record.agentName, `panes[${index}].agentName`, 64),
		spoolRoot,
		cwd,
		status: statusRaw as PaneLifecycle,
		updatedAt: requireNonEmptyString(record.updatedAt, `panes[${index}].updatedAt`, 64),
	};
	if (typeof record.uncertainWrite === "boolean") out.uncertainWrite = record.uncertainWrite;
	if (typeof record.paneClosed === "boolean") out.paneClosed = record.paneClosed;
	if (typeof record.recoveryRequired === "boolean") out.recoveryRequired = record.recoveryRequired;
	return out;
}

export class PaneRegistry {
	readonly filePath: string;
	readonly parentId: string;

	constructor(parentId: string, cacheRoot = momoCacheRoot()) {
		this.parentId = parentId;
		this.filePath = path.join(cacheRoot, "parents", parentId, "registry.json");
		ensurePrivateDir(path.dirname(this.filePath));
	}

	read(): PaneRegistrySnapshot {
		if (!existsSync(this.filePath)) {
			return { version: 1, parentId: this.parentId, panes: [] };
		}
		let parsed: unknown;
		try {
			parsed = JSON.parse(readFileSync(this.filePath, "utf8"));
		} catch (error) {
			throw new RegistryCorruptionError(
				`Pane registry JSON is corrupt at ${this.filePath}: ${
					error instanceof Error ? error.message : String(error)
				}`,
			);
		}
		if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
			throw new RegistryCorruptionError(`Pane registry schema invalid at ${this.filePath}`);
		}
		const record = parsed as Record<string, unknown>;
		if (record.version !== 1 || !Array.isArray(record.panes)) {
			throw new RegistryCorruptionError(`Pane registry version/panes invalid at ${this.filePath}`);
		}
		const panes = record.panes.map((pane, index) => validatePaneRecord(pane, index));
		return {
			version: 1,
			parentId: typeof record.parentId === "string" ? record.parentId : this.parentId,
			panes,
		};
	}

	/** Soft read for startup reconciliation: corruption becomes empty + diagnostic. */
	tryRead(): { snapshot: PaneRegistrySnapshot; corruption?: string } {
		try {
			return { snapshot: this.read() };
		} catch (error) {
			if (error instanceof RegistryCorruptionError) {
				return {
					snapshot: { version: 1, parentId: this.parentId, panes: [] },
					corruption: error.message,
				};
			}
			throw error;
		}
	}

	upsert(record: PaneRecord): void {
		const snapshot = this.read();
		const index = snapshot.panes.findIndex((pane) => pane.workerId === record.workerId);
		if (index >= 0) snapshot.panes[index] = record;
		else snapshot.panes.push(record);
		atomicWriteJson(this.filePath, snapshot);
	}

	list(): PaneRecord[] {
		return this.read().panes;
	}

	remove(workerId: string): void {
		const snapshot = this.read();
		snapshot.panes = snapshot.panes.filter((pane) => pane.workerId !== workerId);
		atomicWriteJson(this.filePath, snapshot);
	}

	clear(workerIds: readonly string[]): void {
		const allowed = new Set(workerIds);
		const snapshot = this.read();
		snapshot.panes = snapshot.panes.filter((pane) => !allowed.has(pane.workerId));
		atomicWriteJson(this.filePath, snapshot);
	}
}

const TERMINAL_STATUSES = new Set<PaneLifecycle>(["completed", "failed", "aborted", "uncertain"]);

/** Cleanup selection: terminal panes only; uncertain requires --force. Never closes active. */
export function selectClosablePanes(
	panes: readonly PaneRecord[],
	options: { force?: boolean } = {},
): { closable: PaneRecord[]; refused: PaneRecord[] } {
	const force = options.force === true;
	const closable = panes.filter((pane) => {
		if (!TERMINAL_STATUSES.has(pane.status)) return false;
		if (pane.status === "uncertain" && !force) return false;
		// Already closed; waiting on lease recovery — do not re-close.
		if (pane.paneClosed === true && pane.recoveryRequired === true) return false;
		return true;
	});
	const closableIds = new Set(closable.map((pane) => pane.workerId));
	const refused = panes.filter((pane) => !closableIds.has(pane.workerId));
	return { closable, refused };
}

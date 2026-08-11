import path from "node:path";
import type { AgentName } from "../roles.js";
import { ensurePrivateDir } from "../ipc/spool.js";
import {
	assertAssignmentId,
	assertPathUnderRoot,
	rolePoolRoot,
} from "./pool-identity.js";

export interface AssignmentSpoolPaths {
	root: string;
	command: string;
	cancel: string;
	heartbeat: string;
	events: string;
	result: string;
	started: string;
}

export interface WorkerControlPaths {
	root: string;
	manifest: string;
	ready: string;
	heartbeat: string;
	active: string;
	tombstone: string;
}

/** Derive assignment spool under the role pool — never trust a stored absolute path. */
export function assignmentSpoolPaths(
	poolRoot: string,
	role: AgentName,
	assignmentId: string,
): AssignmentSpoolPaths {
	assertAssignmentId(assignmentId);
	const roleRoot = rolePoolRoot(poolRoot, role);
	const root = path.join(roleRoot, "assignments", assignmentId);
	assertPathUnderRoot(root, roleRoot);
	return {
		root,
		command: path.join(root, "command.json"),
		cancel: path.join(root, "cancel.json"),
		heartbeat: path.join(root, "heartbeat.json"),
		events: path.join(root, "events.ndjson"),
		result: path.join(root, "result.json"),
		started: path.join(root, "started.json"),
	};
}

export function workerControlPaths(poolRoot: string, role: AgentName): WorkerControlPaths {
	const root = path.join(rolePoolRoot(poolRoot, role), "worker");
	return {
		root,
		manifest: path.join(root, "manifest.json"),
		ready: path.join(root, "ready.json"),
		heartbeat: path.join(root, "heartbeat.json"),
		active: path.join(root, "active.json"),
		tombstone: path.join(root, "generation.tombstone"),
	};
}

export function ensureAssignmentSpool(
	poolRoot: string,
	role: AgentName,
	assignmentId: string,
): AssignmentSpoolPaths {
	const paths = assignmentSpoolPaths(poolRoot, role, assignmentId);
	ensurePrivateDir(paths.root);
	return paths;
}

export interface WorkerManifest {
	version: 2 | 3;
	poolKey: string;
	workerId: string;
	generation: number;
	role: AgentName;
	cwd: string;
	paneId?: string;
	agentName?: string;
	createdAt: string;
	/** Generation-bound launch policy (v3). Omitted on legacy v2 manifests. */
	boundPolicy?: import("../ipc/spool.js").IpcModelPolicy;
}

export interface WorkerActivePointer {
	version: 1;
	assignmentId: string;
	generation: number;
	parentEpoch: string;
	dispatchedAt: string;
}

/** @deprecated Prefer IpcStarted from ipc/spool — kept as path companion type. */
export interface AssignmentStartedMarker {
	version: 1;
	runId: string;
	workerId: string;
	generation: number;
	parentEpoch: string;
	startedAt: string;
}

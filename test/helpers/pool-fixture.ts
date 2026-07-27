import { mkdirSync } from "node:fs";
import path from "node:path";
import { PoolRegistry } from "../../src/herdr/pool-registry.js";
import {
	resolvePoolIdentity,
	stableWorkerId,
} from "../../src/herdr/pool-identity.js";
import {
	assignmentSpoolPaths,
	workerControlPaths,
} from "../../src/herdr/assignment-spool.js";
import { atomicWriteJson } from "../../src/ipc/spool.js";
import type { AgentName } from "../../src/roles.js";

export function setupPoolWorkerFixture(options: {
	cacheRoot: string;
	cwd: string;
	role: AgentName;
	generation?: number;
	assignmentId?: string;
}) {
	const generation = options.generation ?? 1;
	const identity = resolvePoolIdentity({
		cwd: options.cwd,
		canonicalRoot: options.cwd,
		workspaceId: "test-ws",
		socketPath: "test-sock",
	});
	const pool = new PoolRegistry(identity.poolKey, options.cacheRoot);
	const workerId = stableWorkerId(identity.poolKey, options.role);
	const control = workerControlPaths(pool.poolRoot, options.role);
	mkdirSync(control.root, { recursive: true, mode: 0o700 });
	const assignmentId = options.assignmentId ?? "abcdef0123456789";
	const paths = assignmentSpoolPaths(pool.poolRoot, options.role, assignmentId);
	mkdirSync(paths.root, { recursive: true, mode: 0o700 });
	pool.upsert({
		workerId,
		generation,
		generationTombstone: generation,
		role: options.role,
		paneId: "w1:p2",
		agentName: `momo_${options.role}`,
		status: "idle",
		updatedAt: new Date().toISOString(),
	});
	const env: Record<string, string> = {
		MOMO_WORKER: "1",
		MOMO_ROLE: options.role,
		MOMO_IPC_DIR: control.root,
		MOMO_CONTROL_DIR: control.root,
		MOMO_WORKER_ID: workerId,
		MOMO_WORKER_GENERATION: String(generation),
		MOMO_POOL_KEY: identity.poolKey,
		MOMO_POOL_ROOT: pool.poolRoot,
		MOMO_RUN_ID: `g${generation}`,
		MOMO_CWD: options.cwd,
	};
	return { identity, pool, workerId, control, assignmentId, paths, env, generation };
}

export function dispatchAssignment(options: {
	controlRoot: string;
	paths: { command: string };
	assignmentId: string;
	workerId: string;
	generation: number;
	task: string;
	parentEpoch?: string;
}): void {
	const parentEpoch = options.parentEpoch ?? "epoch1";
	atomicWriteJson(options.paths.command, {
		version: 1,
		type: "prompt",
		task: options.task,
		issuedAt: new Date().toISOString(),
		runId: options.assignmentId,
		workerId: options.workerId,
		generation: options.generation,
		parentEpoch,
	});
	atomicWriteJson(path.join(options.controlRoot, "active.json"), {
		version: 1,
		assignmentId: options.assignmentId,
		generation: options.generation,
		parentEpoch,
		dispatchedAt: new Date().toISOString(),
	});
}

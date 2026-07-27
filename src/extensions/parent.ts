/**
 * Momo parent Pi extension (canonical Herdr path only).
 * Registers `delegate` synchronously at factory load so Pi's initial tool
 * selection can see it. Non-Herdr in-process behavior stays in cli/runtime.
 */
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { existsSync } from "node:fs";
import path from "node:path";
import { MOMO_SYSTEM_PROMPT } from "../prompts.js";
import { createDelegateTool } from "../delegation/tool.js";
import { createDelegationRunner } from "../delegation/runner.js";
import { getRole, ROLE_LIST } from "../roles.js";
import {
	createHerdrChildSessionFactory,
	createParentEpoch,
	createStableParentId,
} from "../delegation/herdr-factory.js";
import { HerdrClient } from "../herdr/client.js";
import {
	formatWorkerStatusLine,
	PoolRegistry,
	PoolRegistryCorruptionError,
	selectClosablePoolWorkers,
	clearControlEphemerals,
	isArchivalTombstone,
	type PoolWorkerRecord,
} from "../herdr/pool-registry.js";
import {
	legacyRegistryExists,
	migrateLegacyPaneRegistry,
} from "../herdr/legacy-migration.js";
import { PaneRegistry } from "../herdr/registry.js";
import { resolvePoolIdentity } from "../herdr/pool-identity.js";
import { assignmentSpoolPaths, workerControlPaths } from "../herdr/assignment-spool.js";
import { completeCleanAssignmentLocked } from "../herdr/terminal-transition.js";
import { WriterLeaseManager, LeaseCorruptionError } from "../lease/writer-lease.js";
import { atomicWriteJson, DEFAULT_HEARTBEAT_STALE_MS } from "../ipc/spool.js";
import {
	IpcValidationError,
	tryReadIpcJson,
	validateActivePointer,
	validateHeartbeat,
	validateResult,
	validateStarted,
} from "../ipc/validate.js";
import { queueCount, withRoleLockAsync } from "../herdr/role-queue.js";

export const PARENT_ACTIVE_TOOLS = ["read", "grep", "find", "ls", "delegate"] as const;

export interface InstallMomoParentOptions {
	env?: NodeJS.ProcessEnv;
	cwd?: string;
	client?: HerdrClient;
	poolRegistry?: PoolRegistry;
	leaseManager?: WriterLeaseManager;
	parentEpoch?: string;
	/** @deprecated Legacy v1 pane registry — used only for one-time migration. */
	registry?: PaneRegistry;
}

export function installMomoParent(pi: ExtensionAPI, options: InstallMomoParentOptions = {}): void {
	const env = options.env ?? process.env;
	if (env.MOMO_WORKER === "1") return;
	if (env.MOMO_PARENT !== "1") return;

	const paneId = env.HERDR_PANE_ID;
	if (!paneId) {
		throw new Error(
			"Momo parent extension requires HERDR_PANE_ID (canonical Herdr launch only)",
		);
	}

	const cwd = options.cwd ?? process.cwd();
	const parentId =
		env.MOMO_PARENT_ID ||
		createStableParentId({
			paneId,
			cwd,
			...(env.HERDR_WORKSPACE_ID ? { workspaceId: env.HERDR_WORKSPACE_ID } : {}),
		});
	env.MOMO_PARENT_ID = parentId;
	process.env.MOMO_PARENT_ID = parentId;

	const parentEpoch = options.parentEpoch ?? env.MOMO_PARENT_EPOCH ?? createParentEpoch();
	env.MOMO_PARENT_EPOCH = parentEpoch;
	process.env.MOMO_PARENT_EPOCH = parentEpoch;

	const herdrClient = options.client ?? new HerdrClient({ env });
	const identity = resolvePoolIdentity({
		cwd,
		...(env.HERDR_WORKSPACE_ID ? { workspaceId: env.HERDR_WORKSPACE_ID } : {}),
		...(env.HERDR_SOCKET_PATH ? { socketPath: env.HERDR_SOCKET_PATH } : {}),
	});
	const pool = options.poolRegistry ?? new PoolRegistry(identity.poolKey);
	const leases = options.leaseManager ?? new WriterLeaseManager();
	const legacyRegistry = options.registry ?? new PaneRegistry(parentId);

	const createChildSession = createHerdrChildSessionFactory({
		cwd,
		parentPaneId: paneId,
		parentId,
		parentEpoch,
		client: herdrClient,
		poolRegistry: pool,
		...(env.HERDR_WORKSPACE_ID ? { workspaceId: env.HERDR_WORKSPACE_ID } : {}),
		...(env.HERDR_SOCKET_PATH ? { socketPath: env.HERDR_SOCKET_PATH } : {}),
	});
	const runner = createDelegationRunner({
		cwd,
		roles: ROLE_LIST,
		createChildSession,
	});

	pi.registerTool(createDelegateTool(runner));

	pi.on("session_start", async (_event, ctx) => {
		if (ctx.hasUI !== true) return;
		pi.setActiveTools([...PARENT_ACTIVE_TOOLS]);

		if (legacyRegistryExists(parentId)) {
			try {
				await migrateLegacyPaneRegistry(legacyRegistry, herdrClient, {
					notify: (message) => ctx.ui?.notify?.(message),
				});
			} catch (error) {
				ctx.ui?.notify?.(
					`Legacy migration warning: ${error instanceof Error ? error.message : String(error)}`,
				);
			}
		}

		try {
			await adoptPoolWorkers(pool, herdrClient, identity.poolKey, ctx);
		} catch (error) {
			ctx.ui?.notify?.(
				`Pool adoption warning: ${error instanceof Error ? error.message : String(error)}`,
			);
		}

		try {
			await herdrClient.reportMetadata(paneId, {
				source: "momo:parent",
				displayAgent: "Momo",
				title: "Momo",
				agent: "pi",
			});
			await herdrClient.renamePane(paneId, "Momo");
		} catch {
			// Display naming is best-effort.
		}
	});

	pi.on("session_shutdown", async () => {
		await cancelEpochAssignmentsOnQuit(pool, parentEpoch, herdrClient);
	});

	pi.on("before_agent_start", async () => ({
		systemPrompt: MOMO_SYSTEM_PROMPT,
	}));

	pi.on("tool_call", async (event) => {
		const toolName = String((event as { toolName?: string }).toolName ?? "");
		if (toolName === "bash" || toolName === "edit" || toolName === "write") {
			return { block: true, reason: "Parent Momo cannot mutate the repository; use delegate" };
		}
		return undefined;
	});

	pi.registerCommand("momo-workers", {
		description: "List persistent Momo role-pool workers (state, assignment, queue)",
		handler: async (_args, ctx) => {
			let workers: PoolWorkerRecord[] = [];
			try {
				workers = pool.list();
			} catch (error) {
				ctx.ui?.notify?.(
					error instanceof PoolRegistryCorruptionError
						? error.message
						: `Failed to read pool registry: ${error instanceof Error ? error.message : String(error)}`,
				);
				return;
			}
			if (workers.length === 0) {
				ctx.ui?.notify?.("No Momo role-pool workers are registered.");
				return;
			}
			const lines = workers.map((worker) => formatWorkerStatusLine(worker, pool.poolRoot));
			ctx.ui?.notify?.(lines.join("\n"));
		},
	});

	pi.registerCommand("momo-cleanup", {
		description:
			"Close idle/unhealthy role-pool workers. Use --force for uncertain (exact lease owner). Refuses busy/blocked.",
		handler: async (args, ctx) => {
			const force = String(args || "")
				.split(/\s+/)
				.includes("--force");
			let workers: PoolWorkerRecord[] = [];
			try {
				workers = pool.list();
			} catch (error) {
				ctx.ui?.notify?.(
					error instanceof PoolRegistryCorruptionError
						? error.message
						: `Failed to read pool registry: ${error instanceof Error ? error.message : String(error)}`,
				);
				return;
			}

			const uncertain = workers.filter(
				(worker) => worker.status === "uncertain" && !(worker.paneClosed && worker.recoveryRequired),
			);
			if (force && uncertain.length > 0) {
				const confirmed = await ctx.ui?.confirm?.(
					"Force-clean uncertain workers?",
					`Close ${uncertain.length} uncertain worker(s) and release matching writer leases only when ownerId equals the registry workerId for that cwd.`,
				);
				if (confirmed !== true) {
					ctx.ui?.notify?.("Force cleanup cancelled.");
					return;
				}
			}

			const { closable, refused } = selectClosablePoolWorkers(workers, { force });
			let closed = 0;
			const notes: string[] = [];
			for (const worker of closable) {
				try {
					await withRoleLockAsync(pool.poolRoot, worker.role, async () => {
						const current = pool.getByRole(worker.role);
						if (!current || current.generation !== worker.generation) return;
						if (current.status === "uncertain") {
							// Do not treat a missing lease as proof of safety: it
							// can be a corrupted/partially removed lease.
							const leaseCwd = current.cwd ?? identity.canonicalRoot;
							const owner = leases.peekOwner(leaseCwd);
							if (!owner || owner.ownerId !== current.workerId) {
								notes.push(`refused uncertain ${current.workerId}: lease owner unavailable or mismatched`);
								return;
							}
						}
						if (current.paneId && !current.paneClosed) {
							await herdrClient.closePane(current.paneId);
							// Agent lookup is the confirmation boundary before a
							// forced lease release/archive.
							const info = current.agentName ? await herdrClient.agentGet(current.agentName).catch(() => undefined) : undefined;
							if (info && info.agentStatus !== "done" && info.agentStatus !== "idle") {
								throw new Error("worker did not stop after pane close");
							}
						}
						if (current.status === "uncertain") {
							const leaseCwd = current.cwd ?? identity.canonicalRoot;
							const release = leases.forceReleaseIfOwner(leaseCwd, current.workerId);
							if (!release.released) {
								notes.push(`lease not released for ${current.workerId}: ${release.reason ?? "unknown"}`);
								return;
							}
						}
						clearControlEphemerals(pool.poolRoot, current.role);
						pool.archiveRoleKeepingTombstone(current.role, new Date().toISOString());
						closed += 1;
					});
				} catch (error) {
					ctx.ui?.notify?.(
						`Failed to close ${worker.paneId ?? worker.workerId}: ${
							error instanceof Error ? error.message : String(error)
						}`,
					);
				}
			}
			ctx.ui?.notify?.(
				[
					`Closed ${closed} worker pane(s).`,
					refused.length
						? `Refused ${refused.length} busy/blocked/starting${force ? "" : "/uncertain"} worker(s).`
						: "",
					...notes,
				]
					.filter(Boolean)
					.join(" "),
			);
		},
	});
}

/**
 * Adopt existing pool workers only when registry + manifest + heartbeat + Herdr identity match.
 * Never adopts arbitrary or legacy v1 workers.
 * Registry writes run under the per-role lock with generation/worker/snapshot fences.
 */
export async function adoptPoolWorkers(
	pool: PoolRegistry,
	client: HerdrClient,
	poolKey: string,
	ctx: { ui?: { notify?: (message: string) => void } },
): Promise<void> {
	const leases = new WriterLeaseManager({ cacheRoot: pool.cacheRoot });
	const { snapshot, corruption } = pool.tryRead();
	if (corruption) {
		ctx.ui?.notify?.(corruption);
		return;
	}
	for (const worker of snapshot.workers) {
		if (worker.generation < 1 || !worker.paneId || !worker.agentName) {
			let earlyNote: string | undefined;
			await withRoleLockAsync(pool.poolRoot, worker.role, () => {
				const current = pool.getByRole(worker.role);
				// Exact whole-snapshot fence before any early mutation.
				if (!matchesExactSnapshot(current, worker)) return;

				// Pane-less starting reservation is normal — provisioning owner finalizes/rolls back.
				if (current.status === "starting") return;

				// Tombstone / non-live: never spuriously mutate.
				if (isArchivalTombstone(current) || current.generation < 1) return;

				if (isActiveImplementer(current)) {
					// Exact active implementer with invalid/missing identity → uncertain.
					pool.upsert({
						...current,
						status: "uncertain",
						uncertainWrite: true,
						updatedAt: new Date().toISOString(),
					});
					earlyNote = `Worker ${worker.workerId} missing pane/agent — marked uncertain`;
				} else {
					// Exact invalid nonactive live record → unhealthy.
					pool.upsert({
						...current,
						status: "unhealthy",
						updatedAt: new Date().toISOString(),
					});
					earlyNote = `Worker ${worker.workerId} missing pane/agent — marked unhealthy`;
				}
			});
			if (earlyNote) ctx.ui?.notify?.(earlyNote);
			continue;
		}

		const control = workerControlPaths(pool.poolRoot, worker.role);
		try {
			const manifestRaw = tryReadIpcJson(control.manifest);
			if (!manifestRaw || typeof manifestRaw !== "object") {
				throw new Error("missing manifest");
			}
			const manifest = manifestRaw as {
				version?: number;
				poolKey?: string;
				workerId?: string;
				generation?: number;
				paneId?: string;
				agentName?: string;
				role?: string;
				cwd?: string;
			};
			if (
				manifest.version !== 2 ||
				manifest.poolKey !== poolKey ||
				manifest.workerId !== worker.workerId ||
				manifest.generation !== worker.generation ||
				manifest.paneId !== worker.paneId ||
				manifest.agentName !== worker.agentName ||
				manifest.role !== worker.role
			) {
				throw new Error("manifest identity mismatch");
			}
			// Live manifested workers must carry canonical cwd on registry + manifest.
			if (worker.cwd === undefined || typeof manifest.cwd !== "string") {
				throw new Error("missing canonical cwd on registry or manifest");
			}
			if (manifest.cwd !== worker.cwd) {
				throw new Error("manifest cwd mismatch");
			}

			const heartbeatRaw = tryReadIpcJson(control.heartbeat);
			if (!heartbeatRaw) {
				throw new Error("missing heartbeat");
			}
			validateHeartbeat(heartbeatRaw, {
				runId: `g${worker.generation}`,
				workerId: worker.workerId,
			});
			const heartbeatAt = Date.parse((heartbeatRaw as { at?: string }).at ?? "");
			if (!Number.isFinite(heartbeatAt) || Date.now() - heartbeatAt > DEFAULT_HEARTBEAT_STALE_MS) {
				throw new Error("stale heartbeat");
			}

			const info = await client.agentGet(worker.agentName);
			if (info.paneId !== worker.paneId) {
				throw new Error("Herdr pane identity mismatch");
			}

			// Apply transitions under the role lock with a fresh fence.
			await withRoleLockAsync(pool.poolRoot, worker.role, () => {
				const current = pool.getByRole(worker.role);
				// Exact whole-snapshot fence before every successful-path mutation.
				if (!matchesExactSnapshot(current, worker)) return;

				if (
					(worker.status === "busy" || worker.status === "blocked") &&
					worker.activeAssignmentId
				) {
					const paths = assignmentSpoolPaths(
						pool.poolRoot,
						worker.role,
						worker.activeAssignmentId,
					);
					const resultRaw = tryReadIpcJson(paths.result);
					if (resultRaw) {
						const result = validateResult(resultRaw, {
							runId: worker.activeAssignmentId,
							workerId: worker.workerId,
						});
						if (result.uncertainWrite) {
							pool.upsert({
								...current,
								status: "uncertain",
								uncertainWrite: true,
								updatedAt: new Date().toISOString(),
							});
						} else {
							// Shared idempotent A→B (or idle) terminal transition.
							completeCleanAssignmentLocked({
								pool,
								role: worker.role,
								workerId: worker.workerId,
								generation: worker.generation,
								finishedAssignmentId: worker.activeAssignmentId,
							});
						}
						return;
					}

					if (!getRole(worker.role).canWrite) {
						return;
					}

					// Evidence-based classification for active implementer, no terminal result.
					// Fresh heartbeat already proven above; do not escalate live work to force-cleanup.
					const herdrLive =
						info.agentStatus === "working" || info.agentStatus === "blocked";
					if (herdrLive) {
						// Retain exact busy/blocked current record — live execution is proven.
						return;
					}

					// Herdr idle / done / unknown (or missing): inspect markers + lease.
					markImplementerUncertainIfCrashedEvidence({
						pool,
						leases,
						current,
						worker,
						controlActivePath: control.active,
						paths,
						...(typeof manifest.cwd === "string" ? { manifestCwd: manifest.cwd } : {}),
					});
					return;
				}

				// Nonactive: only mutate when snapshot itself was starting (exact fence above).
				if (worker.status === "starting") {
					pool.upsert({
						...current,
						status:
							info.agentStatus === "idle" || info.agentStatus === "done"
								? "idle"
								: current.status,
						updatedAt: new Date().toISOString(),
					});
				}
			});
		} catch (error) {
			await withRoleLockAsync(pool.poolRoot, worker.role, () => {
				const current = pool.getByRole(worker.role);
				// Exact whole-snapshot fence: idle/starting→busy B while await must not mutate B.
				if (!matchesExactSnapshot(current, worker)) return;

				// Writer uncertain only when exact snapshot is still an active implementer assignment.
				if (
					getRole(worker.role).canWrite &&
					(worker.status === "busy" || worker.status === "blocked") &&
					worker.activeAssignmentId
				) {
					pool.upsert({
						...current,
						status: "uncertain",
						uncertainWrite: true,
						updatedAt: new Date().toISOString(),
					});
					return;
				}

				pool.upsert({
					...current,
					status: "unhealthy",
					updatedAt: new Date().toISOString(),
				});
			});
			ctx.ui?.notify?.(
				`Did not adopt ${worker.workerId}: ${
					error instanceof IpcValidationError || error instanceof Error
						? error.message
						: String(error)
				}`,
			);
		}
	}
}

/**
 * Exact whole-snapshot fence: if current diverged from the adopt snapshot, no-op.
 * Compares generation, workerId, role, status, assignment/epoch, pane/agent, cwd.
 */
function matchesExactSnapshot(
	current: PoolWorkerRecord | undefined,
	snapshot: PoolWorkerRecord,
): current is PoolWorkerRecord {
	if (!current) return false;
	return (
		current.generation === snapshot.generation &&
		current.workerId === snapshot.workerId &&
		current.role === snapshot.role &&
		current.status === snapshot.status &&
		current.activeAssignmentId === snapshot.activeAssignmentId &&
		current.activeParentEpoch === snapshot.activeParentEpoch &&
		current.paneId === snapshot.paneId &&
		current.agentName === snapshot.agentName &&
		current.cwd === snapshot.cwd
	);
}

/** Current live record is an active writer assignment (any id). */
function isActiveImplementer(current: PoolWorkerRecord): boolean {
	return (
		getRole(current.role).canWrite &&
		(current.status === "busy" || current.status === "blocked") &&
		Boolean(current.activeAssignmentId)
	);
}

/**
 * Idle/done/unknown Herdr + fresh heartbeat, no terminal result.
 * Marks uncertain only when started or same-worker lease proves a prior attempt;
 * otherwise retains busy (pre-start or foreign-lease wait). Never touches leases.
 */
function markImplementerUncertainIfCrashedEvidence(options: {
	pool: PoolRegistry;
	leases: WriterLeaseManager;
	current: PoolWorkerRecord;
	worker: PoolWorkerRecord;
	controlActivePath: string;
	paths: ReturnType<typeof assignmentSpoolPaths>;
	manifestCwd?: string;
}): void {
	const { pool, leases, current, worker, controlActivePath, paths } = options;
	const assignmentId = worker.activeAssignmentId!;
	const markUncertain = (): void => {
		pool.upsert({
			...current,
			status: "uncertain",
			uncertainWrite: true,
			updatedAt: new Date().toISOString(),
		});
	};

	let parentEpoch: string;
	try {
		const activeRaw = tryReadIpcJson(controlActivePath);
		const ptr = validateActivePointer(activeRaw, {
			generation: worker.generation,
			assignmentId,
		});
		const expectedEpoch = current.activeParentEpoch ?? worker.activeParentEpoch;
		if (expectedEpoch !== undefined && ptr.parentEpoch !== expectedEpoch) {
			markUncertain();
			return;
		}
		parentEpoch = ptr.parentEpoch;
	} catch {
		markUncertain();
		return;
	}

	let hasValidStarted = false;
	if (existsSync(paths.started)) {
		try {
			const startedRaw = tryReadIpcJson(paths.started);
			validateStarted(startedRaw, {
				runId: assignmentId,
				workerId: worker.workerId,
				generation: worker.generation,
				parentEpoch,
			});
			hasValidStarted = true;
		} catch {
			markUncertain();
			return;
		}
	}

	const cwd = current.cwd ?? worker.cwd ?? options.manifestCwd;
	if (!cwd) {
		markUncertain();
		return;
	}

	let owner: ReturnType<WriterLeaseManager["peekOwner"]>;
	try {
		owner = leases.peekOwner(cwd);
	} catch (error) {
		if (error instanceof LeaseCorruptionError) {
			markUncertain();
			return;
		}
		throw error;
	}

	if (hasValidStarted || (owner && owner.ownerId === worker.workerId)) {
		markUncertain();
		return;
	}

	// Foreign lease + no started => retain busy (waiting). Never touch lease.
	// No lease + no started + fresh heartbeat => retain busy (pre-start).
}

/** Shutdown cancels only assignments owned by this parent epoch. */
export async function cancelEpochAssignmentsOnQuit(
	pool: PoolRegistry,
	parentEpoch: string,
	_client: HerdrClient,
): Promise<void> {
	const { snapshot } = pool.tryRead();
	const roles = [...new Set(snapshot.workers.map((worker) => worker.role))];
	for (const role of roles) {
		await withRoleLockAsync(pool.poolRoot, role, () => {
			const current = pool.getByRole(role);
			if (!current) return;
			if (
				current.status !== "busy" &&
				current.status !== "blocked" &&
				current.status !== "starting"
			) {
				return;
			}
			// Require exact epoch ownership — missing/foreign epoch writes nothing.
			if (current.activeParentEpoch !== parentEpoch) return;
			if (!current.activeAssignmentId) return;
			try {
				const paths = assignmentSpoolPaths(
					pool.poolRoot,
					role,
					current.activeAssignmentId,
				);
				atomicWriteJson(paths.cancel, {
					version: 1,
					runId: current.activeAssignmentId,
					workerId: current.workerId,
					generation: current.generation,
					reason: "parent_session_shutdown",
					issuedAt: new Date().toISOString(),
				});
			} catch {
				// best effort cancel IPC only — never terminal keys on shared panes
			}
		});
	}
}

export default function (pi: ExtensionAPI): void {
	installMomoParent(pi);
}

// Re-export for tests that still import reconcile helpers.
export { queueCount, formatWorkerStatusLine };

/** @deprecated Legacy v1 reconciliation retained for migration-era tests only. */
export async function reconcileRegistry(
	registry: PaneRegistry,
	client: HerdrClient,
	ctx: { ui?: { notify?: (message: string) => void } },
): Promise<void> {
	const { snapshot, corruption } = registry.tryRead();
	if (corruption) {
		ctx.ui?.notify?.(corruption);
		return;
	}
	for (const pane of snapshot.panes) {
		if (pane.status !== "starting" && pane.status !== "ready" && pane.status !== "running") {
			continue;
		}
		const resultPath = path.join(pane.spoolRoot, "result.json");
		try {
			const resultRaw = tryReadIpcJson(resultPath);
			if (resultRaw !== undefined) {
				const result = validateResult(resultRaw, {
					runId: pane.runId,
					workerId: pane.workerId,
				});
				const status =
					result.uncertainWrite === true
						? "uncertain"
						: result.status === "completed"
							? "completed"
							: result.status === "aborted"
								? "aborted"
								: "failed";
				registry.upsert({
					...pane,
					status,
					updatedAt: new Date().toISOString(),
					...(result.uncertainWrite === true ? { uncertainWrite: true } : {}),
				});
				continue;
			}
		} catch (error) {
			const implementer = pane.role === "implementer";
			registry.upsert({
				...pane,
				status: implementer ? "uncertain" : "failed",
				updatedAt: new Date().toISOString(),
				...(implementer ? { uncertainWrite: true } : {}),
			});
			ctx.ui?.notify?.(
				`Corrupt/invalid result for ${pane.workerId}: ${
					error instanceof IpcValidationError || error instanceof Error
						? error.message
						: String(error)
				}`,
			);
			continue;
		}

		try {
			const info = await client.agentGet(pane.agentName);
			const status = info.agentStatus;
			if (status === "working" || status === "blocked") continue;
			const implementer = pane.role === "implementer";
			registry.upsert({
				...pane,
				status: implementer ? "uncertain" : "failed",
				updatedAt: new Date().toISOString(),
				...(implementer ? { uncertainWrite: true } : {}),
			});
			ctx.ui?.notify?.(
				`Reconciled ${pane.workerId}: Herdr ${status ?? "missing"} → ${
					implementer ? "uncertain" : "failed"
				}`,
			);
		} catch {
			const implementer = pane.role === "implementer";
			registry.upsert({
				...pane,
				status: implementer ? "uncertain" : "failed",
				updatedAt: new Date().toISOString(),
				...(implementer ? { uncertainWrite: true } : {}),
			});
		}
	}
}

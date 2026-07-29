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
import { AGENT_NAMES, getRole, ROLE_LIST } from "../roles.js";
import {
	createHerdrChildSessionFactory,
	createParentEpoch,
	createStableParentId,
} from "../delegation/herdr-factory.js";
import { HerdrClient, HerdrCliError, isAgentNotFoundError, isPaneNotFoundError } from "../herdr/client.js";
import {
	formatWorkerStatusLine,
	PoolRegistry,
	PoolRegistryCorruptionError,
	selectClosablePoolWorkers,
	clearControlEphemerals,
	isArchivalTombstone,
	withoutProvisioningOwnership,
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
import { terminalizeAndClearRoleAssignmentsLocked } from "../herdr/cleanup-terminalize.js";
import {
	listOrphanPaneEvidence,
	removeOrphanPaneEvidenceIfUnchanged,
	roleHasOrphanPaneEvidence,
	type OrphanPaneEvidence,
} from "../herdr/orphan-panes.js";
import { WriterLeaseManager, LeaseCorruptionError } from "../lease/writer-lease.js";
import { atomicWriteJson, DEFAULT_HEARTBEAT_STALE_MS, DEFAULT_HEARTBEAT_CLOCK_SKEW_MS } from "../ipc/spool.js";
import {
	IpcValidationError,
	assertHeartbeatFreshness,
	tryReadIpcJson,
	validateActivePointer,
	validateHeartbeat,
	validateResult,
	validateStarted,
} from "../ipc/validate.js";
import {
	cancelQueuedAssignment,
	commitClaim,
	listClaiming,
	listQueue,
	queueCount,
	withRoleLock,
	withRoleLockAsync,
} from "../herdr/role-queue.js";

export const PARENT_ACTIVE_TOOLS = ["read", "grep", "find", "ls", "delegate"] as const;

/**
 * Adoption grace for a read-only assignment that has started but Herdr reports idle
 * with no terminal result yet (settlement/result race). Matches heartbeat stale budget.
 */
export const ADOPTION_STARTED_GRACE_MS = DEFAULT_HEARTBEAT_STALE_MS;

/** Generation/assignment-fenced timers for post-grace read-only adoption rechecks. */
const adoptionGraceRechecks = new Map<string, ReturnType<typeof setTimeout>>();

/** @internal test-only: clear pending adoption grace recheck timers. */
export function __clearAdoptionGraceRechecksForTest(): void {
	for (const timer of adoptionGraceRechecks.values()) clearTimeout(timer);
	adoptionGraceRechecks.clear();
}

/** @internal test-only: pending grace recheck count. */
export function __adoptionGraceRecheckCountForTest(): number {
	return adoptionGraceRechecks.size;
}

/**
 * @internal test-only: runs under each cleanup role lock after acquire, before
 * fresh eligibility / closePane / agentGet / terminalize / lease operations.
 */
let cleanupRoleLockEnteredHook:
	| ((snapshot: PoolWorkerRecord) => void | Promise<void>)
	| undefined;

/** @internal test-only: mutate registry under the cleanup role lock for race tests. */
export function __setCleanupRoleLockEnteredHookForTest(
	hook?: (snapshot: PoolWorkerRecord) => void | Promise<void>,
): void {
	cleanupRoleLockEnteredHook = hook;
}

/** @internal test-only: clear cleanup role-lock race hook. */
export function __resetCleanupRoleLockEnteredHookForTest(): void {
	cleanupRoleLockEnteredHook = undefined;
}

function adoptionGraceRecheckKey(poolRoot: string, worker: PoolWorkerRecord): string {
	return `${poolRoot}:${worker.role}:${worker.generation}:${worker.activeAssignmentId ?? ""}`;
}

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
		leaseManager: leases,
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
			"Close idle/unhealthy role-pool workers and retry orphan panes from supersession close failures. Use --force for uncertain (exact lease owner). Refuses busy/blocked.",
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
			/** Uncertain rows included in the force-confirm dialog (empty if none / cancelled). */
			const confirmedUncertainKeys = new Set<string>();
			if (force && uncertain.length > 0) {
				const confirmed = await ctx.ui?.confirm?.(
					"Force-clean uncertain workers?",
					`Close ${uncertain.length} uncertain worker(s) and release matching writer leases only when ownerId equals the registry workerId for that cwd.`,
				);
				if (confirmed !== true) {
					ctx.ui?.notify?.("Force cleanup cancelled.");
					return;
				}
				for (const worker of uncertain) {
					confirmedUncertainKeys.add(`${worker.workerId}:${worker.generation}`);
				}
			}

			const { closable, refused } = selectClosablePoolWorkers(workers, { force });
			let closed = 0;
			const notes: string[] = [];
			for (const worker of closable) {
				try {
					await withRoleLockAsync(pool.poolRoot, worker.role, async () => {
						if (cleanupRoleLockEnteredHook) {
							await cleanupRoleLockEnteredHook(worker);
						}
						const current = pool.getByRole(worker.role);
						// Stale pool.list eligibility is not authoritative — re-fence live row.
						if (
							!current ||
							current.generation !== worker.generation ||
							current.workerId !== worker.workerId
						) {
							notes.push(
								`refused ${worker.workerId}: generation/worker superseded before cleanup`,
							);
							return;
						}
						const liveEligibility = selectClosablePoolWorkers([current], { force });
						if (liveEligibility.closable.length === 0) {
							const reason =
								current.status === "busy" ||
								current.status === "blocked" ||
								current.status === "starting"
									? `status is ${current.status}`
									: current.status === "uncertain" && !force
										? "uncertain requires --force"
										: isArchivalTombstone(current)
											? "archival tombstone"
											: current.paneClosed && current.recoveryRequired
												? "recoveryRequired"
												: !current.paneId
													? "pane-less / ineligible"
													: `no longer eligible (status=${current.status})`;
							notes.push(`refused ${current.workerId}: ${reason}`);
							return;
						}
						if (current.status === "uncertain") {
							const uncertainKey = `${current.workerId}:${current.generation}`;
							// New uncertainty after the initial snapshot/confirmation must not
							// be force-cleaned without an explicit rerun confirm.
							if (!confirmedUncertainKeys.has(uncertainKey)) {
								notes.push(
									force
										? `refused ${current.workerId}: became uncertain after confirmation — rerun /momo-cleanup --force to confirm`
										: `refused ${current.workerId}: uncertain requires --force`,
								);
								return;
							}
							// Do not treat a missing lease as proof of safety: it
							// can be a corrupted/partially removed lease.
							const leaseCwd = current.cwd ?? identity.canonicalRoot;
							const owner = leases.peekOwner(leaseCwd);
							if (!owner || owner.ownerId !== current.workerId) {
								notes.push(`refused uncertain ${current.workerId}: lease owner unavailable or mismatched`);
								return;
							}
						}

						let record = current;
						// Close pane once, then generation-fence paneClosed before agentGet so a
						// transient confirmation failure can retry without re-closing.
						if (record.paneId && !record.paneClosed) {
							try {
								await herdrClient.closePane(record.paneId);
							} catch (closeError) {
								// Structured missing pane ⇒ already closed. Other close
								// errors retain the row for a later retry.
								if (!isPaneNotFoundError(closeError)) {
									throw closeError;
								}
							}
							const afterClose = pool.getByRole(worker.role);
							if (
								!afterClose ||
								afterClose.generation !== worker.generation ||
								afterClose.workerId !== worker.workerId
							) {
								notes.push(
									`refused ${worker.workerId}: generation/worker changed after pane close`,
								);
								return;
							}
							pool.upsert({
								...afterClose,
								paneClosed: true,
								updatedAt: new Date().toISOString(),
							});
							record = pool.getByRole(worker.role) ?? afterClose;
						}

						// paneClosed or uncertain: only definitive done/idle or structured
						// agent_not_found may proceed. Transient lookup failures retain
						// paneClosed + lease for a later --force retry.
						if (record.paneClosed || record.status === "uncertain") {
							const confirm = await confirmAgentStoppedForCleanup(
								herdrClient,
								record.agentName,
							);
							if (!confirm.ok) {
								notes.push(`refused ${record.workerId}: ${confirm.reason}`);
								return;
							}
						}

						// Terminalize AFTER stop confirmation and BEFORE lease release so a
						// terminalization failure retains paneClosed + original lease/evidence.
						const latestForQueue = pool.getByRole(worker.role);
						if (
							!latestForQueue ||
							latestForQueue.generation !== worker.generation ||
							latestForQueue.workerId !== worker.workerId
						) {
							notes.push(
								`refused ${worker.workerId}: generation/worker changed before queue terminalize`,
							);
							return;
						}
						const cleared = terminalizeAndClearRoleAssignmentsLocked({
							pool,
							role: latestForQueue.role,
							worker: latestForQueue,
							reason: "role_worker_cleanup_before_archive",
						});
						if (!cleared.ok) {
							notes.push(
								`refused ${latestForQueue.workerId}: could not terminalize prior assignments (${cleared.reason})`,
							);
							return;
						}

						if (latestForQueue.status === "uncertain") {
							const leaseCwd = latestForQueue.cwd ?? identity.canonicalRoot;
							const release = leases.forceReleaseIfOwner(leaseCwd, latestForQueue.workerId);
							if (!release.released) {
								const latest = pool.getByRole(worker.role);
								if (
									latest &&
									latest.generation === worker.generation &&
									latest.workerId === worker.workerId
								) {
									pool.upsert({
										...latest,
										paneClosed: true,
										recoveryRequired: true,
										updatedAt: new Date().toISOString(),
									});
								}
								notes.push(
									`lease not released for ${latestForQueue.workerId}: ${release.reason ?? "unknown"}`,
								);
								return;
							}
						}

						clearControlEphemerals(pool.poolRoot, latestForQueue.role);
						pool.archiveRoleKeepingTombstone(latestForQueue.role, new Date().toISOString());
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

			// Role-scoped orphan panes from superseded provision close failures.
			// Never await Herdr/network while holding the role lock:
			//   1) under lock: list + strict-validate + refuse live paneId collision
			//   2) outside lock: closePane
			//   3) reacquire lock: remove only if exact evidence unchanged and still
			//      does not collide; concurrent cleanups are idempotent (close/not-found)
			//      and never delete replaced evidence.
			let orphanClosed = 0;
			for (const roleName of AGENT_NAMES) {
				if (!roleHasOrphanPaneEvidence(pool.poolRoot, roleName)) continue;
				type OrphanCloseCandidate = {
					evidence: OrphanPaneEvidence;
					filePath: string;
				};
				let candidates: OrphanCloseCandidate[] = [];
				try {
					candidates = withRoleLock(pool.poolRoot, roleName, () => {
						const listed = listOrphanPaneEvidence(pool.poolRoot, roleName);
						const live = pool.getByRole(roleName);
						const out: OrphanCloseCandidate[] = [];
						for (const entry of listed) {
							if (!entry.ok) {
								notes.push(
									`retained orphan evidence ${entry.filePath}: ${entry.reason}`,
								);
								continue;
							}
							const { evidence } = entry;
							// Refuse closing any paneId still registered for this role
							// (any generation/status) so stale evidence cannot hit the successor.
							if (live?.paneId && live.paneId === evidence.paneId) {
								notes.push(
									`retained orphan evidence for ${evidence.paneId}: paneId collides with live registry pane (status=${live.status} gen=${live.generation})`,
								);
								continue;
							}
							out.push({ evidence, filePath: entry.filePath });
						}
						return out;
					});
				} catch (error) {
					notes.push(
						`orphan cleanup ${roleName} failed: ${
							error instanceof Error ? error.message : String(error)
						}`,
					);
					continue;
				}

				for (const candidate of candidates) {
					const { evidence } = candidate;
					try {
						await herdrClient.closePane(evidence.paneId);
					} catch (closeError) {
						if (!isPaneNotFoundError(closeError)) {
							notes.push(
								`retained orphan pane ${evidence.paneId} (gen=${evidence.generation}): ${
									closeError instanceof Error
										? closeError.message
										: String(closeError)
								}`,
							);
							continue;
						}
					}

					try {
						const removeOutcome = withRoleLock(pool.poolRoot, roleName, () => {
							const live = pool.getByRole(roleName);
							if (live?.paneId && live.paneId === evidence.paneId) {
								return {
									kind: "collide" as const,
									status: live.status,
									generation: live.generation,
								};
							}
							const removed = removeOrphanPaneEvidenceIfUnchanged(
								pool.poolRoot,
								roleName,
								evidence,
							);
							return { kind: "remove" as const, removed };
						});
						if (removeOutcome.kind === "collide") {
							notes.push(
								`retained orphan evidence for ${evidence.paneId}: paneId collides with live registry pane after close (status=${removeOutcome.status} gen=${removeOutcome.generation})`,
							);
							continue;
						}
						const { removed } = removeOutcome;
						if (removed.status === "removed" || removed.status === "missing") {
							// missing ⇒ concurrent cleanup already cleared the same evidence.
							orphanClosed += 1;
							continue;
						}
						if (removed.status === "changed") {
							notes.push(
								`retained orphan evidence for ${evidence.paneId}: evidence replaced during close`,
							);
							continue;
						}
						notes.push(
							`retained orphan evidence for ${evidence.paneId}: ${removed.reason}`,
						);
					} catch (error) {
						notes.push(
							`orphan cleanup ${roleName} failed after close of ${evidence.paneId}: ${
								error instanceof Error ? error.message : String(error)
							}`,
						);
					}
				}
			}

			ctx.ui?.notify?.(
				[
					`Closed ${closed} worker pane(s).`,
					orphanClosed > 0 ? `Closed ${orphanClosed} orphan pane(s).` : "",
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
			const heartbeat = validateHeartbeat(heartbeatRaw, {
				runId: `g${worker.generation}`,
				workerId: worker.workerId,
			});
			assertHeartbeatFreshness(heartbeat.at, {
				now: Date.now(),
				staleMs: DEFAULT_HEARTBEAT_STALE_MS,
			});

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
						const classified = classifyReadOnlyActiveNoResultAdoption({
							pool,
							current,
							worker,
							controlActivePath: control.active,
							paths,
							agentStatus: info.agentStatus,
							nowMs: Date.now(),
						});
						if (classified.kind === "grace") {
							scheduleReadOnlyAdoptionGraceRecheck({
								pool,
								client,
								poolKey,
								leases,
								ctx,
								snapshot: worker,
								delayMs: classified.recheckDelayMs,
								evidenceKind: classified.evidenceKind,
							});
						}
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
					// Owner-bearing starting: preserve unchanged — exact provisioning proxy
					// promotes/rolls back. Stale-owner recovery is joiner timeout / cleanup.
					if (isOwnerBearingStarting(current)) {
						return;
					}
					if (info.agentStatus === "idle" || info.agentStatus === "done") {
						// Legacy owner-less starting may still be adopted to idle.
						pool.upsert(
							withoutProvisioningOwnership(current, {
								status: "idle",
								updatedAt: new Date().toISOString(),
							}),
						);
					}
				}
			});
		} catch (error) {
			await withRoleLockAsync(pool.poolRoot, worker.role, () => {
				const current = pool.getByRole(worker.role);
				// Exact whole-snapshot fence: idle/starting→busy B while await must not mutate B.
				if (!matchesExactSnapshot(current, worker)) return;

				// Owner-bearing starting: never strip ownership or mark unhealthy on
				// agentGet/validation failure — leave for the provisioning proxy.
				if (isOwnerBearingStarting(current)) {
					return;
				}

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

				pool.upsert(
					withoutProvisioningOwnership(current, {
						status: "unhealthy",
						updatedAt: new Date().toISOString(),
					}),
				);
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
 * Compares generation, workerId, role, status, assignment/epoch, pane/agent, cwd,
 * and provisioning ownership pair.
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
		current.cwd === snapshot.cwd &&
		current.provisioningOwnerId === snapshot.provisioningOwnerId &&
		current.provisioningHeartbeatAt === snapshot.provisioningHeartbeatAt
	);
}

/**
 * Live owner-bearing starting reservation: only the exact provisioning proxy may
 * promote/rollback. Adoption must not strip ownership or transition these rows.
 */
function isOwnerBearingStarting(record: PoolWorkerRecord): boolean {
	return (
		record.status === "starting" &&
		typeof record.provisioningOwnerId === "string" &&
		record.provisioningOwnerId.length > 0 &&
		typeof record.provisioningHeartbeatAt === "string" &&
		record.provisioningHeartbeatAt.length > 0
	);
}

/**
 * Read-only busy/blocked assignment with no terminal result.
 * Must not mark a newly dispatched worker unhealthy merely because Herdr is still
 * idle before the worker poll/start fence. Invalid active/started identity fails
 * closed to unhealthy while preserving active evidence for cleanup.
 *
 * Must run under the role lock with an exact snapshot fence already applied.
 * Returns `grace` when the worker is retained only because evidence is still young —
 * caller should schedule a bounded post-grace recheck carrying `evidenceKind`.
 */
type AdoptionGraceEvidenceKind = "dispatch" | "started";

type ReadOnlyAdoptionClassifyResult =
	| { kind: "unchanged" }
	| { kind: "unhealthy" }
	| { kind: "grace"; recheckDelayMs: number; evidenceKind: AdoptionGraceEvidenceKind };

function classifyReadOnlyActiveNoResultAdoption(options: {
	pool: PoolRegistry;
	current: PoolWorkerRecord;
	worker: PoolWorkerRecord;
	controlActivePath: string;
	paths: ReturnType<typeof assignmentSpoolPaths>;
	agentStatus: string | undefined;
	nowMs: number;
	startedGraceMs?: number;
	maxFutureSkewMs?: number;
}): ReadOnlyAdoptionClassifyResult {
	const { pool, current, worker, controlActivePath, paths } = options;
	const assignmentId = worker.activeAssignmentId!;
	const markUnhealthy = (): ReadOnlyAdoptionClassifyResult => {
		pool.upsert({
			...current,
			status: "unhealthy",
			updatedAt: new Date(options.nowMs).toISOString(),
		});
		return { kind: "unhealthy" };
	};

	let parentEpoch: string;
	let dispatchedAtMs: number;
	try {
		const activeRaw = tryReadIpcJson(controlActivePath);
		const ptr = validateActivePointer(activeRaw, {
			generation: worker.generation,
			assignmentId,
		});
		const expectedEpoch = current.activeParentEpoch ?? worker.activeParentEpoch;
		if (expectedEpoch !== undefined && ptr.parentEpoch !== expectedEpoch) {
			return markUnhealthy();
		}
		parentEpoch = ptr.parentEpoch;
		const parsedDispatch = Date.parse(ptr.dispatchedAt);
		if (!Number.isFinite(parsedDispatch)) {
			return markUnhealthy();
		}
		dispatchedAtMs = parsedDispatch;
	} catch {
		return markUnhealthy();
	}

	let startedAtMs: number | undefined;
	if (existsSync(paths.started)) {
		try {
			const startedRaw = tryReadIpcJson(paths.started);
			const started = validateStarted(startedRaw, {
				runId: assignmentId,
				workerId: worker.workerId,
				generation: worker.generation,
				parentEpoch,
			});
			const parsed = Date.parse(started.startedAt);
			if (!Number.isFinite(parsed)) {
				return markUnhealthy();
			}
			startedAtMs = parsed;
		} catch {
			return markUnhealthy();
		}
	}

	const status = options.agentStatus;
	if (status === "working" || status === "blocked") {
		return { kind: "unchanged" };
	}

	const graceMs = options.startedGraceMs ?? ADOPTION_STARTED_GRACE_MS;
	const maxFutureSkewMs = options.maxFutureSkewMs ?? DEFAULT_HEARTBEAT_CLOCK_SKEW_MS;
	const graceOutcome = (
		atMs: number,
		evidenceKind: AdoptionGraceEvidenceKind,
	): ReadOnlyAdoptionClassifyResult => {
		const ageMs = options.nowMs - atMs;
		if (ageMs < -maxFutureSkewMs) {
			return markUnhealthy();
		}
		if (ageMs <= graceMs) {
			return {
				kind: "grace",
				recheckDelayMs: Math.max(0, graceMs - ageMs),
				evidenceKind,
			};
		}
		return markUnhealthy();
	};

	if (status === "idle") {
		if (startedAtMs === undefined) {
			return graceOutcome(dispatchedAtMs, "dispatch");
		}
		return graceOutcome(startedAtMs, "started");
	}

	// done / unknown / missing: no live execution proof.
	return markUnhealthy();
}

function scheduleReadOnlyAdoptionGraceRecheck(options: {
	pool: PoolRegistry;
	client: HerdrClient;
	poolKey: string;
	leases: WriterLeaseManager;
	ctx: { ui?: { notify?: (message: string) => void } };
	snapshot: PoolWorkerRecord;
	delayMs: number;
	evidenceKind: AdoptionGraceEvidenceKind;
}): void {
	const key = adoptionGraceRecheckKey(options.pool.poolRoot, options.snapshot);
	const previous = adoptionGraceRechecks.get(key);
	if (previous) clearTimeout(previous);

	const timer = setTimeout(() => {
		adoptionGraceRechecks.delete(key);
		void recheckReadOnlyAdoptionAfterGrace(options);
	}, Math.max(0, options.delayMs));
	// Keep the event loop alive under Vitest fake timers; unref in production.
	if (process.env.VITEST !== "true") {
		timer.unref?.();
	}
	adoptionGraceRechecks.set(key, timer);
}

/**
 * Bounded post-grace recheck: full identity/manifest/heartbeat/Herdr/result path with
 * exact snapshot fence. No-ops when generation/assignment advanced (successor B / N+1).
 *
 * At most two timers per generation/assignment:
 * - `dispatch` expiry: if a valid started.json appeared, schedule exactly one
 *   `started` follow-up; otherwise mark unhealthy.
 * - `started` expiry: mark unhealthy unless result / working|blocked / successor.
 * Never reschedules a third timer.
 */
async function recheckReadOnlyAdoptionAfterGrace(options: {
	pool: PoolRegistry;
	client: HerdrClient;
	poolKey: string;
	leases: WriterLeaseManager;
	ctx: { ui?: { notify?: (message: string) => void } };
	snapshot: PoolWorkerRecord;
	evidenceKind: AdoptionGraceEvidenceKind;
}): Promise<void> {
	const { pool, client, poolKey, ctx, snapshot: worker, evidenceKind } = options;
	if (!worker.paneId || !worker.agentName || !worker.activeAssignmentId) return;

	const control = workerControlPaths(pool.poolRoot, worker.role);
	try {
		const currentBefore = pool.getByRole(worker.role);
		if (!matchesExactSnapshot(currentBefore, worker)) return;

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
		const heartbeat = validateHeartbeat(heartbeatRaw, {
			runId: `g${worker.generation}`,
			workerId: worker.workerId,
		});
		assertHeartbeatFreshness(heartbeat.at, {
			now: Date.now(),
			staleMs: DEFAULT_HEARTBEAT_STALE_MS,
		});

		const info = await client.agentGet(worker.agentName);
		if (info.paneId !== worker.paneId) {
			throw new Error("Herdr pane identity mismatch");
		}

		await withRoleLockAsync(pool.poolRoot, worker.role, () => {
			const current = pool.getByRole(worker.role);
			if (!matchesExactSnapshot(current, worker)) return;

			const paths = assignmentSpoolPaths(
				pool.poolRoot,
				worker.role,
				worker.activeAssignmentId!,
			);
			const resultRaw = tryReadIpcJson(paths.result);
			if (resultRaw) {
				const result = validateResult(resultRaw, {
					runId: worker.activeAssignmentId!,
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
					completeCleanAssignmentLocked({
						pool,
						role: worker.role,
						workerId: worker.workerId,
						generation: worker.generation,
						finishedAssignmentId: worker.activeAssignmentId!,
					});
				}
				return;
			}

			// Worker advanced to live Herdr work — preserve busy; no further timer.
			if (info.agentStatus === "working" || info.agentStatus === "blocked") {
				return;
			}

			const markUnhealthy = (): void => {
				pool.upsert({
					...current,
					status: "unhealthy",
					updatedAt: new Date().toISOString(),
				});
			};

			// Started-grace expiry: still idle / no result → cleanup-eligible.
			// Do not preserve solely because started.json exists (that was the grace evidence).
			if (evidenceKind === "started") {
				markUnhealthy();
				return;
			}

			// Dispatch-grace expiry: if a valid start appeared, schedule exactly one
			// started-grace follow-up (same generation/assignment key). Otherwise unhealthy.
			if (existsSync(paths.started)) {
				try {
					const activeRaw = tryReadIpcJson(control.active);
					const ptr = validateActivePointer(activeRaw, {
						generation: worker.generation,
						assignmentId: worker.activeAssignmentId!,
					});
					const started = validateStarted(tryReadIpcJson(paths.started), {
						runId: worker.activeAssignmentId!,
						workerId: worker.workerId,
						generation: worker.generation,
						parentEpoch: ptr.parentEpoch,
					});
					const startedAtMs = Date.parse(started.startedAt);
					if (!Number.isFinite(startedAtMs)) {
						markUnhealthy();
						return;
					}
					const nowMs = Date.now();
					const ageMs = nowMs - startedAtMs;
					const maxFutureSkewMs = DEFAULT_HEARTBEAT_CLOCK_SKEW_MS;
					if (ageMs < -maxFutureSkewMs) {
						markUnhealthy();
						return;
					}
					const graceMs = ADOPTION_STARTED_GRACE_MS;
					if (ageMs > graceMs) {
						// Started evidence already aged out — no second timer.
						markUnhealthy();
						return;
					}
					scheduleReadOnlyAdoptionGraceRecheck({
						...options,
						snapshot: worker,
						delayMs: Math.max(0, graceMs - ageMs),
						evidenceKind: "started",
					});
					return;
				} catch {
					markUnhealthy();
					return;
				}
			}

			// Still idle with no start (or done/unknown) → mark unhealthy.
			markUnhealthy();
		});
	} catch (error) {
		await withRoleLockAsync(pool.poolRoot, worker.role, () => {
			const current = pool.getByRole(worker.role);
			if (!matchesExactSnapshot(current, worker)) return;
			pool.upsert({
				...current,
				status: "unhealthy",
				updatedAt: new Date().toISOString(),
			});
		});
		ctx.ui?.notify?.(
			`Adoption grace recheck failed for ${worker.workerId}: ${
				error instanceof IpcValidationError || error instanceof Error
					? error.message
					: String(error)
			}`,
		);
	}
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

function writeAssignmentCancelIpc(options: {
	poolRoot: string;
	role: PoolWorkerRecord["role"];
	assignmentId: string;
	workerId: string;
	generation: number;
}): void {
	try {
		const paths = assignmentSpoolPaths(options.poolRoot, options.role, options.assignmentId);
		atomicWriteJson(paths.cancel, {
			version: 1,
			runId: options.assignmentId,
			workerId: options.workerId,
			generation: options.generation,
			reason: "parent_session_shutdown",
			issuedAt: new Date().toISOString(),
		});
	} catch {
		// best effort cancel IPC only — never terminal keys on shared panes
	}
}

/**
 * Confirm an agent is definitively stopped (done/idle) or structurally gone.
 * Never treats transient/unknown agentGet failures as stop confirmation.
 */
export async function confirmAgentStoppedForCleanup(
	client: HerdrClient,
	agentName: string | undefined,
): Promise<{ ok: true } | { ok: false; reason: string }> {
	if (!agentName) {
		return {
			ok: false,
			reason: "missing agentName; cannot confirm agent stopped",
		};
	}
	try {
		const info = await client.agentGet(agentName);
		if (info.agentStatus === "done" || info.agentStatus === "idle") {
			return { ok: true };
		}
		return {
			ok: false,
			reason: `agent still ${info.agentStatus}; refusing cleanup`,
		};
	} catch (error) {
		if (isAgentNotFoundError(error)) {
			return { ok: true };
		}
		const detail =
			error instanceof HerdrCliError
				? `${error.code ?? "unknown"}: ${error.message}`
				: error instanceof Error
					? error.message
					: String(error);
		return {
			ok: false,
			reason: `agent lookup failed after pane close (refusing cleanup): ${detail}`,
		};
	}
}

/**
 * Shutdown cancels only assignments owned by this parent epoch.
 * Under each role lock: drop matching FIFO/claiming entries, cancel exact active.
 */
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

			// Remove every queued entry owned by this epoch; leave foreign FIFO order untouched.
			for (const entry of listQueue(pool.poolRoot, role)) {
				if (entry.parentEpoch === parentEpoch) {
					cancelQueuedAssignment(pool.poolRoot, role, entry.assignmentId);
				}
			}

			const activeBusy =
				current.status === "busy" ||
				current.status === "blocked" ||
				current.status === "starting";
			const activeMatchesEpoch =
				activeBusy &&
				current.activeParentEpoch === parentEpoch &&
				Boolean(current.activeAssignmentId);

			// Claiming entries for this epoch: cancel if they are the live active
			// assignment; otherwise remove so they cannot recover/run after quit.
			for (const entry of listClaiming(pool.poolRoot, role)) {
				if (entry.parentEpoch !== parentEpoch) continue;
				const isActiveAssignment =
					activeMatchesEpoch && current.activeAssignmentId === entry.assignmentId;
				if (isActiveAssignment) {
					writeAssignmentCancelIpc({
						poolRoot: pool.poolRoot,
						role,
						assignmentId: entry.assignmentId,
						workerId: current.workerId,
						generation: current.generation,
					});
				} else {
					commitClaim(pool.poolRoot, role, entry);
				}
			}

			// Exact active assignment cancel when this epoch owns the busy pointer.
			if (activeMatchesEpoch && current.activeAssignmentId) {
				writeAssignmentCancelIpc({
					poolRoot: pool.poolRoot,
					role,
					assignmentId: current.activeAssignmentId,
					workerId: current.workerId,
					generation: current.generation,
				});
			}
		});
	}
}

export default function (pi: ExtensionAPI): void {
	installMomoParent(pi);
}

// Re-export for tests that still import reconcile helpers.
export { queueCount, formatWorkerStatusLine };
export { terminalizeAndClearRoleAssignmentsLocked } from "../herdr/cleanup-terminalize.js";

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

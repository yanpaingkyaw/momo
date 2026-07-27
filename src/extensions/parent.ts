/**
 * Momo parent Pi extension (canonical Herdr path only).
 * Registers `delegate` synchronously at factory load so Pi's initial tool
 * selection can see it. Non-Herdr in-process behavior stays in cli/runtime.
 */
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import path from "node:path";
import { MOMO_SYSTEM_PROMPT } from "../prompts.js";
import { createDelegateTool } from "../delegation/tool.js";
import { createDelegationRunner } from "../delegation/runner.js";
import { ROLE_LIST } from "../roles.js";
import {
	createHerdrChildSessionFactory,
	createStableParentId,
} from "../delegation/herdr-factory.js";
import { HerdrClient } from "../herdr/client.js";
import {
	PaneRegistry,
	RegistryCorruptionError,
	selectClosablePanes,
	type PaneRecord,
} from "../herdr/registry.js";
import { WriterLeaseManager, LeaseCorruptionError } from "../lease/writer-lease.js";
import { atomicWriteJson } from "../ipc/spool.js";
import { IpcValidationError, tryReadIpcJson, validateResult } from "../ipc/validate.js";

export const PARENT_ACTIVE_TOOLS = ["read", "grep", "find", "ls", "delegate"] as const;

export interface InstallMomoParentOptions {
	env?: NodeJS.ProcessEnv;
	cwd?: string;
	client?: HerdrClient;
	registry?: PaneRegistry;
	leaseManager?: WriterLeaseManager;
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

	const herdrClient = options.client ?? new HerdrClient({ env });
	const registry = options.registry ?? new PaneRegistry(parentId);
	const leases = options.leaseManager ?? new WriterLeaseManager();

	const createChildSession = createHerdrChildSessionFactory({
		cwd,
		parentPaneId: paneId,
		parentId,
		client: herdrClient,
		registry,
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

		try {
			await reconcileRegistry(registry, herdrClient, ctx);
		} catch (error) {
			ctx.ui?.notify?.(
				`Registry reconciliation warning: ${
					error instanceof Error ? error.message : String(error)
				}`,
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
		await signalActiveWorkersOnQuit(registry, herdrClient);
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
		description: "List retained Momo specialist panes",
		handler: async (_args, ctx) => {
			let panes: PaneRecord[] = [];
			try {
				panes = registry.list();
			} catch (error) {
				ctx.ui?.notify?.(
					error instanceof RegistryCorruptionError
						? error.message
						: `Failed to read registry: ${error instanceof Error ? error.message : String(error)}`,
				);
				return;
			}
			if (panes.length === 0) {
				ctx.ui?.notify?.("No Momo worker panes are registered.");
				return;
			}
			const lines = panes.map(
				(pane) =>
					`- ${pane.workerId} role=${pane.role} status=${pane.status} pane=${pane.paneId} cwd=${pane.cwd}${
						pane.uncertainWrite ? " uncertainWrite" : ""
					}${pane.recoveryRequired ? " recoveryRequired" : ""}${pane.paneClosed ? " paneClosed" : ""}`,
			);
			ctx.ui?.notify?.(lines.join("\n"));
		},
	});

	pi.registerCommand("momo-cleanup", {
		description:
			"Close retained terminal Momo worker panes. Use --force for uncertain panes (requires confirmation); never closes active starting/ready/running.",
		handler: async (args, ctx) => {
			// `--force` starts with non-word chars; do not use \b (it never matches).
			const force = String(args || "")
				.split(/\s+/)
				.includes("--force");
			let panes: PaneRecord[] = [];
			try {
				panes = registry.list();
			} catch (error) {
				ctx.ui?.notify?.(
					error instanceof RegistryCorruptionError
						? error.message
						: `Failed to read registry: ${error instanceof Error ? error.message : String(error)}`,
				);
				return;
			}

			const uncertain = panes.filter(
				(pane) => pane.status === "uncertain" && !(pane.paneClosed && pane.recoveryRequired),
			);
			if (force && uncertain.length > 0) {
				const confirmed = await ctx.ui?.confirm?.(
					"Force-clean uncertain panes?",
					`Close ${uncertain.length} uncertain pane(s) and release matching writer leases only when ownerId equals the registry workerId for that cwd.`,
				);
				if (confirmed !== true) {
					ctx.ui?.notify?.("Force cleanup cancelled.");
					return;
				}
			}

			const { closable, refused } = selectClosablePanes(panes, { force });
			let closed = 0;
			const notes: string[] = [];
			for (const pane of closable) {
				try {
					if (!pane.paneClosed) {
						await herdrClient.closePane(pane.paneId);
					}
					if (pane.status === "uncertain" && force) {
						let release: { released: boolean; reason?: string };
						try {
							release = leases.forceReleaseIfOwner(pane.cwd, pane.workerId);
						} catch (error) {
							const reason =
								error instanceof LeaseCorruptionError
									? error.message
									: error instanceof Error
										? error.message
										: String(error);
							registry.upsert({
								...pane,
								paneClosed: true,
								recoveryRequired: true,
								updatedAt: new Date().toISOString(),
							});
							notes.push(
								`lease recovery failed for ${pane.workerId}; registry retained (${reason})`,
							);
							continue;
						}
						if (!release.released && release.reason !== "no_lease") {
							registry.upsert({
								...pane,
								paneClosed: true,
								recoveryRequired: true,
								updatedAt: new Date().toISOString(),
							});
							notes.push(
								`lease not released for ${pane.workerId}: ${release.reason ?? "unknown"}; registry retained`,
							);
							continue;
						}
					}
					registry.remove(pane.workerId);
					closed += 1;
				} catch (error) {
					ctx.ui?.notify?.(
						`Failed to close ${pane.paneId}: ${error instanceof Error ? error.message : String(error)}`,
					);
				}
			}
			const readyOrphaned = refused.filter((pane) => pane.status === "ready");
			const recoveryHeld = refused.filter((pane) => pane.recoveryRequired);
			ctx.ui?.notify?.(
				[
					`Closed ${closed} pane(s).`,
					refused.length
						? `Refused ${refused.length} non-terminal/active pane(s)${
								force ? "" : " (pass --force for uncertain)"
							}.`
						: "",
					readyOrphaned.length
						? `${readyOrphaned.length} ready pane(s) remain managed in registry until skip/cancel/result.`
						: "",
					recoveryHeld.length
						? `${recoveryHeld.length} recoveryRequired record(s) retained for lease inspection.`
						: "",
					...notes,
				]
					.filter(Boolean)
					.join(" "),
			);
		},
	});
}

function mapResultToRegistryStatus(
	result: { status: "completed" | "failed" | "aborted"; uncertainWrite?: boolean },
): { status: PaneRecord["status"]; uncertainWrite?: boolean } {
	if (result.status === "completed") return { status: "completed" };
	if (result.status === "aborted") return { status: "aborted" };
	if (result.uncertainWrite === true) {
		return { status: "uncertain", uncertainWrite: true };
	}
	return { status: "failed" };
}

function terminalWithoutResult(pane: PaneRecord): PaneRecord {
	const implementer = pane.role === "implementer";
	return {
		...pane,
		status: implementer ? "uncertain" : "failed",
		updatedAt: new Date().toISOString(),
		...(implementer
			? { uncertainWrite: true }
			: pane.uncertainWrite !== undefined
				? { uncertainWrite: pane.uncertainWrite }
				: {}),
	};
}

/** Exported for tests: reconcile active registry against IPC results + Herdr state. */
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
				const mapped = mapResultToRegistryStatus(result);
				registry.upsert({
					...pane,
					status: mapped.status,
					updatedAt: new Date().toISOString(),
					...(mapped.uncertainWrite !== undefined
						? { uncertainWrite: mapped.uncertainWrite }
						: result.uncertainWrite === true
							? { uncertainWrite: true }
							: pane.uncertainWrite !== undefined
								? { uncertainWrite: pane.uncertainWrite }
								: {}),
				});
				continue;
			}
		} catch (error) {
			// Corrupt / identity-mismatched result is terminal, not a startup crash.
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
			if (status === "working" || status === "blocked") {
				// Still active (including approval/input blocked) — never make cleanup eligible.
				continue;
			}
			if (status === "idle" || status === "done") {
				registry.upsert(terminalWithoutResult(pane));
				ctx.ui?.notify?.(
					`Reconciled ${pane.workerId}: Herdr ${status} with no valid result → ${
						pane.role === "implementer" ? "uncertain" : "failed"
					}`,
				);
				continue;
			}
			// unknown / missing / other
			registry.upsert(terminalWithoutResult(pane));
			ctx.ui?.notify?.(
				`Reconciled ${pane.workerId}: Herdr status ${status ?? "missing"} → ${
					pane.role === "implementer" ? "uncertain" : "failed"
				}`,
			);
		} catch {
			registry.upsert(terminalWithoutResult(pane));
			ctx.ui?.notify?.(
				`Reconciled ${pane.workerId}: Herdr agent missing → ${
					pane.role === "implementer" ? "uncertain" : "failed"
				}`,
			);
		}
	}
}

async function signalActiveWorkersOnQuit(
	registry: PaneRegistry,
	client: HerdrClient,
): Promise<void> {
	const { snapshot } = registry.tryRead();
	const active = snapshot.panes.filter(
		(pane) => pane.status === "starting" || pane.status === "ready" || pane.status === "running",
	);
	await Promise.allSettled(
		active.map(async (pane) => {
			try {
				atomicWriteJson(path.join(pane.spoolRoot, "cancel.json"), {
					version: 1,
					runId: pane.runId,
					workerId: pane.workerId,
					reason: "parent_session_shutdown",
					issuedAt: new Date().toISOString(),
				});
			} catch {
				// best effort
			}
			try {
				await client.agentSendKeys(pane.agentName, ["ctrl+c"]);
			} catch {
				// best effort; do not close retained terminal panes
			}
		}),
	);
}

export default function (pi: ExtensionAPI): void {
	installMomoParent(pi);
}

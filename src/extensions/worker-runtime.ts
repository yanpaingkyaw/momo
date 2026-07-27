import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { createWorkspaceDiffTool } from "../delegation/workspace-diff.js";
import { getRole, isAgentName, type AgentName } from "../roles.js";
import {
	IMPLEMENTER_SYSTEM_PROMPT,
	PLANNER_SYSTEM_PROMPT,
	REVIEWER_SYSTEM_PROMPT,
	SCOUT_SYSTEM_PROMPT,
} from "../prompts.js";
import {
	appendEvent,
	atomicWriteJson,
	MAX_IPC_JSON_BYTES,
	type IpcEvent,
	type IpcResult,
} from "../ipc/spool.js";
import {
	IpcValidationError,
	sanitizeAssistantMessages,
	tryReadIpcJson,
	validateCancel,
	validateCommand,
} from "../ipc/validate.js";
import {
	DEFAULT_LEASE_WAIT_MS,
	LeaseWaitCancelledError,
	LeaseWaitTimeoutError,
	WriterLeaseManager,
	createLeaseToken,
} from "../lease/writer-lease.js";

const ROLE_PROMPTS: Record<AgentName, string> = {
	scout: SCOUT_SYSTEM_PROMPT,
	planner: PLANNER_SYSTEM_PROMPT,
	implementer: IMPLEMENTER_SYSTEM_PROMPT,
	reviewer: REVIEWER_SYSTEM_PROMPT,
};

const MUTATION_TOOLS = new Set(["bash", "edit", "write"]);

export interface WorkerRuntimeOptions {
	env?: NodeJS.ProcessEnv;
	leaseManager?: WriterLeaseManager;
	now?: () => number;
	sleep?: (ms: number) => Promise<void>;
	leaseWaitMs?: number;
}

export function installMomoWorker(pi: ExtensionAPI, options: WorkerRuntimeOptions = {}): void {
	const env = options.env ?? process.env;
	if (env.MOMO_WORKER !== "1") return;

	const roleName = env.MOMO_ROLE;
	const ipcDir = env.MOMO_IPC_DIR;
	const workerIdRaw = env.MOMO_WORKER_ID;
	const runIdRaw = env.MOMO_RUN_ID;
	const cwd = env.MOMO_CWD || process.cwd();
	if (!roleName || !isAgentName(roleName) || !ipcDir || !workerIdRaw || !runIdRaw) {
		throw new Error("Momo worker missing MOMO_ROLE / MOMO_IPC_DIR / MOMO_WORKER_ID / MOMO_RUN_ID");
	}
	const workerId: string = workerIdRaw;
	const runId: string = runIdRaw;

	const role = getRole(roleName);
	const sleep =
		options.sleep ?? ((ms: number) => new Promise((resolve) => setTimeout(resolve, ms)));
	const lease =
		options.leaseManager ??
		new WriterLeaseManager({
			sleep,
			...(options.now ? { now: options.now } : {}),
		});
	const leaseOwner = workerId;
	const leaseToken = createLeaseToken();
	const now = options.now ?? Date.now;
	const leaseWaitMs = options.leaseWaitMs ?? DEFAULT_LEASE_WAIT_MS;
	const identity = { runId, workerId };

	let assignedStarted = false;
	let acquiringLease = false;
	let resultWritten = false;
	let mutationToolsEnabled = false;
	let mutationAttempted = false;
	let leaseHeld = false;
	let eventSeq = 0;
	let heartbeatSeq = 0;
	let heartbeatTimer: ReturnType<typeof setInterval> | undefined;
	let pollTimer: ReturnType<typeof setInterval> | undefined;
	const assistantMessages: unknown[] = [];

	const readOnlyTools = role.tools.filter((tool) => !MUTATION_TOOLS.has(tool) && tool !== "workspace_diff");
	const paths = {
		ready: `${ipcDir}/ready.json`,
		command: `${ipcDir}/command.json`,
		cancel: `${ipcDir}/cancel.json`,
		heartbeat: `${ipcDir}/heartbeat.json`,
		events: `${ipcDir}/events.ndjson`,
		result: `${ipcDir}/result.json`,
	};

	// registerTool is allowed during extension loading; session_start is too late
	// for catalog membership when Pi resolves --tools + custom tools at startup.
	if (role.name === "reviewer") {
		pi.registerTool(createWorkspaceDiffTool(cwd));
	}

	function emit(type: IpcEvent["type"], message?: string, extra: Partial<IpcEvent> = {}): void {
		eventSeq += 1;
		const event: IpcEvent = {
			version: 1,
			type,
			at: new Date(now()).toISOString(),
			runId,
			workerId,
			seq: eventSeq,
			...extra,
		};
		if (message !== undefined) event.message = message.slice(0, 8192);
		appendEvent(paths.events, event);
	}

	function activeReadOnlyTools(): string[] {
		const tools = [...readOnlyTools];
		if (role.name === "reviewer") tools.push("workspace_diff");
		return tools;
	}

	function disableMutationTools(): void {
		mutationToolsEnabled = false;
		pi.setActiveTools(activeReadOnlyTools());
	}

	function enableMutationTools(): void {
		if (!role.canWrite) return;
		if (!lease.validate(cwd, leaseOwner, leaseToken)) {
			throw new Error("Writer lease invalid");
		}
		mutationToolsEnabled = true;
		pi.setActiveTools([...readOnlyTools, "bash", "edit", "write"]);
	}

	function writeHeartbeat(): void {
		heartbeatSeq += 1;
		atomicWriteJson(paths.heartbeat, {
			version: 1,
			runId,
			workerId,
			at: new Date(now()).toISOString(),
			seq: heartbeatSeq,
		});
		if (leaseHeld) {
			try {
				lease.heartbeat(cwd, leaseOwner, leaseToken);
			} catch {
				disableMutationTools();
			}
		}
	}

	function writeResultDurable(
		partial: Omit<IpcResult, "version" | "runId" | "workerId" | "finishedAt" | "messages"> & {
			messages?: unknown[];
		},
	): boolean {
		// 1) Disable mutation tools before any lease/result mutation.
		disableMutationTools();

		let status = partial.status;
		let uncertainWrite = partial.uncertainWrite === true;
		let errorMessage = partial.errorMessage;

		// 2) For clean (non-uncertain) outcomes, release and confirm THIS owner+token is gone
		// BEFORE result publish. A foreign owner acquiring immediately is not failure.
		if (leaseHeld && !uncertainWrite) {
			try {
				lease.release(cwd, leaseOwner, leaseToken);
				leaseHeld = false;
				if (lease.stillHeldBy(cwd, leaseOwner, leaseToken)) {
					throw new Error("Writer lease still held by this worker after release");
				}
			} catch (error) {
				status = "failed";
				uncertainWrite = true;
				errorMessage = `Writer lease release failed: ${
					error instanceof Error ? error.message : String(error)
				}`;
				// Retain the lock for supervised cleanup.
			}
		}

		const messages = sanitizeAssistantMessages(
			partial.messages ?? assistantMessages,
			Math.floor(MAX_IPC_JSON_BYTES * 0.75),
		);
		const payload: IpcResult = {
			version: 1,
			runId,
			workerId,
			status,
			messages,
			finishedAt: new Date(now()).toISOString(),
		};
		if (partial.stopReason !== undefined) payload.stopReason = partial.stopReason;
		if (errorMessage !== undefined) payload.errorMessage = errorMessage;
		if (uncertainWrite) payload.uncertainWrite = true;
		if (partial.usage !== undefined) payload.usage = partial.usage;

		// Never publish clean success when release failed / uncertain.
		if (uncertainWrite && payload.status === "completed") {
			payload.status = "failed";
		}

		let written = false;
		for (let attempt = 0; attempt < 3; attempt += 1) {
			try {
				atomicWriteJson(paths.result, payload, MAX_IPC_JSON_BYTES);
				written = true;
				break;
			} catch {
				// retry
			}
		}
		if (!written) return false;

		resultWritten = true;
		if (heartbeatTimer) clearInterval(heartbeatTimer);
		if (pollTimer) clearInterval(pollTimer);
		return true;
	}

	async function handleTerminalSkipOrCancel(reason: string, status: "aborted" | "failed"): Promise<void> {
		if (resultWritten) return;
		writeResultDurable({
			status,
			messages: assistantMessages,
			errorMessage: reason,
			uncertainWrite: role.canWrite && mutationAttempted,
		});
		emit(status === "aborted" ? "aborted" : "failed", reason);
	}

	pi.on("session_start", async (_event, ctx) => {
		if (ctx.hasUI !== true) return;

		// Implementer: read-only until lease. Reviewer: exact role list incl. workspace_diff.
		// Scout/planner: exact read-only list. No prompt can run before this completes.
		pi.setActiveTools(activeReadOnlyTools());

		atomicWriteJson(paths.ready, {
			version: 1,
			runId,
			workerId,
			readyAt: new Date(now()).toISOString(),
		});
		emit("state", "ready", { state: "ready" });

		heartbeatTimer = setInterval(writeHeartbeat, 3_000);
		heartbeatTimer.unref?.();
		writeHeartbeat();

		pollTimer = setInterval(() => {
			void pollCommands(ctx);
		}, 100);
		pollTimer.unref?.();
	});

	function cancelPending(): string | undefined {
		const cancelRaw = tryReadIpcJson(paths.cancel);
		if (!cancelRaw) return undefined;
		// Invalid cancellation IPC is a terminal protocol failure; never silently
		// ignore it and continue toward mutation.
		return validateCancel(cancelRaw, identity).reason || "cancelled";
	}

	async function pollCommands(ctx: ExtensionContext): Promise<void> {
		if (resultWritten || acquiringLease) return;
		try {
			const cancelReason = cancelPending();
			if (cancelReason) {
				if (!assignedStarted) {
					await handleTerminalSkipOrCancel(cancelReason || "cancelled_before_prompt", "aborted");
					return;
				}
				ctx.abort();
				return;
			}

			if (assignedStarted) return;
			const commandRaw = tryReadIpcJson(paths.command);
			if (!commandRaw) return;
			const command = validateCommand(commandRaw, identity);
			if (command.type === "skip" || command.type === "cancel") {
				await handleTerminalSkipOrCancel(command.reason, "aborted");
				return;
			}

			assignedStarted = true;
			if (role.canWrite) {
				acquiringLease = true;
				pi.setActiveTools(activeReadOnlyTools());
				emit("state", "waiting_for_writer_lease", { state: "waiting_for_writer_lease" });
				try {
					await lease.waitAcquire(cwd, leaseOwner, leaseToken, {
						deadlineMs: leaseWaitMs,
						now,
						sleep,
						shouldCancel: async () => cancelPending() !== undefined,
						onWaiting: (info) => {
							emit(
								"state",
								`waiting_for_writer_lease${info.holder ? ` holder=${info.holder}` : ""}`,
								{ state: "waiting_for_writer_lease" },
							);
						},
					});
					leaseHeld = true;
					enableMutationTools();
				} catch (error) {
					if (error instanceof LeaseWaitCancelledError) {
						const reason = cancelPending() || "cancelled_while_waiting_for_lease";
						await handleTerminalSkipOrCancel(reason, "aborted");
						return;
					}
					if (error instanceof LeaseWaitTimeoutError) {
						writeResultDurable({
							status: "failed",
							messages: assistantMessages,
							errorMessage: error.message,
							uncertainWrite: false,
						});
						emit("failed", error.message);
						return;
					}
					throw error;
				} finally {
					acquiringLease = false;
				}
			}
			emit("started", `${role.name} started`);
			// ExtensionContext has no prompt(); inject via ExtensionAPI.sendUserMessage.
			if (command.type !== "prompt") {
				throw new Error("Expected prompt command after skip/cancel filter");
			}
			pi.sendUserMessage(command.task);
		} catch (error) {
			const message =
				error instanceof IpcValidationError || error instanceof Error
					? error.message
					: String(error);
			writeResultDurable({
				status: "failed",
				messages: assistantMessages,
				errorMessage: message,
				uncertainWrite: role.canWrite && mutationAttempted,
			});
			emit("failed", message);
		}
	}

	pi.on("input", async (event, ctx) => {
		if (event.source === "extension") {
			return { action: "continue" as const };
		}
		if (!resultWritten) {
			ctx.ui?.notify?.(
				"Momo worker accepts only the assigned task while queued/running; interactive input ignored.",
			);
			return { action: "handled" as const };
		}
		// After terminal settlement: allow prompts but keep tools read-only.
		disableMutationTools();
		return { action: "continue" as const };
	});

	pi.on("before_agent_start", async () => ({
		systemPrompt: ROLE_PROMPTS[role.name],
	}));

	pi.on("message_update", (event) => {
		const delta = (event as { assistantMessageEvent?: { type?: string; delta?: string } })
			.assistantMessageEvent;
		if (delta?.type === "text_delta" && delta.delta) {
			emit("text", String(delta.delta));
		}
	});

	pi.on("message_end", (event) => {
		const message = (event as { message?: { role?: string } }).message;
		if (message?.role === "assistant") {
			assistantMessages.push(message);
		}
	});

	pi.on("tool_call", async (event) => {
		const toolName = String((event as { toolName?: string }).toolName ?? "");
		if (toolName === "delegate") {
			return { block: true, reason: "Workers cannot use delegate" };
		}
		if (MUTATION_TOOLS.has(toolName)) {
			if (!role.canWrite || !mutationToolsEnabled || !lease.validate(cwd, leaseOwner, leaseToken)) {
				return { block: true, reason: "Mutation tools require a valid writer lease" };
			}
			mutationAttempted = true;
		}
		emit("tool_started", `Running ${toolName}`, { toolName });
		return undefined;
	});

	pi.on("tool_result", (event) => {
		const toolName = String((event as { toolName?: string }).toolName ?? "tool");
		emit("tool_finished", `${toolName} finished`, {
			toolName,
			state: (event as { isError?: boolean }).isError ? "failed" : "ok",
		});
	});

	pi.on("user_bash", async () => ({
		result: {
			output: "Direct user_bash is disabled in Momo workers",
			exitCode: 126,
			cancelled: false,
			truncated: false,
		},
	}));

	pi.on("agent_settled", async (_event, ctx) => {
		if (!assignedStarted || resultWritten) return;
		if (ctx.isIdle() !== true) return;

		const last = assistantMessages[assistantMessages.length - 1] as
			| { stopReason?: string; errorMessage?: string }
			| undefined;
		const stopReason = last?.stopReason;
		const allowedStop =
			typeof stopReason === "string" &&
			["stop", "length", "toolUse", "error", "aborted", "cancelled"].includes(stopReason)
				? stopReason
				: undefined;
		if (stopReason === "aborted") {
			writeResultDurable({
				status: "aborted",
				messages: assistantMessages,
				stopReason: "aborted",
				errorMessage: last?.errorMessage ?? "aborted",
				uncertainWrite: role.canWrite && mutationAttempted,
			});
			emit("aborted", "aborted");
			return;
		}
		if (stopReason === "error") {
			writeResultDurable({
				status: "failed",
				messages: assistantMessages,
				stopReason: "error",
				errorMessage: last?.errorMessage ?? "error",
				uncertainWrite: role.canWrite && mutationAttempted,
			});
			emit("failed", "failed");
			return;
		}
		const ok = writeResultDurable({
			status: "completed",
			messages: assistantMessages,
			...(allowedStop !== undefined ? { stopReason: allowedStop } : {}),
		});
		if (ok) emit("completed", "completed");
	});
}

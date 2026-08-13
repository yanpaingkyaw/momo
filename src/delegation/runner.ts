import {
	createAgentSession,
	createExtensionRuntime,
	SessionManager,
	type CreateAgentSessionOptions,
	type ModelRuntime,
	type ResourceLoader,
	type SettingsManager,
	type ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import type { AgentName, AgentRole } from "../roles.js";
import { getSupportedThinkingLevels } from "@earendil-works/pi-ai";
import {
	findAuthenticatedModel,
	freezeConfigSnapshot,
	isConfigPolicyFeatureActive,
	resolveConcreteAssignmentPolicy,
	type ModelPolicySnapshot,
	type MomoConfigV1,
	type MomoConfigSnapshot,
} from "../config/model-policy.js";
import {
	addUsage,
	calculateDelegationStatus,
	emptyUsage,
	extractLastAssistantText,
	summarizeUsage,
	truncateUtf8,
	type DelegationResult,
	type TaskResult,
} from "./results.js";
import { readMomoConfig } from "../config/momo-config.js";
import { mapWithConcurrency } from "./scheduler.js";

export const MAX_DELEGATED_TASKS = 8;
export const MAX_PARALLEL_CONCURRENCY = 4;
export const CHILD_ABORT_TIMEOUT_MS = 5_000;

export interface DelegatedTask {
	agent: AgentName;
	task: string;
}

export type DelegationRequest =
	| { mode: "single"; agent: AgentName; task: string }
	| { mode: "parallel"; tasks: DelegatedTask[] }
	| { mode: "chain"; steps: DelegatedTask[] };

export type DelegationProgressPhase =
	| "queued"
	| "started"
	| "text"
	| "tool_started"
	| "tool_finished"
	| "completed"
	| "failed"
	| "aborted"
	| "aggregate";

export interface DelegationProgress {
	mode: DelegationRequest["mode"];
	phase: DelegationProgressPhase;
	index?: number;
	agent?: AgentName;
	message: string;
	counts?: {
		queued: number;
		running: number;
		completed: number;
		failed: number;
	};
}

export interface RunDelegationOptions {
	signal?: AbortSignal;
	onProgress?: (progress: DelegationProgress) => void;
}

export interface ChildSession {
	readonly messages: readonly unknown[];
	readonly agent?: {
		waitForIdle(): Promise<void>;
	};
	subscribe(listener: (event: any) => void): () => void;
	prompt(text: string): Promise<void>;
	abort(): Promise<void>;
	/** Terminal-skip a never-prompted worker (Herdr). */
	skip?(reason: string): Promise<void>;
	dispose(): void | Promise<void>;
}

export interface ChildSessionFactoryInput {
	cwd: string;
	role: AgentRole;
	model?: CreateAgentSessionOptions["model"];
	thinkingLevel?: CreateAgentSessionOptions["thinkingLevel"];
	/** Concrete assignment policy when configured; omitted in legacy mode. */
	assignmentPolicy?: ModelPolicySnapshot;
	/** Whether config policy feature is active for this assignment (frozen at allocation). */
	policyFeatureActive?: boolean;
}

export type ChildSessionFactory = (input: ChildSessionFactoryInput) => Promise<ChildSession>;

export interface PiChildSessionFactoryOptions {
	cwd: string;
	agentDir?: string;
	modelRuntime: ModelRuntime;
	settingsManager: SettingsManager;
	model?: CreateAgentSessionOptions["model"];
	thinkingLevel?: CreateAgentSessionOptions["thinkingLevel"];
	agentsFiles?: readonly { path: string; content: string }[];
	reviewerTools?: readonly ToolDefinition[];
}

export interface DelegationRunnerOptions {
	cwd: string;
	roles: readonly AgentRole[];
	createChildSession: ChildSessionFactory;
	maxParallelConcurrency?: number;
	abortTimeoutMs?: number;
	readConfig?: () => MomoConfigV1 | undefined;
	getModelRegistry?: () => import("@earendil-works/pi-coding-agent").ModelRegistry;
}

export interface DelegationRunner {
	run(request: DelegationRequest, options?: RunDelegationOptions): Promise<DelegationResult>;
	validate(request: unknown): asserts request is DelegationRequest;
}

function createChildResourceLoader(
	role: AgentRole,
	agentsFiles: readonly { path: string; content: string }[],
): ResourceLoader {
	return {
		getExtensions: () => ({ extensions: [], errors: [], runtime: createExtensionRuntime() }),
		getSkills: () => ({ skills: [], diagnostics: [] }),
		getPrompts: () => ({ prompts: [], diagnostics: [] }),
		getThemes: () => ({ themes: [], diagnostics: [] }),
		getAgentsFiles: () => ({ agentsFiles: [...agentsFiles] }),
		getSystemPrompt: () => role.systemPrompt,
		getAppendSystemPrompt: () => [],
		extendResources: () => {},
		reload: async () => {},
	};
}

/** Build the production child factory while keeping the runner provider-free in tests. */
export function createPiChildSessionFactory(options: PiChildSessionFactoryOptions): ChildSessionFactory {
	const cwd = options.cwd;
	const agentsFiles = options.agentsFiles ?? [];
	const reviewerTools = options.reviewerTools ?? [];

	return async ({ cwd: childCwd, role, model, thinkingLevel }) => {
		if (childCwd !== cwd) {
			throw new Error("Child working directory must match the parent working directory");
		}

		const resourceLoader = createChildResourceLoader(role, agentsFiles);
		const customTools = role.name === "reviewer" ? [...reviewerTools] : [];
		const { session } = await createAgentSession({
			cwd,
			...(options.agentDir === undefined ? {} : { agentDir: options.agentDir }),
			...(model === undefined ? {} : { model }),
			...(thinkingLevel === undefined ? {} : { thinkingLevel }),
			...(options.model === undefined || model !== undefined ? {} : { model: options.model }),
			...(options.thinkingLevel === undefined || thinkingLevel !== undefined
				? {}
				: { thinkingLevel: options.thinkingLevel }),
			modelRuntime: options.modelRuntime,
			settingsManager: options.settingsManager,
			resourceLoader,
			tools: [...role.tools],
			customTools,
			sessionManager: SessionManager.inMemory(cwd),
		});

		return session as ChildSession;
	};
}

function errorMessage(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

function roleMap(roles: readonly AgentRole[]): ReadonlyMap<AgentName, AgentRole> {
	return new Map(roles.map((role) => [role.name, role]));
}

function assertOnlyKeys(value: Record<string, unknown>, keys: readonly string[], label: string): void {
	const allowed = new Set(keys);
	const unexpected = Object.keys(value).filter((key) => !allowed.has(key));
	if (unexpected.length > 0) {
		throw new Error(`${label} contains unsupported field${unexpected.length === 1 ? "" : "s"}: ${unexpected.join(", ")}`);
	}
}

function validateTask(
	value: unknown,
	label: string,
	roles: ReadonlyMap<AgentName, AgentRole>,
): asserts value is DelegatedTask {
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		throw new Error(`${label} must be an object`);
	}

	const task = value as Record<string, unknown>;
	assertOnlyKeys(task, ["agent", "task"], label);
	if (typeof task.agent !== "string" || !roles.has(task.agent as AgentName)) {
		throw new Error(`${label} has unknown agent: ${String(task.agent)}`);
	}
	if (typeof task.task !== "string" || task.task.trim().length === 0) {
		throw new Error(`${label}.task must contain non-whitespace text`);
	}
}

export function validateDelegationRequest(
	request: unknown,
	roles: ReadonlyMap<AgentName, AgentRole>,
): asserts request is DelegationRequest {
	if (typeof request !== "object" || request === null || Array.isArray(request)) {
		throw new Error("Delegation request must be an object");
	}

	const value = request as Record<string, unknown>;
	if (value.mode === "single") {
		assertOnlyKeys(value, ["mode", "agent", "task"], "Single delegation");
		validateTask({ agent: value.agent, task: value.task }, "Single delegation", roles);
		return;
	}

	if (value.mode === "parallel") {
		assertOnlyKeys(value, ["mode", "tasks"], "Parallel delegation");
		if (!Array.isArray(value.tasks) || value.tasks.length < 1 || value.tasks.length > MAX_DELEGATED_TASKS) {
			throw new Error(`Parallel delegation must contain between 1 and ${MAX_DELEGATED_TASKS} tasks`);
		}
		value.tasks.forEach((task, index) => validateTask(task, `Parallel task ${index + 1}`, roles));
		const writer = value.tasks.find((task) => roles.get((task as DelegatedTask).agent)?.canWrite);
		if (writer) throw new Error("Parallel delegation cannot include a write-capable agent");
		return;
	}

	if (value.mode === "chain") {
		assertOnlyKeys(value, ["mode", "steps"], "Chain delegation");
		if (!Array.isArray(value.steps) || value.steps.length < 1 || value.steps.length > MAX_DELEGATED_TASKS) {
			throw new Error(`Chain delegation must contain between 1 and ${MAX_DELEGATED_TASKS} steps`);
		}
		value.steps.forEach((step, index) => validateTask(step, `Chain step ${index + 1}`, roles));
		return;
	}

	throw new Error(`Unknown delegation mode: ${String(value.mode)}`);
}

function lastAssistantFailure(messages: readonly unknown[]): { stopReason?: string; message?: string } {
	for (let index = messages.length - 1; index >= 0; index--) {
		const message = messages[index] as {
			role?: unknown;
			stopReason?: unknown;
			errorMessage?: unknown;
		};
		if (message?.role !== "assistant") continue;
		return {
			...(typeof message.stopReason === "string" ? { stopReason: message.stopReason } : {}),
			...(typeof message.errorMessage === "string" ? { message: message.errorMessage } : {}),
		};
	}
	return {};
}

function skippedResult(task: DelegatedTask): TaskResult {
	return {
		agent: task.agent,
		task: task.task,
		status: "skipped",
		output: "",
		outputTruncated: false,
		usage: emptyUsage(),
	};
}

function taskOutput(output: string): Pick<TaskResult, "output" | "outputTruncated" | "fullOutput"> {
	const visible = truncateUtf8(output);
	return {
		output: visible.text,
		outputTruncated: visible.truncated,
		...(visible.truncated ? { fullOutput: output } : {}),
	};
}

async function waitWithTimeout(promise: Promise<unknown>, timeoutMs: number): Promise<void> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		await Promise.race([
			promise,
			new Promise<void>((resolve) => {
				timer = setTimeout(resolve, timeoutMs);
			}),
		]);
	} finally {
		if (timer) clearTimeout(timer);
	}
}

export function createDelegationRunner(options: DelegationRunnerOptions): DelegationRunner {
	const cwd = options.cwd;
	const roles = roleMap(options.roles);
	const concurrency = options.maxParallelConcurrency ?? MAX_PARALLEL_CONCURRENCY;
	const abortTimeoutMs = options.abortTimeoutMs ?? CHILD_ABORT_TIMEOUT_MS;
	const readConfig = options.readConfig ?? readMomoConfig;
	let writerTail = Promise.resolve();
	const reportProgress = (runOptions: RunDelegationOptions, progress: DelegationProgress) => {
		try {
			runOptions.onProgress?.(progress);
		} catch {
			// Progress is observational and cannot change orchestration behavior.
		}
	};

	const executeTask = async (
		mode: DelegationRequest["mode"],
		task: DelegatedTask,
		index: number,
		runOptions: RunDelegationOptions,
		runConfig: MomoConfigSnapshot | undefined,
	): Promise<TaskResult> => {
		const role = roles.get(task.agent);
		if (!role) throw new Error(`Unknown agent: ${task.agent}`);
		if (runOptions.signal?.aborted) {
			return {
				...skippedResult(task),
				status: "aborted",
				error: { message: "Delegation aborted before child startup" },
			};
		}

		let session: ChildSession | undefined;
		let unsubscribe: (() => void) | undefined;
		let abortPromise: Promise<void> | undefined;
		let textBuffer = "";
		let lastTextUpdate = 0;
		let textTimer: ReturnType<typeof setTimeout> | undefined;

		const emit = (phase: DelegationProgressPhase, message: string) => {
			reportProgress(runOptions, { mode, phase, index, agent: task.agent, message });
		};
		const flushText = () => {
			if (!textBuffer) return;
			const text = textBuffer;
			textBuffer = "";
			lastTextUpdate = Date.now();
			emit("text", text);
		};
		const onEvent = (event: any) => {
			if (event?.type === "message_update" && event.assistantMessageEvent?.type === "text_delta") {
				textBuffer += String(event.assistantMessageEvent.delta ?? "");
				const delay = Math.max(0, 100 - (Date.now() - lastTextUpdate));
				if (delay === 0) flushText();
				else if (!textTimer) {
					textTimer = setTimeout(() => {
						textTimer = undefined;
						flushText();
					}, delay);
				}
				return;
			}
			if (event?.type === "tool_execution_start") {
				emit("tool_started", `Running ${String(event.toolName ?? "tool")}`);
			} else if (event?.type === "tool_execution_end") {
				emit("tool_finished", `${String(event.toolName ?? "Tool")} ${event.isError ? "failed" : "finished"}`);
			}
		};

		try {
			const registry = options.getModelRegistry?.();
			const policyFeatureActive = runConfig !== undefined;
			let sessionInput: ChildSessionFactoryInput = { cwd, role, policyFeatureActive };
			const assignmentPolicy = resolveConcreteAssignmentPolicy(task.agent, runConfig);
			if (assignmentPolicy && !registry) {
				return {
					agent: task.agent,
					task: task.task,
					status: "failed",
					output: "",
					outputTruncated: false,
					usage: emptyUsage(),
					error: {
						message: "Model registry unavailable for configured assignment policy",
					},
				};
			}
			if (registry && assignmentPolicy) {
				const model = findAuthenticatedModel(registry, assignmentPolicy);
				if (!model) {
					return {
						agent: task.agent,
						task: task.task,
						status: "failed",
						output: "",
						outputTruncated: false,
						usage: emptyUsage(),
						error: {
							message: `Model unavailable or auth missing: ${assignmentPolicy.provider}/${assignmentPolicy.model}`,
						},
					};
				}
				const levels = getSupportedThinkingLevels(model);
				if (!levels.includes(assignmentPolicy.reasoning)) {
					return {
						agent: task.agent,
						task: task.task,
						status: "failed",
						output: "",
						outputTruncated: false,
						usage: emptyUsage(),
						error: {
							message: `Reasoning ${assignmentPolicy.reasoning} unsupported for ${assignmentPolicy.provider}/${assignmentPolicy.model}`,
						},
					};
				}
				sessionInput = {
					...sessionInput,
					model,
					thinkingLevel: assignmentPolicy.reasoning,
					assignmentPolicy,
					policyFeatureActive: true,
				};
			} else if (assignmentPolicy) {
				sessionInput = { ...sessionInput, assignmentPolicy, policyFeatureActive: true };
			}

			// Allocate lazily when the concurrency slot (or single/chain step) runs.
			session = await options.createChildSession(sessionInput);
			unsubscribe = session.subscribe(onEvent);
			const abortChild = () => {
				if (!abortPromise && session) abortPromise = session.abort().catch(() => {});
			};
			runOptions.signal?.addEventListener("abort", abortChild, { once: true });
			try {
				if (runOptions.signal?.aborted) {
					abortChild();
					throw new Error("Delegation aborted before child prompt");
				}
				emit("started", `${task.agent} started`);
				await session.prompt(task.task);
				await session.agent?.waitForIdle();
			} finally {
				runOptions.signal?.removeEventListener("abort", abortChild);
				if (runOptions.signal?.aborted) abortChild();
				if (abortPromise) await waitWithTimeout(abortPromise, abortTimeoutMs);
			}

			flushText();
			const messages = session.messages;
			const usage = summarizeUsage(messages);
			const capturedOutput = extractLastAssistantText(messages) ?? "";
			if (runOptions.signal?.aborted) {
				emit("aborted", `${task.agent} aborted`);
				return {
					agent: task.agent,
					task: task.task,
					status: "aborted",
					...taskOutput(capturedOutput),
					usage,
					error: {
						message: "Delegation aborted",
						stopReason: "aborted",
					},
				};
			}

			const output = extractLastAssistantText(messages);
			const failure = lastAssistantFailure(messages);
			if (failure.stopReason === "error" || failure.stopReason === "aborted") {
				const status = failure.stopReason === "aborted" ? "aborted" : "failed";
				emit(status, `${task.agent} ${status}`);
				return {
					agent: task.agent,
					task: task.task,
					status,
					...taskOutput(output ?? ""),
					usage,
					error: {
						message: failure.message ?? `Child stopped with reason: ${failure.stopReason}`,
						stopReason: failure.stopReason,
					},
				};
			}
			if (!output) {
				emit("failed", `${task.agent} completed without a final response`);
				return {
					agent: task.agent,
					task: task.task,
					status: "failed",
					output: "",
					outputTruncated: false,
					usage,
					error: {
						message: "Child completed without a final response",
						...(failure.stopReason === undefined ? {} : { stopReason: failure.stopReason }),
					},
				};
			}

			const result: TaskResult = {
				agent: task.agent,
				task: task.task,
				status: "completed",
				...taskOutput(output),
				usage,
			};
			emit("completed", `${task.agent} completed`);
			return result;
		} catch (error) {
			const aborted = runOptions.signal?.aborted === true;
			const uncertain =
				typeof error === "object" &&
				error !== null &&
				"uncertainWrite" in error &&
				(error as { uncertainWrite?: unknown }).uncertainWrite === true;
			const status = uncertain ? "failed" : aborted ? "aborted" : "failed";
			const messages = session?.messages ?? [];
			const preserved = extractLastAssistantText(messages) ?? "";
			const failure = lastAssistantFailure(messages);
			emit(status, `${task.agent} ${status}`);
			const stopReason =
				typeof error === "object" &&
				error !== null &&
				"stopReason" in error &&
				typeof (error as { stopReason?: unknown }).stopReason === "string"
					? (error as { stopReason: string }).stopReason
					: failure.stopReason;
			return {
				agent: task.agent,
				task: task.task,
				status,
				...taskOutput(preserved),
				usage: session ? summarizeUsage(messages) : emptyUsage(),
				error: {
					message: uncertain
						? `Uncertain write: ${errorMessage(error)}`
						: aborted
							? "Delegation aborted"
							: errorMessage(error),
					...(stopReason === undefined ? {} : { stopReason }),
				},
			};
		} finally {
			if (textTimer) clearTimeout(textTimer);
			flushText();
			try {
				unsubscribe?.();
			} catch {
				// Preserve the task's primary outcome when event cleanup fails.
			}
			try {
				await session?.dispose();
			} catch {
				// Session cleanup diagnostics must not replace the task result.
			}
		}
	};

	const runTask = async (
		mode: DelegationRequest["mode"],
		task: DelegatedTask,
		index: number,
		runOptions: RunDelegationOptions,
		runConfig: MomoConfigSnapshot | undefined,
	): Promise<TaskResult> => {
		const role = roles.get(task.agent);
		if (!role?.canWrite) return executeTask(mode, task, index, runOptions, runConfig);

		let release!: () => void;
		const lock = new Promise<void>((resolve) => {
			release = resolve;
		});
		const precedingWriter = writerTail;
		writerTail = precedingWriter.then(() => lock);
		await precedingWriter;
		try {
			return await executeTask(mode, task, index, runOptions, runConfig);
		} finally {
			release();
		}
	};

	const snapshotRunConfig = (): MomoConfigSnapshot | undefined => {
		const raw = readConfig();
		if (!raw || !isConfigPolicyFeatureActive(raw)) return undefined;
		return freezeConfigSnapshot(raw);
	};

	const finish = (mode: DelegationRequest["mode"], results: TaskResult[]): DelegationResult => ({
		mode,
		status: calculateDelegationStatus(mode, results),
		results,
		usage: addUsage(...results.map((result) => result.usage)),
	});

	return {
		validate(request: unknown): asserts request is DelegationRequest {
			validateDelegationRequest(request, roles);
		},

		async run(request, runOptions = {}) {
			validateDelegationRequest(request, roles);
			const runConfig = snapshotRunConfig();

			if (request.mode === "single") {
				const task = { agent: request.agent, task: request.task };
				reportProgress(runOptions, { mode: request.mode, phase: "queued", index: 0, agent: task.agent, message: `${task.agent} queued` });
				return finish(request.mode, [await runTask(request.mode, task, 0, runOptions, runConfig)]);
			}

			if (request.mode === "parallel") {
				for (const [index, task] of request.tasks.entries()) {
					reportProgress(runOptions, { mode: request.mode, phase: "queued", index, agent: task.agent, message: `${task.agent} queued` });
				}
				const scheduled = await mapWithConcurrency(
					request.tasks,
					async (task, index, signal) =>
						runTask(request.mode, task, index, {
							...runOptions,
							...(signal === undefined ? {} : { signal }),
						}, runConfig),
					{
						concurrency,
						...(runOptions.signal === undefined ? {} : { signal: runOptions.signal }),
						onStateChange: (counts) => {
							reportProgress(runOptions, {
								mode: request.mode,
								phase: "aggregate",
								message: `${counts.completed} completed, ${counts.running} running, ${counts.queued} queued`,
								counts,
							});
						},
					},
				);
				const results: TaskResult[] = [];
				for (let index = 0; index < scheduled.length; index += 1) {
					const item = scheduled[index];
					const task = request.tasks[index];
					if (!task || !item) throw new Error(`Missing parallel task at index ${index}`);
					if (item.status === "fulfilled") {
						results.push(item.value);
						continue;
					}
					if (item.status === "skipped") {
						results.push(skippedResult(task));
						continue;
					}
					results.push({
						...skippedResult(task),
						status: runOptions.signal?.aborted ? "aborted" : "failed",
						error: { message: errorMessage(item.reason) },
					});
				}
				if (runOptions.signal?.aborted && results.every((result) => result.status === "skipped")) {
					const first = request.tasks[0];
					if (first) {
						results[0] = {
							...skippedResult(first),
							status: "aborted",
							error: { message: "Delegation aborted before child startup" },
						};
					}
				}
				return finish(request.mode, results);
			}

			const results: TaskResult[] = [];
			let previous = "";
			for (let index = 0; index < request.steps.length; index++) {
				const step = request.steps[index];
				if (!step) throw new Error(`Missing chain step at index ${index}`);
				if (runOptions.signal?.aborted) {
					results.push({
						...skippedResult(step),
						status: "aborted",
						error: { message: "Delegation aborted before child startup" },
					});
					for (let rest = index + 1; rest < request.steps.length; rest += 1) {
						const remaining = request.steps[rest];
						if (remaining) results.push(skippedResult(remaining));
					}
					break;
				}
				const task = {
					agent: step.agent,
					task: step.task.replaceAll("{previous}", previous),
				};
				reportProgress(runOptions, { mode: request.mode, phase: "queued", index, agent: task.agent, message: `${task.agent} queued` });
				const result = await runTask(request.mode, task, index, runOptions, runConfig);
				results.push(result);
				if (result.status !== "completed") {
					for (let rest = index + 1; rest < request.steps.length; rest += 1) {
						const remaining = request.steps[rest];
						if (remaining) results.push(skippedResult(remaining));
					}
					break;
				}
				previous = result.output;
			}
			return finish(request.mode, results);
		},
	};
}

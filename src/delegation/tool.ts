import { defineTool } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { ROLE_LIST, type AgentName } from "../roles.js";
import type {
	DelegationProgress,
	DelegationRequest,
	DelegationRunner,
} from "./runner.js";
import {
	createDelegationRunner,
	createPiChildSessionFactory,
	type PiChildSessionFactoryOptions,
} from "./runner.js";
import type { DelegationResult } from "./results.js";
import { createWorkspaceDiffTool } from "./workspace-diff.js";

const AGENT_NAMES = ["scout", "planner", "implementer", "reviewer"] as const;
const MAX_TASKS = 8;

// Pi recommends a direct string enum instead of anyOf/const for Google models.
function stringEnum<const T extends readonly string[]>(
	values: T,
	options: { description?: string } = {},
) {
	return Type.Unsafe<T[number]>({ type: "string", enum: values, ...options });
}

const AgentNameSchema = stringEnum(AGENT_NAMES, {
	description: "Built-in specialist to run",
});

const DelegatedTaskSchema = Type.Object(
	{
		agent: AgentNameSchema,
		task: Type.String({ minLength: 1, description: "A bounded task for this specialist" }),
	},
	{ additionalProperties: false },
);

export const DelegateParameters = Type.Union([
	Type.Object(
		{
			mode: stringEnum(["single"] as const),
			agent: AgentNameSchema,
			task: Type.String({ minLength: 1, description: "A bounded task for this specialist" }),
		},
		{ additionalProperties: false },
	),
	Type.Object(
		{
			mode: stringEnum(["parallel"] as const),
			tasks: Type.Array(DelegatedTaskSchema, { minItems: 1, maxItems: MAX_TASKS }),
		},
		{ additionalProperties: false },
	),
	Type.Object(
		{
			mode: stringEnum(["chain"] as const),
			steps: Type.Array(DelegatedTaskSchema, { minItems: 1, maxItems: MAX_TASKS }),
		},
		{ additionalProperties: false },
	),
]);

export interface RunningDelegationDetails {
	mode: DelegationRequest["mode"];
	status: "running";
	progress: DelegationProgress;
}

export type DelegateToolDetails = RunningDelegationDetails | DelegationResult;

export type CreateDelegateToolOptions = Omit<PiChildSessionFactoryOptions, "reviewerTools">;

function formatUsage(result: DelegationResult): string {
	const usage = result.usage;
	const tokens = `${usage.inputTokens} input, ${usage.outputTokens} output`;
	return `${tokens}, ${usage.turns} turn${usage.turns === 1 ? "" : "s"}, $${usage.costUsd.toFixed(4)}`;
}

export function formatDelegationResult(result: DelegationResult): string {
	const lines = [
		`Delegation ${result.status} (${result.mode}; ${result.results.length} task${result.results.length === 1 ? "" : "s"}).`,
	];
	for (const [index, task] of result.results.entries()) {
		lines.push(`\n${index + 1}. ${task.agent}: ${task.status}`);
		if (task.output) lines.push(task.output);
		if (task.error) {
			const stopReason = task.error.stopReason ? ` (${task.error.stopReason})` : "";
			lines.push(`Error${stopReason}: ${task.error.message}`);
		}
	}
	lines.push(`\nUsage: ${formatUsage(result)}`);
	return lines.join("\n");
}

function isRunner(value: DelegationRunner | CreateDelegateToolOptions): value is DelegationRunner {
	return "run" in value && "validate" in value;
}

export function createDelegateTool(runnerOrOptions: DelegationRunner | CreateDelegateToolOptions) {
	const runner: DelegationRunner = isRunner(runnerOrOptions)
		? runnerOrOptions
		: createDelegationRunner({
				cwd: runnerOrOptions.cwd,
				roles: ROLE_LIST,
				createChildSession: createPiChildSessionFactory({
					...runnerOrOptions,
					reviewerTools: [createWorkspaceDiffTool(runnerOrOptions.cwd)],
				}),
			});

	return defineTool({
		name: "delegate",
		label: "Delegate",
		description: [
			"Delegate bounded work to an isolated specialist agent.",
			"Use single for one task, parallel for up to eight independent read-only tasks,",
			"or chain for sequential tasks whose prompts may include {previous}.",
			"Only the implementer can edit files or run shell commands, and it cannot be used in parallel mode.",
		].join(" "),
		parameters: DelegateParameters,
		async execute(_toolCallId, params, signal, onUpdate) {
			const request = params as unknown as DelegationRequest;
			try {
				runner.validate(request);
			} catch (error) {
				throw new Error(error instanceof Error ? error.message : String(error));
			}

			const result = await runner.run(request, {
				...(signal === undefined ? {} : { signal }),
				...(onUpdate
					? { onProgress: (progress: DelegationProgress) => {
							onUpdate({
								content: [{ type: "text", text: progress.message }],
								details: { mode: request.mode, status: "running", progress } satisfies RunningDelegationDetails,
							});
						} }
					: {}),
			});
			return {
				content: [{ type: "text", text: formatDelegationResult(result) }],
				details: result as DelegateToolDetails,
			};
		},
	});
}

export type { AgentName, DelegationRequest };

import type { AgentName } from "../roles.js";

export const MAX_MODEL_VISIBLE_OUTPUT_BYTES = 50 * 1024;

export interface UsageSummary {
	inputTokens: number;
	outputTokens: number;
	cacheReadTokens: number;
	cacheWriteTokens: number;
	totalTokens: number;
	turns: number;
	costUsd: number;
}

export interface TaskError {
	message: string;
	stopReason?: string;
}

export interface TaskResult {
	agent: AgentName;
	task: string;
	status: "completed" | "failed" | "aborted" | "skipped";
	output: string;
	outputTruncated: boolean;
	/** Full in-memory output when output was truncated for model visibility. */
	fullOutput?: string;
	usage: UsageSummary;
	error?: TaskError;
}

export type DelegationMode = "single" | "parallel" | "chain";

export interface DelegationResult {
	mode: DelegationMode;
	status: "completed" | "partial" | "failed" | "aborted";
	results: TaskResult[];
	usage: UsageSummary;
}

export const EMPTY_USAGE: Readonly<UsageSummary> = Object.freeze({
	inputTokens: 0,
	outputTokens: 0,
	cacheReadTokens: 0,
	cacheWriteTokens: 0,
	totalTokens: 0,
	turns: 0,
	costUsd: 0,
});

export interface TruncatedOutput {
	text: string;
	truncated: boolean;
	omittedBytes: number;
	originalBytes: number;
	visibleBytes: number;
}

interface MessageLike {
	role?: unknown;
	content?: unknown;
	usage?: unknown;
}

interface UsageLike {
	input?: unknown;
	output?: unknown;
	cacheRead?: unknown;
	cacheWrite?: unknown;
	totalTokens?: unknown;
	cost?: unknown;
}

/** Return a mutable zero-valued usage record. */
export function emptyUsage(): UsageSummary {
	return { ...EMPTY_USAGE };
}

function asFiniteNumber(value: unknown): number {
	return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null;
}

/**
 * Find the newest non-blank text block in assistant history. Thinking and tool
 * call blocks are deliberately ignored.
 */
export function extractLastAssistantText(messages: readonly unknown[]): string | undefined {
	for (let messageIndex = messages.length - 1; messageIndex >= 0; messageIndex -= 1) {
		const message = messages[messageIndex] as MessageLike;
		if (!isRecord(message) || message.role !== "assistant" || !Array.isArray(message.content)) {
			continue;
		}

		for (let contentIndex = message.content.length - 1; contentIndex >= 0; contentIndex -= 1) {
			const part = message.content[contentIndex];
			if (isRecord(part) && part.type === "text" && typeof part.text === "string" && part.text.trim()) {
				return part.text;
			}
		}
	}

	return undefined;
}

/**
 * Sum token and cost counters across assistant turns. totalTokens represents
 * only the most recent assistant context, as reported by Pi.
 */
export function summarizeUsage(messages: readonly unknown[]): UsageSummary {
	const summary = emptyUsage();

	for (const candidate of messages) {
		const message = candidate as MessageLike;
		if (!isRecord(message) || message.role !== "assistant") continue;

		const usage = isRecord(message.usage) ? (message.usage as UsageLike) : undefined;
		summary.turns += 1;
		if (!usage) {
			summary.totalTokens = 0;
			continue;
		}

		summary.inputTokens += asFiniteNumber(usage.input);
		summary.outputTokens += asFiniteNumber(usage.output);
		summary.cacheReadTokens += asFiniteNumber(usage.cacheRead);
		summary.cacheWriteTokens += asFiniteNumber(usage.cacheWrite);
		summary.totalTokens = asFiniteNumber(usage.totalTokens);

		const cost = isRecord(usage.cost) ? usage.cost : undefined;
		summary.costUsd += asFiniteNumber(cost?.total);
	}

	return summary;
}

/** Aggregate independent task usage records, including each final context total. */
export function addUsage(...summaries: readonly UsageSummary[]): UsageSummary {
	const aggregate = emptyUsage();
	for (const summary of summaries) {
		aggregate.inputTokens += summary.inputTokens;
		aggregate.outputTokens += summary.outputTokens;
		aggregate.cacheReadTokens += summary.cacheReadTokens;
		aggregate.cacheWriteTokens += summary.cacheWriteTokens;
		aggregate.totalTokens += summary.totalTokens;
		aggregate.turns += summary.turns;
		aggregate.costUsd += summary.costUsd;
	}
	return aggregate;
}

function utf8Prefix(text: string, maximumBytes: number): string {
	let bytes = 0;
	let prefix = "";

	for (const codePoint of text) {
		const codePointBytes = Buffer.byteLength(codePoint, "utf8");
		if (bytes + codePointBytes > maximumBytes) break;
		prefix += codePoint;
		bytes += codePointBytes;
	}

	return prefix;
}

function truncationSuffix(omittedBytes: number): string {
	return `\n\n[Output truncated: ${omittedBytes} bytes omitted. Full output preserved in tool details.]`;
}

/**
 * Limit text by encoded byte length without splitting a Unicode code point.
 * The returned text, including its truncation notice, never exceeds maxBytes.
 */
export function truncateUtf8(
	text: string,
	maxBytes = MAX_MODEL_VISIBLE_OUTPUT_BYTES,
): TruncatedOutput {
	if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) {
		throw new RangeError("maxBytes must be a non-negative safe integer");
	}

	const originalBytes = Buffer.byteLength(text, "utf8");
	if (originalBytes <= maxBytes) {
		return {
			text,
			truncated: false,
			omittedBytes: 0,
			originalBytes,
			visibleBytes: originalBytes,
		};
	}

	let prefix = "";
	let omittedBytes = originalBytes;
	let suffix = truncationSuffix(omittedBytes);

	// The suffix length depends on the omitted count. Recalculate until that
	// count and the selected prefix agree.
	for (let attempt = 0; attempt < 8; attempt += 1) {
		const prefixBudget = Math.max(0, maxBytes - Buffer.byteLength(suffix, "utf8"));
		prefix = utf8Prefix(text, prefixBudget);
		const nextOmittedBytes = originalBytes - Buffer.byteLength(prefix, "utf8");
		const nextSuffix = truncationSuffix(nextOmittedBytes);
		if (nextOmittedBytes === omittedBytes && nextSuffix === suffix) break;
		omittedBytes = nextOmittedBytes;
		suffix = nextSuffix;
	}

	// Tiny custom limits may not have room for the notice. Preserve its start
	// while still honoring the byte limit.
	const suffixBytes = Buffer.byteLength(suffix, "utf8");
	const output = suffixBytes > maxBytes ? utf8Prefix(suffix, maxBytes) : `${prefix}${suffix}`;

	return {
		text: output,
		truncated: true,
		omittedBytes,
		originalBytes,
		visibleBytes: Buffer.byteLength(output, "utf8"),
	};
}

export function calculateDelegationStatus(
	mode: DelegationMode,
	results: readonly Pick<TaskResult, "status">[],
): DelegationResult["status"] {
	if (results.length === 0) return "failed";
	if (results.every((result) => result.status === "completed")) return "completed";

	const hasCompleted = results.some((result) => result.status === "completed");
	const hasAborted = results.some((result) => result.status === "aborted");
	const hasFailed = results.some((result) => result.status === "failed");
	const hasSkipped = results.some((result) => result.status === "skipped");

	if (mode === "parallel" && hasCompleted) return "partial";
	if (hasAborted) return "aborted";
	// Queued work is marked skipped when the parent signal fires before a child
	// starts. A failed chain also has skipped tail steps, so failures win there.
	if (hasSkipped && !hasFailed) return "aborted";
	return "failed";
}

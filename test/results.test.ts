import { describe, expect, it } from "vitest";
import {
	MAX_MODEL_VISIBLE_OUTPUT_BYTES,
	addUsage,
	calculateDelegationStatus,
	emptyUsage,
	extractLastAssistantText,
	summarizeUsage,
	truncateUtf8,
} from "../src/delegation/results.js";

describe("extractLastAssistantText", () => {
	it("returns the newest non-blank assistant text block", () => {
		const messages = [
			{ role: "assistant", content: [{ type: "text", text: "older" }] },
			{ role: "toolResult", content: [{ type: "text", text: "tool output" }] },
			{
				role: "assistant",
				content: [
					{ type: "text", text: "first" },
					{ type: "thinking", thinking: "secret" },
					{ type: "text", text: "  " },
					{ type: "text", text: "last" },
				],
			},
		];

		expect(extractLastAssistantText(messages)).toBe("last");
	});

	it("returns undefined when no assistant text exists", () => {
		expect(extractLastAssistantText([{ role: "assistant", content: [{ type: "text", text: "" }] }])).toBeUndefined();
	});
});

describe("usage accounting", () => {
	it("sums counters and uses the latest assistant totalTokens", () => {
		const usage = summarizeUsage([
			{
				role: "assistant",
				usage: {
					input: 10,
					output: 2,
					cacheRead: 3,
					cacheWrite: 4,
					totalTokens: 19,
					cost: { total: 0.1 },
				},
			},
			{ role: "user" },
			{
				role: "assistant",
				usage: { input: 20, output: 5, totalTokens: 44, cost: { total: 0.25 } },
			},
		]);

		expect(usage).toEqual({
			inputTokens: 30,
			outputTokens: 7,
			cacheReadTokens: 3,
			cacheWriteTokens: 4,
			totalTokens: 44,
			turns: 2,
			costUsd: 0.35,
		});
	});

	it("treats missing usage as zero and aggregates task context totals", () => {
		const missing = summarizeUsage([{ role: "assistant", content: [] }]);
		const complete = { ...emptyUsage(), totalTokens: 12, turns: 2, costUsd: 0.5 };

		expect(missing).toEqual({ ...emptyUsage(), turns: 1 });
		expect(addUsage(missing, complete)).toEqual({
			...emptyUsage(),
			totalTokens: 12,
			turns: 3,
			costUsd: 0.5,
		});
	});
});

describe("truncateUtf8", () => {
	it("leaves output at and below the limit unchanged", () => {
		const exact = "a".repeat(MAX_MODEL_VISIBLE_OUTPUT_BYTES);
		expect(truncateUtf8(exact)).toMatchObject({ text: exact, truncated: false, omittedBytes: 0 });
		expect(truncateUtf8("short")).toMatchObject({ text: "short", truncated: false });
	});

	it("keeps the notice inside the limit and does not split a code point", () => {
		const input = `${"a".repeat(MAX_MODEL_VISIBLE_OUTPUT_BYTES - 2)}\u{1f642}tail`;
		const result = truncateUtf8(input);

		expect(result.truncated).toBe(true);
		expect(result.visibleBytes).toBeLessThanOrEqual(MAX_MODEL_VISIBLE_OUTPUT_BYTES);
		expect(result.text).toContain(`[Output truncated: ${result.omittedBytes} bytes omitted.`);
		expect(result.text).not.toContain("\ufffd");
		expect(result.omittedBytes).toBe(result.originalBytes - Buffer.byteLength(result.text.split("\n\n[Output truncated:")[0]!, "utf8"));
	});
});

describe("calculateDelegationStatus", () => {
	const result = (status: "completed" | "failed" | "aborted" | "skipped") => ({ status });

	it("calculates completed, partial, failed, and aborted outcomes", () => {
		expect(calculateDelegationStatus("single", [result("completed")])).toBe("completed");
		expect(calculateDelegationStatus("parallel", [result("completed"), result("failed")])).toBe("partial");
		expect(calculateDelegationStatus("parallel", [result("failed"), result("failed")])).toBe("failed");
		expect(calculateDelegationStatus("single", [result("aborted")])).toBe("aborted");
		expect(calculateDelegationStatus("parallel", [result("skipped"), result("skipped")])).toBe("aborted");
		expect(calculateDelegationStatus("chain", [result("completed"), result("skipped")])).toBe("aborted");
		expect(calculateDelegationStatus("chain", [result("completed"), result("failed"), result("skipped")])).toBe("failed");
	});
});

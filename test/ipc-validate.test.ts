import { chmodSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
	atomicWriteJson,
	ensurePrivateDir,
	readEventsChunk,
	workerSpoolPaths,
} from "../src/ipc/spool.js";
import {
	assertSafeEnvValue,
	assertHeartbeatFreshness,
	parseJsonFile,
	sanitizeAssistantMessages,
	validateCommand,
	validateEvent,
	validateHeartbeat,
	validateReady,
	validateResult,
	IpcValidationError,
	DEFAULT_HEARTBEAT_CLOCK_SKEW_MS,
} from "../src/ipc/validate.js";
import { extractLastAssistantText } from "../src/delegation/results.js";

describe("IPC validators", () => {
	it("validates ready/command/cancel/heartbeat/result identities and bounds", () => {
		const identity = { runId: "run1", workerId: "scout_1" };
		expect(
			validateReady(
				{ version: 1, runId: "run1", workerId: "scout_1", readyAt: "t" },
				identity,
			).workerId,
		).toBe("scout_1");
		expect(() =>
			validateReady({ version: 1, runId: "other", workerId: "scout_1", readyAt: "t" }, identity),
		).toThrow(IpcValidationError);

		const prompt = validateCommand(
			{
				version: 1,
				type: "prompt",
				task: "do work",
				issuedAt: "t",
				runId: "run1",
				workerId: "scout_1",
			},
			identity,
		);
		expect(prompt.type).toBe("prompt");

		expect(
			validateHeartbeat(
				{ version: 1, runId: "run1", workerId: "scout_1", at: "t", seq: 1 },
				identity,
			).seq,
		).toBe(1);

		const event = validateEvent(
			{
				version: 1,
				type: "text",
				at: "t",
				runId: "run1",
				workerId: "scout_1",
				seq: 1,
				message: "hi",
			},
			identity,
			0,
		);
		expect(event.seq).toBe(1);
		expect(() =>
			validateEvent(
				{
					version: 1,
					type: "text",
					at: "t",
					runId: "run1",
					workerId: "scout_1",
					seq: 1,
					message: "hi",
				},
				identity,
				1,
			),
		).toThrow(/monotonically/);

		expect(
			validateResult(
				{
					version: 1,
					runId: "run1",
					workerId: "scout_1",
					status: "completed",
					messages: [],
					finishedAt: "t",
				},
				identity,
			).status,
		).toBe("completed");
	});

	it("rejects symlinks and group-readable IPC files", () => {
		const root = mkdtempSync(path.join(tmpdir(), "momo-ipc-val-"));
		try {
			const file = path.join(root, "ready.json");
			atomicWriteJson(file, { version: 1, runId: "r", workerId: "w", readyAt: "t" });
			chmodSync(file, 0o644);
			expect(() => parseJsonFile(file)).toThrow(/permissions/);

			const target = path.join(root, "target.json");
			writeFileSync(target, '{"ok":true}\n', "utf8");
			const link = path.join(root, "link.json");
			symlinkSync(target, link);
			expect(() => parseJsonFile(link)).toThrow(/symlink/);
		} finally {
			rmSync(root, { recursive: true, force: true });
		}
	});

	it("caps events file and reports overflow without crashing", () => {
		const root = mkdtempSync(path.join(tmpdir(), "momo-events-"));
		try {
			const paths = workerSpoolPaths(root, "scout_x");
			ensurePrivateDir(paths.root);
			// Write a large events file directly past cap.
			const huge = `${"x".repeat(2 * 1024 * 1024 + 10)}\n`;
			writeFileSync(paths.events, huge, "utf8");
			const chunk = readEventsChunk(paths.events, 0, 2 * 1024 * 1024);
			expect(chunk.overflow).toBe(true);
		} finally {
			rmSync(root, { recursive: true, force: true });
		}
	});

	it("rejects unsafe env keys/values", () => {
		expect(() => assertSafeEnvValue("MOMO_TASK", "x")).toThrow(/Task content/);
		expect(() => assertSafeEnvValue("MOMO_ROLE", "scout\n")).toThrow(/Unsafe env value/);
		expect(() => assertSafeEnvValue("bad-key", "x")).toThrow(/Unsafe env key/);
		assertSafeEnvValue("MOMO_ROLE", "scout");
	});

	it("assertHeartbeatFreshness fails closed on malformed and far-future at", () => {
		const now = Date.parse("2026-07-27T12:00:00.000Z");
		expect(() =>
			assertHeartbeatFreshness("not-a-date", { now, staleMs: 15_000 }),
		).toThrow(/not a valid date/i);
		expect(() =>
			assertHeartbeatFreshness("2026-07-27T12:10:00.000Z", {
				now,
				staleMs: 15_000,
				maxFutureSkewMs: DEFAULT_HEARTBEAT_CLOCK_SKEW_MS,
			}),
		).toThrow(/implausibly in the future/i);
		expect(() =>
			assertHeartbeatFreshness("2026-07-27T11:00:00.000Z", { now, staleMs: 15_000 }),
		).toThrow(/went stale/i);
		// Within skew tolerance: slightly ahead is ok.
		expect(
			assertHeartbeatFreshness("2026-07-27T12:00:03.000Z", {
				now,
				staleMs: 15_000,
				maxFutureSkewMs: DEFAULT_HEARTBEAT_CLOCK_SKEW_MS,
			}).ageMs,
		).toBeLessThan(0);
		expect(
			assertHeartbeatFreshness("2026-07-27T11:59:50.000Z", { now, staleMs: 15_000 }).ageMs,
		).toBe(10_000);
	});
});

describe("sanitizeAssistantMessages", () => {
	function textBytes(messages: unknown[]): number {
		let total = 0;
		for (const message of messages) {
			const record = message as { content?: { text?: string }[] };
			const text = record.content?.[0]?.text ?? "";
			total += Buffer.byteLength(text, "utf8");
		}
		return total;
	}

	function assistant(text: string, extra?: Record<string, unknown>) {
		return {
			role: "assistant",
			content: [{ type: "text", text }],
			...extra,
		};
	}

	it("keeps newest when many old turns would exhaust the budget", () => {
		const old = "OLD_TURN_".repeat(40); // 360 bytes each
		const messages = [
			assistant(old, { stopReason: "stop" }),
			assistant(old),
			assistant(old),
			assistant("FINAL_ANSWER", {
				usage: { input: 1, output: 2, totalTokens: 3 },
				stopReason: "stop",
			}),
		];
		// Fits final only — leftover cannot hold another full old turn.
		const budget = Buffer.byteLength("FINAL_ANSWER", "utf8") + 50;
		const out = sanitizeAssistantMessages(messages, budget);
		expect(textBytes(out)).toBeLessThanOrEqual(budget);
		expect(out).toHaveLength(1);
		expect(extractLastAssistantText(out)).toBe("FINAL_ANSWER");
		expect(JSON.stringify(out)).not.toContain("OLD_TURN_");
		const last = out[0] as {
			usage?: { totalTokens?: number };
			stopReason?: string;
		};
		expect(last.usage?.totalTokens).toBe(3);
		expect(last.stopReason).toBe("stop");
	});

	it("truncates an oversized final turn deterministically within budget", () => {
		const marker = "\n\n[Output truncated for IPC result bound]";
		const huge = "Z".repeat(10_000);
		const budget = 200;
		const out = sanitizeAssistantMessages([assistant(huge)], budget);
		expect(out).toHaveLength(1);
		const text = (out[0] as { content: { text: string }[] }).content[0]!.text;
		expect(Buffer.byteLength(text, "utf8")).toBeLessThanOrEqual(budget);
		expect(Buffer.byteLength(text, "utf8")).toBe(budget);
		expect(text.endsWith(marker)).toBe(true);
		expect(text.startsWith("Z")).toBe(true);
		const again = sanitizeAssistantMessages([assistant(huge)], budget);
		expect((again[0] as { content: { text: string }[] }).content[0]!.text).toBe(text);
	});

	it("returns retained turns in chronological order", () => {
		const out = sanitizeAssistantMessages(
			[
				assistant("first"),
				assistant("second"),
				assistant("third"),
			],
			1_000,
		);
		expect(
			out.map((m) => (m as { content: { text: string }[] }).content[0]!.text),
		).toEqual(["first", "second", "third"]);
	});

	it("skips blank newest so extraction sees prior nonblank final", () => {
		const out = sanitizeAssistantMessages(
			[
				assistant("kept-final"),
				assistant("   \n\t  "),
				{ role: "assistant", content: [{ type: "text", text: "" }] },
				{ role: "user", content: [{ type: "text", text: "ignore" }] },
			],
			1_000,
		);
		expect(out).toHaveLength(1);
		expect(extractLastAssistantText(out)).toBe("kept-final");
	});

	it("honors multibyte, tiny, and zero budgets exactly", () => {
		const marker = "\n\n[Output truncated for IPC result bound]";
		const markerBytes = Buffer.byteLength(marker, "utf8");
		const emoji = "😀"; // 4 UTF-8 bytes
		expect(Buffer.byteLength(emoji, "utf8")).toBe(4);

		const zero = sanitizeAssistantMessages([assistant(`${emoji}final`)], 0);
		expect(zero).toEqual([]);
		expect(textBytes(zero)).toBe(0);

		// Tiny budget: too small for one code point + full marker → original prefix only.
		const tiny = sanitizeAssistantMessages([assistant("ABCDEFGHIJKLMNOP")], 3);
		expect(textBytes(tiny)).toBe(3);
		const tinyText = (tiny[0] as { content: { text: string }[] }).content[0]!.text;
		expect(tinyText).toBe("ABC");
		expect(tinyText.includes("[Output truncated")).toBe(false);
		expect(extractLastAssistantText(tiny)).toBe("ABC");

		// Exact boundary: one ASCII code point + full marker fits → marker used.
		const exactBudget = 1 + markerBytes;
		const exact = sanitizeAssistantMessages([assistant("XYZ".repeat(100))], exactBudget);
		expect(textBytes(exact)).toBe(exactBudget);
		const exactText = (exact[0] as { content: { text: string }[] }).content[0]!.text;
		expect(exactText).toBe(`X${marker}`);
		expect(extractLastAssistantText(exact)).toBe(exactText);

		// One byte under that boundary → original prefix, no marker.
		const underSource = "XYZ".repeat(100);
		const under = sanitizeAssistantMessages([assistant(underSource)], exactBudget - 1);
		expect(textBytes(under)).toBe(exactBudget - 1);
		const underText = (under[0] as { content: { text: string }[] }).content[0]!.text;
		expect(underText).toBe(underSource.slice(0, exactBudget - 1));
		expect(underText.includes("[Output truncated")).toBe(false);
		expect(extractLastAssistantText(under)).toBe(underText);

		const multiBudget = 4;
		const multi = sanitizeAssistantMessages(
			[assistant(`${emoji}${emoji}${emoji}`)],
			multiBudget,
		);
		expect(textBytes(multi)).toBe(multiBudget);
		const multiText = (multi[0] as { content: { text: string }[] }).content[0]!.text;
		expect(multiText).toBe(emoji);
		expect(multiText.includes("\uFFFD")).toBe(false);
		expect(extractLastAssistantText(multi)).toBe(emoji);

		// emoji (4) + marker does not fit in 4 → keep the emoji, not a marker stub.
		const fitOne = sanitizeAssistantMessages([assistant(`${emoji}x`)], 4);
		expect(textBytes(fitOne)).toBe(4);
		const fitText = (fitOne[0] as { content: { text: string }[] }).content[0]!.text;
		expect(fitText).toBe(emoji);
		expect(extractLastAssistantText(fitOne)).toBe(emoji);
	});

	it("integration: extraction/chain sees final answer not stale older turns", () => {
		const stale = "stale-chain-output-".repeat(50);
		const messages = [
			assistant(stale, { errorMessage: "old" }),
			assistant("mid"),
			assistant("CHAIN_FINAL_v2", {
				usage: { input: 9, output: 8, totalTokens: 17 },
				stopReason: "stop",
			}),
		];
		// Budget fits final (+ maybe mid) but not the large stale turn.
		const budget = Buffer.byteLength("CHAIN_FINAL_v2", "utf8") + 8;
		const sanitized = sanitizeAssistantMessages(messages, budget);
		expect(textBytes(sanitized)).toBeLessThanOrEqual(budget);
		expect(extractLastAssistantText(sanitized)).toBe("CHAIN_FINAL_v2");
		expect(extractLastAssistantText(sanitized)).not.toMatch(/stale-chain-output/);
		expect(JSON.stringify(sanitized)).not.toContain("stale-chain-output");
		const last = sanitized[sanitized.length - 1] as {
			usage?: { totalTokens?: number };
			stopReason?: string;
		};
		expect(last.usage?.totalTokens).toBe(17);
		expect(last.stopReason).toBe("stop");
	});
});

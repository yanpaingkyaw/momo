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
	validateCommand,
	validateEvent,
	validateHeartbeat,
	validateReady,
	validateResult,
	IpcValidationError,
	DEFAULT_HEARTBEAT_CLOCK_SKEW_MS,
} from "../src/ipc/validate.js";

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

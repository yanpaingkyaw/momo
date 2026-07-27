import { appendFileSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
	EventsFileCapacityError,
	MAX_EVENTS_FILE_BYTES,
	MAX_IPC_JSON_BYTES,
	appendEvent,
	atomicWriteJson,
	readEventsChunk,
	readEventsSince,
	readJsonFile,
	workerSpoolPaths,
	type IpcEvent,
} from "../src/ipc/spool.js";

function event(seq: number, message: string): IpcEvent {
	return {
		version: 1,
		type: "text",
		at: String(seq),
		runId: "r",
		workerId: "w",
		seq,
		message,
	};
}

describe("IPC spool", () => {
	it("writes and reads atomic JSON with private dirs", () => {
		const root = mkdtempSync(path.join(tmpdir(), "momo-ipc-"));
		try {
			const paths = workerSpoolPaths(root, "scout_abc");
			atomicWriteJson(paths.ready, {
				version: 1,
				runId: "r1",
				workerId: "scout_abc",
				readyAt: "t",
			});
			expect((readJsonFile(paths.ready) as { workerId: string }).workerId).toBe("scout_abc");
		} finally {
			rmSync(root, { recursive: true, force: true });
		}
	});

	it("rejects oversized payloads", () => {
		const root = mkdtempSync(path.join(tmpdir(), "momo-ipc-"));
		try {
			const file = path.join(root, "big.json");
			expect(() => atomicWriteJson(file, { data: "x".repeat(MAX_IPC_JSON_BYTES) })).toThrow(
				/exceeds/,
			);
		} finally {
			rmSync(root, { recursive: true, force: true });
		}
	});

	it("reads ndjson events from an offset", () => {
		const root = mkdtempSync(path.join(tmpdir(), "momo-ipc-"));
		try {
			const file = path.join(root, "events.ndjson");
			appendEvent(file, event(1, "a"));
			appendEvent(file, event(2, "b"));
			const first = readEventsSince(file, 0);
			expect(first.events).toHaveLength(2);
			const second = readEventsSince(file, first.nextOffset);
			expect(second.events).toHaveLength(0);
		} finally {
			rmSync(root, { recursive: true, force: true });
		}
	});

	it("preserves byte offset across append after a complete trailing newline", () => {
		const root = mkdtempSync(path.join(tmpdir(), "momo-ipc-"));
		try {
			const file = path.join(root, "events.ndjson");
			appendEvent(file, event(1, "one"));
			const first = readEventsChunk(file, 0);
			expect(first.lines).toHaveLength(1);
			expect(JSON.parse(first.lines[0]!).message).toBe("one");
			// Must land exactly at EOF, not one byte past (split sentinel bug).
			expect(first.nextOffset).toBe(Buffer.byteLength(`${first.lines[0]}\n`, "utf8"));

			appendEvent(file, event(2, "two"));
			const second = readEventsChunk(file, first.nextOffset);
			expect(second.lines).toHaveLength(1);
			expect(second.lines[0]!.startsWith("{")).toBe(true);
			expect(JSON.parse(second.lines[0]!)).toMatchObject({ seq: 2, message: "two" });
		} finally {
			rmSync(root, { recursive: true, force: true });
		}
	});

	it("handles multiple complete lines and blank-line delimiters", () => {
		const root = mkdtempSync(path.join(tmpdir(), "momo-ipc-"));
		try {
			const file = path.join(root, "events.ndjson");
			const line1 = JSON.stringify(event(1, "α"));
			const line2 = JSON.stringify(event(2, "β"));
			writeFileSync(file, `${line1}\n\n${line2}\n`, "utf8");
			const multi = readEventsChunk(file, 0);
			expect(multi.lines).toEqual([line1, line2]);
			expect(multi.nextOffset).toBe(Buffer.byteLength(`${line1}\n\n${line2}\n`, "utf8"));
			expect(readEventsChunk(file, multi.nextOffset).lines).toEqual([]);
		} finally {
			rmSync(root, { recursive: true, force: true });
		}
	});

	it("leaves partial trailing UTF-8 unread until the line completes", () => {
		const root = mkdtempSync(path.join(tmpdir(), "momo-ipc-"));
		try {
			const file = path.join(root, "events.ndjson");
			const complete = JSON.stringify(event(1, "ok"));
			writeFileSync(file, `${complete}\n`, "utf8");
			const first = readEventsChunk(file, 0);
			expect(first.lines).toEqual([complete]);

			// Incomplete UTF-8 code point (€ = e2 82 ac) with no newline yet.
			const euro = Buffer.from("€", "utf8");
			appendFileSync(file, Buffer.concat([Buffer.from('{"message":"', "utf8"), euro.subarray(0, 2)]));
			const partial = readEventsChunk(file, first.nextOffset);
			expect(partial.lines).toEqual([]);
			expect(partial.nextOffset).toBe(first.nextOffset);

			appendFileSync(file, Buffer.concat([euro.subarray(2), Buffer.from('"}\n', "utf8")]));
			const finished = readEventsChunk(file, partial.nextOffset);
			expect(finished.lines).toHaveLength(1);
			expect(JSON.parse(finished.lines[0]!).message).toBe("€");
		} finally {
			rmSync(root, { recursive: true, force: true });
		}
	});

	it("marks malformed JSON lines without crashing the reader", () => {
		const root = mkdtempSync(path.join(tmpdir(), "momo-ipc-"));
		try {
			const file = path.join(root, "events.ndjson");
			writeFileSync(file, '{"version":1,"type":"text","at":"1","runId":"r","workerId":"w","seq":1}\n{nope\n', "utf8");
			const chunk = readEventsSince(file, 0);
			expect(chunk.events).toHaveLength(1);
			expect(chunk.malformed).toBe(true);
		} finally {
			rmSync(root, { recursive: true, force: true });
		}
	});

	it("allows an exact-fit append and rejects one byte over without mutation", () => {
		const root = mkdtempSync(path.join(tmpdir(), "momo-ipc-"));
		try {
			const file = path.join(root, "events.ndjson");
			const first = event(1, "exact");
			const firstLine = `${JSON.stringify(first)}\n`;
			const firstBytes = Buffer.byteLength(firstLine, "utf8");
			appendEvent(file, first, firstBytes);
			expect(readFileSync(file, "utf8")).toBe(firstLine);
			expect(readFileSync(file).length).toBe(firstBytes);

			const before = readFileSync(file);
			const second = event(2, "x");
			expect(() => appendEvent(file, second, firstBytes)).toThrow(EventsFileCapacityError);
			expect(readFileSync(file)).toEqual(before);
			expect(readEventsSince(file, 0).events).toHaveLength(1);

			// One byte of headroom is still insufficient for any second event line.
			expect(() => appendEvent(file, second, firstBytes + 1)).toThrow(EventsFileCapacityError);
			expect(readFileSync(file)).toEqual(before);
			expect(readFileSync(file).length).toBeLessThanOrEqual(firstBytes + 1);
		} finally {
			rmSync(root, { recursive: true, force: true });
		}
	});

	it("accounts for multibyte UTF-8 when enforcing the total byte cap", () => {
		const root = mkdtempSync(path.join(tmpdir(), "momo-ipc-"));
		try {
			const file = path.join(root, "events.ndjson");
			const first = event(1, "αβγ");
			const firstLine = `${JSON.stringify(first)}\n`;
			const firstBytes = Buffer.byteLength(firstLine, "utf8");
			expect(firstBytes).toBeGreaterThan(firstLine.length);
			appendEvent(file, first, firstBytes);
			const before = readFileSync(file);
			expect(() => appendEvent(file, event(2, "δ"), firstBytes)).toThrow(EventsFileCapacityError);
			expect(readFileSync(file)).toEqual(before);
			expect(JSON.parse(readFileSync(file, "utf8").trim()).message).toBe("αβγ");
		} finally {
			rmSync(root, { recursive: true, force: true });
		}
	});

	it("defaults the total cap to MAX_EVENTS_FILE_BYTES", () => {
		const root = mkdtempSync(path.join(tmpdir(), "momo-ipc-"));
		try {
			const file = path.join(root, "events.ndjson");
			writeFileSync(file, Buffer.alloc(MAX_EVENTS_FILE_BYTES, 0x61), { mode: 0o600 });
			const before = readFileSync(file);
			expect(() => appendEvent(file, event(1, "overflow"))).toThrow(EventsFileCapacityError);
			expect(readFileSync(file)).toEqual(before);
			expect(readFileSync(file).length).toBe(MAX_EVENTS_FILE_BYTES);
		} finally {
			rmSync(root, { recursive: true, force: true });
		}
	});

	it("rejects malformed JSON files", () => {
		const root = mkdtempSync(path.join(tmpdir(), "momo-ipc-"));
		try {
			const file = path.join(root, "bad.json");
			atomicWriteJson(file, { ok: true });
			expect(readJsonFile(file.replace("bad.json", "missing.json"))).toBeUndefined();
			writeFileSync(file, "{nope", "utf8");
			expect(() => readJsonFile(file)).toThrow();
		} finally {
			rmSync(root, { recursive: true, force: true });
		}
	});
});

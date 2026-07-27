import {
	chmodSync,
	existsSync,
	lstatSync,
	mkdirSync,
	openSync,
	readFileSync,
	renameSync,
	rmSync,
	statSync,
	writeFileSync,
	closeSync,
} from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import path from "node:path";
import type { AgentName } from "../roles.js";
import { IpcValidationError } from "./errors.js";

export const IPC_VERSION = 1;
export const MAX_IPC_JSON_BYTES = 256 * 1024;
export const MAX_EVENTS_FILE_BYTES = 2 * 1024 * 1024;
export const DEFAULT_HEARTBEAT_STALE_MS = 15_000;

export type IpcEventType =
	| "queued"
	| "started"
	| "text"
	| "tool_started"
	| "tool_finished"
	| "state"
	| "completed"
	| "failed"
	| "aborted";

export interface IpcManifest {
	version: number;
	runId: string;
	workerId: string;
	role: AgentName;
	cwd: string;
	paneId?: string;
	agentName?: string;
	createdAt: string;
}

export interface IpcReady {
	version: number;
	runId?: string;
	workerId: string;
	readyAt: string;
}

export type IpcCommand =
	| {
			version: number;
			type: "prompt";
			task: string;
			issuedAt: string;
			runId: string;
			workerId: string;
	  }
	| {
			version: number;
			type: "skip" | "cancel";
			reason: string;
			issuedAt: string;
			runId: string;
			workerId: string;
	  };

export interface IpcCancel {
	version: number;
	runId: string;
	workerId: string;
	reason: string;
	issuedAt: string;
}

export interface IpcHeartbeat {
	version: number;
	runId: string;
	workerId: string;
	at: string;
	seq: number;
}

export interface IpcEvent {
	version: number;
	type: IpcEventType;
	at: string;
	runId: string;
	workerId: string;
	seq: number;
	message?: string;
	toolName?: string;
	state?: string;
}

export interface IpcResult {
	version: number;
	runId: string;
	workerId: string;
	status: "completed" | "failed" | "aborted";
	messages: unknown[];
	usage?: unknown;
	stopReason?: string;
	errorMessage?: string;
	uncertainWrite?: boolean;
	finishedAt: string;
}

export interface WorkerSpoolPaths {
	root: string;
	manifest: string;
	ready: string;
	command: string;
	cancel: string;
	heartbeat: string;
	events: string;
	result: string;
}

export function momoCacheRoot(env: NodeJS.ProcessEnv = process.env): string {
	const base = env.XDG_CACHE_HOME || path.join(env.HOME || tmpdir(), ".cache");
	return path.join(base, "momo");
}

export function createRunId(): string {
	return randomUUID().replace(/-/g, "").slice(0, 12);
}

export function createWorkerId(role: AgentName): string {
	return `${role}_${randomUUID().replace(/-/g, "").slice(0, 8)}`;
}

export function workerSpoolPaths(runRoot: string, workerId: string): WorkerSpoolPaths {
	const root = path.join(runRoot, "workers", workerId);
	return {
		root,
		manifest: path.join(root, "manifest.json"),
		ready: path.join(root, "ready.json"),
		command: path.join(root, "command.json"),
		cancel: path.join(root, "cancel.json"),
		heartbeat: path.join(root, "heartbeat.json"),
		events: path.join(root, "events.ndjson"),
		result: path.join(root, "result.json"),
	};
}

export function ensurePrivateDir(dir: string): void {
	mkdirSync(dir, { recursive: true, mode: 0o700 });
	try {
		chmodSync(dir, 0o700);
	} catch {
		// best effort
	}
}

export function atomicWriteJson(filePath: string, value: unknown, maxBytes = MAX_IPC_JSON_BYTES): void {
	const json = `${JSON.stringify(value)}\n`;
	const bytes = Buffer.byteLength(json, "utf8");
	if (bytes > maxBytes) {
		throw new Error(`IPC payload exceeds ${maxBytes} bytes (${bytes})`);
	}
	ensurePrivateDir(path.dirname(filePath));
	if (existsSync(filePath)) {
		const existing = lstatSync(filePath);
		if (existing.isSymbolicLink()) {
			throw new Error(`Refusing to overwrite symlink IPC path: ${filePath}`);
		}
	}
	const tmp = `${filePath}.${process.pid}.${Date.now()}.tmp`;
	const fd = openSync(tmp, "wx", 0o600);
	try {
		writeFileSync(fd, json, { encoding: "utf8" });
	} finally {
		closeSync(fd);
	}
	renameSync(tmp, filePath);
	try {
		chmodSync(filePath, 0o600);
	} catch {
		// ignore
	}
}

export function readJsonFile(filePath: string, maxBytes = MAX_IPC_JSON_BYTES): unknown | undefined {
	if (!existsSync(filePath)) return undefined;
	const stat = lstatSync(filePath);
	if (stat.isSymbolicLink()) {
		throw new Error(`IPC path must not be a symlink: ${filePath}`);
	}
	if (!stat.isFile()) {
		throw new Error(`IPC path must be a regular file: ${filePath}`);
	}
	if (stat.size > maxBytes) {
		throw new Error(`IPC file too large: ${filePath}`);
	}
	return JSON.parse(readFileSync(filePath, "utf8"));
}

export function appendEvent(filePath: string, event: IpcEvent, maxTotalBytes = 2 * 1024 * 1024): void {
	ensurePrivateDir(path.dirname(filePath));
	if (existsSync(filePath)) {
		const stat = lstatSync(filePath);
		if (stat.isSymbolicLink()) {
			throw new Error(`IPC events path must not be a symlink: ${filePath}`);
		}
		if (stat.size > maxTotalBytes) {
			throw new Error(`IPC events file exceeds total size cap: ${filePath}`);
		}
	}
	const line = `${JSON.stringify(event)}\n`;
	if (Buffer.byteLength(line, "utf8") > MAX_IPC_JSON_BYTES) {
		throw new Error("IPC event exceeds size limit");
	}
	writeFileSync(filePath, line, { encoding: "utf8", flag: "a", mode: 0o600 });
}

/**
 * Enforce non-symlink regular owner-private events file within size bound.
 * Missing path is allowed (empty spool). All failures are IpcValidationError.
 */
export function assertEventsFile(
	filePath: string,
	maxTotalBytes = MAX_EVENTS_FILE_BYTES,
): { exists: boolean; size: number } {
	try {
		if (!existsSync(filePath)) {
			return { exists: false, size: 0 };
		}
		const stat = lstatSync(filePath);
		if (stat.isSymbolicLink()) {
			throw new IpcValidationError(`IPC events path must not be a symlink: ${filePath}`);
		}
		if (!stat.isFile()) {
			throw new IpcValidationError(`IPC events path must be a regular file: ${filePath}`);
		}
		if (typeof stat.mode === "number" && (stat.mode & 0o077) !== 0) {
			throw new IpcValidationError(`IPC events file has group/other permissions: ${filePath}`);
		}
		if (stat.size > maxTotalBytes) {
			throw new IpcValidationError(`IPC events file exceeds size bound: ${filePath}`);
		}
		return { exists: true, size: stat.size };
	} catch (error) {
		if (error instanceof IpcValidationError) throw error;
		throw new IpcValidationError(
			`IPC events lstat failed at ${filePath}: ${
				error instanceof Error ? error.message : String(error)
			}`,
		);
	}
}

/**
 * Read complete NDJSON lines from a byte offset.
 * Advances nextOffset only through fully delimited lines (ending in `\n`).
 * A trailing incomplete fragment (no final newline, including partial UTF-8)
 * is left unread so a later append can complete it.
 *
 * Low-level: does not enforce mode/private checks (prefer readEventsIncrementally).
 */
export function readEventsChunk(
	filePath: string,
	offsetBytes: number,
	maxTotalBytes = MAX_EVENTS_FILE_BYTES,
): { lines: string[]; nextOffset: number; overflow: boolean } {
	if (!existsSync(filePath)) {
		return { lines: [], nextOffset: offsetBytes, overflow: false };
	}
	const stat = lstatSync(filePath);
	if (stat.isSymbolicLink()) {
		throw new Error(`IPC events path must not be a symlink: ${filePath}`);
	}
	if (stat.size > maxTotalBytes) {
		return { lines: [], nextOffset: offsetBytes, overflow: true };
	}
	const buf = readFileSync(filePath);
	if (offsetBytes > buf.length) {
		return { lines: [], nextOffset: buf.length, overflow: false };
	}
	if (offsetBytes === buf.length) {
		return { lines: [], nextOffset: offsetBytes, overflow: false };
	}

	const lines: string[] = [];
	let lineStart = offsetBytes;
	let pos = offsetBytes;
	while (pos < buf.length) {
		if (buf[pos] === 0x0a) {
			const line = buf.subarray(lineStart, pos).toString("utf8");
			pos += 1;
			lineStart = pos;
			if (line.trim()) lines.push(line);
			continue;
		}
		pos += 1;
	}
	// Incomplete trailing bytes (no final \n): keep nextOffset at lineStart.
	return { lines, nextOffset: lineStart, overflow: false };
}

/**
 * Fail-closed NDJSON incremental reader for production factory polling.
 * Enforces regular owner-private file + size bound; wraps lstat/read/parse
 * failures as IpcValidationError (never silent ignore).
 */
export function readEventsIncrementally(
	filePath: string,
	offsetBytes: number,
	maxTotalBytes = MAX_EVENTS_FILE_BYTES,
): { events: unknown[]; nextOffset: number } {
	const meta = assertEventsFile(filePath, maxTotalBytes);
	if (!meta.exists) {
		return { events: [], nextOffset: offsetBytes };
	}
	let chunk: { lines: string[]; nextOffset: number; overflow: boolean };
	try {
		chunk = readEventsChunk(filePath, offsetBytes, maxTotalBytes);
	} catch (error) {
		if (error instanceof IpcValidationError) throw error;
		throw new IpcValidationError(
			`IPC events read failed at ${filePath}: ${
				error instanceof Error ? error.message : String(error)
			}`,
		);
	}
	if (chunk.overflow) {
		throw new IpcValidationError(`IPC events file exceeds size bound: ${filePath}`);
	}
	const events: unknown[] = [];
	for (const line of chunk.lines) {
		try {
			events.push(JSON.parse(line));
		} catch (error) {
			throw new IpcValidationError(
				`Malformed IPC event line at ${filePath}: ${
					error instanceof Error ? error.message : String(error)
				}`,
			);
		}
	}
	return { events, nextOffset: chunk.nextOffset };
}

/** Parse NDJSON events without fail-closed validation (tests / low-level readers). */
export function readEventsSince(
	filePath: string,
	offsetBytes: number,
): { events: IpcEvent[]; nextOffset: number; overflow: boolean; malformed: boolean } {
	const chunk = readEventsChunk(filePath, offsetBytes);
	const events: IpcEvent[] = [];
	let malformed = false;
	for (const line of chunk.lines) {
		try {
			events.push(JSON.parse(line) as IpcEvent);
		} catch {
			malformed = true;
		}
	}
	return {
		events,
		nextOffset: chunk.nextOffset,
		overflow: chunk.overflow,
		malformed,
	};
}

export function cwdHash(cwd: string): string {
	return createHash("sha256").update(cwd).digest("hex").slice(0, 16);
}

export function removeSpool(root: string): void {
	rmSync(root, { recursive: true, force: true });
}

export function fileSize(filePath: string): number {
	if (!existsSync(filePath)) return 0;
	return statSync(filePath).size;
}

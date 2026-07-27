import { existsSync, lstatSync, readFileSync } from "node:fs";
import type { AgentName } from "../roles.js";
import { isAgentName } from "../roles.js";
import { IpcValidationError } from "./errors.js";
import {
	IPC_VERSION,
	MAX_IPC_JSON_BYTES,
	type IpcActivePointer,
	type IpcCancel,
	type IpcCommand,
	type IpcEvent,
	type IpcEventType,
	type IpcHeartbeat,
	type IpcManifest,
	type IpcReady,
	type IpcResult,
	type IpcStarted,
} from "./spool.js";

export { IpcValidationError } from "./errors.js";
export { MAX_EVENTS_FILE_BYTES } from "./spool.js";
export const MAX_EVENT_MESSAGE_CHARS = 8_192;
export const MAX_TASK_CHARS = 100_000;

const EVENT_TYPES = new Set<IpcEventType>([
	"queued",
	"started",
	"text",
	"tool_started",
	"tool_finished",
	"state",
	"completed",
	"failed",
	"aborted",
]);

const ALLOWED_STOP_REASONS = new Set([
	"stop",
	"length",
	"toolUse",
	"error",
	"aborted",
	"cancelled",
]);

function assertRegularPrivateFile(filePath: string): void {
	const stat = lstatSync(filePath);
	if (stat.isSymbolicLink()) {
		throw new IpcValidationError(`IPC path must not be a symlink: ${filePath}`);
	}
	if (!stat.isFile()) {
		throw new IpcValidationError(`IPC path must be a regular file: ${filePath}`);
	}
	if (stat.size > MAX_IPC_JSON_BYTES) {
		throw new IpcValidationError(`IPC file exceeds size bound: ${filePath}`);
	}
	// On Unix, prefer owner-only files when mode is available.
	if (typeof stat.mode === "number" && (stat.mode & 0o077) !== 0) {
		throw new IpcValidationError(`IPC file has group/other permissions: ${filePath}`);
	}
}

function asRecord(value: unknown, label: string): Record<string, unknown> {
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		throw new IpcValidationError(`${label} must be an object`);
	}
	return value as Record<string, unknown>;
}

function requireString(record: Record<string, unknown>, key: string, label: string, max = 4096): string {
	const value = record[key];
	if (typeof value !== "string" || value.length === 0 || value.length > max) {
		throw new IpcValidationError(`${label}.${key} must be a non-empty string <= ${max}`);
	}
	if (value.includes("\0")) {
		throw new IpcValidationError(`${label}.${key} must not contain NUL`);
	}
	return value;
}

function requireVersion(record: Record<string, unknown>, label: string): number {
	const version = record.version;
	if (version !== IPC_VERSION) {
		throw new IpcValidationError(`${label}.version must be ${IPC_VERSION}`);
	}
	return IPC_VERSION;
}

export function parseJsonFile(filePath: string): unknown {
	assertRegularPrivateFile(filePath);
	try {
		return JSON.parse(readFileSync(filePath, "utf8"));
	} catch (error) {
		throw new IpcValidationError(
			`IPC JSON corrupt at ${filePath}: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
}

/** Read optional IPC JSON via the private regular-file path; missing => undefined. */
export function tryReadIpcJson(filePath: string): unknown | undefined {
	if (!existsSync(filePath)) return undefined;
	return parseJsonFile(filePath);
}

function optionalBoundedString(
	record: Record<string, unknown>,
	key: string,
	label: string,
	max: number,
): string | undefined {
	if (!(key in record) || record[key] === undefined) return undefined;
	return requireString(record, key, label, max);
}

export function validateManifest(value: unknown, expected?: { runId: string; workerId: string }): IpcManifest {
	const record = asRecord(value, "manifest");
	requireVersion(record, "manifest");
	const runId = requireString(record, "runId", "manifest", 64);
	const workerId = requireString(record, "workerId", "manifest", 64);
	const role = requireString(record, "role", "manifest", 32);
	if (!isAgentName(role)) throw new IpcValidationError("manifest.role invalid");
	if (expected && (runId !== expected.runId || workerId !== expected.workerId)) {
		throw new IpcValidationError("manifest identity mismatch");
	}
	const paneId = optionalBoundedString(record, "paneId", "manifest", 128);
	const agentName = optionalBoundedString(record, "agentName", "manifest", 64);
	return {
		version: IPC_VERSION,
		runId,
		workerId,
		role: role as AgentName,
		cwd: requireString(record, "cwd", "manifest", 4096),
		createdAt: requireString(record, "createdAt", "manifest", 64),
		...(paneId !== undefined ? { paneId } : {}),
		...(agentName !== undefined ? { agentName } : {}),
	};
}

export function validateReady(
	value: unknown,
	expected: { runId: string; workerId: string },
): IpcReady & { runId: string } {
	const record = asRecord(value, "ready");
	requireVersion(record, "ready");
	const workerId = requireString(record, "workerId", "ready", 64);
	const runId = requireString(record, "runId", "ready", 64);
	if (workerId !== expected.workerId || runId !== expected.runId) {
		throw new IpcValidationError("ready identity mismatch");
	}
	return {
		version: IPC_VERSION,
		workerId,
		runId,
		readyAt: requireString(record, "readyAt", "ready", 64),
	};
}

export function validateCommand(
	value: unknown,
	expected: {
		runId: string;
		workerId: string;
		generation?: number;
		parentEpoch?: string;
	},
): IpcCommand {
	const record = asRecord(value, "command");
	requireVersion(record, "command");
	const runId = requireString(record, "runId", "command", 64);
	const workerId = requireString(record, "workerId", "command", 64);
	if (runId !== expected.runId || workerId !== expected.workerId) {
		throw new IpcValidationError("command identity mismatch");
	}
	const rawGeneration = record.generation;
	const generation = typeof rawGeneration === "number" ? rawGeneration : undefined;
	if (
		rawGeneration !== undefined &&
		(generation === undefined ||
			!Number.isInteger(generation) ||
			generation < 1 ||
			(expected.generation !== undefined && generation !== expected.generation))
	) {
		throw new IpcValidationError("command generation mismatch");
	}
	if (expected.generation !== undefined && generation !== expected.generation) {
		throw new IpcValidationError("command generation mismatch");
	}
	const type = requireString(record, "type", "command", 32);
	if (type !== "prompt" && type !== "skip" && type !== "cancel") {
		throw new IpcValidationError("command.type invalid");
	}
	const issuedAt = requireString(record, "issuedAt", "command", 64);

	// Persistent generation-aware commands require parentEpoch. Legacy commands
	// without generation keep optional epoch compatibility.
	const generationAware = expected.generation !== undefined || generation !== undefined;
	let parentEpoch: string | undefined;
	if (generationAware) {
		parentEpoch = requireString(record, "parentEpoch", "command", 64);
		if (expected.parentEpoch !== undefined && parentEpoch !== expected.parentEpoch) {
			throw new IpcValidationError("command parentEpoch mismatch");
		}
	} else {
		parentEpoch = optionalBoundedString(record, "parentEpoch", "command", 64);
	}

	const epochFields = {
		...(generation !== undefined ? { generation } : {}),
		...(parentEpoch !== undefined ? { parentEpoch } : {}),
	};

	if (type === "prompt") {
		const task = requireString(record, "task", "command", MAX_TASK_CHARS);
		return { version: IPC_VERSION, type, task, issuedAt, runId, workerId, ...epochFields };
	}
	const reason = requireString(record, "reason", "command", 1024);
	return { version: IPC_VERSION, type, reason, issuedAt, runId, workerId, ...epochFields };
}

/**
 * Strict active.json validator. Rejects null/array/primitive/malformed shapes.
 * Exact generation match when expected.generation is provided.
 */
export function validateActivePointer(
	value: unknown,
	expected?: { generation?: number; assignmentId?: string },
): IpcActivePointer {
	if (value === null || typeof value !== "object" || Array.isArray(value)) {
		throw new IpcValidationError("active pointer must be a non-null object");
	}
	const record = value as Record<string, unknown>;
	if (record.version !== 1) {
		throw new IpcValidationError("active.version must be 1");
	}
	const assignmentId = requireString(record, "assignmentId", "active", 64);
	if (expected?.assignmentId !== undefined && assignmentId !== expected.assignmentId) {
		throw new IpcValidationError("active.assignmentId mismatch");
	}
	const generation = record.generation;
	if (typeof generation !== "number" || !Number.isInteger(generation) || generation < 1) {
		throw new IpcValidationError("active.generation invalid");
	}
	if (expected?.generation !== undefined && generation !== expected.generation) {
		throw new IpcValidationError("active.generation mismatch");
	}
	const parentEpoch = requireString(record, "parentEpoch", "active", 64);
	const dispatchedAt = requireString(record, "dispatchedAt", "active", 64);
	return {
		version: 1,
		assignmentId,
		generation,
		parentEpoch,
		dispatchedAt,
	};
}

/**
 * Strict assignment-local started.json validator (at-most-once prompt fence).
 */
export function validateStarted(
	value: unknown,
	expected: {
		runId: string;
		workerId: string;
		generation: number;
		parentEpoch: string;
	},
): IpcStarted {
	if (value === null || typeof value !== "object" || Array.isArray(value)) {
		throw new IpcValidationError("started marker must be a non-null object");
	}
	const record = value as Record<string, unknown>;
	if (record.version !== 1) {
		throw new IpcValidationError("started.version must be 1");
	}
	const runId = requireString(record, "runId", "started", 64);
	const workerId = requireString(record, "workerId", "started", 64);
	if (runId !== expected.runId || workerId !== expected.workerId) {
		throw new IpcValidationError("started identity mismatch");
	}
	const generation = record.generation;
	if (typeof generation !== "number" || !Number.isInteger(generation) || generation < 1) {
		throw new IpcValidationError("started.generation invalid");
	}
	if (generation !== expected.generation) {
		throw new IpcValidationError("started.generation mismatch");
	}
	const parentEpoch = requireString(record, "parentEpoch", "started", 64);
	if (parentEpoch !== expected.parentEpoch) {
		throw new IpcValidationError("started.parentEpoch mismatch");
	}
	const startedAt = requireString(record, "startedAt", "started", 64);
	return {
		version: 1,
		runId,
		workerId,
		generation,
		parentEpoch,
		startedAt,
	};
}

export function validateCancel(value: unknown, expected: { runId: string; workerId: string; generation?: number }): IpcCancel {
	const record = asRecord(value, "cancel");
	requireVersion(record, "cancel");
	const runId = requireString(record, "runId", "cancel", 64);
	const workerId = requireString(record, "workerId", "cancel", 64);
	if (runId !== expected.runId || workerId !== expected.workerId) {
		throw new IpcValidationError("cancel identity mismatch");
	}
	const rawGeneration = record.generation;
	const generation = typeof rawGeneration === "number" ? rawGeneration : undefined;
	if (rawGeneration !== undefined && (generation === undefined || !Number.isInteger(generation) || generation < 1 || (expected.generation !== undefined && generation !== expected.generation))) {
		throw new IpcValidationError("cancel generation mismatch");
	}
	return {
		version: IPC_VERSION,
		runId,
		workerId,
		reason: requireString(record, "reason", "cancel", 1024),
		issuedAt: requireString(record, "issuedAt", "cancel", 64),
		...(generation !== undefined ? { generation } : {}),
	};
}

export function validateHeartbeat(
	value: unknown,
	expected: { runId: string; workerId: string },
): IpcHeartbeat {
	const record = asRecord(value, "heartbeat");
	requireVersion(record, "heartbeat");
	const runId = requireString(record, "runId", "heartbeat", 64);
	const workerId = requireString(record, "workerId", "heartbeat", 64);
	if (runId !== expected.runId || workerId !== expected.workerId) {
		throw new IpcValidationError("heartbeat identity mismatch");
	}
	const seq = record.seq;
	if (typeof seq !== "number" || !Number.isSafeInteger(seq) || seq < 1) {
		throw new IpcValidationError("heartbeat.seq invalid");
	}
	return {
		version: IPC_VERSION,
		runId,
		workerId,
		at: requireString(record, "at", "heartbeat", 64),
		seq,
	};
}

export function validateEvent(
	value: unknown,
	expected: { runId: string; workerId: string },
	previousSeq: number,
): IpcEvent {
	const record = asRecord(value, "event");
	requireVersion(record, "event");
	const runId = requireString(record, "runId", "event", 64);
	const workerId = requireString(record, "workerId", "event", 64);
	if (runId !== expected.runId || workerId !== expected.workerId) {
		throw new IpcValidationError("event identity mismatch");
	}
	const type = requireString(record, "type", "event", 32);
	if (!EVENT_TYPES.has(type as IpcEventType)) {
		throw new IpcValidationError(`event.type invalid: ${type}`);
	}
	const seq = record.seq;
	if (typeof seq !== "number" || !Number.isSafeInteger(seq) || seq <= previousSeq) {
		throw new IpcValidationError("event.seq must be monotonically increasing");
	}
	const event: IpcEvent = {
		version: IPC_VERSION,
		type: type as IpcEventType,
		at: requireString(record, "at", "event", 64),
		runId,
		workerId,
		seq,
	};
	if (typeof record.message === "string") {
		if (record.message.length > MAX_EVENT_MESSAGE_CHARS) {
			throw new IpcValidationError("event.message too long");
		}
		event.message = record.message;
	}
	if (typeof record.toolName === "string") event.toolName = record.toolName.slice(0, 128);
	if (typeof record.state === "string") event.state = record.state.slice(0, 64);
	return event;
}

export function validateResult(value: unknown, expected: { runId: string; workerId: string }): IpcResult {
	const record = asRecord(value, "result");
	requireVersion(record, "result");
	const runId = requireString(record, "runId", "result", 64);
	const workerId = requireString(record, "workerId", "result", 64);
	if (runId !== expected.runId || workerId !== expected.workerId) {
		throw new IpcValidationError("result identity mismatch");
	}
	const status = requireString(record, "status", "result", 32);
	if (status !== "completed" && status !== "failed" && status !== "aborted") {
		throw new IpcValidationError("result.status invalid");
	}
	if (!Array.isArray(record.messages)) {
		throw new IpcValidationError("result.messages must be an array");
	}
	const messages = validateResultMessages(record.messages);
	const result: IpcResult = {
		version: IPC_VERSION,
		runId,
		workerId,
		status: status as IpcResult["status"],
		messages,
		finishedAt: requireString(record, "finishedAt", "result", 64),
	};
	if (typeof record.stopReason === "string") {
		if (record.stopReason.length > 64 || record.stopReason.includes("\0")) {
			throw new IpcValidationError("result.stopReason invalid");
		}
		if (!ALLOWED_STOP_REASONS.has(record.stopReason)) {
			throw new IpcValidationError(`result.stopReason not allowed: ${record.stopReason}`);
		}
		result.stopReason = record.stopReason;
	}
	if (typeof record.errorMessage === "string") {
		if (record.errorMessage.includes("\0") || record.errorMessage.length > 8192) {
			throw new IpcValidationError("result.errorMessage invalid");
		}
		result.errorMessage = record.errorMessage;
	}
	if (typeof record.uncertainWrite === "boolean") result.uncertainWrite = record.uncertainWrite;
	if (record.usage !== undefined) result.usage = validateUsage(record.usage);
	return result;
}

function validateUsage(value: unknown): unknown {
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		throw new IpcValidationError("result.usage must be an object");
	}
	const usage = value as Record<string, unknown>;
	for (const [key, entry] of Object.entries(usage)) {
		if (key.includes("\0") || key.length > 64) {
			throw new IpcValidationError("result.usage key invalid");
		}
		if (typeof entry === "number") {
			if (!Number.isFinite(entry) || entry < 0 || entry > 1e15) {
				throw new IpcValidationError(`result.usage.${key} out of bounds`);
			}
			continue;
		}
		if (typeof entry === "object" && entry !== null && !Array.isArray(entry)) {
			for (const [nestedKey, nested] of Object.entries(entry as Record<string, unknown>)) {
				if (typeof nested !== "number" || !Number.isFinite(nested) || nested < 0 || nested > 1e15) {
					throw new IpcValidationError(`result.usage.${key}.${nestedKey} invalid`);
				}
			}
			continue;
		}
		throw new IpcValidationError(`result.usage.${key} must be numeric`);
	}
	return usage;
}

function validateResultMessages(messages: unknown[]): unknown[] {
	const out: unknown[] = [];
	for (const message of messages) {
		if (typeof message !== "object" || message === null || Array.isArray(message)) {
			throw new IpcValidationError("result.messages entries must be objects");
		}
		const record = message as Record<string, unknown>;
		if (record.role !== "assistant") {
			throw new IpcValidationError("result.messages must be assistant-only");
		}
		if (!Array.isArray(record.content) || record.content.length !== 1) {
			throw new IpcValidationError("result.messages content must be a single text part");
		}
		const part = record.content[0];
		if (typeof part !== "object" || part === null) {
			throw new IpcValidationError("result.messages content part invalid");
		}
		const content = part as Record<string, unknown>;
		if (content.type !== "text" || typeof content.text !== "string" || content.text.includes("\0")) {
			throw new IpcValidationError("result.messages text part invalid");
		}
		const cleaned: Record<string, unknown> = {
			role: "assistant",
			content: [{ type: "text", text: content.text }],
		};
		if (record.usage !== undefined) cleaned.usage = validateUsage(record.usage);
		if (typeof record.stopReason === "string") {
			if (!ALLOWED_STOP_REASONS.has(record.stopReason)) {
				throw new IpcValidationError("result.messages stopReason invalid");
			}
			cleaned.stopReason = record.stopReason;
		}
		if (typeof record.errorMessage === "string") {
			if (record.errorMessage.includes("\0") || record.errorMessage.length > 8192) {
				throw new IpcValidationError("result.messages errorMessage invalid");
			}
			cleaned.errorMessage = record.errorMessage;
		}
		const keys = Object.keys(record).filter(
			(key) => !["role", "content", "usage", "stopReason", "errorMessage"].includes(key),
		);
		if (keys.length > 0) {
			throw new IpcValidationError(`result.messages contains forbidden fields: ${keys.join(",")}`);
		}
		out.push(cleaned);
	}
	return out;
}

/** Keep only assistant text + usage/stop/error fields for parent synthesis. */
export function sanitizeAssistantMessages(messages: readonly unknown[], maxBytes: number): unknown[] {
	const sanitized: unknown[] = [];
	let total = 0;
	for (const message of messages) {
		if (typeof message !== "object" || message === null) continue;
		const record = message as Record<string, unknown>;
		if (record.role !== "assistant" || !Array.isArray(record.content)) continue;
		const textParts: { type: "text"; text: string }[] = [];
		for (const part of record.content) {
			if (typeof part !== "object" || part === null) continue;
			const content = part as Record<string, unknown>;
			if (content.type === "text" && typeof content.text === "string") {
				textParts.push({ type: "text", text: content.text });
			}
		}
		if (textParts.length === 0) continue;
		let text = textParts.map((part) => part.text).join("");
		const budget = Math.max(0, maxBytes - total);
		const bytes = Buffer.byteLength(text, "utf8");
		if (bytes > budget) {
			// Truncate to budget without splitting code points.
			let out = "";
			let used = 0;
			for (const cp of text) {
				const size = Buffer.byteLength(cp, "utf8");
				if (used + size > budget) break;
				out += cp;
				used += size;
			}
			text = `${out}\n\n[Output truncated for IPC result bound]`;
		}
		total += Buffer.byteLength(text, "utf8");
		const cleaned: Record<string, unknown> = {
			role: "assistant",
			content: [{ type: "text", text }],
		};
		if (record.usage && typeof record.usage === "object") cleaned.usage = record.usage;
		if (typeof record.stopReason === "string") cleaned.stopReason = record.stopReason;
		if (typeof record.errorMessage === "string") cleaned.errorMessage = record.errorMessage;
		sanitized.push(cleaned);
		if (total >= maxBytes) break;
	}
	return sanitized;
}

export function assertSafeEnvValue(key: string, value: string): void {
	if (!/^[A-Z][A-Z0-9_]*$/.test(key)) {
		throw new Error(`Unsafe env key: ${key}`);
	}
	if (value.includes("\0") || value.includes("\n") || value.includes("\r")) {
		throw new Error(`Unsafe env value for ${key}`);
	}
	if (key.includes("TASK") || key.toLowerCase().includes("prompt")) {
		throw new Error(`Task content must not be placed in env: ${key}`);
	}
}

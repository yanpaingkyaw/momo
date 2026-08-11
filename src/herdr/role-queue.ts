import {
	existsSync,
	mkdirSync,
	readdirSync,
	renameSync,
	rmSync,
	writeFileSync,
	openSync,
	closeSync,
	chmodSync,
} from "node:fs";
import path from "node:path";
import type { AgentName } from "../roles.js";
import {
	atomicWriteJson,
	ensurePrivateDir,
	MAX_IPC_JSON_BYTES,
} from "../ipc/spool.js";
import { parseJsonFile } from "../ipc/validate.js";
import {
	assertAssignmentId,
	createAssignmentId,
	createLockToken,
	ensurePoolLayout,
	rolePoolRoot,
} from "./pool-identity.js";

/** @deprecated Unused — automatic stale takeover is disabled (fail closed). */
export const LOCK_STALE_MS = 30_000;
/** @deprecated Unused — automatic missing-owner deletion is disabled (fail closed). */
export const LOCK_MISSING_OWNER_GRACE_MS = 5_000;
export const LOCK_HEARTBEAT_MS = 2_000;

import type { IpcModelPolicy } from "../ipc/spool.js";
import { IPC_PROTOCOL_CAPABILITY } from "../ipc/spool.js";
import { assertExactKeys as assertIpcExactKeys, parseDiscriminatedIpcPolicy, validateModelPolicyField } from "../ipc/validate.js";

export interface QueueEntry {
	version: 1;
	capability?: number;
	assignmentId: string;
	workerId: string;
	generation: number;
	parentEpoch: string;
	task: string;
	enqueuedAt: string;
	seq: number;
	modelPolicy?: IpcModelPolicy;
}

export interface LockOwnerRecord {
	version: 1;
	token: string;
	pid: number;
	at: string;
	heartbeatAt: string;
}

export class QueueLockError extends Error {
	readonly initError?: unknown;
	readonly rollbackError?: unknown;

	constructor(
		message: string,
		options?: { cause?: unknown; initError?: unknown; rollbackError?: unknown },
	) {
		super(
			message,
			options?.cause !== undefined ? { cause: options.cause } : undefined,
		);
		this.name = "QueueLockError";
		if (options?.initError !== undefined) this.initError = options.initError;
		if (options?.rollbackError !== undefined) this.rollbackError = options.rollbackError;
	}
}

/** Callback threw/rejected `undefined` — bare undefined cannot be rethrown with throw semantics. */
export class RoleLockCallbackFailure extends Error {
	readonly callbackValue: unknown;

	constructor(callbackValue: unknown) {
		super(
			callbackValue === undefined
				? "Role lock callback threw undefined"
				: callbackValue instanceof Error
					? callbackValue.message
					: String(callbackValue),
		);
		this.name = "RoleLockCallbackFailure";
		this.callbackValue = callbackValue;
		if (callbackValue !== undefined) {
			this.cause = callbackValue;
		}
	}
}

export class QueueCorruptionError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "QueueCorruptionError";
	}
}

const ASSIGNMENT_ID_RE = /^[a-z0-9]{8,32}$/i;

function sleepAsync(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Non-busy synchronous sleep via Atomics.wait. */
function sleepSync(ms: number): void {
	const sab = new SharedArrayBuffer(4);
	const view = new Int32Array(sab);
	Atomics.wait(view, 0, 0, Math.max(1, ms));
}

function ownerPath(lockDir: string): string {
	return path.join(lockDir, "owner.json");
}

function writeOwnerAtomicDefault(lockDir: string, owner: LockOwnerRecord): void {
	const filePath = ownerPath(lockDir);
	const tmp = `${filePath}.${process.pid}.${Date.now()}.tmp`;
	const fd = openSync(tmp, "wx", 0o600);
	try {
		writeFileSync(fd, `${JSON.stringify(owner)}\n`, { encoding: "utf8" });
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

function removeLockDirDefault(lockDir: string): void {
	rmSync(lockDir, { recursive: true, force: true });
}

function formatLockError(error: unknown): string {
	if (error instanceof Error) return error.message;
	return String(error);
}

/** @internal test-only: injectable acquire init / rollback primitives. */
let writeOwnerAtomicForAcquire = writeOwnerAtomicDefault;
let removeLockDirForAcquireRollback = removeLockDirDefault;
let removeLockDirForRelease = removeLockDirDefault;
let afterLockOwnerInitializedForTest: ((lockDir: string) => void) | undefined;

/** @internal test-only: inject owner-write / heartbeat-init / rollback-rm faults. */
export function __setLockAcquireHooksForTest(hooks?: {
	writeOwnerAtomic?: (lockDir: string, owner: LockOwnerRecord) => void;
	removeLockDir?: (lockDir: string) => void;
	/** Runs after owner.json + heartbeat timer start (heartbeat-init fault point). */
	afterOwnerInitialized?: (lockDir: string) => void;
}): void {
	writeOwnerAtomicForAcquire = hooks?.writeOwnerAtomic ?? writeOwnerAtomicDefault;
	removeLockDirForAcquireRollback = hooks?.removeLockDir ?? removeLockDirDefault;
	afterLockOwnerInitializedForTest = hooks?.afterOwnerInitialized;
}

/** @internal test-only: inject release-time lock directory removal faults. */
export function __setLockReleaseHooksForTest(hooks?: {
	removeLockDir?: (lockDir: string) => void;
}): void {
	removeLockDirForRelease = hooks?.removeLockDir ?? removeLockDirDefault;
}

/** @internal test-only */
export function __resetLockReleaseHooksForTest(): void {
	removeLockDirForRelease = removeLockDirDefault;
}

function writeOwnerAtomic(lockDir: string, owner: LockOwnerRecord): void {
	writeOwnerAtomicForAcquire(lockDir, owner);
}

export function readLockOwnerForTest(lockDir: string): LockOwnerRecord | undefined {
	return readOwner(lockDir);
}

function readOwner(lockDir: string): LockOwnerRecord | undefined {
	const filePath = ownerPath(lockDir);
	if (!existsSync(filePath)) return undefined;
	try {
		const parsed = parseJsonFile(filePath) as Record<string, unknown>;
		if (
			parsed.version !== 1 ||
			typeof parsed.token !== "string" ||
			typeof parsed.pid !== "number" ||
			typeof parsed.at !== "string" ||
			typeof parsed.heartbeatAt !== "string"
		) {
			return undefined;
		}
		return {
			version: 1,
			token: parsed.token,
			pid: parsed.pid,
			at: parsed.at,
			heartbeatAt: parsed.heartbeatAt,
		};
	} catch {
		return undefined;
	}
}

/**
 * Cross-parent exclusive lock via mkdir.
 * Automatic stale/dead-owner takeover is intentionally disabled: acquisition
 * fails closed (times out) until the live owner releases or an operator
 * removes the lock directory. Owner release is token-verified and never
 * deletes a successor's lock.
 */
export class RoleTransactionLock {
	readonly lockDir: string;
	private held = false;
	private token: string | undefined;
	private heartbeatTimer: ReturnType<typeof setInterval> | undefined;

	constructor(
		poolRoot: string,
		role: AgentName,
		private readonly options: {
			now?: () => number;
			/** @deprecated Ignored — automatic stale takeover is disabled. */
			staleMs?: number;
			/** @deprecated Ignored — automatic missing-owner deletion is disabled. */
			missingOwnerGraceMs?: number;
			sleep?: (ms: number) => Promise<void>;
		} = {},
	) {
		this.lockDir = path.join(rolePoolRoot(poolRoot, role), "tx.lock");
		void this.options.staleMs;
		void this.options.missingOwnerGraceMs;
	}

	acquireSync(timeoutMs = 10_000): void {
		const now = this.options.now ?? Date.now;
		ensurePrivateDir(path.dirname(this.lockDir));
		const deadline = now() + timeoutMs;
		while (now() < deadline) {
			if (this.tryAcquireOnce(now())) return;
			// Fail closed: never auto-delete/rename a stale or owner-less lock.
			sleepSync(25);
		}
		throw new QueueLockError(`Timed out acquiring role transaction lock at ${this.lockDir}`);
	}

	async acquire(timeoutMs = 10_000): Promise<void> {
		const now = this.options.now ?? Date.now;
		const sleep = this.options.sleep ?? sleepAsync;
		ensurePrivateDir(path.dirname(this.lockDir));
		const deadline = now() + timeoutMs;
		while (now() < deadline) {
			if (this.tryAcquireOnce(now())) return;
			// Fail closed: never auto-delete/rename a stale or owner-less lock.
			await sleep(25);
		}
		throw new QueueLockError(`Timed out acquiring role transaction lock at ${this.lockDir}`);
	}

	private tryAcquireOnce(nowMs: number): boolean {
		/** True only when this call's mkdirSync created tx.lock (never EEXIST peers). */
		let createdByThisCall = false;
		try {
			mkdirSync(this.lockDir, { mode: 0o700 });
			createdByThisCall = true;
			const token = createLockToken();
			const at = new Date(nowMs).toISOString();
			writeOwnerAtomic(this.lockDir, {
				version: 1,
				token,
				pid: process.pid,
				at,
				heartbeatAt: at,
			});
			this.token = token;
			this.held = true;
			this.startHeartbeat();
			afterLockOwnerInitializedForTest?.(this.lockDir);
			return true;
		} catch (error) {
			const code = (error as NodeJS.ErrnoException).code;
			if (code === "EEXIST") {
				// Peer (or pre-existing) lock — never remove.
				return false;
			}
			// Owner atomic creation / heartbeat init failed after mkdir: reset local
			// state and remove only the directory this call created.
			this.stopHeartbeat();
			this.held = false;
			this.token = undefined;
			if (createdByThisCall) {
				try {
					removeLockDirForAcquireRollback(this.lockDir);
				} catch (rollbackError) {
					throw new QueueLockError(
						`Failed to initialize role lock at ${this.lockDir}; rollback remove failed — lock directory may remain. init=${formatLockError(error)}; rollback=${formatLockError(rollbackError)}`,
						{ cause: error, initError: error, rollbackError },
					);
				}
			}
			throw error;
		}
	}

	/**
	 * Synchronous owner release: token must still match. Without automatic
	 * takeover, the lock directory cannot be recycled by a peer while we hold
	 * it, so token-check then rmSync cannot delete a successor.
	 */
	release(): void {
		if (!this.held || !this.token) return;
		this.stopHeartbeat();
		const lockDir = this.lockDir;
		const token = this.token;
		this.held = false;
		this.token = undefined;
		try {
			releaseRoleTransactionLock(lockDir, token);
		} catch {
			// best effort — lock path may remain for supervised recovery
		}
	}

	/** Stop heartbeat, clear held state, validate token, then remove (throws on failure). */
	releaseStrict(): void {
		if (!this.held || !this.token) return;
		this.stopHeartbeat();
		const lockDir = this.lockDir;
		const token = this.token;
		this.held = false;
		this.token = undefined;
		releaseRoleTransactionLock(lockDir, token);
	}

	isHeartbeatActiveForTest(): boolean {
		return this.heartbeatTimer !== undefined;
	}

	getTokenForTest(): string | undefined {
		return this.token;
	}

	private startHeartbeat(): void {
		this.stopHeartbeat();
		const now = this.options.now ?? Date.now;
		this.heartbeatTimer = setInterval(() => {
			if (!this.held || !this.token) return;
			try {
				const owner = readOwner(this.lockDir);
				if (!owner || owner.token !== this.token) return;
				writeOwnerAtomic(this.lockDir, {
					...owner,
					heartbeatAt: new Date(now()).toISOString(),
				});
			} catch {
				// ignore
			}
		}, LOCK_HEARTBEAT_MS);
		this.heartbeatTimer.unref?.();
	}

	private stopHeartbeat(): void {
		if (this.heartbeatTimer) {
			clearInterval(this.heartbeatTimer);
			this.heartbeatTimer = undefined;
		}
	}
}

function releaseRoleTransactionLock(lockDir: string, token: string): void {
	const owner = readOwner(lockDir);
	if (!owner || owner.token !== token) {
		throw new QueueLockError("Role transaction lock release refused: token mismatch");
	}
	try {
		removeLockDirForRelease(lockDir);
	} catch (error) {
		throw new QueueLockError(
			`Role transaction lock release removal failed at ${lockDir}: ${formatLockError(error)}`,
			{ cause: error },
		);
	}
}

function rethrowCallbackFailure(callbackError: unknown): never {
	if (callbackError === undefined) {
		throw new RoleLockCallbackFailure(undefined);
	}
	throw callbackError;
}

function finalizeReleaseFromCallback(
	callbackFailed: boolean,
	callbackError: unknown,
	releaseFailed: boolean,
	releaseError: unknown,
): void {
	if (callbackFailed && releaseFailed) {
		throw new AggregateError(
			[callbackError, releaseError],
			"Role lock operation failed and lock release failed",
		);
	}
	if (releaseFailed) throw releaseError;
	if (callbackFailed) rethrowCallbackFailure(callbackError);
}

export function withRoleLock<T>(
	poolRoot: string,
	role: AgentName,
	fn: () => T,
	options?: {
		now?: () => number;
		staleMs?: number;
		timeoutMs?: number;
		sleep?: (ms: number) => Promise<void>;
	},
): T {
	const lock = new RoleTransactionLock(poolRoot, role, options);
	lock.acquireSync(options?.timeoutMs ?? 10_000);
	let callbackFailed = false;
	let callbackError: unknown;
	try {
		return fn();
	} catch (error) {
		callbackFailed = true;
		callbackError = error;
	} finally {
		let releaseFailed = false;
		let releaseError: unknown;
		try {
			lock.releaseStrict();
		} catch (error) {
			releaseFailed = true;
			releaseError = error;
		}
		finalizeReleaseFromCallback(callbackFailed, callbackError, releaseFailed, releaseError);
	}
	throw new Error("unreachable role lock callback");
}

export async function withRoleLockAsync<T>(
	poolRoot: string,
	role: AgentName,
	fn: () => Promise<T> | T,
	options?: {
		now?: () => number;
		staleMs?: number;
		timeoutMs?: number;
		sleep?: (ms: number) => Promise<void>;
	},
): Promise<T> {
	const lock = new RoleTransactionLock(poolRoot, role, options);
	await lock.acquire(options?.timeoutMs ?? 10_000);
	let callbackFailed = false;
	let callbackError: unknown;
	let result: T | undefined;
	try {
		result = await fn();
	} catch (error) {
		callbackFailed = true;
		callbackError = error;
	} finally {
		let releaseFailed = false;
		let releaseError: unknown;
		try {
			lock.releaseStrict();
		} catch (error) {
			releaseFailed = true;
			releaseError = error;
		}
		finalizeReleaseFromCallback(callbackFailed, callbackError, releaseFailed, releaseError);
	}
	return result as T;
}

function queueDir(poolRoot: string, role: AgentName): string {
	return path.join(rolePoolRoot(poolRoot, role), "queue");
}

function claimingDir(poolRoot: string, role: AgentName): string {
	return path.join(rolePoolRoot(poolRoot, role), "claiming");
}

function queueFileName(seq: number, assignmentId: string): string {
	return `${String(seq).padStart(8, "0")}-${assignmentId}.json`;
}

export function validateQueueEntry(value: unknown, label: string): QueueEntry {
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		throw new QueueCorruptionError(`${label} must be an object`);
	}
	const record = value as Record<string, unknown>;
	if (record.version !== 1) throw new QueueCorruptionError(`${label}.version invalid`);
	if (typeof record.assignmentId !== "string" || !ASSIGNMENT_ID_RE.test(record.assignmentId)) {
		throw new QueueCorruptionError(`${label}.assignmentId invalid`);
	}
	if (typeof record.workerId !== "string" || !record.workerId || record.workerId.length > 64) {
		throw new QueueCorruptionError(`${label}.workerId invalid`);
	}
	if (typeof record.generation !== "number" || !Number.isInteger(record.generation) || record.generation < 1) {
		throw new QueueCorruptionError(`${label}.generation invalid`);
	}
	if (typeof record.parentEpoch !== "string" || !record.parentEpoch || record.parentEpoch.length > 64) {
		throw new QueueCorruptionError(`${label}.parentEpoch invalid`);
	}
	if (typeof record.task !== "string" || record.task.length === 0 || record.task.length > 100_000) {
		throw new QueueCorruptionError(`${label}.task invalid`);
	}
	if (typeof record.enqueuedAt !== "string" || !record.enqueuedAt) {
		throw new QueueCorruptionError(`${label}.enqueuedAt invalid`);
	}
	if (typeof record.seq !== "number" || !Number.isInteger(record.seq) || record.seq < 1) {
		throw new QueueCorruptionError(`${label}.seq invalid`);
	}
	const capability =
		record.capability === undefined
			? undefined
			: record.capability === IPC_PROTOCOL_CAPABILITY
				? IPC_PROTOCOL_CAPABILITY
				: (() => {
						throw new QueueCorruptionError(`${label}.capability invalid`);
					})();
	const ipcPolicy = parseDiscriminatedIpcPolicy(record, label);
	if (capability !== undefined && ipcPolicy.capability !== capability) {
		throw new QueueCorruptionError(`${label}.capability mismatch`);
	}
	const modelPolicy = ipcPolicy.modelPolicy;
	if (ipcPolicy.capability === IPC_PROTOCOL_CAPABILITY) {
		assertIpcExactKeys(
			record,
			[
				"version",
				"capability",
				"assignmentId",
				"workerId",
				"generation",
				"parentEpoch",
				"task",
				"enqueuedAt",
				"seq",
				"modelPolicy",
			],
			label,
		);
	} else if ("capability" in record || "modelPolicy" in record) {
		throw new QueueCorruptionError(`${label} legacy entry must omit capability and modelPolicy`);
	}
	return {
		version: 1,
		assignmentId: record.assignmentId,
		workerId: record.workerId,
		generation: record.generation,
		parentEpoch: record.parentEpoch,
		task: record.task,
		enqueuedAt: record.enqueuedAt,
		seq: record.seq,
		...(ipcPolicy.capability !== undefined ? { capability: ipcPolicy.capability } : {}),
		...(modelPolicy !== undefined ? { modelPolicy } : {}),
	};
}

function nextSeq(poolRoot: string, role: AgentName): number {
	const dir = queueDir(poolRoot, role);
	ensurePrivateDir(dir);
	const claiming = claimingDir(poolRoot, role);
	ensurePrivateDir(claiming);
	const names = [
		...(existsSync(dir) ? readdirSync(dir).filter((name) => name.endsWith(".json")) : []),
		...(existsSync(claiming) ? readdirSync(claiming).filter((name) => name.endsWith(".json")) : []),
	];
	let max = 0;
	for (const name of names) {
		const match = /^(\d+)-/.exec(name);
		if (match) max = Math.max(max, Number(match[1]));
	}
	return max + 1;
}

export function enqueueAssignment(
	poolRoot: string,
	role: AgentName,
	entry: Omit<QueueEntry, "version" | "seq" | "enqueuedAt"> & { enqueuedAt?: string },
	now: () => number = Date.now,
): QueueEntry {
	ensurePoolLayout(poolRoot, role);
	assertAssignmentId(entry.assignmentId);
	const seq = nextSeq(poolRoot, role);
	const full: QueueEntry = {
		version: 1,
		assignmentId: entry.assignmentId,
		workerId: entry.workerId,
		generation: entry.generation,
		parentEpoch: entry.parentEpoch,
		task: entry.task,
		enqueuedAt: entry.enqueuedAt ?? new Date(now()).toISOString(),
		seq,
		...(entry.capability !== undefined ? { capability: entry.capability } : {}),
		...(entry.modelPolicy !== undefined ? { modelPolicy: entry.modelPolicy } : {}),
	};
	validateQueueEntry(full, "queue entry");
	const filePath = path.join(queueDir(poolRoot, role), queueFileName(seq, entry.assignmentId));
	atomicWriteJson(filePath, full, MAX_IPC_JSON_BYTES);
	return full;
}

function readQueueFile(filePath: string): QueueEntry {
	const parsed = parseJsonFile(filePath);
	return validateQueueEntry(parsed, path.basename(filePath));
}

export function listQueue(poolRoot: string, role: AgentName): QueueEntry[] {
	const dir = queueDir(poolRoot, role);
	if (!existsSync(dir)) return [];
	const names = readdirSync(dir)
		.filter((name) => name.endsWith(".json"))
		.sort();
	return names.map((name) => readQueueFile(path.join(dir, name)));
}

export function listClaiming(poolRoot: string, role: AgentName): QueueEntry[] {
	const dir = claimingDir(poolRoot, role);
	if (!existsSync(dir)) return [];
	const names = readdirSync(dir)
		.filter((name) => name.endsWith(".json"))
		.sort();
	return names.map((name) => readQueueFile(path.join(dir, name)));
}

export function queueCount(poolRoot: string, role: AgentName): number {
	return listQueue(poolRoot, role).length + listClaiming(poolRoot, role).length;
}

export function cancelQueuedAssignment(
	poolRoot: string,
	role: AgentName,
	assignmentId: string,
): boolean {
	assertAssignmentId(assignmentId);
	const dir = queueDir(poolRoot, role);
	if (!existsSync(dir)) return false;
	for (const name of readdirSync(dir)) {
		if (name.endsWith(`-${assignmentId}.json`)) {
			const filePath = path.join(dir, name);
			readQueueFile(filePath);
			rmSync(filePath, { force: true });
			return true;
		}
	}
	return false;
}

/**
 * Durable claim step 1: atomically rename head queue → claiming/.
 * Does NOT remove until commitClaim succeeds.
 */
export function beginClaimHead(
	poolRoot: string,
	role: AgentName,
	expectedGeneration: number,
): QueueEntry | undefined {
	const entries = listQueue(poolRoot, role);
	const head = entries[0];
	if (!head) return undefined;
	if (head.generation !== expectedGeneration) {
		throw new QueueCorruptionError(
			`Queue generation fence mismatch: entry=${head.generation} expected=${expectedGeneration}`,
		);
	}
	const src = path.join(queueDir(poolRoot, role), queueFileName(head.seq, head.assignmentId));
	const dest = path.join(claimingDir(poolRoot, role), queueFileName(head.seq, head.assignmentId));
	ensurePrivateDir(claimingDir(poolRoot, role));
	renameSync(src, dest);
	return head;
}

/** Durable claim step 2: remove claiming entry after command+active+registry durable. */
export function commitClaim(poolRoot: string, role: AgentName, entry: QueueEntry): void {
	const dest = path.join(claimingDir(poolRoot, role), queueFileName(entry.seq, entry.assignmentId));
	if (existsSync(dest)) {
		rmSync(dest, { force: true });
	}
}

/** Recover incomplete claims for idempotent replay. */
export function recoverClaiming(
	poolRoot: string,
	role: AgentName,
	expectedGeneration: number,
): QueueEntry[] {
	return listClaiming(poolRoot, role).map((entry) => {
		if (entry.generation !== expectedGeneration) {
			throw new QueueCorruptionError(
				`Claiming generation fence mismatch: entry=${entry.generation} expected=${expectedGeneration}`,
			);
		}
		return entry;
	});
}

export function peekHead(poolRoot: string, role: AgentName): QueueEntry | undefined {
	const claiming = listClaiming(poolRoot, role);
	if (claiming[0]) return claiming[0];
	return listQueue(poolRoot, role)[0];
}

/**
 * FIFO queue/claim priority head (claiming precedes queue). Use under the role
 * lock when deciding idle direct dispatch vs enqueue.
 */
export function pendingRoleQueueHead(
	poolRoot: string,
	role: AgentName,
): QueueEntry | undefined {
	return peekHead(poolRoot, role);
}

/**
 * True when claiming or queued work for the same generation would be overtaken
 * by a direct idle dispatch of assignmentId.
 */
export function roleHasPendingWorkAheadOfAssignment(
	poolRoot: string,
	role: AgentName,
	generation: number,
	assignmentId: string,
): boolean {
	for (const entry of listClaiming(poolRoot, role)) {
		if (entry.generation !== generation) continue;
		if (entry.assignmentId !== assignmentId) return true;
	}
	for (const entry of listQueue(poolRoot, role)) {
		if (entry.generation !== generation) continue;
		if (entry.assignmentId !== assignmentId) return true;
	}
	return false;
}

export function newQueuedAssignmentId(): string {
	return createAssignmentId();
}

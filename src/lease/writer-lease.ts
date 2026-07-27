import {
	existsSync,
	mkdirSync,
	readFileSync,
	renameSync,
	rmSync,
	rmdirSync,
	writeFileSync,
	openSync,
	closeSync,
	chmodSync,
	realpathSync,
} from "node:fs";
import { randomBytes } from "node:crypto";
import path from "node:path";
import { cwdHash, momoCacheRoot } from "../ipc/spool.js";

export const LEASE_STALE_MS = 20_000;
/** Default wait for cross-process implementer serialization (matches session prompt bound). */
export const DEFAULT_LEASE_WAIT_MS = 60 * 60 * 1000;
export const DEFAULT_LEASE_RETRY_MS = 250;

export interface WriterLeaseRecord {
	version: 1;
	cwd: string;
	ownerId: string;
	token: string;
	heartbeatAt: string;
	acquiredAt: string;
	pid: number;
}

export interface WriterLeaseOptions {
	cacheRoot?: string;
	now?: () => number;
	staleMs?: number;
	sleep?: (ms: number) => Promise<void>;
}

export interface WaitAcquireOptions {
	deadlineMs?: number;
	intervalMs?: number;
	now?: () => number;
	sleep?: (ms: number) => Promise<void>;
	shouldCancel?: () => boolean | Promise<boolean>;
	onWaiting?: (info: { holder?: string; waitedMs: number }) => void;
}

export class LeaseWaitTimeoutError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "LeaseWaitTimeoutError";
	}
}

export class LeaseWaitCancelledError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "LeaseWaitCancelledError";
	}
}

export function createLeaseToken(): string {
	return randomBytes(32).toString("hex");
}

function leaseDirForCwd(cwd: string, cacheRoot: string): string {
	const resolved = realpathSync(cwd);
	return path.join(cacheRoot, "leases", cwdHash(resolved));
}

function ownerPath(lockDir: string): string {
	return path.join(lockDir, "owner.json");
}

export class LeaseCorruptionError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "LeaseCorruptionError";
	}
}

function readOwner(lockDir: string): WriterLeaseRecord | undefined {
	const filePath = ownerPath(lockDir);
	if (!existsSync(filePath)) return undefined;
	let parsed: unknown;
	try {
		parsed = JSON.parse(readFileSync(filePath, "utf8"));
	} catch (error) {
		throw new LeaseCorruptionError(
			`Corrupt writer lease owner JSON at ${filePath}: ${
				error instanceof Error ? error.message : String(error)
			}`,
		);
	}
	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
		throw new LeaseCorruptionError(`Corrupt writer lease owner record at ${filePath}`);
	}
	const record = parsed as Record<string, unknown>;
	if (
		record.version !== 1 ||
		typeof record.ownerId !== "string" ||
		typeof record.token !== "string" ||
		typeof record.cwd !== "string" ||
		typeof record.heartbeatAt !== "string" ||
		typeof record.acquiredAt !== "string"
	) {
		throw new LeaseCorruptionError(`Corrupt writer lease owner record at ${filePath}`);
	}
	return {
		version: 1,
		cwd: record.cwd,
		ownerId: record.ownerId,
		token: record.token,
		heartbeatAt: record.heartbeatAt,
		acquiredAt: record.acquiredAt,
		pid: typeof record.pid === "number" ? record.pid : 0,
	};
}

function writeOwner(lockDir: string, record: WriterLeaseRecord): void {
	const filePath = ownerPath(lockDir);
	const tmp = `${filePath}.${process.pid}.${randomBytes(4).toString("hex")}.tmp`;
	const fd = openSync(tmp, "wx", 0o600);
	try {
		writeFileSync(fd, `${JSON.stringify(record, null, 2)}\n`, "utf8");
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

export class WriterLeaseManager {
	private readonly cacheRoot: string;
	private readonly now: () => number;
	private readonly staleMs: number;
	private readonly sleep: (ms: number) => Promise<void>;

	constructor(options: WriterLeaseOptions = {}) {
		this.cacheRoot = options.cacheRoot ?? momoCacheRoot();
		this.now = options.now ?? Date.now;
		this.staleMs = options.staleMs ?? LEASE_STALE_MS;
		this.sleep =
			options.sleep ?? ((ms: number) => new Promise((resolve) => setTimeout(resolve, ms)));
	}

	dirFor(cwd: string): string {
		return leaseDirForCwd(cwd, this.cacheRoot);
	}

	/**
	 * Atomically acquire a cwd writer lease via exclusive mkdir.
	 * Never steals a foreign lease, including stale/uncertain ones.
	 */
	acquire(cwd: string, ownerId: string, token: string): WriterLeaseRecord {
		if (!ownerId || !token || token.length < 32) {
			throw new Error("Writer lease token must be a cryptographically random secret");
		}
		const resolved = realpathSync(cwd);
		const lockDir = this.dirFor(cwd);
		mkdirSync(path.dirname(lockDir), { recursive: true, mode: 0o700 });

		const now = this.now();
		const record: WriterLeaseRecord = {
			version: 1,
			cwd: resolved,
			ownerId,
			token,
			heartbeatAt: new Date(now).toISOString(),
			acquiredAt: new Date(now).toISOString(),
			pid: process.pid,
		};

		try {
			mkdirSync(lockDir, { recursive: false });
		} catch (error) {
			const code = (error as { code?: string }).code;
			if (code !== "EEXIST") throw error;
			const existing = readOwner(lockDir);
			if (!existing) {
				throw new Error("Writer lease lock directory exists without owner metadata");
			}
			const sameOwner = existing.ownerId === ownerId && existing.token === token;
			if (!sameOwner) {
				throw new Error(
					`Writer lease held by ${existing.ownerId}; refusing automatic steal of foreign lease`,
				);
			}
			// Same owner renew path.
			record.acquiredAt = existing.acquiredAt;
			writeOwner(lockDir, record);
			return record;
		}

		try {
			writeOwner(lockDir, record);
			return record;
		} catch (error) {
			try {
				rmSync(ownerPath(lockDir), { force: true });
				rmdirSync(lockDir);
			} catch {
				// best effort
			}
			throw error;
		}
	}

	heartbeat(cwd: string, ownerId: string, token: string): WriterLeaseRecord {
		const lockDir = this.dirFor(cwd);
		const existing = readOwner(lockDir);
		if (!existing || existing.ownerId !== ownerId || existing.token !== token) {
			throw new Error("Cannot heartbeat writer lease: ownership mismatch");
		}
		const record: WriterLeaseRecord = {
			...existing,
			heartbeatAt: new Date(this.now()).toISOString(),
			pid: process.pid,
		};
		writeOwner(lockDir, record);
		return record;
	}

	validate(cwd: string, ownerId: string, token: string): boolean {
		const existing = readOwner(this.dirFor(cwd));
		if (!existing) return false;
		if (existing.ownerId !== ownerId || existing.token !== token) return false;
		const age = this.now() - Date.parse(existing.heartbeatAt);
		return Number.isFinite(age) && age <= this.staleMs;
	}

	/** Release only when caller proves ownership. */
	release(cwd: string, ownerId: string, token: string): void {
		const lockDir = this.dirFor(cwd);
		if (!existsSync(lockDir)) return;
		const existing = readOwner(lockDir);
		if (!existing) {
			throw new Error("Cannot release writer lease: missing owner metadata");
		}
		if (existing.ownerId !== ownerId || existing.token !== token) {
			throw new Error("Cannot release writer lease: ownership mismatch");
		}
		rmSync(ownerPath(lockDir), { force: true });
		rmdirSync(lockDir);
	}

	/**
	 * After a successful ownership-checked release, confirm this owner+token is gone.
	 * A different (foreign) owner acquiring immediately is OK — must not treat as failure.
	 */
	stillHeldBy(cwd: string, ownerId: string, token: string): boolean {
		const existing = this.peekOwner(cwd);
		if (!existing) return false;
		return existing.ownerId === ownerId && existing.token === token;
	}

	/**
	 * Cancellation-aware wait/retry acquire. Never steals a foreign lease.
	 * Bounded by deadlineMs (default one hour).
	 */
	async waitAcquire(
		cwd: string,
		ownerId: string,
		token: string,
		options: WaitAcquireOptions = {},
	): Promise<WriterLeaseRecord> {
		const now = options.now ?? this.now;
		const sleep = options.sleep ?? this.sleep;
		const deadlineMs = options.deadlineMs ?? DEFAULT_LEASE_WAIT_MS;
		const intervalMs = options.intervalMs ?? DEFAULT_LEASE_RETRY_MS;
		const started = now();
		const deadline = started + deadlineMs;

		while (now() <= deadline) {
			if (options.shouldCancel && (await options.shouldCancel())) {
				throw new LeaseWaitCancelledError("Writer lease wait cancelled");
			}
			try {
				return this.acquire(cwd, ownerId, token);
			} catch (error) {
				const message = error instanceof Error ? error.message : String(error);
				if (!/refusing automatic steal|without owner metadata|held by/i.test(message)) {
					throw error;
				}
				const holder = (() => {
					try {
						return this.peekOwner(cwd)?.ownerId;
					} catch {
						return undefined;
					}
				})();
				options.onWaiting?.({
					...(holder !== undefined ? { holder } : {}),
					waitedMs: Math.max(0, now() - started),
				});
				const remaining = deadline - now();
				if (remaining <= 0) break;
				await sleep(Math.min(intervalMs, remaining));
			}
		}
		if (options.shouldCancel && (await options.shouldCancel())) {
			throw new LeaseWaitCancelledError("Writer lease wait cancelled");
		}
		throw new LeaseWaitTimeoutError(
			`Timed out waiting for writer lease after ${deadlineMs}ms`,
		);
	}

	/** True when a lock directory currently exists for cwd. */
	isLocked(cwd: string): boolean {
		return existsSync(this.dirFor(cwd));
	}

	peekOwner(cwd: string): WriterLeaseRecord | undefined {
		const lockDir = this.dirFor(cwd);
		if (!existsSync(lockDir)) return undefined;
		return readOwner(lockDir);
	}

	/**
	 * Supervised force-release for cleanup: only when lock ownerId exactly matches
	 * the expected workerId. Never removes another owner's lease.
	 */
	forceReleaseIfOwner(cwd: string, expectedOwnerId: string): {
		released: boolean;
		reason?: string;
	} {
		const lockDir = this.dirFor(cwd);
		if (!existsSync(lockDir)) {
			return { released: false, reason: "no_lease" };
		}
		const existing = readOwner(lockDir);
		if (!existing) {
			return { released: false, reason: "missing_owner_metadata" };
		}
		const resolved = realpathSync(cwd);
		if (existing.ownerId !== expectedOwnerId) {
			return {
				released: false,
				reason: `owner_mismatch:${existing.ownerId}`,
			};
		}
		if (existing.cwd !== resolved) {
			return { released: false, reason: "cwd_mismatch" };
		}
		rmSync(ownerPath(lockDir), { force: true });
		rmdirSync(lockDir);
		return { released: true };
	}
}

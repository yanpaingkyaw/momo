import {
	closeSync,
	existsSync,
	lstatSync,
	mkdirSync,
	openSync,
	readdirSync,
	renameSync,
	rmdirSync,
	rmSync,
	writeFileSync,
	chmodSync,
} from "node:fs";
import path from "node:path";
import { ensurePrivateDir } from "../ipc/spool.js";
import { createLockToken } from "./pool-identity.js";
import {
	LOCK_HEARTBEAT_MS,
	QueueLockError,
	readLockOwnerForTest,
	type LockOwnerRecord,
} from "./role-queue.js";

export interface PaneLifecycleLockDirIdentity {
	dev: number;
	ino: number;
}

function ownerPath(lockDir: string): string {
	return path.join(lockDir, "owner.json");
}

function sleepAsync(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function sleepSync(ms: number): void {
	const sab = new SharedArrayBuffer(4);
	const view = new Int32Array(sab);
	Atomics.wait(view, 0, 0, Math.max(1, ms));
}

function readOwner(lockDir: string): LockOwnerRecord | undefined {
	return readLockOwnerForTest(lockDir);
}

function formatLockError(error: unknown): string {
	if (error instanceof Error) return error.message;
	return String(error);
}

function captureLockDirIdentity(lockDir: string): PaneLifecycleLockDirIdentity {
	const stat = lstatSync(lockDir);
	return { dev: stat.dev, ino: stat.ino };
}

/** @internal test-only */
let captureLockDirIdentityForAcquire = captureLockDirIdentity;

/** Remove only the exact empty lock directory we created (inode/dev fenced). */
function rmdirOwnCreatedEmptyLockDir(
	lockDir: string,
	identity: PaneLifecycleLockDirIdentity,
	createdDir: boolean,
): void {
	if (!createdDir) return;
	try {
		if (!existsSync(lockDir)) return;
		const stat = lstatSync(lockDir);
		if (stat.dev !== identity.dev || stat.ino !== identity.ino) return;
		if (!stat.isDirectory()) return;
		const entries = readdirSync(lockDir);
		if (entries.length === 0) {
			rmdirSync(lockDir);
		}
	} catch {
		// supervised recovery may be required
	}
}

function rollbackOwnCreatedLockDir(
	lockDir: string,
	identity: PaneLifecycleLockDirIdentity,
	createdDir: boolean,
): void {
	rmdirOwnCreatedEmptyLockDir(lockDir, identity, createdDir);
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

function restoreQuarantineToLiveLock(lockDir: string, quarantine: string): void {
	try {
		if (!existsSync(lockDir) && existsSync(quarantine)) {
			renameSync(quarantine, lockDir);
		}
	} catch {
		// quarantine dir remains as evidence for supervised recovery
	}
}

function releasePaneLifecycleLock(
	lockDir: string,
	token: string,
	identity: PaneLifecycleLockDirIdentity,
): void {
	const owner = readOwner(lockDir);
	if (!owner || owner.token !== token) {
		throw new QueueLockError("Pane lifecycle lock release refused: token mismatch");
	}
	const liveStat = lstatSync(lockDir);
	if (liveStat.dev !== identity.dev || liveStat.ino !== identity.ino) {
		throw new QueueLockError("Pane lifecycle lock release refused: lock directory identity mismatch");
	}
	const quarantine = `${lockDir}.released.${token}`;
	if (existsSync(quarantine)) {
		throw new QueueLockError("Pane lifecycle lock release refused: quarantine path already exists");
	}
	try {
		renameSync(lockDir, quarantine);
	} catch (error) {
		throw new QueueLockError(
			`Pane lifecycle lock release quarantine failed: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
	const quarantinedOwner = readOwner(quarantine);
	let quarantineStat: ReturnType<typeof lstatSync>;
	try {
		quarantineStat = lstatSync(quarantine);
	} catch (error) {
		restoreQuarantineToLiveLock(lockDir, quarantine);
		throw new QueueLockError(
			`Pane lifecycle lock release refused after quarantine stat: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
	if (quarantineStat.dev !== identity.dev || quarantineStat.ino !== identity.ino) {
		restoreQuarantineToLiveLock(lockDir, quarantine);
		throw new QueueLockError("Pane lifecycle lock release refused: quarantine identity mismatch");
	}
	if (!quarantinedOwner || quarantinedOwner.token !== token) {
		restoreQuarantineToLiveLock(lockDir, quarantine);
		throw new QueueLockError("Pane lifecycle lock release refused after quarantine revalidation");
	}
	rmSync(quarantine, { recursive: true, force: true });
}

function finalizeReleaseFromCallback(callbackError: unknown, releaseError: unknown): void {
	if (callbackError !== undefined && releaseError !== undefined) {
		throw new AggregateError(
			[callbackError, releaseError],
			"Pane lifecycle operation failed and lock release failed",
		);
	}
	if (releaseError !== undefined) throw releaseError;
	if (callbackError !== undefined) throw callbackError;
}

/** @internal test-only */
let writeOwnerAtomicForAcquire = writeOwnerAtomicDefault;
/** @internal test-only */
let afterPaneLifecycleLockInitializedForTest: ((lockDir: string) => void) | undefined;

/** @internal test-only */
export function __setPaneLifecycleLockHooksForTest(hooks?: {
	writeOwnerAtomic?: (lockDir: string, owner: LockOwnerRecord) => void;
	afterOwnerInitialized?: (lockDir: string) => void;
	captureLockDirIdentity?: (lockDir: string) => PaneLifecycleLockDirIdentity;
}): void {
	writeOwnerAtomicForAcquire = hooks?.writeOwnerAtomic ?? writeOwnerAtomicDefault;
	afterPaneLifecycleLockInitializedForTest = hooks?.afterOwnerInitialized;
	captureLockDirIdentityForAcquire =
		hooks?.captureLockDirIdentity ?? captureLockDirIdentity;
}

/** @internal test-only */
export function __releasePaneLifecycleLockForTest(
	lockDir: string,
	token: string,
	identity: PaneLifecycleLockDirIdentity,
): void {
	releasePaneLifecycleLock(lockDir, token, identity);
}

/** @internal test-only */
export function __capturePaneLifecycleLockIdentityForTest(
	lockDir: string,
): PaneLifecycleLockDirIdentity {
	return captureLockDirIdentity(lockDir);
}

export function paneLifecycleLockDir(poolRoot: string): string {
	return path.join(poolRoot, "pane-lifecycle.lock");
}

type AcquiredPaneLifecycleLock = {
	lockDir: string;
	token: string;
	createdDir: boolean;
	identity: PaneLifecycleLockDirIdentity;
};

/**
 * Pool-wide token-safe pane lifecycle lock (split/persist/close). Separate from
 * role locks; no automatic stale takeover. Nested role locks must acquire this
 * first when both are needed (global → role).
 */
export class PaneLifecycleLock {
	readonly lockDir: string;
	private held = false;
	private token: string | undefined;
	private identity: PaneLifecycleLockDirIdentity | undefined;
	private createdDir = false;
	private heartbeatTimer: ReturnType<typeof setInterval> | undefined;

	constructor(
		poolRoot: string,
		private readonly options: {
			now?: () => number;
			sleep?: (ms: number) => Promise<void>;
		} = {},
	) {
		this.lockDir = paneLifecycleLockDir(poolRoot);
	}

	acquireSync(timeoutMs = 15_000): void {
		const now = this.options.now ?? Date.now;
		const deadline = now() + timeoutMs;
		while (now() < deadline) {
			if (this.tryAcquireOnce(now())) return;
			sleepSync(25);
		}
		throw new QueueLockError(`Timed out acquiring pane lifecycle lock at ${this.lockDir}`);
	}

	async acquire(timeoutMs = 15_000): Promise<void> {
		const now = this.options.now ?? Date.now;
		const sleep = this.options.sleep ?? sleepAsync;
		const deadline = now() + timeoutMs;
		while (now() < deadline) {
			if (this.tryAcquireOnce(now())) return;
			await sleep(25);
		}
		throw new QueueLockError(`Timed out acquiring pane lifecycle lock at ${this.lockDir}`);
	}

	private tryAcquireOnce(nowMs: number): boolean {
		let createdByThisCall = false;
		let identity: PaneLifecycleLockDirIdentity | undefined;
		const token = createLockToken();
		try {
			mkdirSync(this.lockDir, { mode: 0o700 });
			createdByThisCall = true;
			identity = captureLockDirIdentityForAcquire(this.lockDir);
			const at = new Date(nowMs).toISOString();
			writeOwnerAtomicForAcquire(this.lockDir, {
				version: 1,
				token,
				pid: process.pid,
				at,
				heartbeatAt: at,
			});
			this.token = token;
			this.identity = identity;
			this.createdDir = createdByThisCall;
			this.held = true;
			this.startHeartbeat();
			afterPaneLifecycleLockInitializedForTest?.(this.lockDir);
			return true;
		} catch (error) {
			const code = (error as NodeJS.ErrnoException).code;
			if (code === "EEXIST") return false;
			this.stopHeartbeat();
			this.held = false;
			this.token = undefined;
			this.identity = undefined;
			this.createdDir = false;
			if (createdByThisCall && identity) {
				try {
					const owner = readOwner(this.lockDir);
					if (!owner || owner.token === token) {
						rollbackOwnCreatedLockDir(this.lockDir, identity, true);
					}
				} catch (rollbackError) {
					throw new QueueLockError(
						`Failed to initialize pane lifecycle lock at ${this.lockDir}; rollback remove failed — lock directory may remain. init=${formatLockError(error)}; rollback=${formatLockError(rollbackError)}`,
						{ cause: error, initError: error, rollbackError },
					);
				}
			}
			throw error;
		}
	}

	release(): void {
		if (!this.held || !this.token || !this.identity) return;
		this.stopHeartbeat();
		const acquired: AcquiredPaneLifecycleLock = {
			lockDir: this.lockDir,
			token: this.token,
			createdDir: this.createdDir,
			identity: this.identity,
		};
		this.held = false;
		this.token = undefined;
		this.identity = undefined;
		this.createdDir = false;
		try {
			releasePaneLifecycleLock(acquired.lockDir, acquired.token, acquired.identity);
		} catch {
			// best effort — quarantine may remain for supervised recovery
		}
	}

	/** Stop heartbeat, clear held state, then quarantine release (throws on failure). */
	releaseStrict(): void {
		if (!this.held || !this.token || !this.identity) return;
		this.stopHeartbeat();
		const acquired: AcquiredPaneLifecycleLock = {
			lockDir: this.lockDir,
			token: this.token,
			createdDir: this.createdDir,
			identity: this.identity,
		};
		this.held = false;
		this.token = undefined;
		this.identity = undefined;
		this.createdDir = false;
		releasePaneLifecycleLock(acquired.lockDir, acquired.token, acquired.identity);
	}

	isHeartbeatActiveForTest(): boolean {
		return this.heartbeatTimer !== undefined;
	}

	/** @internal test-only */
	stopHeartbeatForTest(): void {
		this.stopHeartbeat();
	}

	getTokenForTest(): string | undefined {
		return this.token;
	}

	getIdentityForTest(): PaneLifecycleLockDirIdentity | undefined {
		return this.identity;
	}

	private startHeartbeat(): void {
		this.stopHeartbeat();
		const now = this.options.now ?? Date.now;
		this.heartbeatTimer = setInterval(() => {
			if (!this.held || !this.token) return;
			try {
				const owner = readOwner(this.lockDir);
				if (!owner || owner.token !== this.token) return;
				writeOwnerAtomicForAcquire(this.lockDir, {
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

export function withPaneLifecycleLock<T>(
	poolRoot: string,
	fn: () => T,
	options?: {
		now?: () => number;
		timeoutMs?: number;
		sleep?: (ms: number) => Promise<void>;
	},
): T {
	ensurePrivateDir(poolRoot);
	const lock = new PaneLifecycleLock(poolRoot, options);
	lock.acquireSync(options?.timeoutMs ?? 15_000);
	let callbackError: unknown;
	try {
		return fn();
	} catch (error) {
		callbackError = error;
	} finally {
		let releaseError: unknown;
		try {
			lock.releaseStrict();
		} catch (error) {
			releaseError = error;
		}
		finalizeReleaseFromCallback(callbackError, releaseError);
	}
	throw new Error("unreachable pane lifecycle lock callback");
}

export async function withPaneLifecycleLockAsync<T>(
	poolRoot: string,
	fn: () => Promise<T> | T,
	options?: {
		now?: () => number;
		timeoutMs?: number;
		sleep?: (ms: number) => Promise<void>;
	},
): Promise<T> {
	ensurePrivateDir(poolRoot);
	const lock = new PaneLifecycleLock(poolRoot, options);
	await lock.acquire(options?.timeoutMs ?? 15_000);
	let callbackError: unknown;
	let result: T | undefined;
	try {
		result = await fn();
	} catch (error) {
		callbackError = error;
	} finally {
		let releaseError: unknown;
		try {
			lock.releaseStrict();
		} catch (error) {
			releaseError = error;
		}
		finalizeReleaseFromCallback(callbackError, releaseError);
	}
	return result as T;
}

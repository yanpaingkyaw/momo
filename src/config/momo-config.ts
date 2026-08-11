import {
	closeSync,
	constants,
	existsSync,
	fstatSync,
	lstatSync,
	mkdirSync,
	openSync,
	readFileSync,
	readdirSync,
	renameSync,
	rmdirSync,
	rmSync,
	unlinkSync,
	writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import path from "node:path";
import { randomBytes } from "node:crypto";
import {
	MOMO_CONFIG_VERSION,
	ModelPolicyValidationError,
	validateMomoConfig,
	type ModelPolicySnapshot,
	type MomoConfigV1,
	type MomoPolicyScope,
} from "./model-policy.js";

export const MAX_MOMO_CONFIG_BYTES = 32_768;

export class MomoConfigError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "MomoConfigError";
	}
}

export class MomoConfigLockError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "MomoConfigLockError";
	}
}

interface LockOwnerRecord {
	version: 1;
	token: string;
	pid: number;
	at: string;
}

export interface LockDirIdentity {
	dev: number;
	ino: number;
}

function currentUid(): number | undefined {
	if (typeof process.getuid !== "function") return undefined;
	return process.getuid();
}

function assertOwnedByCurrentUser(stat: { uid?: number }, label: string): void {
	const uid = currentUid();
	if (uid === undefined) return;
	if (typeof stat.uid === "number" && stat.uid !== uid) {
		throw new MomoConfigError(`${label} not owned by current user`);
	}
}

export function momoConfigDir(env: NodeJS.ProcessEnv = process.env): string {
	const base = env.XDG_CONFIG_HOME || path.join(env.HOME || homedir(), ".config");
	return path.join(base, "momo");
}

export function momoConfigPath(env: NodeJS.ProcessEnv = process.env): string {
	return path.join(momoConfigDir(env), "config.json");
}

function configLockDir(configDir: string): string {
	return path.join(configDir, ".config.lock");
}

function ownerPath(lockDir: string): string {
	return path.join(lockDir, "owner.json");
}

function assertOwnedRegularFile(filePath: string, maxBytes: number): void {
	if (!existsSync(filePath)) {
		throw new MomoConfigError(`Config file missing: ${filePath}`);
	}
	const stat = lstatSync(filePath);
	if (stat.isSymbolicLink()) {
		throw new MomoConfigError(`Config path must not be a symlink: ${filePath}`);
	}
	if (!stat.isFile()) {
		throw new MomoConfigError(`Config path must be a regular file: ${filePath}`);
	}
	if (stat.size > maxBytes) {
		throw new MomoConfigError(`Config file exceeds size bound (${maxBytes} bytes)`);
	}
	assertOwnedByCurrentUser(stat, `Config file ${filePath}`);
	if (typeof stat.mode === "number" && (stat.mode & 0o077) !== 0) {
		throw new MomoConfigError("Config file has group/other permissions");
	}
}

function assertOwnedMomoDirectory(dirPath: string): void {
	if (!existsSync(dirPath)) return;
	const stat = lstatSync(dirPath);
	if (stat.isSymbolicLink()) {
		throw new MomoConfigError(`Config directory must not be a symlink: ${dirPath}`);
	}
	if (!stat.isDirectory()) {
		throw new MomoConfigError(`Config path must be a directory: ${dirPath}`);
	}
	assertOwnedByCurrentUser(stat, `Config directory ${dirPath}`);
	if (typeof stat.mode === "number" && (stat.mode & 0o077) !== 0) {
		throw new MomoConfigError("Config directory has group/other permissions");
	}
}

/** Parent ~/.config may be 0755; Momo-owned dir must be private. */
export function ensureMomoConfigDir(configDir: string = momoConfigDir()): string {
	const parent = path.dirname(configDir);
	if (existsSync(parent)) {
		const parentStat = lstatSync(parent);
		if (parentStat.isSymbolicLink()) {
			throw new MomoConfigError(`Config parent must not be a symlink: ${parent}`);
		}
	}
	mkdirSync(configDir, { recursive: true, mode: 0o700 });
	assertOwnedMomoDirectory(configDir);
	return configDir;
}

function parseConfigFile(filePath: string): MomoConfigV1 {
	assertOwnedRegularFile(filePath, MAX_MOMO_CONFIG_BYTES);
	let parsed: unknown;
	try {
		parsed = JSON.parse(readFileSync(filePath, "utf8"));
	} catch (error) {
		throw new MomoConfigError(
			`Config JSON corrupt: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
	try {
		return validateMomoConfig(parsed);
	} catch (error) {
		if (error instanceof ModelPolicyValidationError) {
			throw new MomoConfigError(error.message);
		}
		throw error;
	}
}

/** Read config when present; absent file => undefined (legacy mode). */
export function readMomoConfig(options: { configPath?: string } = {}): MomoConfigV1 | undefined {
	const filePath = options.configPath ?? momoConfigPath();
	if (!existsSync(filePath)) return undefined;
	return parseConfigFile(filePath);
}

/** Fail closed when config exists but is invalid (launch paths). */
export function readMomoConfigOrThrow(options: { configPath?: string } = {}): MomoConfigV1 | undefined {
	return readMomoConfig(options);
}

function removeConfigFile(configDir: string): void {
	const filePath = path.join(configDir, "config.json");
	if (existsSync(filePath)) {
		unlinkSync(filePath);
	}
}

function atomicWriteConfig(configDir: string, config: MomoConfigV1): void {
	ensureMomoConfigDir(configDir);
	const filePath = path.join(configDir, "config.json");
	const payload = `${JSON.stringify(config, null, 2)}\n`;
	if (Buffer.byteLength(payload, "utf8") > MAX_MOMO_CONFIG_BYTES) {
		throw new MomoConfigError("Config exceeds size bound");
	}
	const tmp = `${filePath}.${process.pid}.${Date.now()}.tmp`;
	let fd: number | undefined;
	try {
		fd = openSync(tmp, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL, 0o600);
		writeFileSync(fd, payload, { encoding: "utf8" });
		closeSync(fd);
		fd = undefined;
		renameSync(tmp, filePath);
	} catch (error) {
		if (fd !== undefined) {
			try {
				closeSync(fd);
			} catch {
				// ignore
			}
		}
		try {
			unlinkSync(tmp);
		} catch {
			// ignore tmp cleanup failure
		}
		throw error;
	}
}

/** Persist validated config under an already-held config lock (or standalone). */
export function persistMomoConfig(
	configDir: string,
	config: MomoConfigV1 | null,
): MomoConfigV1 | undefined {
	return persistConfig(configDir, config);
}

function persistConfig(configDir: string, config: MomoConfigV1 | null): MomoConfigV1 | undefined {
	if (config === null) {
		removeConfigFile(configDir);
		return undefined;
	}
	let validated: MomoConfigV1;
	try {
		validated = validateMomoConfig(config);
	} catch (error) {
		if (error instanceof ModelPolicyValidationError) {
			throw new MomoConfigError(error.message);
		}
		throw error;
	}
	if (Object.keys(validated.policies).length === 0) {
		removeConfigFile(configDir);
		return undefined;
	}
	atomicWriteConfig(configDir, validated);
	return validated;
}

function readLockOwner(lockDir: string): LockOwnerRecord | undefined {
	const filePath = ownerPath(lockDir);
	if (!existsSync(filePath)) return undefined;
	try {
		const fd = openSync(filePath, constants.O_RDONLY | constants.O_NOFOLLOW);
		try {
			const stat = fstatSync(fd);
			if (!stat.isFile()) return undefined;
			assertOwnedByCurrentUser(stat, `Lock owner ${filePath}`);
			if (typeof stat.mode === "number" && (stat.mode & 0o077) !== 0) return undefined;
			const parsed = JSON.parse(readFileSync(fd, "utf8")) as Record<string, unknown>;
			if (
				parsed.version !== 1 ||
				typeof parsed.token !== "string" ||
				typeof parsed.pid !== "number" ||
				typeof parsed.at !== "string"
			) {
				return undefined;
			}
			return parsed as unknown as LockOwnerRecord;
		} finally {
			closeSync(fd);
		}
	} catch {
		return undefined;
	}
}

function captureLockDirIdentity(lockDir: string): LockDirIdentity {
	const stat = lstatSync(lockDir);
	return { dev: stat.dev, ino: stat.ino };
}

/** Remove only the exact empty lock directory we created (inode/dev fenced). */
function rmdirOwnCreatedEmptyLockDir(
	lockDir: string,
	identity: LockDirIdentity,
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
	identity: LockDirIdentity,
	createdDir: boolean,
): void {
	rmdirOwnCreatedEmptyLockDir(lockDir, identity, createdDir);
}

function acquireConfigLock(configDir: string): {
	lockDir: string;
	token: string;
	createdDir: boolean;
	identity: LockDirIdentity;
} {
	ensureMomoConfigDir(configDir);
	const lockDir = configLockDir(configDir);
	const token = randomBytes(16).toString("hex");
	let createdDir = false;
	let identity: LockDirIdentity | undefined;
	try {
		mkdirSync(lockDir, { mode: 0o700 });
		createdDir = true;
		identity = captureLockDirIdentity(lockDir);
	} catch (error) {
		const code = (error as NodeJS.ErrnoException).code;
		if (code === "EEXIST") {
			throw new MomoConfigLockError(
				"Config lock held by another process; supervised recovery required if stale",
			);
		}
		throw new MomoConfigLockError(
			`Config lock failed: ${error instanceof Error ? error.message : String(error)}`,
		);
	}

	const owner: LockOwnerRecord = {
		version: 1,
		token,
		pid: process.pid,
		at: new Date().toISOString(),
	};
	const filePath = ownerPath(lockDir);
	const tmp = `${filePath}.${process.pid}.${Date.now()}.tmp`;
	let ownerFd: number | undefined;
	try {
		ownerFd = openSync(tmp, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL, 0o600);
		writeFileSync(ownerFd, `${JSON.stringify(owner)}\n`, { encoding: "utf8" });
		closeSync(ownerFd);
		ownerFd = undefined;
		renameSync(tmp, filePath);
	} catch (error) {
		if (ownerFd !== undefined) {
			try {
				closeSync(ownerFd);
			} catch {
				// ignore
			}
		}
		try {
			unlinkSync(tmp);
		} catch {
			// ignore
		}
		if (identity) {
			rollbackOwnCreatedLockDir(lockDir, identity, createdDir);
		}
		throw new MomoConfigLockError(
			`Config lock owner init failed: ${error instanceof Error ? error.message : String(error)}`,
		);
	}

	return { lockDir, token, createdDir, identity: identity! };
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

function releaseConfigLock(lockDir: string, token: string, identity: LockDirIdentity): void {
	const owner = readLockOwner(lockDir);
	if (!owner || owner.token !== token) {
		throw new MomoConfigLockError("Config lock release refused: token mismatch");
	}
	const liveStat = lstatSync(lockDir);
	if (liveStat.dev !== identity.dev || liveStat.ino !== identity.ino) {
		throw new MomoConfigLockError("Config lock release refused: lock directory identity mismatch");
	}
	const quarantine = `${lockDir}.released.${token}`;
	if (existsSync(quarantine)) {
		throw new MomoConfigLockError("Config lock release refused: quarantine path already exists");
	}
	try {
		renameSync(lockDir, quarantine);
	} catch (error) {
		throw new MomoConfigLockError(
			`Config lock release quarantine failed: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
	const quarantinedOwner = readLockOwner(quarantine);
	let quarantineStat: ReturnType<typeof lstatSync>;
	try {
		quarantineStat = lstatSync(quarantine);
	} catch (error) {
		restoreQuarantineToLiveLock(lockDir, quarantine);
		throw new MomoConfigLockError(
			`Config lock release refused after quarantine stat: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
	if (quarantineStat.dev !== identity.dev || quarantineStat.ino !== identity.ino) {
		restoreQuarantineToLiveLock(lockDir, quarantine);
		throw new MomoConfigLockError("Config lock release refused: quarantine identity mismatch");
	}
	if (!quarantinedOwner || quarantinedOwner.token !== token) {
		restoreQuarantineToLiveLock(lockDir, quarantine);
		throw new MomoConfigLockError("Config lock release refused after quarantine revalidation");
	}
	rmSync(quarantine, { recursive: true, force: true });
}

function finalizeConfigLockRelease(
	acquired: ReturnType<typeof acquireConfigLock>,
	callbackError: unknown,
): void {
	try {
		releaseConfigLock(acquired.lockDir, acquired.token, acquired.identity);
	} catch (releaseError) {
		if (callbackError !== undefined) {
			throw new AggregateError(
				[callbackError, releaseError],
				"Config operation failed and lock release failed",
			);
		}
		throw releaseError;
	}
}

export function withMomoConfigLock<T>(
	fn: () => T,
	options: { configDir?: string } = {},
): T {
	const configDir = options.configDir ?? momoConfigDir();
	const acquired = acquireConfigLock(configDir);
	let callbackError: unknown;
	try {
		return fn();
	} catch (error) {
		callbackError = error;
		throw error;
	} finally {
		finalizeConfigLockRelease(acquired, callbackError);
	}
}

export async function withMomoConfigLockAsync<T>(
	fn: () => Promise<T>,
	options: { configDir?: string } = {},
): Promise<T> {
	const configDir = options.configDir ?? momoConfigDir();
	const acquired = acquireConfigLock(configDir);
	let callbackError: unknown;
	try {
		return await fn();
	} catch (error) {
		callbackError = error;
		throw error;
	} finally {
		finalizeConfigLockRelease(acquired, callbackError);
	}
}

function assertClearAllowed(scope: MomoPolicyScope, config: MomoConfigV1): void {
	if (scope === "default") {
		const overrides = Object.keys(config.policies).filter((key) => key !== "default");
		if (overrides.length > 0) {
			throw new MomoConfigError(
				`Cannot clear default while overrides remain (${overrides.join(", ")}); clear overrides first`,
			);
		}
	}
}

function buildNextConfig(
	existing: MomoConfigV1 | undefined,
	scope: MomoPolicyScope,
	policy: ModelPolicySnapshot | undefined,
): MomoConfigV1 | null {
	const base: MomoConfigV1 = existing ?? { version: MOMO_CONFIG_VERSION, policies: {} };
	const next: MomoConfigV1 = {
		version: MOMO_CONFIG_VERSION,
		policies: { ...base.policies },
	};
	if (policy === undefined) {
		assertClearAllowed(scope, next);
		delete next.policies[scope];
	} else {
		if (scope !== "default" && !base.policies.default) {
			throw new MomoConfigError("Configure config.policies.default first");
		}
		next.policies[scope] = policy;
	}
	if (Object.keys(next.policies).length === 0) return null;
	return validateMomoConfig(next);
}

export interface ConfigMutationResult {
	previous: MomoConfigV1 | undefined;
	next: MomoConfigV1 | undefined;
}

export function momoConfigsEqual(
	a: MomoConfigV1 | undefined,
	b: MomoConfigV1 | undefined,
): boolean {
	if (a === undefined && b === undefined) return true;
	if (a === undefined || b === undefined) return false;
	return JSON.stringify(a) === JSON.stringify(b);
}

export interface MutateMomoConfigOptions {
	configDir?: string;
	/** Optimistic concurrency: locked read must match this snapshot or mutation aborts. */
	expectedPrevious?: MomoConfigV1 | undefined;
}

/** Token-fenced config read/compute/persist under lock. */
export function mutateMomoConfig(
	compute: (existing: MomoConfigV1 | undefined) => MomoConfigV1 | null,
	options: MutateMomoConfigOptions = {},
): ConfigMutationResult {
	return withMomoConfigLock(() => {
		const configDir = options.configDir ?? momoConfigDir();
		const filePath = path.join(configDir, "config.json");
		const previous = readMomoConfig({ configPath: filePath });
		if ("expectedPrevious" in options && !momoConfigsEqual(previous, options.expectedPrevious)) {
			throw new MomoConfigError(
				"Config changed concurrently since transaction start; no changes persisted",
			);
		}
		const rawNext = compute(previous);
		const next = persistConfig(configDir, rawNext);
		return { previous, next };
	}, options);
}

export function setScopePolicy(
	scope: MomoPolicyScope,
	policy: ModelPolicySnapshot,
	options: { configDir?: string } = {},
): MomoConfigV1 {
	const result = mutateMomoConfig(
		(existing) => buildNextConfig(existing, scope, policy),
		options,
	);
	if (!result.next) {
		throw new MomoConfigError("setScopePolicy produced empty config");
	}
	return result.next;
}

export function clearScopePolicy(
	scope: MomoPolicyScope,
	options: { configDir?: string } = {},
): MomoConfigV1 | undefined {
	const result = mutateMomoConfig(
		(existing) => {
			if (!existing) return null;
			return buildNextConfig(existing, scope, undefined);
		},
		options,
	);
	return result.next;
}

/** @internal test-only */
export function __writeRawConfigFileForTest(
	configDir: string,
	content: string,
): void {
	ensureMomoConfigDir(configDir);
	const filePath = path.join(configDir, "config.json");
	const fd = openSync(filePath, constants.O_WRONLY | constants.O_CREAT | constants.O_TRUNC, 0o600);
	try {
		writeFileSync(fd, content, { encoding: "utf8" });
	} finally {
		closeSync(fd);
	}
}

/** @internal test-only */
export function __releaseConfigLockForTest(
	lockDir: string,
	token: string,
	identity: LockDirIdentity,
): void {
	releaseConfigLock(lockDir, token, identity);
}

/** @internal test-only */
export function __removeConfigForTest(configDir: string): void {
	rmSync(configDir, { recursive: true, force: true });
}

/** @internal test-only */
export function __readLockOwnerForTest(configDir: string): LockOwnerRecord | undefined {
	return readLockOwner(configLockDir(configDir));
}

/** @internal test-only */
export function __lockDirForTest(configDir: string): string {
	return configLockDir(configDir);
}

/** @internal test-only */
export function __rmdirOwnCreatedEmptyLockDirForTest(
	lockDir: string,
	identity: LockDirIdentity,
): void {
	rmdirOwnCreatedEmptyLockDir(lockDir, identity, true);
}

/** @internal test-only */
export function __captureLockDirIdentityForTest(lockDir: string): LockDirIdentity {
	return captureLockDirIdentity(lockDir);
}

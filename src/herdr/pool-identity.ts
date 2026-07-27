import { execFileSync } from "node:child_process";
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { existsSync, lstatSync, realpathSync } from "node:fs";
import path from "node:path";
import type { AgentName } from "../roles.js";
import { ensurePrivateDir, momoCacheRoot } from "../ipc/spool.js";

export const POOL_REGISTRY_VERSION = 2 as const;
export const ASSIGNMENT_ID_RE = /^[a-z0-9]{8,32}$/i;

export interface PoolIdentityInput {
	cwd: string;
	workspaceId?: string;
	socketPath?: string;
	canonicalRoot?: string;
}

export interface PoolIdentity {
	canonicalRoot: string;
	workspaceId: string;
	socketPath: string;
	poolKey: string;
}

/** Resolve git toplevel via fixed argv; fall back to realpath(cwd). */
export function resolveCanonicalRoot(cwd: string): string {
	const resolvedCwd = realpathSync(cwd);
	try {
		const toplevel = execFileSync("git", ["rev-parse", "--show-toplevel"], {
			cwd: resolvedCwd,
			encoding: "utf8",
			stdio: ["ignore", "pipe", "ignore"],
			timeout: 5_000,
		})
			.trim()
			.replace(/\0/g, "");
		if (toplevel) return realpathSync(toplevel);
	} catch {
		// not a git repo or git unavailable
	}
	return resolvedCwd;
}

/** Canonicalize socket path when it exists as a filesystem path. */
export function canonicalizeSocketIdentity(socketPath: string): string {
	if (!socketPath) return "";
	try {
		if (existsSync(socketPath)) {
			const stat = lstatSync(socketPath);
			if (!stat.isSymbolicLink()) {
				return realpathSync(socketPath);
			}
			// Symlink: resolve target without following into unexpected trees blindly.
			return realpathSync(socketPath);
		}
	} catch {
		// keep normalized string form
	}
	return path.normalize(socketPath);
}

export function computePoolKey(material: {
	canonicalRoot: string;
	workspaceId: string;
	socketPath: string;
}): string {
	const raw = `${material.canonicalRoot}\0${material.workspaceId}\0${material.socketPath}`;
	return createHash("sha256").update(raw).digest("hex").slice(0, 24);
}

export function resolvePoolIdentity(input: PoolIdentityInput): PoolIdentity {
	// Always canonicalize supplied roots so subdirectory parents share one identity.
	const canonicalRoot =
		input.canonicalRoot !== undefined
			? realpathSync(input.canonicalRoot)
			: resolveCanonicalRoot(input.cwd);
	const workspaceId = input.workspaceId ?? "";
	const socketPath = canonicalizeSocketIdentity(input.socketPath ?? "");
	return {
		canonicalRoot,
		workspaceId,
		socketPath,
		poolKey: computePoolKey({ canonicalRoot, workspaceId, socketPath }),
	};
}

export function poolRootPath(poolKey: string, cacheRoot = momoCacheRoot()): string {
	if (!/^[a-f0-9]{16,64}$/i.test(poolKey)) {
		throw new Error(`Invalid poolKey: ${poolKey}`);
	}
	return path.join(cacheRoot, "pools", poolKey);
}

/** Ensure poolRoot matches cacheRoot/pools/poolKey derivation. */
export function assertPoolRootMatchesKey(
	poolRoot: string,
	poolKey: string,
	cacheRoot = momoCacheRoot(),
): string {
	const expected = poolRootPath(poolKey, cacheRoot);
	const resolvedExpected = realpathSync(existsSync(expected) ? expected : path.dirname(expected));
	const resolvedActual = realpathSync(existsSync(poolRoot) ? poolRoot : path.dirname(poolRoot));
	const expectedFull = path.join(cacheRoot, "pools", poolKey);
	if (path.resolve(poolRoot) !== path.resolve(expectedFull) && resolvedActual !== resolvedExpected) {
		// Compare normalized absolute forms.
		if (path.resolve(poolRoot) !== path.resolve(expected)) {
			throw new Error(`Pool root does not match poolKey derivation: ${poolRoot}`);
		}
	}
	return poolRoot;
}

export function rolePoolRoot(poolRoot: string, role: AgentName): string {
	return path.join(poolRoot, "roles", role);
}

export function stableWorkerId(poolKey: string, role: AgentName): string {
	const digest = createHash("sha256").update(`${poolKey}\0${role}`).digest("hex").slice(0, 8);
	return `${role}_${digest}`;
}

export function herdrAgentNameForWorker(workerId: string): string {
	const compact = `momo_${workerId}`.replace(/[^a-z0-9_-]/gi, "_").toLowerCase();
	return compact.slice(0, 32);
}

export function createAssignmentId(): string {
	return randomUUID().replace(/-/g, "").slice(0, 16);
}

export function createParentEpoch(): string {
	return randomUUID().replace(/-/g, "").slice(0, 12);
}

export function createLockToken(): string {
	return randomBytes(16).toString("hex");
}

export function assertAssignmentId(assignmentId: string): string {
	if (!ASSIGNMENT_ID_RE.test(assignmentId)) {
		throw new Error(`Invalid assignmentId: ${assignmentId}`);
	}
	return assignmentId;
}

export function ensurePoolLayout(poolRoot: string, role: AgentName): void {
	ensurePrivateDir(poolRoot);
	ensurePrivateDir(rolePoolRoot(poolRoot, role));
	ensurePrivateDir(path.join(rolePoolRoot(poolRoot, role), "queue"));
	ensurePrivateDir(path.join(rolePoolRoot(poolRoot, role), "claiming"));
	ensurePrivateDir(path.join(rolePoolRoot(poolRoot, role), "worker"));
	ensurePrivateDir(path.join(rolePoolRoot(poolRoot, role), "assignments"));
}

/**
 * Robust containment: resolve an existing ancestor of candidate/root and ensure
 * the absolute candidate stays under root. Creates no directories.
 */
export function assertPathUnderRoot(candidate: string, root: string): string {
	const absoluteRoot = path.resolve(root);
	const absoluteCandidate = path.resolve(candidate);

	function existingAncestor(p: string): string {
		let cur = p;
		while (cur !== path.dirname(cur)) {
			if (existsSync(cur)) {
				try {
					return realpathSync(cur);
				} catch {
					return cur;
				}
			}
			cur = path.dirname(cur);
		}
		return cur;
	}

	const resolvedRootAncestor = existingAncestor(absoluteRoot);
	const resolvedCandidateAncestor = existingAncestor(absoluteCandidate);

	// Candidate must share the root ancestor prefix.
	if (
		resolvedCandidateAncestor !== resolvedRootAncestor &&
		!resolvedCandidateAncestor.startsWith(resolvedRootAncestor + path.sep) &&
		!absoluteCandidate.startsWith(absoluteRoot + path.sep) &&
		absoluteCandidate !== absoluteRoot
	) {
		throw new Error(`Path escapes pool root: ${candidate}`);
	}
	if (!absoluteCandidate.startsWith(absoluteRoot + path.sep) && absoluteCandidate !== absoluteRoot) {
		// Also compare via resolved ancestors when root itself does not exist yet.
		const rel = path.relative(absoluteRoot, absoluteCandidate);
		if (rel.startsWith("..") || path.isAbsolute(rel)) {
			throw new Error(`Path escapes pool root: ${candidate}`);
		}
	}
	return candidate;
}

export function controlRunId(generation: number): string {
	if (!Number.isInteger(generation) || generation < 1) {
		throw new Error(`Invalid generation: ${generation}`);
	}
	return `g${generation}`;
}

export function isProcessAlive(pid: number): boolean {
	if (!Number.isInteger(pid) || pid <= 0) return false;
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		const err = error as NodeJS.ErrnoException;
		if (err.code === "EPERM") return true;
		return false;
	}
}

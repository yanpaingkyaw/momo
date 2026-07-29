/**
 * Durable role-scoped orphan-pane cleanup evidence.
 * Written when a provisioned pane cannot be closed after supersession / ownership
 * loss — never mutates the current registry successor.
 */
import { createHash } from "node:crypto";
import {
	existsSync,
	lstatSync,
	readdirSync,
	rmSync,
} from "node:fs";
import path from "node:path";
import type { AgentName } from "../roles.js";
import {
	atomicWriteJson,
	ensurePrivateDir,
	MAX_IPC_JSON_BYTES,
} from "../ipc/spool.js";
import { parseJsonFile } from "../ipc/validate.js";
import { rolePoolRoot } from "./pool-identity.js";

export class OrphanPaneEvidenceError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "OrphanPaneEvidenceError";
	}
}

/** Close failed and durable evidence could not be persisted — never silent. */
export class OrphanPaneEvidencePersistenceError extends Error {
	readonly paneId: string;
	readonly reason: string;
	readonly evidenceError: unknown;

	constructor(options: {
		paneId: string;
		reason: string;
		evidenceError: unknown;
		message?: string;
	}) {
		const detail =
			options.evidenceError instanceof Error
				? options.evidenceError.message
				: String(options.evidenceError);
		super(
			options.message ??
				`Orphan pane ${options.paneId} close failed (${options.reason}) and evidence persistence failed: ${detail}. Fix storage and run /momo-cleanup.`,
			options.evidenceError instanceof Error
				? { cause: options.evidenceError }
				: undefined,
		);
		this.name = "OrphanPaneEvidencePersistenceError";
		this.paneId = options.paneId;
		this.reason = options.reason;
		this.evidenceError = options.evidenceError;
	}
}

export interface OrphanPaneEvidence {
	version: 1;
	generation: number;
	workerId: string;
	paneId: string;
	agentName: string;
	reason: string;
	createdAt: string;
}

export function orphanPanesDir(poolRoot: string, role: AgentName): string {
	return path.join(rolePoolRoot(poolRoot, role), "orphan-panes");
}

function requireNonEmptyString(value: unknown, label: string, max: number): string {
	if (typeof value !== "string" || value.length === 0 || value.length > max || value.includes("\0")) {
		throw new OrphanPaneEvidenceError(`orphan pane evidence ${label} invalid`);
	}
	return value;
}

/** Collision-resistant canonical filename from paneId (SHA-256 hex). */
export function orphanEvidenceFileName(paneId: string): string {
	if (typeof paneId !== "string" || paneId.length === 0 || paneId.includes("\0")) {
		throw new OrphanPaneEvidenceError("orphan pane evidence paneId invalid for filename");
	}
	return `${createHash("sha256").update(paneId, "utf8").digest("hex")}.json`;
}

export function orphanEvidencePath(poolRoot: string, role: AgentName, paneId: string): string {
	return path.join(orphanPanesDir(poolRoot, role), orphanEvidenceFileName(paneId));
}

function assertOwnerPrivateDir(dirPath: string): void {
	const st = lstatSync(dirPath);
	if (st.isSymbolicLink()) {
		throw new OrphanPaneEvidenceError(`orphan-panes directory must not be a symlink: ${dirPath}`);
	}
	if (!st.isDirectory()) {
		throw new OrphanPaneEvidenceError(`orphan-panes path is not a directory: ${dirPath}`);
	}
	if (typeof st.mode === "number" && (st.mode & 0o077) !== 0) {
		throw new OrphanPaneEvidenceError(
			`orphan-panes directory has group/other permissions: ${dirPath}`,
		);
	}
}

function assertOwnerPrivateFile(filePath: string): void {
	const st = lstatSync(filePath);
	if (st.isSymbolicLink()) {
		throw new OrphanPaneEvidenceError(`orphan evidence must not be a symlink: ${filePath}`);
	}
	if (!st.isFile()) {
		throw new OrphanPaneEvidenceError(`orphan evidence must be a regular file: ${filePath}`);
	}
	if (typeof st.mode === "number" && (st.mode & 0o077) !== 0) {
		throw new OrphanPaneEvidenceError(
			`orphan evidence has group/other permissions: ${filePath}`,
		);
	}
	if (st.size > MAX_IPC_JSON_BYTES) {
		throw new OrphanPaneEvidenceError(`orphan evidence oversized: ${filePath}`);
	}
}

export function validateOrphanPaneEvidence(value: unknown, label = "orphan"): OrphanPaneEvidence {
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		throw new OrphanPaneEvidenceError(`${label} must be an object`);
	}
	const record = value as Record<string, unknown>;
	if (record.version !== 1) {
		throw new OrphanPaneEvidenceError(`${label}.version invalid`);
	}
	if (
		typeof record.generation !== "number" ||
		!Number.isInteger(record.generation) ||
		record.generation < 1
	) {
		throw new OrphanPaneEvidenceError(`${label}.generation invalid`);
	}
	const paneId = requireNonEmptyString(record.paneId, `${label}.paneId`, 256);
	const workerId = requireNonEmptyString(record.workerId, `${label}.workerId`, 64);
	const agentName = requireNonEmptyString(record.agentName, `${label}.agentName`, 128);
	const reason = requireNonEmptyString(record.reason, `${label}.reason`, 256);
	const createdAt = requireNonEmptyString(record.createdAt, `${label}.createdAt`, 64);
	if (!Number.isFinite(Date.parse(createdAt))) {
		throw new OrphanPaneEvidenceError(`${label}.createdAt invalid`);
	}
	return {
		version: 1,
		generation: record.generation,
		workerId,
		paneId,
		agentName,
		reason,
		createdAt,
	};
}

/** @internal test-only: inject evidence atomic write failures. */
let orphanEvidenceWriteImpl: (filePath: string, value: unknown) => void = atomicWriteJson;

/** @internal test-only */
export function __setOrphanEvidenceWriteForTest(
	write?: (filePath: string, value: unknown) => void,
): void {
	orphanEvidenceWriteImpl = write ?? atomicWriteJson;
}

/**
 * Persist orphan-pane evidence. Never overwrites registry. Same paneId replaces
 * prior evidence for that pane only (idempotent retry of the same orphan).
 */
export function writeOrphanPaneEvidence(
	poolRoot: string,
	role: AgentName,
	evidence: Omit<OrphanPaneEvidence, "version"> & { version?: 1 },
): OrphanPaneEvidence {
	const validated = validateOrphanPaneEvidence({ version: 1, ...evidence });
	const dir = orphanPanesDir(poolRoot, role);
	ensurePrivateDir(dir);
	assertOwnerPrivateDir(dir);
	const filePath = orphanEvidencePath(poolRoot, role, validated.paneId);
	orphanEvidenceWriteImpl(filePath, validated);
	assertOwnerPrivateFile(filePath);
	if (path.basename(filePath) !== orphanEvidenceFileName(validated.paneId)) {
		throw new OrphanPaneEvidenceError("orphan evidence path basename mismatch after write");
	}
	return validated;
}

/**
 * True when any orphan-panes directory entry exists — including `*.tmp` atomic-write
 * leftovers and malformed files (fail closed; never ignore interrupted writes).
 */
export function roleHasOrphanPaneEvidence(poolRoot: string, role: AgentName): boolean {
	const dir = orphanPanesDir(poolRoot, role);
	if (!existsSync(dir)) return false;
	try {
		const st = lstatSync(dir);
		if (st.isSymbolicLink() || !st.isDirectory()) return true;
		return readdirSync(dir).length > 0;
	} catch {
		return true;
	}
}

export function assertNoOrphanPaneEvidence(poolRoot: string, role: AgentName): void {
	if (!roleHasOrphanPaneEvidence(poolRoot, role)) return;
	throw new Error(
		`Role ${role} has orphan pane cleanup evidence; run /momo-cleanup before provisioning a new physical worker`,
	);
}

export type ListedOrphanPane =
	| { ok: true; evidence: OrphanPaneEvidence; filePath: string }
	| { ok: false; filePath: string; reason: string };

/**
 * List orphan evidence under the role directory. Symlinks, oversized, group/other
 * modes, filename/paneId mismatches, malformed files, and `*.tmp` atomic-write
 * leftovers fail closed as ok:false — never silently filtered or removed.
 */
export function listOrphanPaneEvidence(poolRoot: string, role: AgentName): ListedOrphanPane[] {
	const dir = orphanPanesDir(poolRoot, role);
	if (!existsSync(dir)) return [];
	let names: string[];
	try {
		assertOwnerPrivateDir(dir);
		names = readdirSync(dir);
	} catch (error) {
		return [
			{
				ok: false,
				filePath: dir,
				reason: error instanceof Error ? error.message : String(error),
			},
		];
	}
	const out: ListedOrphanPane[] = [];
	for (const name of names) {
		const filePath = path.join(dir, name);
		if (name.endsWith(".tmp")) {
			out.push({
				ok: false,
				filePath,
				reason:
					"interrupted atomic write (temp orphan evidence); retained until resolved — run /momo-cleanup after fixing storage",
			});
			continue;
		}
		try {
			assertOwnerPrivateFile(filePath);
			const evidence = validateOrphanPaneEvidence(parseJsonFile(filePath), name);
			const expectedName = orphanEvidenceFileName(evidence.paneId);
			if (name !== expectedName) {
				out.push({
					ok: false,
					filePath,
					reason: `filename does not match canonical paneId hash (expected ${expectedName})`,
				});
				continue;
			}
			out.push({ ok: true, evidence, filePath });
		} catch (error) {
			out.push({
				ok: false,
				filePath,
				reason: error instanceof Error ? error.message : String(error),
			});
		}
	}
	return out;
}

export function orphanPaneEvidenceEquals(
	a: OrphanPaneEvidence,
	b: OrphanPaneEvidence,
): boolean {
	return (
		a.version === b.version &&
		a.generation === b.generation &&
		a.workerId === b.workerId &&
		a.paneId === b.paneId &&
		a.agentName === b.agentName &&
		a.reason === b.reason &&
		a.createdAt === b.createdAt
	);
}

export type RemoveOrphanEvidenceIfUnchangedResult =
	| { status: "removed" }
	| { status: "missing" }
	| { status: "changed" }
	| { status: "invalid"; reason: string };

/**
 * Remove evidence only when the on-disk file still matches the exact candidate
 * content observed before close. Concurrent replacement / mutation retains.
 */
export function removeOrphanPaneEvidenceIfUnchanged(
	poolRoot: string,
	role: AgentName,
	expected: OrphanPaneEvidence,
): RemoveOrphanEvidenceIfUnchangedResult {
	const filePath = orphanEvidencePath(poolRoot, role, expected.paneId);
	try {
		if (!existsSync(filePath)) return { status: "missing" };
		const st = lstatSync(filePath);
		if (st.isSymbolicLink()) {
			throw new OrphanPaneEvidenceError("refusing to remove symlink orphan evidence");
		}
		if (typeof st.mode === "number" && (st.mode & 0o077) !== 0) {
			throw new OrphanPaneEvidenceError(
				`refusing to remove orphan evidence with group/other permissions: ${filePath}`,
			);
		}
		if (!st.isFile()) {
			return {
				status: "invalid",
				reason: `orphan evidence is not a regular file: ${filePath}`,
			};
		}
		let current: OrphanPaneEvidence;
		try {
			current = validateOrphanPaneEvidence(parseJsonFile(filePath), path.basename(filePath));
		} catch (error) {
			return {
				status: "invalid",
				reason: error instanceof Error ? error.message : String(error),
			};
		}
		const expectedName = orphanEvidenceFileName(current.paneId);
		if (path.basename(filePath) !== expectedName) {
			return {
				status: "invalid",
				reason: `filename does not match canonical paneId hash (expected ${expectedName})`,
			};
		}
		if (!orphanPaneEvidenceEquals(current, expected)) {
			return { status: "changed" };
		}
		rmSync(filePath, { force: true });
		return { status: "removed" };
	} catch (error) {
		if (error instanceof OrphanPaneEvidenceError) throw error;
		throw new OrphanPaneEvidenceError(
			`failed to remove orphan evidence for ${expected.paneId}: ${
				error instanceof Error ? error.message : String(error)
			}`,
		);
	}
}

/** Remove evidence only after definitive close / pane-not-found (unconditional). */
export function removeOrphanPaneEvidence(
	poolRoot: string,
	role: AgentName,
	paneId: string,
): void {
	const filePath = orphanEvidencePath(poolRoot, role, paneId);
	try {
		if (!existsSync(filePath)) return;
		const st = lstatSync(filePath);
		if (st.isSymbolicLink()) {
			throw new OrphanPaneEvidenceError("refusing to remove symlink orphan evidence");
		}
		if (typeof st.mode === "number" && (st.mode & 0o077) !== 0) {
			throw new OrphanPaneEvidenceError(
				`refusing to remove orphan evidence with group/other permissions: ${filePath}`,
			);
		}
		rmSync(filePath, { force: true });
	} catch (error) {
		if (error instanceof OrphanPaneEvidenceError) throw error;
		throw new OrphanPaneEvidenceError(
			`failed to remove orphan evidence for ${paneId}: ${
				error instanceof Error ? error.message : String(error)
			}`,
		);
	}
}

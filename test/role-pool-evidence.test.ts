import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { atomicWriteJson } from "../src/ipc/spool.js";
import { workerControlPaths } from "../src/herdr/assignment-spool.js";
import { PoolRegistry } from "../src/herdr/pool-registry.js";
import { resolvePoolIdentity } from "../src/herdr/pool-identity.js";
import {
	assertRolePoolPristineForProvision,
	roleHasProvisionBlockingEvidence,
	RolePoolEvidenceError,
} from "../src/herdr/role-pool-evidence.js";

function tempDir(): string {
	return mkdtempSync(path.join(tmpdir(), "momo-evidence-"));
}

describe("role pool provision evidence", () => {
	const dirs: string[] = [];
	afterEach(() => {
		for (const dir of dirs) {
			try {
				rmSync(dir, { recursive: true, force: true });
			} catch {
				// ignore
			}
		}
		dirs.length = 0;
	});

	it("allows pristine role pool", () => {
		const cacheRoot = tempDir();
		dirs.push(cacheRoot);
		const cwd = tempDir();
		dirs.push(cwd);
		const identity = resolvePoolIdentity({ cwd, canonicalRoot: cwd, workspaceId: "ws", socketPath: "s" });
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		expect(roleHasProvisionBlockingEvidence(pool.poolRoot, "scout")).toBe(false);
		expect(() => assertRolePoolPristineForProvision(pool.poolRoot, "scout")).not.toThrow();
	});

	it("fail-closed when control manifest exists without registry row", () => {
		const cacheRoot = tempDir();
		dirs.push(cacheRoot);
		const cwd = tempDir();
		dirs.push(cwd);
		const identity = resolvePoolIdentity({ cwd, canonicalRoot: cwd, workspaceId: "ws", socketPath: "s" });
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const control = workerControlPaths(pool.poolRoot, "scout");
		mkdirSync(control.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(control.manifest, {
			version: 2,
			poolKey: identity.poolKey,
			workerId: "w1",
			generation: 1,
			role: "scout",
			cwd,
			createdAt: new Date().toISOString(),
		});
		expect(roleHasProvisionBlockingEvidence(pool.poolRoot, "scout")).toBe(true);
		expect(() => assertRolePoolPristineForProvision(pool.poolRoot, "scout")).toThrow(
			RolePoolEvidenceError,
		);
	});
});

import { spawn } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { WriterLeaseManager, createLeaseToken } from "../src/lease/writer-lease.js";

const tokenA = createLeaseToken();
const tokenB = createLeaseToken();

describe("writer lease", () => {
	it("serializes two owners on the same cwd and refuses ownership mismatch", () => {
		const cacheRoot = mkdtempSync(path.join(tmpdir(), "momo-lease-"));
		const cwd = mkdtempSync(path.join(tmpdir(), "momo-cwd-"));
		try {
			const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });
			leases.acquire(cwd, "owner-a", tokenA);
			expect(() => leases.acquire(cwd, "owner-b", tokenB)).toThrow(/refusing automatic steal/);
			expect(() => leases.heartbeat(cwd, "owner-b", tokenB)).toThrow(/ownership mismatch/);
			expect(() => leases.release(cwd, "owner-b", tokenB)).toThrow(/ownership mismatch/);
			leases.heartbeat(cwd, "owner-a", tokenA);
			expect(leases.validate(cwd, "owner-a", tokenA)).toBe(true);
			leases.release(cwd, "owner-a", tokenA);
			leases.acquire(cwd, "owner-b", tokenB);
			expect(leases.validate(cwd, "owner-b", tokenB)).toBe(true);
		} finally {
			rmSync(cacheRoot, { recursive: true, force: true });
			rmSync(cwd, { recursive: true, force: true });
		}
	});

	it("never auto-steals a stale foreign lease", () => {
		const cacheRoot = mkdtempSync(path.join(tmpdir(), "momo-lease-"));
		const cwd = mkdtempSync(path.join(tmpdir(), "momo-cwd-"));
		try {
			let now = 1_000;
			const leases = new WriterLeaseManager({
				cacheRoot,
				staleMs: 100,
				now: () => now,
			});
			leases.acquire(cwd, "owner-a", tokenA);
			now = 1_050;
			expect(() => leases.acquire(cwd, "owner-b", tokenB)).toThrow(/refusing automatic steal/);
			now = 50_000;
			expect(() => leases.acquire(cwd, "owner-b", tokenB)).toThrow(/refusing automatic steal/);
			expect(leases.validate(cwd, "owner-a", tokenA)).toBe(false);
		} finally {
			rmSync(cacheRoot, { recursive: true, force: true });
			rmSync(cwd, { recursive: true, force: true });
		}
	});

	it("requires cryptographically random tokens", () => {
		const cacheRoot = mkdtempSync(path.join(tmpdir(), "momo-lease-"));
		const cwd = mkdtempSync(path.join(tmpdir(), "momo-cwd-"));
		try {
			const leases = new WriterLeaseManager({ cacheRoot });
			expect(() => leases.acquire(cwd, "owner", "short")).toThrow(/cryptographically random/);
			expect(createLeaseToken().length).toBeGreaterThanOrEqual(64);
		} finally {
			rmSync(cacheRoot, { recursive: true, force: true });
			rmSync(cwd, { recursive: true, force: true });
		}
	});

	it("release ordering: release only under ownership proof", () => {
		const cacheRoot = mkdtempSync(path.join(tmpdir(), "momo-lease-"));
		const cwd = mkdtempSync(path.join(tmpdir(), "momo-cwd-"));
		try {
			const leases = new WriterLeaseManager({ cacheRoot, now: () => 1_000 });
			const token = createLeaseToken();
			leases.acquire(cwd, "worker1", token);
			expect(leases.validate(cwd, "worker1", token)).toBe(true);
			leases.release(cwd, "worker1", token);
			expect(leases.validate(cwd, "worker1", token)).toBe(false);
			leases.release(cwd, "worker1", token);
			leases.acquire(cwd, "worker2", createLeaseToken());
			expect(() => leases.release(cwd, "worker1", token)).toThrow(/ownership mismatch/);
		} finally {
			rmSync(cacheRoot, { recursive: true, force: true });
			rmSync(cwd, { recursive: true, force: true });
		}
	});

	it("concurrent child-process acquisition yields exactly one winner", async () => {
		const cacheRoot = mkdtempSync(path.join(tmpdir(), "momo-lease-race-"));
		const cwd = mkdtempSync(path.join(tmpdir(), "momo-cwd-race-"));
		const scriptDir = mkdtempSync(path.join(tmpdir(), "momo-lease-script-"));
		const scriptPath = path.join(scriptDir, "race.cjs");
		const barrierPath = path.join(scriptDir, "barrier");
		mkdirSync(path.join(cacheRoot, "leases"), { recursive: true });

		writeFileSync(
			scriptPath,
			`
const { mkdirSync, writeFileSync, existsSync, realpathSync } = require("node:fs");
const { createHash, randomBytes } = require("node:crypto");
const path = require("node:path");

const cacheRoot = process.argv[2];
const cwd = process.argv[3];
const barrier = process.argv[4];
const ownerId = process.argv[5];
const token = randomBytes(32).toString("hex");
const hash = createHash("sha256").update(realpathSync(cwd)).digest("hex").slice(0, 16);
const lockDir = path.join(cacheRoot, "leases", hash);

const start = Date.now();
while (!existsSync(barrier) && Date.now() - start < 5000) {}
try {
  mkdirSync(lockDir, { recursive: false });
  writeFileSync(path.join(lockDir, "owner.json"), JSON.stringify({
    version: 1,
    cwd: realpathSync(cwd),
    ownerId,
    token,
    heartbeatAt: new Date().toISOString(),
    acquiredAt: new Date().toISOString(),
    pid: process.pid
  }), { flag: "wx" });
  process.stdout.write("won");
  process.exit(0);
} catch {
  process.stdout.write("lost");
  process.exit(2);
}
`,
			"utf8",
		);

		try {
			const children = [1, 2].map(
				(n) =>
					new Promise<string>((resolve, reject) => {
						const child = spawn(
							process.execPath,
							[scriptPath, cacheRoot, cwd, barrierPath, `owner-${n}`],
							{ stdio: ["ignore", "pipe", "pipe"] },
						);
						let out = "";
						let err = "";
						child.stdout.on("data", (buf) => {
							out += String(buf);
						});
						child.stderr.on("data", (buf) => {
							err += String(buf);
						});
						child.on("error", reject);
						child.on("exit", (code) => {
							if (!out && err) reject(new Error(err));
							else resolve(out.trim() || `exit:${code}`);
						});
					}),
			);

			await new Promise((r) => setTimeout(r, 80));
			writeFileSync(barrierPath, "go", "utf8");
			const results = await Promise.all(children);
			const wins = results.filter((r) => r === "won").length;
			const losses = results.filter((r) => r === "lost").length;
			expect(results, `race results=${JSON.stringify(results)}`).toEqual(
				expect.arrayContaining(["won", "lost"]),
			);
			expect(wins).toBe(1);
			expect(losses).toBe(1);

			const leases = new WriterLeaseManager({ cacheRoot });
			expect(() => leases.acquire(cwd, "late", createLeaseToken())).toThrow(
				/refusing automatic steal/,
			);
		} finally {
			rmSync(cacheRoot, { recursive: true, force: true });
			rmSync(cwd, { recursive: true, force: true });
			rmSync(scriptDir, { recursive: true, force: true });
		}
	});
});

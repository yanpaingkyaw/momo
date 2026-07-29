import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { PaneRegistry, selectClosablePanes } from "../src/herdr/registry.js";

describe("pane registry retention and cleanup", () => {
	it("retains completed panes until removed", () => {
		const cacheRoot = mkdtempSync(path.join(tmpdir(), "momo-reg-"));
		try {
			const registry = new PaneRegistry("parent", cacheRoot);
			registry.upsert({
				workerId: "scout_1",
				runId: "run1",
				role: "scout",
				paneId: "w1:p2",
				agentName: "momo_scout_1",
				spoolRoot: "/tmp/spool",
				cwd: "/tmp/repo",
				status: "completed",
				updatedAt: new Date().toISOString(),
			});
			registry.upsert({
				workerId: "impl_1",
				runId: "run1",
				role: "implementer",
				paneId: "w1:p3",
				agentName: "momo_impl_1",
				spoolRoot: "/tmp/spool2",
				cwd: "/tmp/repo",
				status: "uncertain",
				uncertainWrite: true,
				updatedAt: new Date().toISOString(),
			});
			expect(registry.list()).toHaveLength(2);
			const withoutForce = selectClosablePanes(registry.list(), { force: false });
			expect(withoutForce.closable.map((pane) => pane.workerId)).toEqual(["scout_1"]);
			const withForce = selectClosablePanes(registry.list(), { force: true });
			expect(withForce.closable.map((pane) => pane.workerId).sort()).toEqual([
				"impl_1",
				"scout_1",
			]);
			registry.remove("scout_1");
			expect(registry.list().map((pane) => pane.workerId)).toEqual(["impl_1"]);
		} finally {
			rmSync(cacheRoot, { recursive: true, force: true });
		}
	});

	it("never closes ready/running/starting even with --force", () => {
		const panes = [
			{
				workerId: "a",
				runId: "r",
				role: "scout" as const,
				paneId: "w1:p1",
				agentName: "momo_a",
				spoolRoot: "/tmp/a",
				cwd: "/tmp/repo",
				status: "ready" as const,
				updatedAt: "t",
			},
			{
				workerId: "b",
				runId: "r",
				role: "scout" as const,
				paneId: "w1:p2",
				agentName: "momo_b",
				spoolRoot: "/tmp/b",
				cwd: "/tmp/repo",
				status: "running" as const,
				updatedAt: "t",
			},
			{
				workerId: "c",
				runId: "r",
				role: "scout" as const,
				paneId: "w1:p3",
				agentName: "momo_c",
				spoolRoot: "/tmp/c",
				cwd: "/tmp/repo",
				status: "aborted" as const,
				updatedAt: "t",
			},
		];
		const selected = selectClosablePanes(panes, { force: true });
		expect(selected.closable.map((pane) => pane.workerId)).toEqual(["c"]);
		expect(selected.refused.map((pane) => pane.workerId).sort()).toEqual(["a", "b"]);
	});
});

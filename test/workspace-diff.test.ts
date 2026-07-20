import { describe, expect, it } from "vitest";
import { collectWorkspaceDiff, type GitCommandRunner } from "../src/delegation/workspace-diff.js";

describe("collectWorkspaceDiff", () => {
	it("runs only the fixed status, unstaged diff, and staged diff commands", async () => {
		const calls: Array<{ cwd: string; args: readonly string[] }> = [];
		const runCommand: GitCommandRunner = async (cwd, args) => {
			calls.push({ cwd, args: [...args] });
			return { stdout: args[0] === "status" ? " M src/a.ts\n" : "diff output\n", stderr: "" };
		};

		const result = await collectWorkspaceDiff("/repo", { runCommand });

		expect(calls).toEqual([
			{ cwd: "/repo", args: ["status", "--short"] },
			{ cwd: "/repo", args: ["diff", "--no-ext-diff", "--no-color"] },
			{ cwd: "/repo", args: ["diff", "--cached", "--no-ext-diff", "--no-color"] },
		]);
		expect(result.details.isGitRepository).toBe(true);
		expect(result.text).toContain("## Working tree status\n\n M src/a.ts");
		expect(result.text).toContain("## Staged changes\n\ndiff output");
	});

	it("returns a non-fatal explanation outside Git and stops after status", async () => {
		let calls = 0;
		const result = await collectWorkspaceDiff("/not-a-repo", {
			runCommand: async () => {
				calls += 1;
				throw { code: 128, stderr: "fatal: not a git repository" };
			},
		});

		expect(calls).toBe(1);
		expect(result.details.isGitRepository).toBe(false);
		expect(result.text).toContain("not a Git repository");
		expect(result.text).toContain("fatal: not a git repository");
	});

	it("contains partial diff failures without exposing arbitrary error properties", async () => {
		const runCommand: GitCommandRunner = async (_cwd, args) => {
			if (args.includes("--cached")) {
				throw { stderr: "permission denied", env: { SECRET: "do not expose" } };
			}
			return { stdout: "", stderr: "" };
		};

		const result = await collectWorkspaceDiff("/repo", { runCommand });

		expect(result.details.isGitRepository).toBe(true);
		expect(result.details.errors).toEqual(["Staged changes: permission denied"]);
		expect(result.text).toContain("Unable to collect this section: permission denied");
		expect(result.text).not.toContain("SECRET");
	});

	it("applies the shared UTF-8 model-visible output limit", async () => {
		const result = await collectWorkspaceDiff("/repo", {
			maxBytes: 200,
			runCommand: async () => ({ stdout: "\u{1f642}".repeat(100), stderr: "" }),
		});

		expect(Buffer.byteLength(result.text, "utf8")).toBeLessThanOrEqual(200);
		expect(result.text).not.toContain("\ufffd");
		expect(result.details.outputTruncated).toBe(true);
		expect(result.details.fullOutput.length).toBeGreaterThan(result.text.length);
	});
});

import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { defineTool } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { MAX_MODEL_VISIBLE_OUTPUT_BYTES, truncateUtf8 } from "./results.js";

const execFileAsync = promisify(execFile);

const GIT_COMMANDS = [
	{ label: "Working tree status", args: ["status", "--short"] },
	{ label: "Unstaged changes", args: ["diff", "--no-ext-diff", "--no-color"] },
	{ label: "Staged changes", args: ["diff", "--cached", "--no-ext-diff", "--no-color"] },
] as const;

export interface GitCommandOutput {
	stdout: string;
	stderr: string;
}

export type GitCommandRunner = (cwd: string, args: readonly string[]) => Promise<GitCommandOutput>;

export interface WorkspaceDiffDetails {
	isGitRepository: boolean;
	outputTruncated: boolean;
	omittedBytes: number;
	fullOutput: string;
	errors: string[];
}

export interface WorkspaceDiffResult {
	text: string;
	details: WorkspaceDiffDetails;
}

async function runGit(cwd: string, args: readonly string[]): Promise<GitCommandOutput> {
	const result = await execFileAsync("git", [...args], {
		cwd,
		encoding: "utf8",
		maxBuffer: 16 * 1024 * 1024,
		shell: false,
	});
	return { stdout: result.stdout, stderr: result.stderr };
}

function errorText(error: unknown): string {
	if (typeof error !== "object" || error === null) return "Git command failed";
	const candidate = error as { stderr?: unknown; stdout?: unknown; code?: unknown };
	const stderr = typeof candidate.stderr === "string" ? candidate.stderr.trim() : "";
	const stdout = typeof candidate.stdout === "string" ? candidate.stdout.trim() : "";
	const code = typeof candidate.code === "number" || typeof candidate.code === "string" ? candidate.code : undefined;
	return stderr || stdout || (code === undefined ? "Git command failed" : `Git command failed with exit code ${code}`);
}

function section(label: string, output: GitCommandOutput): string {
	const stdout = output.stdout.trimEnd();
	const stderr = output.stderr.trimEnd();
	const body = [stdout, stderr && `[stderr]\n${stderr}`].filter(Boolean).join("\n");
	return `## ${label}\n\n${body || "(no changes)"}`;
}

/** Collect the fixed reviewer-safe Git status and diff commands. */
export async function collectWorkspaceDiff(
	cwd: string,
	options: { runCommand?: GitCommandRunner; maxBytes?: number } = {},
): Promise<WorkspaceDiffResult> {
	const runCommand = options.runCommand ?? runGit;
	const maxBytes = options.maxBytes ?? MAX_MODEL_VISIBLE_OUTPUT_BYTES;
	const errors: string[] = [];
	const sections: string[] = [];

	let status: GitCommandOutput;
	try {
		status = await runCommand(cwd, GIT_COMMANDS[0].args);
	} catch (error) {
		const detail = errorText(error);
		const fullOutput = `Workspace diff is unavailable because the working directory is not a Git repository.\n\n${detail}`;
		const visible = truncateUtf8(fullOutput, maxBytes);
		return {
			text: visible.text,
			details: {
				isGitRepository: false,
				outputTruncated: visible.truncated,
				omittedBytes: visible.omittedBytes,
				fullOutput,
				errors: [detail],
			},
		};
	}

	sections.push(section(GIT_COMMANDS[0].label, status));

	for (const command of GIT_COMMANDS.slice(1)) {
		try {
			sections.push(section(command.label, await runCommand(cwd, command.args)));
		} catch (error) {
			const detail = errorText(error);
			errors.push(`${command.label}: ${detail}`);
			sections.push(`## ${command.label}\n\nUnable to collect this section: ${detail}`);
		}
	}

	const fullOutput = sections.join("\n\n");
	const visible = truncateUtf8(fullOutput, maxBytes);
	return {
		text: visible.text,
		details: {
			isGitRepository: true,
			outputTruncated: visible.truncated,
			omittedBytes: visible.omittedBytes,
			fullOutput,
			errors,
		},
	};
}

/** Create the argument-free custom tool exposed only to reviewer sessions. */
export function createWorkspaceDiffTool(cwd: string) {
	return defineTool({
		name: "workspace_diff",
		label: "Workspace Diff",
		description: "Show the fixed Git status, unstaged diff, and staged diff for the current workspace.",
		parameters: Type.Object({}, { additionalProperties: false }),
		execute: async () => {
			const result = await collectWorkspaceDiff(cwd);
			return {
				content: [{ type: "text" as const, text: result.text }],
				details: result.details,
			};
		},
	});
}

#!/usr/bin/env node

import { readFileSync, realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { runMomo } from "./runtime.js";
import { execCanonicalPi, planLaunch } from "./launch.js";

interface CliOptions {
	action: "run" | "help" | "version";
	initialMessage?: string;
	rawArgs: string[];
}

const HELP = `Momo - a Pi SDK coding orchestrator

Usage:
  momo [initial task...]
  momo --help
  momo --version

Options:
  -h, --help       Show this help
  -v, --version    Show the installed version

Environment:
  MOMO_BACKEND=auto|herdr|inprocess
  MOMO_PI_BINARY=pi
  MOMO_HERDR_BINARY=herdr

Outside Herdr (or MOMO_BACKEND=inprocess), Momo uses the in-process runtime.
Inside Herdr with auto/herdr, Momo fail-closes on incompatible preflight and
replaces itself with canonical Pi (kind pi, display Momo) plus extensions.`;

export function parseCliArgs(args: readonly string[]): CliOptions {
	if (args.includes("--help") || args.includes("-h")) {
		return { action: "help", rawArgs: [...args] };
	}
	if (args.includes("--version") || args.includes("-v")) {
		return { action: "version", rawArgs: [...args] };
	}

	const unknownOption = args.find((arg) => arg.startsWith("-"));
	if (unknownOption) {
		throw new Error(`Unknown option: ${unknownOption}`);
	}

	const initialMessage = args.join(" ").trim();
	return initialMessage
		? { action: "run", initialMessage, rawArgs: [...args] }
		: { action: "run", rawArgs: [...args] };
}

export function getPackageVersion(): string {
	const packagePath = fileURLToPath(new URL("../package.json", import.meta.url));
	const packageJson = JSON.parse(readFileSync(packagePath, "utf8")) as { version?: unknown };
	if (typeof packageJson.version !== "string") {
		throw new Error("package.json does not contain a valid version");
	}
	return packageJson.version;
}

export async function main(args: readonly string[] = process.argv.slice(2)): Promise<number> {
	let options: CliOptions;
	try {
		options = parseCliArgs(args);
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		process.stderr.write(`${message}\nRun 'momo --help' for usage.\n`);
		return 2;
	}

	if (options.action === "help") {
		process.stdout.write(`${HELP}\n`);
		return 0;
	}
	if (options.action === "version") {
		process.stdout.write(`${getPackageVersion()}\n`);
		return 0;
	}

	try {
		const decision = await planLaunch(options.rawArgs);
		if (decision.mode === "fail") {
			process.stderr.write(`Momo failed to start: ${decision.error}\n`);
			return 1;
		}
		if (decision.mode === "exec-pi") {
			execCanonicalPi(decision.piArgs ?? [], decision.env ?? process.env);
			return 1; // unreachable unless execve fails to replace
		}

		// In-process path keeps process.title=momo
		process.title = "momo";
		await runMomo(options.initialMessage ? { initialMessage: options.initialMessage } : {});
		return 0;
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		process.stderr.write(`Momo failed to start: ${message}\n`);
		return 1;
	}
}

export function isEntryPoint(moduleUrl: string, executablePath: string | undefined): boolean {
	if (!executablePath) {
		return false;
	}

	try {
		return realpathSync(fileURLToPath(moduleUrl)) === realpathSync(executablePath);
	} catch {
		return false;
	}
}

if (isEntryPoint(import.meta.url, process.argv[1])) {
	void main().then((exitCode) => {
		process.exitCode = exitCode;
	});
}

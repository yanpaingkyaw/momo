#!/usr/bin/env node

import { readFileSync, realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { runMomo } from "./runtime.js";

interface CliOptions {
  action: "run" | "help" | "version";
  initialMessage?: string;
}

const HELP = `Momo - a Pi SDK coding orchestrator

Usage:
  momo [initial task...]
  momo --help
  momo --version

Options:
  -h, --help       Show this help
  -v, --version    Show the installed version`;

export function parseCliArgs(args: readonly string[]): CliOptions {
  if (args.includes("--help") || args.includes("-h")) {
    return { action: "help" };
  }
  if (args.includes("--version") || args.includes("-v")) {
    return { action: "version" };
  }

  const unknownOption = args.find((arg) => arg.startsWith("-"));
  if (unknownOption) {
    throw new Error(`Unknown option: ${unknownOption}`);
  }

  const initialMessage = args.join(" ").trim();
  return initialMessage ? { action: "run", initialMessage } : { action: "run" };
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
  process.title = "momo";
  void main().then((exitCode) => {
    process.exitCode = exitCode;
  });
}

import { accessSync, constants, existsSync, realpathSync } from "node:fs";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { homedir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { TESTED_PI_VERSION } from "./herdr/client.js";

const execFileAsync = promisify(execFile);
const packageRoot = path.resolve(fileURLToPath(new URL("..", import.meta.url)));

export function getPackageRoot(): string {
	return packageRoot;
}

export function getParentExtensionPath(): string {
	return path.join(packageRoot, "dist", "extensions", "parent.js");
}

export function getWorkerExtensionPath(): string {
	return path.join(packageRoot, "dist", "extensions", "worker.js");
}

export function getHerdrPiExtensionPath(
	env: NodeJS.ProcessEnv = process.env,
): string | undefined {
	const agentDir = env.PI_CODING_AGENT_DIR || path.join(homedir(), ".pi", "agent");
	const candidate = path.join(agentDir, "extensions", "herdr-agent-state.ts");
	return existsSync(candidate) ? candidate : undefined;
}

export function resolvePiBinary(env: NodeJS.ProcessEnv = process.env): string {
	if (env.MOMO_PI_BINARY && env.MOMO_PI_BINARY.trim()) {
		const requested = env.MOMO_PI_BINARY.trim();
		if (requested.includes("\0") || requested.includes("\n")) {
			throw new Error("MOMO_PI_BINARY contains unsafe characters");
		}
		return requested;
	}
	return "pi";
}

function which(command: string, env: NodeJS.ProcessEnv): string | undefined {
	const pathEnv = env.PATH ?? process.env.PATH ?? "";
	for (const dir of pathEnv.split(path.delimiter)) {
		const candidate = path.join(dir, command);
		try {
			accessSync(candidate, constants.X_OK);
			return candidate;
		} catch {
			// continue
		}
	}
	return undefined;
}

function realExecutable(filePath: string): string {
	return realpathSync(filePath);
}

/** Resolve absolute PATH `pi` (not MOMO_PI_BINARY override). */
export function resolvePathPiExecutable(env: NodeJS.ProcessEnv = process.env): string {
	const found = which("pi", env);
	if (!found) {
		throw new Error("Pi binary not found on PATH: pi");
	}
	return realExecutable(found);
}

/**
 * Resolve Pi for process image replacement. In Herdr mode, MOMO_PI_BINARY must
 * resolve to the same real executable as PATH `pi`.
 */
export function resolveCanonicalPiPath(
	env: NodeJS.ProcessEnv = process.env,
	options: { herdrMode?: boolean } = {},
): string {
	const pathPi = resolvePathPiExecutable(env);
	if (!env.MOMO_PI_BINARY?.trim()) {
		return pathPi;
	}
	const requested = resolvePiBinary(env);
	const absolute = path.isAbsolute(requested)
		? requested
		: which(requested, env);
	if (!absolute) {
		throw new Error(`MOMO_PI_BINARY not found: ${requested}`);
	}
	try {
		accessSync(absolute, constants.X_OK);
	} catch {
		throw new Error(`MOMO_PI_BINARY is not executable: ${absolute}`);
	}
	const resolved = realExecutable(absolute);
	if (options.herdrMode !== false && env.HERDR_ENV === "1") {
		if (resolved !== pathPi) {
			throw new Error(
				`MOMO_PI_BINARY (${resolved}) must resolve to the same executable as PATH pi (${pathPi}) in Herdr mode`,
			);
		}
	}
	return resolved;
}

export async function resolveInstalledPiVersion(
	env: NodeJS.ProcessEnv = process.env,
	binary?: string,
): Promise<string> {
	const target = binary ?? resolveCanonicalPiPath(env);
	const result = await execFileAsync(target, ["--version"], {
		encoding: "utf8",
		env,
		shell: false,
		maxBuffer: 1024 * 1024,
	});
	const text = `${result.stdout}\n${result.stderr}`.trim();
	const match = text.match(/(\d+\.\d+\.\d+)/);
	if (!match?.[1]) {
		throw new Error(`Unable to parse Pi version from: ${text}`);
	}
	return match[1];
}

export async function assertSupportedPiVersion(
	env: NodeJS.ProcessEnv = process.env,
): Promise<string> {
	const binary = resolveCanonicalPiPath(env);
	const version = await resolveInstalledPiVersion(env, binary);
	if (version !== TESTED_PI_VERSION) {
		throw new Error(`Unsupported Pi version ${version}; required ${TESTED_PI_VERSION}`);
	}
	return version;
}

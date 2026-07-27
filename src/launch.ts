import { accessSync, constants } from "node:fs";
import path from "node:path";
import { detectHerdrEnv, parseMomoBackend, selectBackend } from "./herdr/env.js";
import { HerdrClient, preflightHerdr, TESTED_PI_VERSION } from "./herdr/client.js";
import {
	getHerdrPiExtensionPath,
	getParentExtensionPath,
	resolveCanonicalPiPath,
	resolveInstalledPiVersion,
	resolvePathPiExecutable,
} from "./paths.js";
import { createStableParentId } from "./delegation/herdr-factory.js";

export interface LaunchDecision {
	mode: "inprocess" | "exec-pi" | "fail";
	reason: string;
	piArgs?: string[];
	env?: NodeJS.ProcessEnv;
	error?: string;
}

function which(command: string): string | undefined {
	const pathEnv = process.env.PATH ?? "";
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

export async function planLaunch(
	cliArgs: readonly string[],
	options: {
		env?: NodeJS.ProcessEnv;
		client?: HerdrClient;
		herdrPiExtensionPath?: string | undefined;
		resolvePiVersion?: () => Promise<string>;
	} = {},
): Promise<LaunchDecision> {
	const env = { ...process.env, ...(options.env ?? {}) };
	const backend = parseMomoBackend(env.MOMO_BACKEND);
	const herdr = detectHerdrEnv(env);

	const selectionOptions: Parameters<typeof selectBackend>[0] = { backend, herdr };
	if (backend === "herdr" || (backend === "auto" && herdr.herdrEnv)) {
		const client = options.client ?? new HerdrClient({ env });
		const herdrPiExtensionPath = Object.hasOwn(options, "herdrPiExtensionPath")
			? options.herdrPiExtensionPath
			: getHerdrPiExtensionPath(env);
		try {
			// Herdr workers launch via PATH `pi`; reject mismatched MOMO_PI_BINARY.
			const pathPi = resolvePathPiExecutable(env);
			const canonical = resolveCanonicalPiPath(env, { herdrMode: true });
			if (canonical !== pathPi) {
				throw new Error(
					`MOMO_PI_BINARY must match PATH pi (${pathPi}); got ${canonical}`,
				);
			}
			const preflight = await preflightHerdr(client, herdr, {
				herdrPiExtensionPath,
				resolvePiVersion:
					options.resolvePiVersion ??
					(async () => {
						const version = await resolveInstalledPiVersion(env, canonical);
						if (version !== TESTED_PI_VERSION) {
							throw new Error(
								`Unsupported Pi version ${version}; required ${TESTED_PI_VERSION}`,
							);
						}
						return version;
					}),
			});
			selectionOptions.preflightOk = preflight.ok;
			if (!preflight.ok) selectionOptions.preflightError = preflight.error;
		} catch (error) {
			selectionOptions.preflightOk = false;
			selectionOptions.preflightError =
				error instanceof Error ? error.message : String(error);
		}
	}

	const selection = selectBackend(selectionOptions);

	if (selection.backend === "fail-closed") {
		return { mode: "fail", reason: selection.reason, error: selection.reason };
	}
	if (selection.backend === "inprocess") {
		return { mode: "inprocess", reason: selection.reason };
	}

	const parentExtension = getParentExtensionPath();
	const herdrExtension = Object.hasOwn(options, "herdrPiExtensionPath")
		? options.herdrPiExtensionPath
		: getHerdrPiExtensionPath(env);
	if (!herdrExtension) {
		return {
			mode: "fail",
			reason: "Official Herdr Pi lifecycle extension is required",
			error: "Official Herdr Pi lifecycle extension is required (herdr integration install pi)",
		};
	}

	const parentId = createStableParentId({
		paneId: herdr.paneId || "unknown",
		cwd: process.cwd(),
		...(herdr.workspaceId ? { workspaceId: herdr.workspaceId } : {}),
	});
	const piArgs = [
		"--name",
		"Momo",
		"--tools",
		"read,grep,find,ls,delegate",
		"--no-extensions",
		"-e",
		parentExtension,
		"-e",
		herdrExtension,
	];
	// Forward non-flag initial task args only (help/version already handled).
	const initial = cliArgs.filter((arg) => !arg.startsWith("-")).join(" ").trim();
	if (initial) {
		// Task content belongs in argv for Pi interactive start message only — not env.
		piArgs.push(initial);
	}

	return {
		mode: "exec-pi",
		reason: selection.reason,
		piArgs,
		env: {
			...env,
			MOMO_PARENT: "1",
			MOMO_PARENT_ID: parentId,
			MOMO_BACKEND: env.MOMO_BACKEND ?? "auto",
		},
	};
}

/** Replace this process with canonical Pi. Never returns on success. */
export function execCanonicalPi(piArgs: readonly string[], env: NodeJS.ProcessEnv): void {
	const resolved = resolveCanonicalPiPath(env, { herdrMode: env.HERDR_ENV === "1" });
	const execve = (process as NodeJS.Process & {
		execve?: (file: string, args?: readonly string[], env?: NodeJS.ProcessEnv) => never;
	}).execve;
	if (typeof execve !== "function") {
		throw new Error("process.execve is required for Herdr parent launch on this Node version");
	}
	// Do not keep process.title=momo on this path; Pi becomes the foreground process image.
	execve(resolved, [resolved, ...piArgs], env);
}

export { which };

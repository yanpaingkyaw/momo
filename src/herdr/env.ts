export type MomoBackend = "auto" | "herdr" | "inprocess";

export interface HerdrEnv {
	readonly herdrEnv: boolean;
	readonly socketPath: string | undefined;
	readonly paneId: string | undefined;
	readonly workspaceId: string | undefined;
	readonly tabId: string | undefined;
	readonly platformSupported: boolean;
}

export function parseMomoBackend(value: string | undefined = process.env.MOMO_BACKEND): MomoBackend {
	const normalized = (value ?? "auto").trim().toLowerCase();
	if (normalized === "herdr" || normalized === "inprocess" || normalized === "auto") {
		return normalized;
	}
	throw new Error(`Invalid MOMO_BACKEND=${value}. Expected auto|herdr|inprocess.`);
}

export function detectHerdrEnv(
	env: NodeJS.ProcessEnv = process.env,
	platform: NodeJS.Platform = process.platform,
): HerdrEnv {
	return {
		herdrEnv: env.HERDR_ENV === "1",
		socketPath: typeof env.HERDR_SOCKET_PATH === "string" && env.HERDR_SOCKET_PATH ? env.HERDR_SOCKET_PATH : undefined,
		paneId: typeof env.HERDR_PANE_ID === "string" && env.HERDR_PANE_ID ? env.HERDR_PANE_ID : undefined,
		workspaceId:
			typeof env.HERDR_WORKSPACE_ID === "string" && env.HERDR_WORKSPACE_ID
				? env.HERDR_WORKSPACE_ID
				: undefined,
		tabId: typeof env.HERDR_TAB_ID === "string" && env.HERDR_TAB_ID ? env.HERDR_TAB_ID : undefined,
		platformSupported: platform === "darwin" || platform === "linux",
	};
}

export type BackendSelection =
	| { backend: "inprocess"; reason: string }
	| { backend: "herdr"; reason: string }
	| { backend: "fail-closed"; reason: string };

/** Choose specialist backend per SPEC §32.1. */
export function selectBackend(
	options: {
		backend?: MomoBackend;
		herdr?: HerdrEnv;
		preflightOk?: boolean;
		preflightError?: string;
	} = {},
): BackendSelection {
	const backend = options.backend ?? parseMomoBackend();
	const herdr = options.herdr ?? detectHerdrEnv();

	if (backend === "inprocess") {
		return { backend: "inprocess", reason: "MOMO_BACKEND=inprocess" };
	}

	if (backend === "herdr") {
		if (!herdr.herdrEnv) {
			return { backend: "fail-closed", reason: "MOMO_BACKEND=herdr but HERDR_ENV is not set" };
		}
		if (!herdr.platformSupported) {
			return { backend: "fail-closed", reason: "Herdr mode requires macOS or Linux" };
		}
		if (options.preflightOk === false) {
			return {
				backend: "fail-closed",
				reason: options.preflightError ?? "Herdr preflight failed",
			};
		}
		return { backend: "herdr", reason: "MOMO_BACKEND=herdr" };
	}

	// auto
	if (!herdr.herdrEnv) {
		return { backend: "inprocess", reason: "Herdr not detected" };
	}
	if (!herdr.platformSupported) {
		return { backend: "fail-closed", reason: "Herdr detected on unsupported platform" };
	}
	if (options.preflightOk === false) {
		return {
			backend: "fail-closed",
			reason: options.preflightError ?? "Herdr detected but preflight failed",
		};
	}
	return { backend: "herdr", reason: "Herdr detected (auto)" };
}

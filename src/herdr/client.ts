import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { assertSafeEnvValue } from "../ipc/validate.js";

const execFileAsync = promisify(execFile);

export const TESTED_HERDR_PROTOCOL = 17;
export const TESTED_HERDR_CLI_PREFIX = "0.7.";
export const TESTED_PI_VERSION = "0.82.1";

export interface CommandResult {
	stdout: string;
	stderr: string;
	code: number;
}

/** OS-level timeout for Herdr CLI child processes (ms). */
export const HERDR_CLI_TIMEOUT_MS = 30_000;

export type CommandRunner = (
	file: string,
	args: readonly string[],
	options?: { env?: NodeJS.ProcessEnv; cwd?: string; timeoutMs?: number },
) => Promise<CommandResult>;

export async function defaultCommandRunner(
	file: string,
	args: readonly string[],
	options: { env?: NodeJS.ProcessEnv; cwd?: string; timeoutMs?: number } = {},
): Promise<CommandResult> {
	try {
		const result = await execFileAsync(file, [...args], {
			encoding: "utf8",
			maxBuffer: 16 * 1024 * 1024,
			shell: false,
			env: options.env ?? process.env,
			cwd: options.cwd,
			timeout: options.timeoutMs ?? HERDR_CLI_TIMEOUT_MS,
			killSignal: "SIGKILL",
		});
		return { stdout: result.stdout, stderr: result.stderr, code: 0 };
	} catch (error) {
		const err = error as {
			stdout?: string;
			stderr?: string;
			code?: number | string | null;
			killed?: boolean;
			signal?: string | null;
			message?: string;
		};
		if (err.killed || err.signal === "SIGKILL") {
			return {
				stdout: typeof err.stdout === "string" ? err.stdout : "",
				stderr:
					typeof err.stderr === "string" && err.stderr
						? err.stderr
						: `Herdr CLI timed out after ${options.timeoutMs ?? HERDR_CLI_TIMEOUT_MS}ms`,
				code: 1,
			};
		}
		return {
			stdout: typeof err.stdout === "string" ? err.stdout : "",
			stderr: typeof err.stderr === "string" ? err.stderr : (err.message ?? String(error)),
			code: typeof err.code === "number" ? err.code : 1,
		};
	}
}

export interface HerdrEnvelope {
	ok: boolean;
	id?: string;
	result?: unknown;
	error?: { code?: string; message?: string };
}

export interface AgentStartInfo {
	paneId: string;
	name: string;
	agent: string;
	interactiveReady: boolean;
	agentStatus?: string;
}

/** Herdr 0.7.5 agent statuses observed via agent get/wait. */
export type HerdrAgentStatus = "idle" | "done" | "unknown" | string;

export interface AgentGetInfo {
	agentStatus?: HerdrAgentStatus;
	name?: string;
	paneId?: string;
	raw: Record<string, unknown>;
}

/**
 * Parse Herdr agent get result. Real shape:
 * `{ type: "agent_info", agent: { agent_status, name, pane_id, ... } }`
 */
export function parseAgentGetResult(result: unknown): AgentGetInfo {
	const record = asRecord(result, "agent.get result");
	const nested =
		typeof record.agent === "object" && record.agent !== null && !Array.isArray(record.agent)
			? (record.agent as Record<string, unknown>)
			: record;
	const info: AgentGetInfo = { raw: record };
	if (typeof nested.agent_status === "string") info.agentStatus = nested.agent_status;
	else if (typeof nested.status === "string") info.agentStatus = nested.status;
	if (typeof nested.name === "string") info.name = nested.name;
	if (typeof nested.pane_id === "string") info.paneId = nested.pane_id;
	return info;
}

/** Typed Herdr CLI failure preserving structured error.code when present. */
export class HerdrCliError extends Error {
	readonly code: string | undefined;
	readonly label: string;

	constructor(label: string, options: { code?: string; message: string }) {
		super(`${label} failed: ${options.message}`);
		this.name = "HerdrCliError";
		this.label = label;
		this.code = options.code;
	}
}

export const AGENT_PANE_BUSY_CODE = "agent_pane_busy";

export function isAgentPaneBusyError(error: unknown): boolean {
	if (error instanceof HerdrCliError) {
		if (error.code === AGENT_PANE_BUSY_CODE) return true;
		return /is not an available shell/i.test(error.message);
	}
	if (error instanceof Error) {
		return (
			error.message.includes(AGENT_PANE_BUSY_CODE) ||
			/is not an available shell/i.test(error.message)
		);
	}
	return false;
}

/** Bound for waiting until a freshly split pane becomes an available shell. */
export const AGENT_START_BUSY_RETRY_MS = 8_000;
export const AGENT_START_BUSY_INTERVAL_MS = 150;

export function parseHerdrJson(stdout: string, stderr: string = ""): HerdrEnvelope {
	const text = stdout.trim() || stderr.trim();
	if (!text) {
		throw new Error("Herdr CLI returned empty output");
	}

	let parsed: unknown;
	try {
		parsed = JSON.parse(text);
	} catch {
		const lines = text.split("\n").map((line) => line.trim()).filter(Boolean);
		const last = lines[lines.length - 1];
		if (!last) throw new Error("Herdr CLI returned non-JSON output");
		parsed = JSON.parse(last);
	}

	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
		throw new Error("Herdr CLI JSON envelope must be an object");
	}

	const record = parsed as Record<string, unknown>;
	if ("error" in record && record.error) {
		const error = record.error as Record<string, unknown>;
		const envelope: HerdrEnvelope = {
			ok: false,
			error: {
				message:
					typeof error.message === "string"
						? error.message
						: typeof record.error === "string"
							? record.error
							: "Herdr CLI error",
			},
		};
		if (typeof record.id === "string") envelope.id = record.id;
		if (typeof error.code === "string" && envelope.error) envelope.error.code = error.code;
		return envelope;
	}

	if ("result" in record || "id" in record) {
		const envelope: HerdrEnvelope = {
			ok: true,
			result: record.result ?? record,
		};
		if (typeof record.id === "string") envelope.id = record.id;
		return envelope;
	}

	if (record.ok === false) {
		return {
			ok: false,
			error: {
				message: typeof record.message === "string" ? record.message : "Herdr CLI reported failure",
			},
		};
	}

	return { ok: true, result: record };
}

export function requireHerdrResult(envelope: HerdrEnvelope, label: string): unknown {
	if (!envelope.ok) {
		const message = envelope.error?.message ?? envelope.error?.code ?? "unknown error";
		throw new HerdrCliError(label, {
			message,
			...(typeof envelope.error?.code === "string" ? { code: envelope.error.code } : {}),
		});
	}
	return envelope.result;
}

function asRecord(value: unknown, label: string): Record<string, unknown> {
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		throw new Error(`${label} must be an object`);
	}
	return value as Record<string, unknown>;
}

export function parsePaneSplitResult(result: unknown): { paneId: string } {
	const record = asRecord(result, "pane.split result");
	const pane = asRecord(record.pane ?? record, "pane.split pane");
	const paneId = pane.pane_id;
	if (typeof paneId !== "string" || !paneId.includes(":")) {
		throw new Error("herdr pane split response missing pane_id");
	}
	return { paneId };
}

export function parseAgentStartResult(
	result: unknown,
	expected: { paneId: string; name: string; kind?: string },
): AgentStartInfo {
	const record = asRecord(result, "agent.start result");
	// CLI may nest under agent / pane / result.
	const candidate =
		(typeof record.agent === "object" && record.agent !== null && !Array.isArray(record.agent)
			? (record.agent as Record<string, unknown>)
			: undefined) ??
		(typeof record.pane === "object" && record.pane !== null && !Array.isArray(record.pane)
			? (record.pane as Record<string, unknown>)
			: undefined) ??
		record;

	const paneId = candidate.pane_id;
	const name = candidate.name;
	const agent = candidate.agent ?? candidate.display_agent ?? expected.kind ?? "pi";
	const interactiveReady = candidate.interactive_ready;
	const agentStatus = typeof candidate.agent_status === "string" ? candidate.agent_status : undefined;

	if (typeof paneId !== "string" || paneId !== expected.paneId) {
		throw new Error("agent.start response pane_id mismatch");
	}
	if (typeof name !== "string" || name !== expected.name) {
		throw new Error("agent.start response name mismatch");
	}
	if (typeof agent !== "string" || agent.length === 0) {
		throw new Error("agent.start response missing agent label");
	}
	if (interactiveReady !== true && agentStatus !== "idle" && agentStatus !== "working") {
		// Success contract: ready for interactive input. Accept explicit ready or known live statuses.
		if (interactiveReady !== true) {
			throw new Error("agent.start response is not interactively ready");
		}
	}

	return {
		paneId,
		name,
		agent,
		interactiveReady: interactiveReady === true || agentStatus === "idle" || agentStatus === "working",
		...(agentStatus ? { agentStatus } : {}),
	};
}

export interface HerdrClientOptions {
	herdrBinary?: string;
	runCommand?: CommandRunner;
	env?: NodeJS.ProcessEnv;
	now?: () => number;
	sleep?: (ms: number) => Promise<void>;
	agentStartBusyRetryMs?: number;
	agentStartBusyIntervalMs?: number;
}

export class HerdrClient {
	readonly herdrBinary: string;
	private readonly runCommand: CommandRunner;
	private readonly env: NodeJS.ProcessEnv;
	private readonly now: () => number;
	private readonly sleep: (ms: number) => Promise<void>;
	private readonly agentStartBusyRetryMs: number;
	private readonly agentStartBusyIntervalMs: number;

	constructor(options: HerdrClientOptions = {}) {
		this.herdrBinary = options.herdrBinary ?? process.env.MOMO_HERDR_BINARY ?? "herdr";
		this.runCommand = options.runCommand ?? defaultCommandRunner;
		this.env = options.env ?? process.env;
		this.now = options.now ?? Date.now;
		this.sleep =
			options.sleep ?? ((ms: number) => new Promise((resolve) => setTimeout(resolve, ms)));
		this.agentStartBusyRetryMs = options.agentStartBusyRetryMs ?? AGENT_START_BUSY_RETRY_MS;
		this.agentStartBusyIntervalMs =
			options.agentStartBusyIntervalMs ?? AGENT_START_BUSY_INTERVAL_MS;
	}

	async run(args: readonly string[]): Promise<HerdrEnvelope> {
		const result = await this.runCommand(this.herdrBinary, args, { env: this.env });
		if (result.code !== 0 && !result.stdout.trim()) {
			throw new Error(
				`Herdr CLI exited ${result.code}: ${result.stderr.trim() || "no output"}`,
			);
		}
		try {
			return parseHerdrJson(result.stdout, result.stderr);
		} catch (error) {
			if (result.code !== 0) {
				throw new Error(
					`Herdr CLI exited ${result.code}: ${result.stderr.trim() || result.stdout.trim()}`,
				);
			}
			throw error;
		}
	}

	/**
	 * Narrow runner for Herdr commands that succeed with exit 0 and intentionally
	 * empty stdout/stderr (Herdr 0.7.5 `pane report-metadata`). Does not relax
	 * JSON parsing used by split/start/get/etc.
	 */
	private async runOkAllowEmpty(args: readonly string[], label: string): Promise<void> {
		const result = await this.runCommand(this.herdrBinary, args, { env: this.env });
		if (result.code !== 0) {
			throw new Error(
				`${label} failed: Herdr CLI exited ${result.code}: ${
					result.stderr.trim() || result.stdout.trim() || "no output"
				}`,
			);
		}
		const text = `${result.stdout}${result.stderr}`.trim();
		if (!text) return;
		// If Herdr starts returning a body, still validate it as a success envelope.
		const envelope = parseHerdrJson(result.stdout, result.stderr);
		requireHerdrResult(envelope, label);
	}

	async version(): Promise<string> {
		const result = await this.runCommand(this.herdrBinary, ["--version"], { env: this.env });
		const text = `${result.stdout}\n${result.stderr}`.trim();
		const match = text.match(/(\d+\.\d+\.\d+)/);
		if (!match?.[1]) {
			throw new Error(`Unable to parse Herdr version from: ${text}`);
		}
		return match[1];
	}

	async protocol(): Promise<number> {
		const envelope = await this.run(["api", "schema", "--json"]);
		const result = asRecord(requireHerdrResult(envelope, "herdr api schema"), "schema");
		const protocol = result.protocol;
		if (typeof protocol !== "number" || !Number.isFinite(protocol)) {
			throw new Error("Herdr schema is missing numeric protocol");
		}
		return protocol;
	}

	async splitPane(options: {
		pane?: string;
		direction: "right" | "down";
		cwd: string;
		env?: Record<string, string>;
		noFocus?: boolean;
	}): Promise<{ paneId: string }> {
		const args = [
			"pane",
			"split",
			"--cwd",
			options.cwd,
			"--direction",
			options.direction,
		];
		if (options.pane) args.push("--pane", options.pane);
		else args.push("--current");
		if (options.noFocus !== false) args.push("--no-focus");
		for (const [key, value] of Object.entries(options.env ?? {})) {
			assertSafeEnvValue(key, value);
			args.push("--env", `${key}=${value}`);
		}
		const envelope = await this.run(args);
		return parsePaneSplitResult(requireHerdrResult(envelope, "herdr pane split"));
	}

	async renamePane(paneId: string, name: string): Promise<void> {
		// Herdr 0.7.5: pane rename <pane_id> <label...>
		const envelope = await this.run(["pane", "rename", paneId, name]);
		requireHerdrResult(envelope, "herdr pane rename");
	}

	async reportMetadata(
		paneId: string,
		options: {
			source: string;
			displayAgent?: string;
			title?: string;
			agent?: string;
		},
	): Promise<void> {
		const args = ["pane", "report-metadata", paneId, "--source", options.source];
		if (options.displayAgent) args.push("--display-agent", options.displayAgent);
		if (options.title) args.push("--title", options.title);
		if (options.agent) args.push("--agent", options.agent);
		await this.runOkAllowEmpty(args, "herdr pane report-metadata");
	}

	async agentStart(options: {
		name: string;
		paneId: string;
		kind?: string;
		timeoutMs?: number;
		agentArgs?: readonly string[];
	}): Promise<AgentStartInfo> {
		const kind = options.kind ?? "pi";
		const args = [
			"agent",
			"start",
			options.name,
			"--kind",
			kind,
			"--pane",
			options.paneId,
		];
		if (options.timeoutMs !== undefined) {
			args.push("--timeout", String(options.timeoutMs));
		}
		if (options.agentArgs?.length) {
			args.push("--", ...options.agentArgs);
		}

		const deadline = this.now() + this.agentStartBusyRetryMs;
		let lastBusy: HerdrCliError | undefined;
		for (;;) {
			try {
				const envelope = await this.run(args);
				return parseAgentStartResult(requireHerdrResult(envelope, "herdr agent start"), {
					paneId: options.paneId,
					name: options.name,
					kind,
				});
			} catch (error) {
				if (!isAgentPaneBusyError(error)) throw error;
				lastBusy =
					error instanceof HerdrCliError
						? error
						: new HerdrCliError("herdr agent start", {
								code: AGENT_PANE_BUSY_CODE,
								message: error instanceof Error ? error.message : String(error),
							});
				if (this.now() >= deadline) {
					throw new HerdrCliError("herdr agent start", {
						code: lastBusy.code ?? AGENT_PANE_BUSY_CODE,
						message: `${lastBusy.message} (pane shell readiness timed out after ${this.agentStartBusyRetryMs}ms)`,
					});
				}
				await this.sleep(this.agentStartBusyIntervalMs);
			}
		}
	}

	async agentSendKeys(target: string, keys: readonly string[]): Promise<void> {
		const envelope = await this.run(["agent", "send-keys", target, ...keys]);
		requireHerdrResult(envelope, "herdr agent send-keys");
	}

	async agentWait(
		target: string,
		options: { until?: readonly string[]; timeoutMs?: number } = {},
	): Promise<void> {
		const args = ["agent", "wait", target];
		for (const state of options.until ?? []) {
			args.push("--until", state);
		}
		if (options.timeoutMs !== undefined) {
			args.push("--timeout", String(options.timeoutMs));
		}
		const envelope = await this.run(args);
		requireHerdrResult(envelope, "herdr agent wait");
	}

	async agentGet(target: string): Promise<AgentGetInfo> {
		const envelope = await this.run(["agent", "get", target]);
		return parseAgentGetResult(requireHerdrResult(envelope, "herdr agent get"));
	}

	async closePane(paneId: string): Promise<void> {
		// Herdr 0.7.5: pane close <pane_id>
		const envelope = await this.run(["pane", "close", paneId]);
		requireHerdrResult(envelope, "herdr pane close");
	}
}

export async function preflightHerdr(
	client: HerdrClient,
	herdr: ReturnType<typeof import("./env.js").detectHerdrEnv>,
	options: {
		piVersion?: string;
		herdrPiExtensionPath?: string | undefined;
		resolvePiVersion?: () => Promise<string>;
	} = {},
): Promise<{ ok: true; version: string; protocol: number; piVersion: string } | { ok: false; error: string }> {
	if (!herdr.herdrEnv) {
		return { ok: false, error: "HERDR_ENV is not set" };
	}
	if (!herdr.socketPath) {
		return { ok: false, error: "HERDR_SOCKET_PATH is missing" };
	}
	if (!herdr.paneId) {
		return { ok: false, error: "HERDR_PANE_ID is missing" };
	}
	if (!herdr.platformSupported) {
		return { ok: false, error: "Unsupported platform for Herdr mode" };
	}
	if (!options.herdrPiExtensionPath) {
		return {
			ok: false,
			error: "Official Herdr Pi lifecycle extension is required (herdr integration install pi)",
		};
	}

	try {
		const version = await client.version();
		if (!version.startsWith(TESTED_HERDR_CLI_PREFIX)) {
			return {
				ok: false,
				error: `Unsupported Herdr CLI version ${version}; tested line is 0.7.x`,
			};
		}
		const protocol = await client.protocol();
		if (protocol !== TESTED_HERDR_PROTOCOL) {
			return {
				ok: false,
				error: `Unsupported Herdr protocol ${protocol}; tested protocol is ${TESTED_HERDR_PROTOCOL}`,
			};
		}
		const piVersion =
			options.piVersion ??
			(options.resolvePiVersion ? await options.resolvePiVersion() : TESTED_PI_VERSION);
		if (piVersion !== TESTED_PI_VERSION) {
			return {
				ok: false,
				error: `Unsupported Pi version ${piVersion}; required ${TESTED_PI_VERSION}`,
			};
		}
		return { ok: true, version, protocol, piVersion };
	} catch (error) {
		return {
			ok: false,
			error: error instanceof Error ? error.message : String(error),
		};
	}
}

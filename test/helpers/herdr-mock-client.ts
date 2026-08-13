import {
	HerdrClient,
	type CommandRunner,
	type HerdrClientOptions,
} from "../../src/herdr/client.js";

export function structuredAgentNotFoundStdout(id = "agent-get-not-found"): {
	code: number;
	stdout: string;
	stderr: string;
} {
	return {
		code: 1,
		stdout: JSON.stringify({
			id,
			error: { code: "agent_not_found", message: "agent not found" },
		}),
		stderr: "",
	};
}

function isGenericEmptyAgentGetResponse(result: {
	code: number;
	stdout: string;
}): boolean {
	if (result.code !== 0) return false;
	try {
		const parsed = JSON.parse(result.stdout) as Record<string, unknown>;
		if (parsed.error) return false;
		const payload = parsed.result;
		if (payload === undefined || payload === null) return true;
		if (typeof payload === "object" && !Array.isArray(payload) && Object.keys(payload).length === 0) {
			return true;
		}
		if (typeof payload === "object" && payload !== null && !Array.isArray(payload)) {
			const record = payload as Record<string, unknown>;
			if (record.type === "agent_info") {
				const agent = record.agent;
				if (typeof agent !== "object" || agent === null || Array.isArray(agent)) {
					return true;
				}
				const nested = agent as Record<string, unknown>;
				return (
					typeof nested.name !== "string" &&
					typeof nested.pane_id !== "string" &&
					typeof nested.agent_status !== "string" &&
					typeof nested.status !== "string"
				);
			}
		}
	} catch {
		return false;
	}
	return false;
}

function argsAreAgentGet(args: readonly string[]): boolean {
	return args[0] === "agent" && args[1] === "get";
}

const defaultOkHandler: CommandRunner = async () => ({
	code: 0,
	stdout: JSON.stringify({ id: "ok", result: {} }),
	stderr: "",
});

/**
 * Wrap a test runCommand so generic catch-all agent get responses become structured
 * agent_not_found. Explicit live-agent or typed error responses pass through unchanged.
 */
export function wrapRunCommandWithDefaultAgentNotFound(
	handler?: CommandRunner,
): CommandRunner {
	return async (file, args, env) => {
		const result = handler
			? await handler(file, args, env)
			: await defaultOkHandler(file, args, env);
		if (argsAreAgentGet(args) && isGenericEmptyAgentGetResponse(result)) {
			return structuredAgentNotFoundStdout();
		}
		return result;
	};
}

/** Test Herdr client: fail-closed agent absence unless mocks return structured not_found or live agent info. */
export function createTestHerdrClient(options: HerdrClientOptions = {}): HerdrClient {
	return new HerdrClient({
		...options,
		runCommand: wrapRunCommandWithDefaultAgentNotFound(options.runCommand),
	});
}

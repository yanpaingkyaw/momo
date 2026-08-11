import { describe, expect, it } from "vitest";
import {
	createTestHerdrClient,
	structuredAgentNotFoundStdout,
	wrapRunCommandWithDefaultAgentNotFound,
} from "./helpers/herdr-mock-client.js";

describe("createTestHerdrClient agent get defaults", () => {
	it("maps generic catch-all agent get to structured agent_not_found", async () => {
		const client = createTestHerdrClient({
			runCommand: async () => ({
				code: 0,
				stdout: JSON.stringify({ id: "ok", result: {} }),
				stderr: "",
			}),
		});
		await expect(client.agentGet("momo_scout")).rejects.toMatchObject({
			code: "agent_not_found",
		});
	});

	it("preserves explicit live agent responses", async () => {
		const client = createTestHerdrClient({
			runCommand: async (_file, args) => {
				if (args[0] === "agent" && args[1] === "get") {
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "g",
							result: {
								type: "agent_info",
								agent: {
									agent_status: "idle",
									name: "momo_scout",
									pane_id: "w1:p1",
								},
							},
						}),
						stderr: "",
					};
				}
				return structuredAgentNotFoundStdout();
			},
		});
		const info = await client.agentGet("momo_scout");
		expect(info.agentStatus).toBe("idle");
		expect(info.name).toBe("momo_scout");
	});

	it("wrapRunCommand leaves non-agent commands untouched", async () => {
		const inner = wrapRunCommandWithDefaultAgentNotFound(async (_file, args) => {
			if (args[0] === "pane" && args[1] === "list") {
				return {
					code: 0,
					stdout: JSON.stringify({ id: "ok", result: { panes: [] } }),
					stderr: "",
				};
			}
			return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
		});
		const result = await inner("herdr", ["pane", "list"], { env: process.env });
		expect(JSON.parse(result.stdout).result).toEqual({ panes: [] });
	});
});

import { describe, expect, it } from "vitest";
import {
	AGENT_PANE_BUSY_CODE,
	HerdrCliError,
	HerdrClient,
	parseAgentStartResult,
	parseHerdrJson,
	parsePaneSplitResult,
	preflightHerdr,
	requireHerdrResult,
} from "../src/herdr/client.js";
import type { detectHerdrEnv } from "../src/herdr/env.js";

describe("Herdr JSON validation", () => {
	it("parses result envelopes", () => {
		const envelope = parseHerdrJson(
			JSON.stringify({ id: "cli:1", result: { pane: { pane_id: "w1:p2" } } }),
		);
		expect(envelope.ok).toBe(true);
		expect(parsePaneSplitResult(requireHerdrResult(envelope, "split")).paneId).toBe("w1:p2");
	});

	it("rejects non-JSON", () => {
		expect(() => parseHerdrJson("not-json")).toThrow(/JSON/);
	});

	it("surfaces error envelopes with typed HerdrCliError code", () => {
		const envelope = parseHerdrJson(
			JSON.stringify({ id: "cli:1", error: { code: "agent_not_ready", message: "not ready" } }),
		);
		expect(envelope.ok).toBe(false);
		try {
			requireHerdrResult(envelope, "prompt");
			expect.unreachable("expected throw");
		} catch (error) {
			expect(error).toBeInstanceOf(HerdrCliError);
			expect((error as HerdrCliError).code).toBe("agent_not_ready");
			expect((error as HerdrCliError).message).toMatch(/not ready/);
		}
	});

	it("rejects agent.start {ok:true} mocks without real AgentInfo fields", () => {
		expect(() =>
			parseAgentStartResult({ ok: true }, { paneId: "w1:p2", name: "momo_scout" }),
		).toThrow(/pane_id|ready/i);
	});

	it("accepts real-shaped agent.start success", () => {
		const info = parseAgentStartResult(
			{
				pane_id: "w1:p2",
				name: "momo_scout",
				agent: "pi",
				interactive_ready: true,
				agent_status: "idle",
			},
			{ paneId: "w1:p2", name: "momo_scout" },
		);
		expect(info.interactiveReady).toBe(true);
		expect(info.agent).toBe("pi");
	});
});

describe("Herdr 0.7.5 argv shapes", () => {
	it("uses exact argv for every command used by Momo", async () => {
		const calls: string[][] = [];
		const client = new HerdrClient({
			herdrBinary: "herdr",
			runCommand: async (_file, args) => {
				calls.push([...args]);
				if (args[0] === "pane" && args[1] === "split") {
					return {
						code: 0,
						stdout: JSON.stringify({ id: "x", result: { pane: { pane_id: "w1:p9" } } }),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "start") {
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "s",
							result: {
								pane_id: "w1:p9",
								name: "momo_worker",
								agent: "pi",
								interactive_ready: true,
							},
						}),
						stderr: "",
					};
				}
				if (args[0] === "agent" && args[1] === "get") {
					return {
						code: 0,
						stdout: JSON.stringify({
							id: "g",
							result: { name: "momo_worker", agent_status: "idle" },
						}),
						stderr: "",
					};
				}
				if (args[0] === "api" && args[1] === "schema") {
					return {
						code: 0,
						stdout: JSON.stringify({ protocol: 17, schema_version: 1 }),
						stderr: "",
					};
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
			},
		});

		await client.splitPane({
			pane: "w1:p1",
			direction: "right",
			cwd: "/repo",
			env: { MOMO_WORKER: "1" },
		});
		await client.renamePane("w1:p9", "Momo scout");
		await client.reportMetadata("w1:p9", {
			source: "momo:parent",
			displayAgent: "Momo scout",
			title: "Momo scout",
			agent: "pi",
		});
		await client.agentStart({ name: "momo_worker", paneId: "w1:p9", kind: "pi" });
		await client.agentSendKeys("momo_worker", ["ctrl+c"]);
		await client.agentWait("momo_worker", { until: ["idle"], timeoutMs: 1000 });
		await client.agentGet("momo_worker");
		await client.closePane("w1:p9");
		await client.protocol();

		const byPrefix = (a: string, b: string) => calls.find((c) => c[0] === a && c[1] === b);

		const split = byPrefix("pane", "split");
		expect(split).toEqual(
			expect.arrayContaining(["pane", "split", "--cwd", "/repo", "--direction", "right", "--pane", "w1:p1", "--no-focus", "--env", "MOMO_WORKER=1"]),
		);
		expect(split?.includes("--direction")).toBe(true);

		expect(byPrefix("pane", "rename")).toEqual(["pane", "rename", "w1:p9", "Momo scout"]);
		expect(byPrefix("pane", "close")).toEqual(["pane", "close", "w1:p9"]);
		expect(byPrefix("pane", "report-metadata")?.[0]).toBe("pane");
		expect(byPrefix("agent", "start")).toEqual(
			expect.arrayContaining(["agent", "start", "momo_worker", "--kind", "pi", "--pane", "w1:p9"]),
		);
		expect(byPrefix("agent", "send-keys")).toEqual(["agent", "send-keys", "momo_worker", "ctrl+c"]);
		expect(byPrefix("agent", "wait")).toEqual(
			expect.arrayContaining(["agent", "wait", "momo_worker", "--until", "idle", "--timeout", "1000"]),
		);
		expect(byPrefix("agent", "get")).toEqual(["agent", "get", "momo_worker"]);
		expect(byPrefix("api", "schema")).toEqual(["api", "schema", "--json"]);
	});

	it("rejects unsafe env values on pane split", async () => {
		const client = new HerdrClient({
			runCommand: async () => ({ code: 0, stdout: "{}", stderr: "" }),
		});
		await expect(
			client.splitPane({
				direction: "down",
				cwd: "/repo",
				env: { MOMO_TASK: "secret task" },
			}),
		).rejects.toThrow(/Task content must not be placed in env|Unsafe/);
	});

	it("accepts Herdr 0.7.5 report-metadata exit 0 with empty stdout/stderr", async () => {
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				expect(args.slice(0, 2)).toEqual(["pane", "report-metadata"]);
				return { code: 0, stdout: "", stderr: "" };
			},
		});
		await expect(
			client.reportMetadata("w1:p9", {
				source: "momo:parent",
				displayAgent: "Momo scout",
				title: "Momo scout",
				agent: "pi",
			}),
		).resolves.toBeUndefined();
	});

	it("fails report-metadata on nonzero exit even with empty output", async () => {
		const client = new HerdrClient({
			runCommand: async () => ({ code: 1, stdout: "", stderr: "pane missing" }),
		});
		await expect(
			client.reportMetadata("w1:p9", { source: "momo:parent" }),
		).rejects.toThrow(/report-metadata failed.*exited 1.*pane missing/);
	});

	it("keeps strict JSON requirement for split on empty exit 0", async () => {
		const client = new HerdrClient({
			runCommand: async () => ({ code: 0, stdout: "", stderr: "" }),
		});
		await expect(
			client.splitPane({ direction: "right", cwd: "/repo" }),
		).rejects.toThrow(/empty output|JSON/);
	});

	it("retries agent start only for agent_pane_busy then succeeds once", async () => {
		let attempts = 0;
		let now = 0;
		const sleeps: number[] = [];
		const client = new HerdrClient({
			now: () => now,
			sleep: async (ms) => {
				sleeps.push(ms);
				now += ms;
			},
			agentStartBusyRetryMs: 1_000,
			agentStartBusyIntervalMs: 100,
			runCommand: async (_file, args) => {
				if (!(args[0] === "agent" && args[1] === "start")) {
					return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
				}
				attempts += 1;
				if (attempts <= 2) {
					return {
						code: 1,
						stdout: JSON.stringify({
							id: "s",
							error: {
								code: AGENT_PANE_BUSY_CODE,
								message: "agent target pane w1:p9 is not an available shell",
							},
						}),
						stderr: "",
					};
				}
				return {
					code: 0,
					stdout: JSON.stringify({
						id: "s",
						result: {
							pane_id: "w1:p9",
							name: "momo_worker",
							agent: "pi",
							interactive_ready: true,
							agent_status: "idle",
						},
					}),
					stderr: "",
				};
			},
		});

		const info = await client.agentStart({ name: "momo_worker", paneId: "w1:p9", kind: "pi" });
		expect(info.paneId).toBe("w1:p9");
		expect(attempts).toBe(3);
		expect(sleeps).toEqual([100, 100]);
	});

	it("does not retry unrelated agent start errors", async () => {
		let attempts = 0;
		const client = new HerdrClient({
			agentStartBusyRetryMs: 1_000,
			agentStartBusyIntervalMs: 50,
			sleep: async () => {
				throw new Error("should not sleep");
			},
			runCommand: async () => {
				attempts += 1;
				return {
					code: 1,
					stdout: JSON.stringify({
						id: "s",
						error: { code: "agent_not_ready", message: "not ready" },
					}),
					stderr: "",
				};
			},
		});
		await expect(
			client.agentStart({ name: "momo_worker", paneId: "w1:p9" }),
		).rejects.toMatchObject({ code: "agent_not_ready" });
		expect(attempts).toBe(1);
	});

	it("bounds agent_pane_busy retries until timeout", async () => {
		let attempts = 0;
		let now = 0;
		const client = new HerdrClient({
			now: () => now,
			sleep: async (ms) => {
				now += ms;
			},
			agentStartBusyRetryMs: 250,
			agentStartBusyIntervalMs: 100,
			runCommand: async () => {
				attempts += 1;
				return {
					code: 1,
					stdout: JSON.stringify({
						id: "s",
						error: {
							code: AGENT_PANE_BUSY_CODE,
							message: "agent target pane w1:p9 is not an available shell",
						},
					}),
					stderr: "",
				};
			},
		});
		await expect(client.agentStart({ name: "momo_worker", paneId: "w1:p9" })).rejects.toMatchObject(
			{
				code: AGENT_PANE_BUSY_CODE,
				message: expect.stringMatching(/timed out after 250ms/),
			},
		);
		expect(attempts).toBeGreaterThanOrEqual(2);
		expect(attempts).toBeLessThanOrEqual(4);
		expect(now).toBeLessThanOrEqual(350);
	});
});

describe("preflightHerdr", () => {
	const herdr = {
		herdrEnv: true,
		socketPath: "/tmp/x.sock",
		paneId: "w1:p1",
		platformSupported: true,
	} as ReturnType<typeof detectHerdrEnv>;

	it("requires official Herdr Pi extension and exact Pi 0.82.1", async () => {
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args.includes("--version")) return { code: 0, stdout: "herdr 0.7.5\n", stderr: "" };
				if (args[0] === "api") {
					return { code: 0, stdout: JSON.stringify({ protocol: 17 }), stderr: "" };
				}
				return { code: 0, stdout: "{}", stderr: "" };
			},
		});
		const missingExt = await preflightHerdr(client, herdr, {
			herdrPiExtensionPath: undefined,
			piVersion: "0.82.1",
		});
		expect(missingExt.ok).toBe(false);
		if (!missingExt.ok) expect(missingExt.error).toMatch(/Herdr Pi lifecycle extension/);

		const badPi = await preflightHerdr(client, herdr, {
			herdrPiExtensionPath: "/tmp/herdr-agent-state.ts",
			piVersion: "0.80.0",
		});
		expect(badPi.ok).toBe(false);
		if (!badPi.ok) expect(badPi.error).toMatch(/Unsupported Pi version/);

		const ok = await preflightHerdr(client, herdr, {
			herdrPiExtensionPath: "/tmp/herdr-agent-state.ts",
			piVersion: "0.82.1",
		});
		expect(ok.ok).toBe(true);
	});
});

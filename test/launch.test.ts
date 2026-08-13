import { describe, expect, it } from "vitest";
import { HerdrClient } from "../src/herdr/client.js";
import { createTestHerdrClient } from "./helpers/herdr-mock-client.js";
import { planLaunch } from "../src/launch.js";

describe("planLaunch", () => {
	it("uses inprocess outside Herdr", async () => {
		const decision = await planLaunch([], {
			env: { ...process.env, HERDR_ENV: "", MOMO_BACKEND: "auto" },
		});
		expect(decision.mode).toBe("inprocess");
	});

	it("fail-closes on incompatible Herdr preflight", async () => {
		const client = createTestHerdrClient({
			runCommand: async (_file, args) => {
				if (args.includes("--version")) {
					return { code: 0, stdout: "herdr 0.6.0\n", stderr: "" };
				}
				return {
					code: 0,
					stdout: JSON.stringify({ protocol: 16, schema_version: 1 }),
					stderr: "",
				};
			},
		});
		const decision = await planLaunch([], {
			client,
			herdrPiExtensionPath: "/tmp/herdr-agent-state.ts",
			resolvePiVersion: async () => "0.82.1",
			env: {
				...process.env,
				HERDR_ENV: "1",
				HERDR_SOCKET_PATH: "/tmp/x.sock",
				HERDR_PANE_ID: "w1:p1",
				MOMO_BACKEND: "auto",
			},
		});
		expect(decision.mode).toBe("fail");
	});

	it("fail-closes when official Herdr Pi extension is missing", async () => {
		const client = createTestHerdrClient({
			runCommand: async (_file, args) => {
				if (args.includes("--version")) return { code: 0, stdout: "herdr 0.7.5\n", stderr: "" };
				if (args[0] === "api") {
					return { code: 0, stdout: JSON.stringify({ protocol: 17 }), stderr: "" };
				}
				return { code: 0, stdout: "{}", stderr: "" };
			},
		});
		const decision = await planLaunch([], {
			client,
			herdrPiExtensionPath: undefined,
			resolvePiVersion: async () => "0.82.1",
			env: {
				...process.env,
				HERDR_ENV: "1",
				HERDR_SOCKET_PATH: "/tmp/x.sock",
				HERDR_PANE_ID: "w1:p1",
				MOMO_BACKEND: "auto",
			},
		});
		expect(decision.mode).toBe("fail");
		expect(decision.error).toMatch(/Herdr Pi lifecycle extension/);
	});

	it("fail-closes on incompatible MOMO_PI_BINARY / Pi version", async () => {
		const client = createTestHerdrClient({
			runCommand: async (_file, args) => {
				if (args.includes("--version")) return { code: 0, stdout: "herdr 0.7.5\n", stderr: "" };
				if (args[0] === "api") {
					return { code: 0, stdout: JSON.stringify({ protocol: 17 }), stderr: "" };
				}
				return { code: 0, stdout: "{}", stderr: "" };
			},
		});
		const decision = await planLaunch([], {
			client,
			herdrPiExtensionPath: "/tmp/herdr-agent-state.ts",
			resolvePiVersion: async () => {
				throw new Error("Unsupported Pi version 0.80.0; required 0.82.1");
			},
			env: {
				...process.env,
				HERDR_ENV: "1",
				HERDR_SOCKET_PATH: "/tmp/x.sock",
				HERDR_PANE_ID: "w1:p1",
				MOMO_BACKEND: "herdr",
				MOMO_PI_BINARY: "/tmp/wrong-pi",
			},
		});
		expect(decision.mode).toBe("fail");
		expect(decision.error).toMatch(/MOMO_PI_BINARY|Unsupported Pi version|same executable/);
	});

	it("fail-closes when MOMO_PI_BINARY resolves to a different executable than PATH pi", async () => {
		const client = createTestHerdrClient({
			runCommand: async (_file, args) => {
				if (args.includes("--version")) return { code: 0, stdout: "herdr 0.7.5\n", stderr: "" };
				if (args[0] === "api") {
					return { code: 0, stdout: JSON.stringify({ protocol: 17 }), stderr: "" };
				}
				return { code: 0, stdout: "{}", stderr: "" };
			},
		});
		const decision = await planLaunch([], {
			client,
			herdrPiExtensionPath: "/tmp/herdr-agent-state.ts",
			resolvePiVersion: async () => "0.82.1",
			env: {
				...process.env,
				HERDR_ENV: "1",
				HERDR_SOCKET_PATH: "/tmp/x.sock",
				HERDR_PANE_ID: "w1:p1",
				MOMO_BACKEND: "herdr",
				// node itself is executable but not PATH pi
				MOMO_PI_BINARY: process.execPath,
			},
		});
		expect(decision.mode).toBe("fail");
		expect(decision.error).toMatch(/same executable|MOMO_PI_BINARY/);
	});

	it("plans exec-pi when Herdr preflight passes", async () => {
		const client = createTestHerdrClient({
			runCommand: async (_file, args) => {
				if (args.includes("--version")) {
					return { code: 0, stdout: "herdr 0.7.5\n", stderr: "" };
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
		const decision = await planLaunch(["fix", "tests"], {
			client,
			herdrPiExtensionPath: "/tmp/herdr-agent-state.ts",
			resolvePiVersion: async () => "0.82.1",
			env: {
				...process.env,
				HERDR_ENV: "1",
				HERDR_SOCKET_PATH: "/tmp/x.sock",
				HERDR_PANE_ID: "w1:p1",
				MOMO_BACKEND: "auto",
			},
		});
		expect(decision.mode).toBe("exec-pi");
		expect(decision.piArgs).toContain("--name");
		expect(decision.piArgs).toContain("Momo");
		expect(decision.piArgs).toContain("/tmp/herdr-agent-state.ts");
		expect(decision.piArgs).toContain("read,grep,find,ls,delegate");
		expect(decision.piArgs?.at(-1)).toBe("fix tests");
		expect(decision.env?.MOMO_PARENT).toBe("1");
		expect(decision.env?.MOMO_TASK).toBeUndefined();
	});
});

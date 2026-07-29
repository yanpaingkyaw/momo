import { describe, expect, it } from "vitest";
import { detectHerdrEnv, parseMomoBackend, selectBackend } from "../src/herdr/env.js";

describe("backend selection", () => {
	it("defaults to inprocess when Herdr is absent", () => {
		expect(
			selectBackend({
				backend: "auto",
				herdr: detectHerdrEnv({} as NodeJS.ProcessEnv, "darwin"),
			}),
		).toEqual({ backend: "inprocess", reason: "Herdr not detected" });
	});

	it("selects herdr when detected and preflight ok", () => {
		expect(
			selectBackend({
				backend: "auto",
				herdr: {
					herdrEnv: true,
					socketPath: "/tmp/herdr.sock",
					paneId: "w1:p1",
					workspaceId: "w1",
					tabId: "w1:t1",
					platformSupported: true,
				},
				preflightOk: true,
			}).backend,
		).toBe("herdr");
	});

	it("fail-closes when Herdr is detected but preflight fails", () => {
		const result = selectBackend({
			backend: "auto",
			herdr: {
				herdrEnv: true,
				socketPath: "/tmp/herdr.sock",
				paneId: "w1:p1",
				workspaceId: "w1",
				tabId: "w1:t1",
				platformSupported: true,
			},
			preflightOk: false,
			preflightError: "bad protocol",
		});
		expect(result).toEqual({ backend: "fail-closed", reason: "bad protocol" });
	});

	it("honors explicit inprocess override inside Herdr", () => {
		expect(
			selectBackend({
				backend: "inprocess",
				herdr: {
					herdrEnv: true,
					socketPath: "/tmp/herdr.sock",
					paneId: "w1:p1",
					workspaceId: "w1",
					tabId: "w1:t1",
					platformSupported: true,
				},
			}).backend,
		).toBe("inprocess");
	});

	it("parses MOMO_BACKEND values", () => {
		expect(parseMomoBackend("auto")).toBe("auto");
		expect(parseMomoBackend("herdr")).toBe("herdr");
		expect(parseMomoBackend("inprocess")).toBe("inprocess");
		expect(() => parseMomoBackend("nope")).toThrow(/Invalid MOMO_BACKEND/);
	});
});

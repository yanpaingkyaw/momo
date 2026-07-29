import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { installMomoParent, PARENT_ACTIVE_TOOLS } from "../src/extensions/parent.js";
import { HerdrClient } from "../src/herdr/client.js";
import { PaneRegistry } from "../src/herdr/registry.js";
import { planLaunch } from "../src/launch.js";

const tempDirs: string[] = [];

afterEach(() => {
	while (tempDirs.length > 0) {
		const dir = tempDirs.pop();
		if (dir) rmSync(dir, { recursive: true, force: true });
	}
});

const RUNTIME_NOT_READY =
	"Extension runtime not initialized. Action methods cannot be called during extension loading.";

function createFakePi() {
	const handlers = new Map<string, Function[]>();
	const order: string[] = [];
	let runtimeReady = false;

	const assertRuntime = (action: string) => {
		if (!runtimeReady) {
			throw new Error(RUNTIME_NOT_READY);
		}
		order.push(action);
	};

	return {
		order,
		handlers,
		get runtimeReady() {
			return runtimeReady;
		},
		registerTool: vi.fn((tool: { name?: string }) => {
			// Registration is allowed during extension loading.
			order.push(`registerTool:${tool?.name ?? "unknown"}`);
		}),
		setActiveTools: vi.fn((tools: string[]) => {
			assertRuntime(`setActiveTools:${tools.join(",")}`);
		}),
		sendUserMessage: vi.fn((text: string) => {
			assertRuntime(`sendUserMessage:${text}`);
		}),
		registerCommand: vi.fn((name: string) => {
			order.push(`registerCommand:${name}`);
		}),
		on(event: string, handler: Function) {
			order.push(`on:${event}`);
			const list = handlers.get(event) ?? [];
			list.push(handler);
			handlers.set(event, list);
		},
		async emit(event: string, payload: unknown = {}, ctx?: unknown) {
			if (event === "session_start") {
				runtimeReady = true;
			}
			order.push(`emit:${event}`);
			const list = handlers.get(event) ?? [];
			for (const handler of list) {
				await handler(payload, ctx);
			}
		},
	};
}

describe("canonical Herdr parent extension", () => {
	it("registers delegate synchronously at factory load but defers setActiveTools to session_start", async () => {
		const cwd = mkdtempSync(path.join(tmpdir(), "momo-parent-cwd-"));
		const cacheRoot = mkdtempSync(path.join(tmpdir(), "momo-parent-cache-"));
		tempDirs.push(cwd, cacheRoot);
		const pi = createFakePi();

		expect(() =>
			installMomoParent(pi as unknown as ExtensionAPI, {
				cwd,
				env: {
					MOMO_PARENT: "1",
					MOMO_PARENT_ID: "parentlive1",
					HERDR_ENV: "1",
					HERDR_PANE_ID: "w1:p1",
					MOMO_BACKEND: "auto",
				},
				client: new HerdrClient({
					runCommand: async () => ({
						code: 0,
						stdout: JSON.stringify({ id: "ok", result: {} }),
						stderr: "",
					}),
				}),
				registry: new PaneRegistry("parentlive1", cacheRoot),
			}),
		).not.toThrow();

		const registerIdx = pi.order.findIndex((step) => step === "registerTool:delegate");
		const sessionStartHandlerIdx = pi.order.findIndex((step) => step === "on:session_start");
		expect(registerIdx).toBeGreaterThanOrEqual(0);
		expect(sessionStartHandlerIdx).toBeGreaterThanOrEqual(0);
		expect(registerIdx).toBeLessThan(sessionStartHandlerIdx);
		expect(pi.setActiveTools).not.toHaveBeenCalled();
		expect(pi.runtimeReady).toBe(false);

		expect(pi.registerTool).toHaveBeenCalledTimes(1);
		const tool = pi.registerTool.mock.calls[0]?.[0] as { name?: string };
		expect(tool?.name).toBe("delegate");

		await pi.emit("session_start", {}, { hasUI: true, cwd });
		expect(pi.setActiveTools).toHaveBeenCalledWith([...PARENT_ACTIVE_TOOLS]);
		expect(pi.registerCommand).toHaveBeenCalledWith("momo-workers", expect.any(Object));
		expect(pi.registerCommand).toHaveBeenCalledWith("momo-cleanup", expect.any(Object));
		expect(pi.registerTool).toHaveBeenCalledTimes(1);
	});

	it("launch --tools includes delegate for canonical Herdr parent", async () => {
		const client = new HerdrClient({
			runCommand: async (_file, args) => {
				if (args.includes("--version")) return { code: 0, stdout: "herdr 0.7.5\n", stderr: "" };
				if (args[0] === "api") {
					return { code: 0, stdout: JSON.stringify({ protocol: 17 }), stderr: "" };
				}
				return { code: 0, stdout: JSON.stringify({ id: "ok", result: {} }), stderr: "" };
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
		expect(decision.mode).toBe("exec-pi");
		const toolsIdx = decision.piArgs?.indexOf("--tools") ?? -1;
		expect(toolsIdx).toBeGreaterThanOrEqual(0);
		expect(decision.piArgs?.[toolsIdx + 1]).toBe("read,grep,find,ls,delegate");
	});
});

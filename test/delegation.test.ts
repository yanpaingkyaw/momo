import { describe, expect, it } from "vitest";
import { ROLE_LIST } from "../src/roles.js";
import {
	createDelegationRunner,
	type ChildSession,
	type ChildSessionFactory,
} from "../src/delegation/runner.js";

function assistant(text: string) {
	return {
		role: "assistant",
		content: [{ type: "text", text }],
		usage: { input: 2, output: 3, totalTokens: 5, cost: { total: 0.01 } },
	};
}

interface FakeState {
	prompts: string[];
	disposals: number;
	aborts: number;
}

function fakeFactory(
	state: FakeState,
	response: (prompt: string) => string = (prompt) => `response: ${prompt}`,
): ChildSessionFactory {
	return async () => {
		let messages: readonly unknown[] = [];
		return {
			get messages() {
				return messages;
			},
			agent: { waitForIdle: async () => {} },
			subscribe: () => () => {},
			async prompt(prompt) {
				state.prompts.push(prompt);
				messages = [assistant(response(prompt))];
			},
			async abort() {
				state.aborts += 1;
			},
			dispose() {
				state.disposals += 1;
			},
		} satisfies ChildSession;
	};
}

function state(): FakeState {
	return { prompts: [], disposals: 0, aborts: 0 };
}

describe("delegation runner", () => {
	it("validates the complete request before creating children", async () => {
		const current = state();
		const runner = createDelegationRunner({
			cwd: "/repo",
			roles: ROLE_LIST,
			createChildSession: fakeFactory(current),
		});

		await expect(
			runner.run({
				mode: "parallel",
				tasks: [{ agent: "implementer", task: "edit a file" }],
			}),
		).rejects.toThrow("write-capable");
		expect(current.prompts).toEqual([]);
		expect(current.disposals).toBe(0);
	});

	it("runs chains sequentially and expands every previous placeholder", async () => {
		const current = state();
		const runner = createDelegationRunner({
			cwd: "/repo",
			roles: ROLE_LIST,
			createChildSession: fakeFactory(current, (prompt) => (prompt === "first" ? "evidence" : "done")),
		});

		const result = await runner.run({
			mode: "chain",
			steps: [
				{ agent: "scout", task: "first" },
				{ agent: "planner", task: "use {previous} and {previous}" },
			],
		});

		expect(current.prompts).toEqual(["first", "use evidence and evidence"]);
		expect(current.disposals).toBe(2);
		expect(result.status).toBe("completed");
		expect(result.results[1]?.task).toBe("use evidence and evidence");
		expect(result.usage).toMatchObject({ inputTokens: 4, outputTokens: 6, turns: 2 });
	});

	it("stops a failed chain and marks remaining steps skipped", async () => {
		const current = state();
		const runner = createDelegationRunner({
			cwd: "/repo",
			roles: ROLE_LIST,
			createChildSession: async () => ({
				messages: [],
				agent: { waitForIdle: async () => {} },
				subscribe: () => () => {},
				prompt: async () => {
					throw new Error("provider unavailable");
				},
				abort: async () => {},
				dispose: () => {
					current.disposals += 1;
				},
			}),
		});

		const result = await runner.run({
			mode: "chain",
			steps: [
				{ agent: "scout", task: "fail" },
				{ agent: "planner", task: "must not run" },
			],
		});

		expect(result.status).toBe("failed");
		expect(result.results.map(({ status }) => status)).toEqual(["failed", "skipped"]);
		expect(result.results[0]?.error?.message).toBe("provider unavailable");
		// Lazy chain: only the failed step allocated a session.
		expect(current.disposals).toBe(1);
	});

	it("serializes writers across concurrent delegations", async () => {
		const gates: Array<() => void> = [];
		let active = 0;
		let maximumActive = 0;
		const runner = createDelegationRunner({
			cwd: "/repo",
			roles: ROLE_LIST,
			createChildSession: async () => {
				let messages: readonly unknown[] = [];
				return {
					get messages() {
						return messages;
					},
					agent: { waitForIdle: async () => {} },
					subscribe: () => () => {},
					prompt: async () => {
						active += 1;
						maximumActive = Math.max(maximumActive, active);
						await new Promise<void>((resolve) => gates.push(resolve));
						active -= 1;
						messages = [assistant("done")];
					},
					abort: async () => {},
					dispose: () => {},
				};
			},
		});

		const first = runner.run({ mode: "single", agent: "implementer", task: "first" });
		const second = runner.run({ mode: "single", agent: "implementer", task: "second" });
		await new Promise((resolve) => setTimeout(resolve, 0));
		expect(gates).toHaveLength(1);
		gates.shift()?.();
		await new Promise((resolve) => setTimeout(resolve, 0));
		expect(gates).toHaveLength(1);
		gates.shift()?.();

		await Promise.all([first, second]);
		expect(maximumActive).toBe(1);
	});

	it("reports a pre-aborted parallel request as aborted and starts no children", async () => {
		const current = state();
		const controller = new AbortController();
		controller.abort();
		const runner = createDelegationRunner({
			cwd: "/repo",
			roles: ROLE_LIST,
			createChildSession: fakeFactory(current),
		});

		const result = await runner.run(
			{
				mode: "parallel",
				tasks: [
					{ agent: "scout", task: "one" },
					{ agent: "planner", task: "two" },
				],
			},
			{ signal: controller.signal },
		);

		expect(result.status).toBe("aborted");
		expect(result.results.map(({ status }) => status)).toEqual(["aborted", "skipped"]);
		expect(current.prompts).toEqual([]);
	});

	it("does not let a progress callback failure change the result", async () => {
		const current = state();
		const runner = createDelegationRunner({
			cwd: "/repo",
			roles: ROLE_LIST,
			createChildSession: fakeFactory(current),
		});

		const result = await runner.run(
			{ mode: "single", agent: "scout", task: "inspect" },
			{
				onProgress() {
					throw new Error("display unavailable");
				},
			},
		);

		expect(result.status).toBe("completed");
		expect(current.disposals).toBe(1);
	});

	it("limits model-visible output while retaining full details in memory", async () => {
		const current = state();
		const fullOutput = "x".repeat(60 * 1024);
		const runner = createDelegationRunner({
			cwd: "/repo",
			roles: ROLE_LIST,
			createChildSession: fakeFactory(current, () => fullOutput),
		});

		const result = await runner.run({ mode: "single", agent: "scout", task: "large result" });
		const task = result.results[0];

		expect(task).toBeDefined();
		if (!task) throw new Error("Expected a task result");
		expect(Buffer.byteLength(task.output, "utf8")).toBeLessThanOrEqual(50 * 1024);
		expect(task.outputTruncated).toBe(true);
		expect(task.fullOutput).toBe(fullOutput);
	});

	it("marks the interrupted chain step aborted and later steps skipped", async () => {
		const current = state();
		const controller = new AbortController();
		const runner = createDelegationRunner({
			cwd: "/repo",
			roles: ROLE_LIST,
			createChildSession: fakeFactory(current, () => "first result"),
		});

		const result = await runner.run(
			{
				mode: "chain",
				steps: [
					{ agent: "scout", task: "one" },
					{ agent: "planner", task: "two" },
					{ agent: "reviewer", task: "three" },
				],
			},
			{
				signal: controller.signal,
				onProgress(progress) {
					if (progress.phase === "completed") controller.abort();
				},
			},
		);

		expect(result.status).toBe("aborted");
		expect(result.results.map(({ status }) => status)).toEqual(["completed", "aborted", "skipped"]);
		expect(current.prompts).toEqual(["one"]);
	});

	it("aborts an active child and disposes its session", async () => {
		const controller = new AbortController();
		let resolvePrompt: (() => void) | undefined;
		const current = state();
		const runner = createDelegationRunner({
			cwd: "/repo",
			roles: ROLE_LIST,
			createChildSession: async () => ({
				messages: [],
				agent: { waitForIdle: async () => {} },
				subscribe: () => () => {},
				prompt: () => new Promise<void>((resolve) => {
					resolvePrompt = resolve;
				}),
				abort: async () => {
					current.aborts += 1;
					resolvePrompt?.();
				},
				dispose: () => {
					current.disposals += 1;
				},
			}),
		});

		const pending = runner.run(
			{ mode: "single", agent: "scout", task: "wait" },
			{ signal: controller.signal },
		);
		await new Promise((resolve) => setTimeout(resolve, 0));
		controller.abort();
		const result = await pending;

		expect(result.status).toBe("aborted");
		expect(current.aborts).toBe(1);
		expect(current.disposals).toBe(1);
	});

	it("fails a child that completes without assistant text", async () => {
		const current = state();
		const runner = createDelegationRunner({
			cwd: "/repo",
			roles: ROLE_LIST,
			createChildSession: async () => ({
				messages: [],
				agent: { waitForIdle: async () => {} },
				subscribe: () => () => {},
				prompt: async () => {},
				abort: async () => {},
				dispose: () => {
					current.disposals += 1;
				},
			}),
		});

		const result = await runner.run({ mode: "single", agent: "scout", task: "inspect" });

		expect(result.status).toBe("failed");
		expect(result.results[0]?.error?.message).toBe("Child completed without a final response");
		expect(current.disposals).toBe(1);
	});

	it("preserves successful work when session disposal fails", async () => {
		const current = state();
		const runner = createDelegationRunner({
			cwd: "/repo",
			roles: ROLE_LIST,
			createChildSession: async () => {
				let messages: readonly unknown[] = [];
				return {
					get messages() {
						return messages;
					},
					agent: { waitForIdle: async () => {} },
					subscribe: () => () => {},
					prompt: async () => {
						messages = [assistant("done")];
					},
					abort: async () => {},
					dispose: () => {
						current.disposals += 1;
						throw new Error("cleanup failed");
					},
				};
			},
		});

		const result = await runner.run({ mode: "single", agent: "scout", task: "inspect" });

		expect(result.status).toBe("completed");
		expect(current.disposals).toBe(1);
	});

	it("prefers uncertainWrite over signal.aborted for implementer cancel", async () => {
		const controller = new AbortController();
		const runner = createDelegationRunner({
			cwd: "/repo",
			roles: ROLE_LIST,
			createChildSession: async () => ({
				get messages() {
					return [];
				},
				agent: {
					waitForIdle: async () => {
						throw Object.assign(new Error("Worker cancel unresolved after cancel IPC"), {
							uncertainWrite: true,
							stopReason: "error",
						});
					},
				},
				subscribe: () => () => {},
				async prompt() {
					controller.abort();
				},
				async abort() {},
				dispose() {},
			}),
		});

		const result = await runner.run(
			{ mode: "single", agent: "implementer", task: "edit a file" },
			{ signal: controller.signal },
		);

		expect(result.status).toBe("failed");
		expect(result.results).toHaveLength(1);
		expect(result.results[0]?.status).toBe("failed");
		expect(result.results[0]?.error?.message).toMatch(/^Uncertain write:/);
		expect(result.results[0]?.error?.stopReason).toBe("error");
	});

	it("keeps clean acknowledged abort as aborted when signal fires", async () => {
		const controller = new AbortController();
		const runner = createDelegationRunner({
			cwd: "/repo",
			roles: ROLE_LIST,
			createChildSession: async () => ({
				get messages() {
					return [];
				},
				agent: {
					waitForIdle: async () => {
						throw Object.assign(new Error("Worker aborted"), {
							stopReason: "aborted",
						});
					},
				},
				subscribe: () => () => {},
				async prompt() {
					controller.abort();
				},
				async abort() {},
				dispose() {},
			}),
		});

		const result = await runner.run(
			{ mode: "single", agent: "scout", task: "inspect" },
			{ signal: controller.signal },
		);

		expect(result.status).toBe("aborted");
		expect(result.results[0]?.status).toBe("aborted");
		expect(result.results[0]?.error?.message).toBe("Delegation aborted");
		expect(result.results[0]?.error?.stopReason).toBe("aborted");
	});

	it("returns structured failure when configured policy exists but registry is absent", async () => {
		const current = state();
		const runner = createDelegationRunner({
			cwd: "/repo",
			roles: ROLE_LIST,
			readConfig: () => ({
				version: 1,
				policies: {
					default: { provider: "openai", model: "gpt-4o", reasoning: "off" },
				},
			}),
			createChildSession: fakeFactory(current),
		});

		const result = await runner.run({ mode: "single", agent: "scout", task: "inspect" });
		expect(result.status).toBe("failed");
		expect(result.results[0]?.status).toBe("failed");
		expect(result.results[0]?.error?.message).toMatch(/registry unavailable/i);
		expect(current.prompts).toEqual([]);
	});
});

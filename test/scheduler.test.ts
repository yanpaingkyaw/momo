import { describe, expect, it } from "vitest";
import { mapWithConcurrency } from "../src/delegation/scheduler.js";

function deferred<T>() {
	let resolve!: (value: T) => void;
	let reject!: (reason?: unknown) => void;
	const promise = new Promise<T>((resolvePromise, rejectPromise) => {
		resolve = resolvePromise;
		reject = rejectPromise;
	});
	return { promise, resolve, reject };
}

async function nextTurn(): Promise<void> {
	await new Promise<void>((resolve) => setTimeout(resolve, 0));
}

describe("mapWithConcurrency", () => {
	it("bounds active work and starts queued tasks as slots open", async () => {
		const gates = Array.from({ length: 5 }, () => deferred<number>());
		const started: number[] = [];
		let active = 0;
		let maximumActive = 0;

		const scheduled = mapWithConcurrency(
			[0, 1, 2, 3, 4],
			async (_item, index) => {
				started.push(index);
				active += 1;
				maximumActive = Math.max(maximumActive, active);
				try {
					return await gates[index]!.promise;
				} finally {
					active -= 1;
				}
			},
			{ concurrency: 2 },
		);

		await nextTurn();
		expect(started).toEqual([0, 1]);
		gates[1]!.resolve(10);
		await nextTurn();
		expect(started).toEqual([0, 1, 2]);

		gates[0]!.resolve(0);
		gates[2]!.resolve(20);
		await nextTurn();
		gates[3]!.resolve(30);
		gates[4]!.resolve(40);

		const results = await scheduled;
		expect(maximumActive).toBe(2);
		expect(results).toEqual([
			{ status: "fulfilled", value: 0 },
			{ status: "fulfilled", value: 10 },
			{ status: "fulfilled", value: 20 },
			{ status: "fulfilled", value: 30 },
			{ status: "fulfilled", value: 40 },
		]);
	});

	it("preserves input order and contains worker failures", async () => {
		const results = await mapWithConcurrency(
			[30, 5, 10],
			async (delay, index) => {
				await new Promise((resolve) => setTimeout(resolve, delay));
				if (index === 1) throw new Error("expected failure");
				return index;
			},
			{ concurrency: 3 },
		);

		expect(results[0]).toEqual({ status: "fulfilled", value: 0 });
		expect(results[1]).toMatchObject({ status: "rejected", reason: expect.any(Error) });
		expect(results[2]).toEqual({ status: "fulfilled", value: 2 });
	});

	it("does not start queued work after cancellation", async () => {
		const controller = new AbortController();
		const gate = deferred<string>();
		const started: number[] = [];

		const scheduled = mapWithConcurrency(
			[0, 1, 2],
			async (_item, index) => {
				started.push(index);
				return gate.promise;
			},
			{ concurrency: 1, signal: controller.signal },
		);

		await nextTurn();
		controller.abort();
		gate.resolve("finished");

		expect(await scheduled).toEqual([
			{ status: "fulfilled", value: "finished" },
			{ status: "skipped" },
			{ status: "skipped" },
		]);
		expect(started).toEqual([0]);
	});

	it("reports immutable snapshots without letting update errors affect work", async () => {
		const snapshots: Array<{ queued: number; running: number; completed: number }> = [];
		const results = await mapWithConcurrency([1], async (value) => value * 2, {
			concurrency: 1,
			onStateChange(state) {
				snapshots.push({ ...state });
				if (state.running === 1) throw new Error("display failure");
			},
		});

		expect(results).toEqual([{ status: "fulfilled", value: 2 }]);
		expect(snapshots.at(-1)).toMatchObject({ queued: 0, running: 0, completed: 1 });
	});
});

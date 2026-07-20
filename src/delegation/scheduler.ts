export interface SchedulerState {
	queued: number;
	running: number;
	completed: number;
	failed: number;
	skipped: number;
}

export type SchedulerResult<T> =
	| { status: "fulfilled"; value: T }
	| { status: "rejected"; reason: unknown }
	| { status: "skipped" };

export interface SchedulerOptions {
	concurrency: number;
	signal?: AbortSignal;
	onStateChange?: (state: Readonly<SchedulerState>) => void;
}

export type SchedulerWorker<TInput, TOutput> = (
	item: TInput,
	index: number,
	signal: AbortSignal | undefined,
) => Promise<TOutput>;

/**
 * Run work with a real concurrency bound. Worker failures are captured so an
 * unrelated sibling cannot reject or cancel the whole schedule.
 */
export async function mapWithConcurrency<TInput, TOutput>(
	items: readonly TInput[],
	worker: SchedulerWorker<TInput, TOutput>,
	options: SchedulerOptions,
): Promise<SchedulerResult<TOutput>[]> {
	if (!Number.isSafeInteger(options.concurrency) || options.concurrency < 1) {
		throw new RangeError("concurrency must be a positive safe integer");
	}

	const results = new Array<SchedulerResult<TOutput>>(items.length);
	const state: SchedulerState = {
		queued: items.length,
		running: 0,
		completed: 0,
		failed: 0,
		skipped: 0,
	};
	let nextIndex = 0;

	const emitState = () => {
		try {
			options.onStateChange?.({ ...state });
		} catch {
			// Progress reporting must not change the scheduler result.
		}
	};

	const skipQueued = () => {
		if (nextIndex >= items.length) return;

		for (let index = nextIndex; index < items.length; index += 1) {
			results[index] = { status: "skipped" };
			state.skipped += 1;
			state.queued -= 1;
		}
		nextIndex = items.length;
		emitState();
	};

	const abortListener = () => skipQueued();
	options.signal?.addEventListener("abort", abortListener, { once: true });

	try {
		emitState();
		if (options.signal?.aborted) skipQueued();

		const runWorker = async () => {
			while (true) {
				if (options.signal?.aborted) skipQueued();
				if (nextIndex >= items.length) return;

				const index = nextIndex;
				nextIndex += 1;
				state.queued -= 1;
				state.running += 1;
				emitState();

				try {
					const value = await worker(items[index] as TInput, index, options.signal);
					results[index] = { status: "fulfilled", value };
					state.completed += 1;
				} catch (reason) {
					results[index] = { status: "rejected", reason };
					state.failed += 1;
				} finally {
					state.running -= 1;
					emitState();
				}
			}
		};

		const workerCount = Math.min(options.concurrency, items.length);
		await Promise.all(Array.from({ length: workerCount }, runWorker));
		return results;
	} finally {
		options.signal?.removeEventListener("abort", abortListener);
	}
}

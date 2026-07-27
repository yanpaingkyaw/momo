/** Shared IPC validation error (keeps spool ↔ validate import cycle free). */
export class IpcValidationError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "IpcValidationError";
	}
}

/** Events NDJSON would exceed the total file byte cap; existing log is left unchanged. */
export class EventsFileCapacityError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "EventsFileCapacityError";
	}
}

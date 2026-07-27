/** Shared IPC validation error (keeps spool ↔ validate import cycle free). */
export class IpcValidationError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "IpcValidationError";
	}
}

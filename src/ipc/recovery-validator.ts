import type { AgentRole } from "../roles.js";
import { IPC_PROTOCOL_CAPABILITY, type IpcModelPolicy } from "./spool.js";
import type { IpcCommand, IpcResult, IpcStarted } from "./spool.js";
import {
	assertIpcPoliciesEqualRecords,
	type ValidatedWorkerManifest,
} from "./validate.js";

export type RecoveryFence = "unhealthy" | "uncertain";

export type RecoveryValidationOutcome =
	| { ok: true }
	| { ok: false; reason: string; fence: RecoveryFence };

export interface AssignmentRecoveryInput {
	role: AgentRole;
	protocolVersion: 2 | 3;
	command?: IpcCommand;
	manifest?: Pick<ValidatedWorkerManifest, "version" | "boundPolicy">;
	registryBoundPolicy?: IpcModelPolicy;
	started?: IpcStarted;
	result?: IpcResult;
	hasLeaseEvidence?: boolean;
}

function policiesEqual(a: IpcModelPolicy, b: IpcModelPolicy): boolean {
	return (
		a.provider === b.provider && a.model === b.model && a.reasoning === b.reasoning
	);
}

function mismatch(reason: string, input: AssignmentRecoveryInput): RecoveryValidationOutcome {
	const implementerUncertain =
		input.role.canWrite &&
		(input.hasLeaseEvidence === true ||
			input.started !== undefined ||
			input.result?.status === "completed" ||
			input.result?.uncertainWrite === true);
	const fence: RecoveryFence = implementerUncertain ? "uncertain" : "unhealthy";
	return { ok: false, reason, fence };
}

function expectV3Policy(
	label: string,
	expected: IpcModelPolicy,
	actual: IpcModelPolicy | undefined,
	input: AssignmentRecoveryInput,
): RecoveryValidationOutcome | undefined {
	if (!actual) {
		return mismatch(`${label} missing v3 modelPolicy`, input);
	}
	if (!policiesEqual(expected, actual)) {
		return mismatch(`${label} modelPolicy mismatch`, input);
	}
	return undefined;
}

/**
 * Shared recovery validator for worker reconciliation, parent adoption terminal
 * advance, and live settlement paths.
 *
 * v3: manifest v3 + registry bound + durable prompt command + started/result chain.
 * completed requires started + modelPolicyApplied true.
 * early failed/aborted without started requires modelPolicyApplied false.
 */
export function validateAssignmentRecoveryChain(
	input: AssignmentRecoveryInput,
): RecoveryValidationOutcome {
	const v3 = input.protocolVersion === 3;

	if (v3) {
		if (input.manifest?.version !== 3 || !input.manifest.boundPolicy) {
			return mismatch("v3 recovery requires manifest v3 with concrete boundPolicy", input);
		}
		const manifestPolicy = input.manifest.boundPolicy;
		if (!input.registryBoundPolicy) {
			return mismatch("v3 recovery requires registry boundPolicy", input);
		}
		try {
			assertIpcPoliciesEqualRecords(
				manifestPolicy,
				input.registryBoundPolicy,
				"manifest→registry",
			);
		} catch {
			return mismatch("manifest→registry boundPolicy mismatch", input);
		}
		if (!input.command || input.command.type !== "prompt") {
			return mismatch("v3 recovery requires durable prompt command", input);
		}
		if (input.command.capability !== IPC_PROTOCOL_CAPABILITY || !input.command.modelPolicy) {
			return mismatch("v3 command missing capability/modelPolicy", input);
		}
		const cmdMismatch = expectV3Policy(
			"command",
			manifestPolicy,
			input.command.modelPolicy,
			input,
		);
		if (cmdMismatch) return cmdMismatch;
		if (input.started) {
			if (input.started.capability !== IPC_PROTOCOL_CAPABILITY || !input.started.modelPolicy) {
				return mismatch("v3 started missing capability/modelPolicy", input);
			}
			const startedMismatch = expectV3Policy(
				"started",
				manifestPolicy,
				input.started.modelPolicy,
				input,
			);
			if (startedMismatch) return startedMismatch;
		}
		if (input.result) {
			if (input.result.capability !== IPC_PROTOCOL_CAPABILITY || !input.result.modelPolicy) {
				return mismatch("v3 result missing capability/modelPolicy", input);
			}
			const resultMismatch = expectV3Policy(
				"result",
				manifestPolicy,
				input.result.modelPolicy,
				input,
			);
			if (resultMismatch) return resultMismatch;
		}
	} else {
		if (input.manifest?.boundPolicy) {
			return mismatch("v2 recovery must not include manifest boundPolicy", input);
		}
		if (input.registryBoundPolicy) {
			return mismatch("v2 recovery must not include registry boundPolicy", input);
		}
		if (input.command?.type === "prompt") {
			if ("capability" in input.command || "modelPolicy" in input.command) {
				return mismatch("v2 command must not include v3 policy fields", input);
			}
		}
		if (input.started && ("capability" in input.started || "modelPolicy" in input.started)) {
			return mismatch("v2 started must not include v3 policy fields", input);
		}
		if (input.result && ("capability" in input.result || "modelPolicy" in input.result)) {
			return mismatch("v2 result must not include v3 policy fields", input);
		}
	}

	if (input.result) {
		const terminal = input.result.status;
		if (v3) {
			if (terminal === "completed") {
				if (!input.started) {
					return mismatch("completed v3 result requires matching started.json", input);
				}
				if (input.result.modelPolicyApplied !== true) {
					return mismatch("completed v3 result requires modelPolicyApplied=true", input);
				}
			} else if (
				(terminal === "failed" || terminal === "aborted") &&
				!input.started &&
				input.result.modelPolicyApplied !== false
			) {
				return mismatch(
					"early failed/aborted v3 without started requires modelPolicyApplied=false",
					input,
				);
			}
		} else if ("modelPolicyApplied" in input.result) {
			return mismatch("v2 result must not include modelPolicyApplied", input);
		}
	}

	return { ok: true };
}

/** Infer protocol version from durable manifest when present; else registry boundPolicy. */
export function inferRecoveryProtocolVersion(options: {
	manifestVersion?: 2 | 3;
	registryBoundPolicy?: IpcModelPolicy;
	command?: IpcCommand;
	result?: IpcResult;
}): 2 | 3 {
	if (options.manifestVersion === 3) return 3;
	if (options.manifestVersion === 2) return 2;
	if (options.registryBoundPolicy) return 3;
	if (
		options.command?.type === "prompt" &&
		options.command.capability === IPC_PROTOCOL_CAPABILITY &&
		options.command.modelPolicy
	) {
		return 3;
	}
	if (
		options.result &&
		options.result.capability === IPC_PROTOCOL_CAPABILITY &&
		options.result.modelPolicy
	) {
		return 3;
	}
	return 2;
}

import { describe, expect, it } from "vitest";
import { IPC_PROTOCOL_CAPABILITY } from "../src/config/model-policy.js";
import { getRole } from "../src/roles.js";
import { validateAssignmentRecoveryChain } from "../src/ipc/recovery-validator.js";
import type { IpcCommand, IpcResult, IpcStarted } from "../src/ipc/spool.js";

const policy = { provider: "openai", model: "gpt-4o", reasoning: "off" as const };

describe("validateAssignmentRecoveryChain", () => {
	it("accepts consistent v3 chain for completed result", () => {
		const command: IpcCommand = {
			version: 1,
			type: "prompt",
			task: "t",
			issuedAt: new Date().toISOString(),
			runId: "a1",
			workerId: "w1",
			generation: 1,
			parentEpoch: "e1",
			capability: IPC_PROTOCOL_CAPABILITY,
			modelPolicy: policy,
		};
		const started: IpcStarted = {
			version: 1,
			runId: "a1",
			workerId: "w1",
			generation: 1,
			parentEpoch: "e1",
			startedAt: new Date().toISOString(),
			capability: IPC_PROTOCOL_CAPABILITY,
			modelPolicy: policy,
		};
		const result: IpcResult = {
			version: 1,
			runId: "a1",
			workerId: "w1",
			status: "completed",
			messages: [],
			finishedAt: new Date().toISOString(),
			capability: IPC_PROTOCOL_CAPABILITY,
			modelPolicy: policy,
			modelPolicyApplied: true,
		};
		expect(
			validateAssignmentRecoveryChain({
				role: getRole("scout"),
				protocolVersion: 3,
				command,
				manifest: { version: 3, boundPolicy: policy },
				registryBoundPolicy: policy,
				started,
				result,
			}).ok,
		).toBe(true);
	});

	it("flags completed v3 without started as unhealthy", () => {
		const result: IpcResult = {
			version: 1,
			runId: "a1",
			workerId: "w1",
			status: "completed",
			messages: [],
			finishedAt: new Date().toISOString(),
			capability: IPC_PROTOCOL_CAPABILITY,
			modelPolicy: policy,
			modelPolicyApplied: true,
		};
		const outcome = validateAssignmentRecoveryChain({
			role: getRole("scout"),
			protocolVersion: 3,
			manifest: { version: 3, boundPolicy: policy },
			registryBoundPolicy: policy,
			result,
		});
		expect(outcome.ok).toBe(false);
		if (!outcome.ok) expect(outcome.fence).toBe("unhealthy");
	});

	it("flags early failed v3 without started when modelPolicyApplied is not false", () => {
		const result: IpcResult = {
			version: 1,
			runId: "a1",
			workerId: "w1",
			status: "failed",
			messages: [],
			finishedAt: new Date().toISOString(),
			capability: IPC_PROTOCOL_CAPABILITY,
			modelPolicy: policy,
			modelPolicyApplied: true,
		};
		const outcome = validateAssignmentRecoveryChain({
			role: getRole("implementer"),
			protocolVersion: 3,
			manifest: { version: 3, boundPolicy: policy },
			registryBoundPolicy: policy,
			result,
			hasLeaseEvidence: true,
		});
		expect(outcome.ok).toBe(false);
		if (!outcome.ok) expect(outcome.fence).toBe("uncertain");
	});

	it("flags command→manifest policy mismatch", () => {
		const command: IpcCommand = {
			version: 1,
			type: "prompt",
			task: "t",
			issuedAt: new Date().toISOString(),
			runId: "a1",
			workerId: "w1",
			generation: 1,
			parentEpoch: "e1",
			capability: IPC_PROTOCOL_CAPABILITY,
			modelPolicy: { provider: "openai", model: "gpt-4o-mini", reasoning: "off" },
		};
		const outcome = validateAssignmentRecoveryChain({
			role: getRole("scout"),
			protocolVersion: 3,
			command,
			manifest: { version: 3, boundPolicy: policy },
			registryBoundPolicy: policy,
		});
		expect(outcome.ok).toBe(false);
	});

	it("flags v3 terminal result without durable command", () => {
		const result: IpcResult = {
			version: 1,
			runId: "a1",
			workerId: "w1",
			status: "completed",
			messages: [],
			finishedAt: new Date().toISOString(),
			capability: IPC_PROTOCOL_CAPABILITY,
			modelPolicy: policy,
			modelPolicyApplied: true,
		};
		const started: IpcStarted = {
			version: 1,
			runId: "a1",
			workerId: "w1",
			generation: 1,
			parentEpoch: "e1",
			startedAt: new Date().toISOString(),
			capability: IPC_PROTOCOL_CAPABILITY,
			modelPolicy: policy,
		};
		const scoutOutcome = validateAssignmentRecoveryChain({
			role: getRole("scout"),
			protocolVersion: 3,
			manifest: { version: 3, boundPolicy: policy },
			registryBoundPolicy: policy,
			started,
			result,
		});
		expect(scoutOutcome.ok).toBe(false);
		if (!scoutOutcome.ok) expect(scoutOutcome.fence).toBe("unhealthy");

		const implOutcome = validateAssignmentRecoveryChain({
			role: getRole("implementer"),
			protocolVersion: 3,
			manifest: { version: 3, boundPolicy: policy },
			registryBoundPolicy: policy,
			started,
			result,
			hasLeaseEvidence: true,
		});
		expect(implOutcome.ok).toBe(false);
		if (!implOutcome.ok) expect(implOutcome.fence).toBe("uncertain");
	});
});

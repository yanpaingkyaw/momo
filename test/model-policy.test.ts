import {
	chmodSync,
	closeSync,
	existsSync,
	lstatSync,
	mkdirSync,
	mkdtempSync,
	openSync,
	readFileSync,
	rmSync,
	symlinkSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { Model } from "@earendil-works/pi-ai";
import type { ModelRegistry } from "@earendil-works/pi-coding-agent";
import {
	applyModelPolicy,
	assertWorkerPolicyCompatible,
	effectivePolicyForScope,
	IPC_PROTOCOL_CAPABILITY,
	ModelPolicyApplyError,
	ModelPolicyValidationError,
	policySnapshotsEqual,
	verifySessionModelPolicy,
	WorkerPolicyMismatchError,
	WORKER_POLICY_MISMATCH_MESSAGE,
	validateMomoConfig,
	validateModelPolicySnapshot,
} from "../src/config/model-policy.js";
import {
	__removeConfigForTest,
	__writeRawConfigFileForTest,
	MomoConfigError,
	MomoConfigLockError,
	clearScopePolicy,
	momoConfigDir,
	mutateMomoConfig,
	persistMomoConfig,
	readMomoConfig,
	setScopePolicy,
	withMomoConfigLock,
	__captureLockDirIdentityForTest,
	__releaseConfigLockForTest,
	__rmdirOwnCreatedEmptyLockDirForTest,
	ensureMomoConfigDir,
} from "../src/config/momo-config.js";
import {
	validateCommand,
	validateModelPolicyField,
	assertIpcModelPolicyMatches,
	validateWorkerManifest,
} from "../src/ipc/validate.js";
import { IpcValidationError } from "../src/ipc/errors.js";
import { validateQueueEntry } from "../src/herdr/role-queue.js";

const tempDirs: string[] = [];

afterEach(() => {
	while (tempDirs.length > 0) {
		const dir = tempDirs.pop();
		if (dir) rmSync(dir, { recursive: true, force: true });
	}
});

function tempConfigDir(): string {
	const dir = mkdtempSync(path.join(tmpdir(), "momo-config-"));
	tempDirs.push(dir);
	return dir;
}

function mockModel(provider: string, id: string, reasoning = true): Model<any> {
	return {
		provider,
		id,
		name: id,
		reasoning,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 128_000,
		maxTokens: 8192,
	} as Model<any>;
}

function mockRegistry(models: Model<any>[], authed: Set<string>): ModelRegistry {
	return {
		getAvailable: () => models.filter((m) => authed.has(`${m.provider}/${m.id}`)),
		find: (provider, modelId) => models.find((m) => m.provider === provider && m.id === modelId),
		hasConfiguredAuth: (model) => authed.has(`${model.provider}/${model.id}`),
	} as ModelRegistry;
}

describe("model policy config", () => {
	it("absent config preserves legacy (undefined)", () => {
		const dir = tempConfigDir();
		expect(readMomoConfig({ configPath: path.join(dir, "config.json") })).toBeUndefined();
	});

	it("rejects unknown keys and unsupported providers", () => {
		expect(() =>
			validateMomoConfig({
				version: 1,
				policies: {},
				extra: true,
			}),
		).toThrow(ModelPolicyValidationError);
		expect(() =>
			validateModelPolicySnapshot({
				provider: "cursor",
				model: "gpt-4",
				reasoning: "high",
			}),
		).toThrow(/unsupported/);
	});

	it("writes atomically with owner-only permissions", () => {
		const dir = tempConfigDir();
		setScopePolicy(
			"default",
			{ provider: "anthropic", model: "claude-sonnet-4-5", reasoning: "high" },
			{ configDir: dir },
		);
		const filePath = path.join(dir, "config.json");
		expect(existsSync(filePath)).toBe(true);
		const stat = lstatSync(filePath);
		expect(stat.isFile()).toBe(true);
		expect(stat.isSymbolicLink()).toBe(false);
		expect((stat.mode & 0o077) === 0).toBe(true);
		const parsed = readMomoConfig({ configPath: filePath });
		expect(parsed?.policies.default?.model).toBe("claude-sonnet-4-5");
	});

	it("rejects symlink config file", () => {
		const dir = tempConfigDir();
		const target = path.join(dir, "real.json");
		writeFileSync(target, "{}", "utf8");
		const link = path.join(dir, "config.json");
		symlinkSync(target, link);
		expect(() => readMomoConfig({ configPath: link })).toThrow(MomoConfigError);
	});

	it("rejects world-readable config file", () => {
		const dir = tempConfigDir();
		__writeRawConfigFileForTest(
			dir,
			JSON.stringify({
				version: 1,
				policies: {
					default: { provider: "openai", model: "gpt-4o", reasoning: "off" },
				},
			}),
		);
		chmodSync(path.join(dir, "config.json"), 0o644);
		expect(() => readMomoConfig({ configPath: path.join(dir, "config.json") })).toThrow(
			MomoConfigError,
		);
	});

	it("fail-closed config lock on concurrent acquire", () => {
		const dir = tempConfigDir();
		const lockDir = path.join(dir, ".config.lock");
		const fd = openSync(lockDir, "wx");
		closeSync(fd);
		expect(() => withMomoConfigLock(() => undefined, { configDir: dir })).toThrow(
			MomoConfigLockError,
		);
		__removeConfigForTest(dir);
	});

	it("resolves precedence role override > default", () => {
		const config = validateMomoConfig({
			version: 1,
			policies: {
				default: { provider: "openai", model: "gpt-4o", reasoning: "off" },
				scout: { provider: "anthropic", model: "claude-sonnet-4-5", reasoning: "high" },
			},
		});
		expect(effectivePolicyForScope("scout", config)?.provider).toBe("anthropic");
		expect(effectivePolicyForScope("planner", config)?.model).toBe("gpt-4o");
		expect(effectivePolicyForScope("parent", config)?.provider).toBe("openai");
	});
});

describe("applyModelPolicy", () => {
	it("fails when model unavailable or auth missing", async () => {
		const registry = mockRegistry([mockModel("openai", "gpt-4o")], new Set());
		await expect(
			applyModelPolicy(
				{
					setModel: async () => true,
					setThinkingLevel: () => {},
					getThinkingLevel: () => "off",
					getModel: () => undefined,
				},
				registry,
				{ provider: "openai", model: "gpt-4o", reasoning: "off" },
			),
		).rejects.toBeInstanceOf(ModelPolicyApplyError);
	});

	it("fails when reasoning unsupported (no silent clamp)", async () => {
		const model = mockModel("anthropic", "claude-sonnet-4-5");
		const registry = mockRegistry([model], new Set(["anthropic/claude-sonnet-4-5"]));
		let current = model;
		await expect(
			applyModelPolicy(
				{
					setModel: async (m) => {
						current = m;
						return true;
					},
					setThinkingLevel: () => {},
					getThinkingLevel: () => "off",
					getModel: () => current,
				},
				registry,
				{ provider: "anthropic", model: "claude-sonnet-4-5", reasoning: "max" },
			),
		).rejects.toThrow(/Reasoning max unsupported/);
	});

	it("verifies exact provider/model/reasoning after set", async () => {
		const model = mockModel("openai-codex", "gpt-5.2-codex");
		const registry = mockRegistry([model], new Set(["openai-codex/gpt-5.2-codex"]));
		let current = model;
		let thinking: "high" | "off" = "off";
		await applyModelPolicy(
			{
				setModel: async (m) => {
					current = m;
					return true;
				},
				setThinkingLevel: (level) => {
					thinking = level as "high";
				},
				getThinkingLevel: () => thinking,
				getModel: () => current,
			},
			registry,
			{ provider: "openai-codex", model: "gpt-5.2-codex", reasoning: "high" },
		);
		expect(thinking).toBe("high");
	});
});

describe("IPC modelPolicy validation", () => {
	it("rejects policy without capability on prompt command", () => {
		expect(() =>
			validateCommand(
				{
					version: 1,
					type: "prompt",
					task: "hello",
					issuedAt: new Date().toISOString(),
					runId: "run-1",
					workerId: "worker-1",
					generation: 1,
					parentEpoch: "epoch-1",
					modelPolicy: {
						provider: "google",
						model: "gemini-2.5-pro",
						reasoning: "medium",
					},
				},
				{ runId: "run-1", workerId: "worker-1", generation: 1, parentEpoch: "epoch-1" },
			),
		).toThrow(IpcValidationError);
	});

	it("accepts v3 capability + concrete modelPolicy on prompt command", () => {
		const command = validateCommand(
			{
				version: 1,
				capability: IPC_PROTOCOL_CAPABILITY,
				type: "prompt",
				task: "hello",
				issuedAt: new Date().toISOString(),
				runId: "run-1",
				workerId: "worker-1",
				generation: 1,
				parentEpoch: "epoch-1",
				modelPolicy: {
					provider: "google",
					model: "gemini-2.5-pro",
					reasoning: "medium",
				},
			},
			{ runId: "run-1", workerId: "worker-1", generation: 1, parentEpoch: "epoch-1" },
		);
		expect(command.type).toBe("prompt");
		if (command.type === "prompt") {
			expect(command.modelPolicy?.provider).toBe("google");
		}
	});

	it("rejects secret-like fields via strict schema", () => {
		expect(() =>
			validateModelPolicyField(
				{
					provider: "openai",
					model: "gpt-4o",
					reasoning: "off",
					apiKey: "sk-secret",
				},
				"command.modelPolicy",
			),
		).toThrow(ModelPolicyValidationError);
	});

	it("validates v3 queue entry modelPolicy for FIFO stability", () => {
		const entry = validateQueueEntry(
			{
				version: 1,
				capability: IPC_PROTOCOL_CAPABILITY,
				assignmentId: "abc12345",
				workerId: "worker-scout",
				generation: 1,
				parentEpoch: "epoch-1",
				task: "task",
				enqueuedAt: new Date().toISOString(),
				seq: 1,
				modelPolicy: {
					provider: "openrouter",
					model: "anthropic/claude-sonnet-4",
					reasoning: "low",
				},
			},
			"queue",
		);
		expect(entry.modelPolicy?.reasoning).toBe("low");
	});
});

describe("momo config path", () => {
	it("uses XDG_CONFIG_HOME/momo/config.json", () => {
		expect(momoConfigDir({ XDG_CONFIG_HOME: "/tmp/xdg" } as NodeJS.ProcessEnv)).toBe(
			"/tmp/xdg/momo",
		);
	});

	it("allows standard 0755 parent ~/.config while Momo dir stays private", () => {
		const parent = tempConfigDir();
		const configDir = path.join(parent, "momo");
		chmodSync(parent, 0o755);
		mkdirSync(configDir, { recursive: true, mode: 0o700 });
		setScopePolicy(
			"default",
			{ provider: "openai", model: "gpt-4o", reasoning: "off" },
			{ configDir },
		);
		expect(readMomoConfig({ configPath: path.join(configDir, "config.json") })?.policies.default?.model).toBe(
			"gpt-4o",
		);
	});
});

describe("worker generation-bound policy", () => {
	it("same policy is compatible", () => {
		const policy = { provider: "openai", model: "gpt-4o", reasoning: "off" as const };
		expect(() => assertWorkerPolicyCompatible(policy, policy)).not.toThrow();
	});

	it("mismatch fails closed with cleanup guidance", () => {
		expect(() =>
			assertWorkerPolicyCompatible(
				{ provider: "openai", model: "gpt-4o", reasoning: "off" },
				{ provider: "anthropic", model: "claude-sonnet-4-5", reasoning: "high" },
			),
		).toThrow(WorkerPolicyMismatchError);
		expect(WORKER_POLICY_MISMATCH_MESSAGE).toMatch(/momo-cleanup/);
	});

	it("verify-only rejects session mismatch before prompt", () => {
		const model = mockModel("openai", "gpt-4o");
		expect(() =>
			verifySessionModelPolicy(
				{ model, thinkingLevel: "off" },
				{ provider: "anthropic", model: "claude-sonnet-4-5", reasoning: "high" },
			),
		).toThrow(ModelPolicyApplyError);
	});
});

describe("IPC capability v3", () => {
	it("requires modelPolicy field on v3 command", () => {
		expect(() =>
			validateCommand(
				{
					version: 1,
					capability: IPC_PROTOCOL_CAPABILITY,
					type: "prompt",
					task: "hello",
					issuedAt: new Date().toISOString(),
					runId: "run-1",
					workerId: "worker-1",
					generation: 1,
					parentEpoch: "epoch-1",
				},
				{ runId: "run-1", workerId: "worker-1", generation: 1, parentEpoch: "epoch-1" },
			),
		).toThrow(IpcValidationError);
	});

	it("rejects null modelPolicy on v3 command", () => {
		expect(() =>
			validateCommand(
				{
					version: 1,
					capability: IPC_PROTOCOL_CAPABILITY,
					type: "prompt",
					task: "hello",
					issuedAt: new Date().toISOString(),
					runId: "run-1",
					workerId: "worker-1",
					generation: 1,
					parentEpoch: "epoch-1",
					modelPolicy: null,
				},
				{ runId: "run-1", workerId: "worker-1", generation: 1, parentEpoch: "epoch-1" },
			),
		).toThrow(IpcValidationError);
	});

	it("rejects unknown top-level keys on v3 command", () => {
		expect(() =>
			validateCommand(
				{
					version: 1,
					capability: IPC_PROTOCOL_CAPABILITY,
					type: "prompt",
					task: "hello",
					issuedAt: new Date().toISOString(),
					runId: "run-1",
					workerId: "worker-1",
					generation: 1,
					parentEpoch: "epoch-1",
					modelPolicy: null,
					extra: true,
				},
				{ runId: "run-1", workerId: "worker-1", generation: 1, parentEpoch: "epoch-1" },
			),
		).toThrow(IpcValidationError);
	});

	it("result mismatch refusal", () => {
		expect(() =>
			assertIpcModelPolicyMatches(
				{ provider: "openai", model: "gpt-4o", reasoning: "off" },
				{ provider: "anthropic", model: "claude-sonnet-4-5", reasoning: "high" },
				"result",
			),
		).toThrow(IpcValidationError);
	});

	it("queue preserves concrete modelPolicy through validateQueueEntry", () => {
		const policy = { provider: "openai", model: "gpt-4o", reasoning: "off" as const };
		const entry = validateQueueEntry(
			{
				version: 1,
				capability: IPC_PROTOCOL_CAPABILITY,
				assignmentId: "abc12345",
				workerId: "worker-scout",
				generation: 1,
				parentEpoch: "epoch-1",
				task: "task",
				enqueuedAt: new Date().toISOString(),
				seq: 1,
				modelPolicy: policy,
			},
			"queue",
		);
		expect(entry.modelPolicy).toEqual(policy);
	});
});

describe("applyModelPolicy rollback", () => {
	it("rolls back model and reasoning on setModel failure", async () => {
		const prior = mockModel("openai", "gpt-4o");
		const next = mockModel("anthropic", "claude-sonnet-4-5");
		const registry = mockRegistry([prior, next], new Set(["openai/gpt-4o", "anthropic/claude-sonnet-4-5"]));
		let current: Model<any> | undefined = prior;
		let thinking: "off" | "high" = "off";
		await expect(
			applyModelPolicy(
				{
					setModel: async (m) => {
						current = m;
						return false;
					},
					setThinkingLevel: (level) => {
						thinking = level as "high";
					},
					getThinkingLevel: () => thinking,
					getModel: () => current,
				},
				registry,
				{ provider: "anthropic", model: "claude-sonnet-4-5", reasoning: "high" },
			),
		).rejects.toBeInstanceOf(ModelPolicyApplyError);
		expect(current?.provider).toBe("openai");
		expect(thinking).toBe("off");
	});
});

describe("second review release blockers", () => {
	it("requires default when any policy scope is configured", () => {
		expect(() =>
			validateMomoConfig({
				version: 1,
				policies: {
					scout: { provider: "openai", model: "gpt-4o", reasoning: "off" },
				},
			}),
		).toThrow(ModelPolicyValidationError);
	});

	it("disallows clearing default while role overrides remain", () => {
		const configDir = tempConfigDir();
		setScopePolicy(
			"default",
			{ provider: "openai", model: "gpt-4o", reasoning: "off" },
			{ configDir },
		);
		setScopePolicy(
			"scout",
			{ provider: "openai", model: "gpt-4o-mini", reasoning: "off" },
			{ configDir },
		);
		expect(() => clearScopePolicy("default", { configDir })).toThrow(MomoConfigError);
	});

	it("validateWorkerManifest rejects v3 without concrete boundPolicy", () => {
		expect(() =>
			validateWorkerManifest(
				{
					version: 3,
					poolKey: "pk",
					workerId: "w1",
					generation: 1,
					role: "scout",
					cwd: "/tmp",
					createdAt: new Date().toISOString(),
				},
				{
					poolKey: "pk",
					workerId: "w1",
					generation: 1,
					role: "scout",
				},
			),
		).toThrow(IpcValidationError);
	});

	it("validateWorkerManifest rejects v2 with boundPolicy field", () => {
		expect(() =>
			validateWorkerManifest(
				{
					version: 2,
					poolKey: "pk",
					workerId: "w1",
					generation: 1,
					role: "scout",
					cwd: "/tmp",
					createdAt: new Date().toISOString(),
					boundPolicy: { provider: "openai", model: "gpt-4o", reasoning: "off" },
				},
				{
					poolKey: "pk",
					workerId: "w1",
					generation: 1,
					role: "scout",
				},
			),
		).toThrow(IpcValidationError);
	});

	it("setScopePolicy refuses parent/role override before default", () => {
		const dir = tempConfigDir();
		expect(() =>
			setScopePolicy(
				"scout",
				{ provider: "openai", model: "gpt-4o", reasoning: "off" },
				{ configDir: dir },
			),
		).toThrow(/Configure config.policies.default first/);
		expect(readMomoConfig({ configPath: path.join(dir, "config.json") })).toBeUndefined();
	});

	it("delegation runner reads config once per run (chain shares snapshot)", async () => {
		const { createDelegationRunner } = await import("../src/delegation/runner.js");
		const { ROLE_LIST } = await import("../src/roles.js");
		let reads = 0;
		const config = {
			version: 1 as const,
			policies: {
				default: { provider: "openai", model: "gpt-4o", reasoning: "off" as const },
			},
		};
		const runner = createDelegationRunner({
			cwd: process.cwd(),
			roles: ROLE_LIST,
			readConfig: () => {
				reads += 1;
				return config;
			},
			createChildSession: async () => ({
				messages: [],
				subscribe: () => () => {},
				prompt: async () => {},
				abort: async () => {},
				dispose: () => {},
				agent: { waitForIdle: async () => {} },
			}),
		});
		await runner.run({
			mode: "chain",
			steps: [
				{ agent: "scout", task: "a" },
				{ agent: "scout", task: "b" },
			],
		});
		expect(reads).toBe(1);
	});

	it("terminal transition copies queue capability/modelPolicy into next command", async () => {
		const { completeCleanAssignmentLocked } = await import("../src/herdr/terminal-transition.js");
		const { PoolRegistry } = await import("../src/herdr/pool-registry.js");
		const { resolvePoolIdentity, stableWorkerId } = await import("../src/herdr/pool-identity.js");
		const { enqueueAssignment } = await import("../src/herdr/role-queue.js");
		const { assignmentSpoolPaths } = await import("../src/herdr/assignment-spool.js");
		const { tryReadIpcJson: readIpc } = await import("../src/ipc/validate.js");
		const { atomicWriteJson } = await import("../src/ipc/spool.js");
		const { withRoleLock } = await import("../src/herdr/role-queue.js");

		const cacheRoot = tempConfigDir();
		const cwd = tempConfigDir();
		const identity = resolvePoolIdentity({ cwd, canonicalRoot: cwd, workspaceId: "ws", socketPath: "s" });
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const policy = { provider: "openai", model: "gpt-4o", reasoning: "off" as const };
		const finishedId = "finished01";
		const nextId = "nextassign02";

		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "p1",
			agentName: "momo_scout",
			cwd,
			status: "busy",
			activeAssignmentId: finishedId,
			activeParentEpoch: "e1",
			boundPolicy: policy,
			updatedAt: new Date().toISOString(),
		});

		enqueueAssignment(pool.poolRoot, "scout", {
			assignmentId: nextId,
			workerId,
			generation: 1,
			parentEpoch: "e1",
			task: "queued task",
			capability: IPC_PROTOCOL_CAPABILITY,
			modelPolicy: policy,
		});

		const finishedPaths = assignmentSpoolPaths(pool.poolRoot, "scout", finishedId);
		mkdirSync(finishedPaths.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(finishedPaths.result, {
			version: 1,
			runId: finishedId,
			workerId,
			status: "completed",
			messages: [],
			finishedAt: new Date().toISOString(),
			capability: IPC_PROTOCOL_CAPABILITY,
			modelPolicy: policy,
			modelPolicyApplied: true,
		});

		withRoleLock(pool.poolRoot, "scout", () =>
			completeCleanAssignmentLocked({
				pool,
				role: "scout",
				workerId,
				generation: 1,
				finishedAssignmentId: finishedId,
			}),
		);

		const nextCommand = readIpc(assignmentSpoolPaths(pool.poolRoot, "scout", nextId).command) as {
			capability?: number;
			modelPolicy?: typeof policy;
		};
		expect(nextCommand.capability).toBe(IPC_PROTOCOL_CAPABILITY);
		expect(nextCommand.modelPolicy).toEqual(policy);
	});

	it("removes empty pre-owner lock dir on inode/dev fenced rollback", () => {
		const dir = tempConfigDir();
		ensureMomoConfigDir(dir);
		const lockDir = path.join(dir, ".config.lock");
		mkdirSync(lockDir, { mode: 0o700 });
		const identity = __captureLockDirIdentityForTest(lockDir);
		__rmdirOwnCreatedEmptyLockDirForTest(lockDir, identity);
		expect(existsSync(lockDir)).toBe(false);
	});

	it("mutateMomoConfig rejects optimistic previous mismatch under lock", () => {
		const dir = tempConfigDir();
		const initial = setScopePolicy(
			"default",
			{ provider: "openai", model: "gpt-4o", reasoning: "off" },
			{ configDir: dir },
		);
		setScopePolicy(
			"scout",
			{ provider: "openai", model: "gpt-4o-mini", reasoning: "off" },
			{ configDir: dir },
		);
		expect(() =>
			mutateMomoConfig(() => null, { configDir: dir, expectedPrevious: initial }),
		).toThrow(MomoConfigError);
		expect(readMomoConfig({ configPath: path.join(dir, "config.json") })?.policies.scout).toBeDefined();
	});

	it("terminal idle rebuild preserves boundPolicy for configured reuse", async () => {
		const { completeCleanAssignmentLocked } = await import("../src/herdr/terminal-transition.js");
		const { PoolRegistry } = await import("../src/herdr/pool-registry.js");
		const { resolvePoolIdentity, stableWorkerId } = await import("../src/herdr/pool-identity.js");
		const { assignmentSpoolPaths } = await import("../src/herdr/assignment-spool.js");
		const { atomicWriteJson } = await import("../src/ipc/spool.js");
		const { withRoleLock } = await import("../src/herdr/role-queue.js");

		const cacheRoot = tempConfigDir();
		const cwd = tempConfigDir();
		const identity = resolvePoolIdentity({ cwd, canonicalRoot: cwd, workspaceId: "ws", socketPath: "s" });
		const pool = new PoolRegistry(identity.poolKey, cacheRoot);
		const workerId = stableWorkerId(identity.poolKey, "scout");
		const policy = { provider: "openai", model: "gpt-4o", reasoning: "off" as const };
		const finishedId = "finished01";

		pool.upsert({
			workerId,
			generation: 1,
			generationTombstone: 1,
			role: "scout",
			paneId: "p1",
			agentName: "momo_scout",
			cwd,
			status: "busy",
			activeAssignmentId: finishedId,
			activeParentEpoch: "e1",
			boundPolicy: policy,
			updatedAt: new Date().toISOString(),
		});

		const finishedPaths = assignmentSpoolPaths(pool.poolRoot, "scout", finishedId);
		mkdirSync(finishedPaths.root, { recursive: true, mode: 0o700 });
		atomicWriteJson(finishedPaths.result, {
			version: 1,
			runId: finishedId,
			workerId,
			status: "completed",
			messages: [],
			finishedAt: new Date().toISOString(),
			capability: IPC_PROTOCOL_CAPABILITY,
			modelPolicy: policy,
			modelPolicyApplied: true,
		});

		withRoleLock(pool.poolRoot, "scout", () =>
			completeCleanAssignmentLocked({
				pool,
				role: "scout",
				workerId,
				generation: 1,
				finishedAssignmentId: finishedId,
			}),
		);

		const idle = pool.getByRole("scout");
		expect(idle?.status).toBe("idle");
		expect(idle?.boundPolicy).toEqual(policy);
	});

	it("configured worker refuses legacy dispatch when boundPolicy absent", () => {
		const policy = { provider: "openai", model: "gpt-4o", reasoning: "off" as const };
		expect(() => assertWorkerPolicyCompatible(undefined, policy)).toThrow(WorkerPolicyMismatchError);
		expect(() => assertWorkerPolicyCompatible(policy, undefined)).toThrow(WorkerPolicyMismatchError);
	});

	it("lock release validates quarantine dev/ino and owner token", () => {
		const dir = tempConfigDir();
		const acquired = withMomoConfigLock(() => "ok", { configDir: dir });
		expect(acquired).toBe("ok");
		const lockDir = path.join(dir, ".config.lock");
		expect(existsSync(lockDir)).toBe(false);
	});

	it("lock release preserves callback error when release also fails", () => {
		const dir = tempConfigDir();
		expect(() =>
			withMomoConfigLock(
				() => {
					const lockDir = path.join(dir, ".config.lock");
					const owner = JSON.parse(readFileSync(path.join(lockDir, "owner.json"), "utf8")) as {
						token: string;
					};
					mkdirSync(`${lockDir}.released.${owner.token}`, { mode: 0o700 });
					throw new MomoConfigError("mutation failed");
				},
				{ configDir: dir },
			),
		).toThrow(AggregateError);
	});

	it("persistMomoConfig validates every non-null write", () => {
		const dir = tempConfigDir();
		ensureMomoConfigDir(dir);
		expect(() =>
			persistMomoConfig(dir, {
				version: 1,
				policies: {},
			}),
		).toThrow(MomoConfigError);
		expect(() =>
			persistMomoConfig(dir, {
				version: 1,
				policies: {
					scout: { provider: "openai", model: "gpt-4o", reasoning: "off" },
				},
			}),
		).toThrow(MomoConfigError);
		expect(existsSync(path.join(dir, "config.json"))).toBe(false);
	});

	it("persistMomoConfig null deletes the file and restores legacy mode", () => {
		const dir = tempConfigDir();
		setScopePolicy(
			"default",
			{ provider: "openai", model: "gpt-4o", reasoning: "off" },
			{ configDir: dir },
		);
		expect(existsSync(path.join(dir, "config.json"))).toBe(true);
		expect(persistMomoConfig(dir, null)).toBeUndefined();
		expect(readMomoConfig({ configPath: path.join(dir, "config.json") })).toBeUndefined();
	});

	it("mutateMomoConfig rejects malformed policy snapshots before write", () => {
		const dir = tempConfigDir();
		expect(() =>
			mutateMomoConfig(
				(existing) => ({
					version: 1,
					policies: {
						default: { provider: "cursor", model: "gpt-4", reasoning: "off" },
						...(existing?.policies ?? {}),
					},
				}),
				{ configDir: dir },
			),
		).toThrow(MomoConfigError);
		expect(readMomoConfig({ configPath: path.join(dir, "config.json") })).toBeUndefined();
	});

	it("mutateMomoConfig rejects concurrent snapshot override", () => {
		const dir = tempConfigDir();
		const initial = setScopePolicy(
			"default",
			{ provider: "openai", model: "gpt-4o", reasoning: "off" },
			{ configDir: dir },
		);
		setScopePolicy(
			"scout",
			{ provider: "openai", model: "gpt-4o-mini", reasoning: "off" },
			{ configDir: dir },
		);
		expect(() =>
			mutateMomoConfig(
				(existing) =>
					existing
						? {
								...existing,
								policies: {
									...existing.policies,
									planner: { provider: "openai", model: "gpt-4o", reasoning: "off" },
								},
							}
						: null,
				{ configDir: dir, expectedPrevious: initial },
			),
		).toThrow(/concurrently/);
		expect(readMomoConfig({ configPath: path.join(dir, "config.json") })?.policies.planner).toBeUndefined();
	});
});

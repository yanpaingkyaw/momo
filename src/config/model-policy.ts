import type { ThinkingLevel } from "@earendil-works/pi-agent-core";
import type { Model } from "@earendil-works/pi-ai";
import { getSupportedThinkingLevels } from "@earendil-works/pi-ai";
import type { ModelRegistry } from "@earendil-works/pi-coding-agent";
import type { IpcModelPolicy } from "../ipc/spool.js";

/** Credential-free model policy snapshot (config + IPC). */
export interface ModelPolicySnapshot {
	provider: string;
	model: string;
	reasoning: ThinkingLevel;
}

export const MOMO_CONFIG_VERSION = 1 as const;
export const IPC_PROTOCOL_CAPABILITY = 3 as const;

export const MOMO_POLICY_SCOPES = [
	"default",
	"parent",
	"scout",
	"planner",
	"implementer",
	"reviewer",
] as const;

export type MomoPolicyScope = (typeof MOMO_POLICY_SCOPES)[number];

export const AGENT_POLICY_SCOPES = ["scout", "planner", "implementer", "reviewer"] as const;
export type AgentPolicyScope = (typeof AGENT_POLICY_SCOPES)[number];

/** Supported provider IDs (Cursor subscription excluded). */
export const SUPPORTED_PROVIDERS = [
	"openai-codex",
	"anthropic",
	"openai",
	"openrouter",
	"google",
	"opencode",
	"opencode-go",
] as const;

export type SupportedProvider = (typeof SUPPORTED_PROVIDERS)[number];

export const THINKING_LEVELS = [
	"off",
	"minimal",
	"low",
	"medium",
	"high",
	"xhigh",
	"max",
] as const;

const SUPPORTED_PROVIDER_SET = new Set<string>(SUPPORTED_PROVIDERS);
const THINKING_LEVEL_SET = new Set<string>(THINKING_LEVELS);
const POLICY_SCOPE_SET = new Set<string>(MOMO_POLICY_SCOPES);

const CONTROL_CHAR_RE = /[\u0000-\u001f\u007f]/;
const MAX_PROVIDER_LEN = 64;
const MAX_MODEL_ID_LEN = 256;

export class ModelPolicyValidationError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "ModelPolicyValidationError";
	}
}

export class ModelPolicyApplyError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "ModelPolicyApplyError";
	}
}

export class WorkerPolicyMismatchError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "WorkerPolicyMismatchError";
	}
}

export const WORKER_POLICY_MISMATCH_MESSAGE =
	"Worker generation-bound model policy mismatch; run /momo-cleanup to recreate workers before dispatching a different policy.";

export interface MomoConfigV1 {
	version: typeof MOMO_CONFIG_VERSION;
	policies: Partial<Record<MomoPolicyScope, ModelPolicySnapshot>>;
}

/** Immutable config snapshot frozen once per delegation run or assignment. */
export type MomoConfigSnapshot = Readonly<{
	version: MomoConfigV1["version"];
	policies: Readonly<Partial<Record<MomoPolicyScope, Readonly<ModelPolicySnapshot>>>>;
}>;

function deepFreezePolicy(policy: ModelPolicySnapshot): Readonly<ModelPolicySnapshot> {
	return Object.freeze({ ...policy });
}

export function freezeConfigSnapshot(config: MomoConfigV1): MomoConfigSnapshot {
	const policies: Partial<Record<MomoPolicyScope, Readonly<ModelPolicySnapshot>>> = {};
	for (const scope of MOMO_POLICY_SCOPES) {
		const policy = config.policies[scope];
		if (policy) policies[scope] = deepFreezePolicy(policy);
	}
	return Object.freeze({
		version: config.version,
		policies: Object.freeze(policies),
	});
}

function assertNoControlChars(value: string, label: string): void {
	if (value.includes("\0") || CONTROL_CHAR_RE.test(value)) {
		throw new ModelPolicyValidationError(`${label} must not contain control characters`);
	}
}

function assertOnlyKeys(record: Record<string, unknown>, allowed: readonly string[], label: string): void {
	const unexpected = Object.keys(record).filter((key) => !allowed.includes(key));
	if (unexpected.length > 0) {
		throw new ModelPolicyValidationError(
			`${label} contains unknown key${unexpected.length === 1 ? "" : "s"}: ${unexpected.join(", ")}`,
		);
	}
}

export function validateModelPolicySnapshot(value: unknown, label = "modelPolicy"): ModelPolicySnapshot {
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		throw new ModelPolicyValidationError(`${label} must be an object`);
	}
	const record = value as Record<string, unknown>;
	assertOnlyKeys(record, ["provider", "model", "reasoning"], label);

	const provider = record.provider;
	if (typeof provider !== "string" || provider.length === 0 || provider.length > MAX_PROVIDER_LEN) {
		throw new ModelPolicyValidationError(`${label}.provider invalid`);
	}
	assertNoControlChars(provider, `${label}.provider`);
	if (!SUPPORTED_PROVIDER_SET.has(provider)) {
		throw new ModelPolicyValidationError(`${label}.provider unsupported: ${provider}`);
	}

	const model = record.model;
	if (typeof model !== "string" || model.length === 0 || model.length > MAX_MODEL_ID_LEN) {
		throw new ModelPolicyValidationError(`${label}.model invalid`);
	}
	assertNoControlChars(model, `${label}.model`);

	const reasoning = record.reasoning;
	if (typeof reasoning !== "string" || !THINKING_LEVEL_SET.has(reasoning)) {
		throw new ModelPolicyValidationError(`${label}.reasoning invalid`);
	}

	return { provider, model, reasoning: reasoning as ThinkingLevel };
}

export function validateMomoConfig(value: unknown): MomoConfigV1 {
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		throw new ModelPolicyValidationError("config must be an object");
	}
	const record = value as Record<string, unknown>;
	assertOnlyKeys(record, ["version", "policies"], "config");

	if (record.version !== MOMO_CONFIG_VERSION) {
		throw new ModelPolicyValidationError(`config.version must be ${MOMO_CONFIG_VERSION}`);
	}

	const policiesRaw = record.policies;
	if (typeof policiesRaw !== "object" || policiesRaw === null || Array.isArray(policiesRaw)) {
		throw new ModelPolicyValidationError("config.policies must be an object");
	}
	const policiesRecord = policiesRaw as Record<string, unknown>;
	for (const key of Object.keys(policiesRecord)) {
		if (!POLICY_SCOPE_SET.has(key)) {
			throw new ModelPolicyValidationError(`config.policies unknown scope: ${key}`);
		}
	}

	const policies: Partial<Record<MomoPolicyScope, ModelPolicySnapshot>> = {};
	for (const scope of MOMO_POLICY_SCOPES) {
		if (!(scope in policiesRecord)) continue;
		policies[scope] = validateModelPolicySnapshot(policiesRecord[scope], `config.policies.${scope}`);
	}

	if (Object.keys(policies).length === 0) {
		throw new ModelPolicyValidationError("config.policies must not be empty");
	}
	if (!policies.default) {
		throw new ModelPolicyValidationError("config.policies.default is required");
	}

	return { version: MOMO_CONFIG_VERSION, policies };
}

export function isConfigPolicyFeatureActive(config: MomoConfigV1 | undefined): boolean {
	if (!config) return false;
	return Object.keys(config.policies).length > 0;
}

/** Role override > default; parent override > default. Requires default when config active. */
export function effectivePolicyForScope(
	scope: MomoPolicyScope,
	config: MomoConfigV1,
): ModelPolicySnapshot | undefined {
	if (scope === "default") return config.policies.default;
	const specific = config.policies[scope];
	if (specific) return specific;
	return config.policies.default;
}

export function resolveRolePolicySnapshot(
	role: string,
	config: MomoConfigV1 | undefined,
): ModelPolicySnapshot | undefined {
	if (!config || !isConfigPolicyFeatureActive(config)) return undefined;
	if ((MOMO_POLICY_SCOPES as readonly string[]).includes(role)) {
		return effectivePolicyForScope(role as MomoPolicyScope, config);
	}
	return undefined;
}

/** Concrete policy for an assignment when config is active; undefined in legacy mode. */
export function resolveConcreteAssignmentPolicy(
	role: string,
	config: MomoConfigV1 | undefined,
): ModelPolicySnapshot | undefined {
	if (!isConfigPolicyFeatureActive(config)) return undefined;
	const policy = resolveRolePolicySnapshot(role, config);
	if (!policy) {
		throw new ModelPolicyValidationError(
			`No concrete policy for role ${role}; config.policies.default is required`,
		);
	}
	return policy;
}

export function policySnapshotsEqual(
	a: ModelPolicySnapshot | undefined,
	b: ModelPolicySnapshot | undefined,
): boolean {
	if (a === undefined && b === undefined) return true;
	if (a === undefined || b === undefined) return false;
	return a.provider === b.provider && a.model === b.model && a.reasoning === b.reasoning;
}

export function ipcPoliciesEqual(
	a: IpcModelPolicy | undefined,
	b: IpcModelPolicy | undefined,
): boolean {
	if (a === undefined && b === undefined) return true;
	if (a === undefined || b === undefined) return false;
	return a.provider === b.provider && a.model === b.model && a.reasoning === b.reasoning;
}

export function toIpcModelPolicy(snapshot: ModelPolicySnapshot): IpcModelPolicy {
	return {
		provider: snapshot.provider,
		model: snapshot.model,
		reasoning: snapshot.reasoning,
	};
}

export function ipcV3PolicyFields(policy: ModelPolicySnapshot): {
	capability: typeof IPC_PROTOCOL_CAPABILITY;
	modelPolicy: IpcModelPolicy;
} {
	return {
		capability: IPC_PROTOCOL_CAPABILITY,
		modelPolicy: toIpcModelPolicy(policy),
	};
}

export function formatPolicyLine(scope: MomoPolicyScope, policy: ModelPolicySnapshot | undefined): string {
	if (!policy) return `${scope}: (unset)`;
	return `${scope}: ${policy.provider}/${policy.model} reasoning=${policy.reasoning}`;
}

export interface AvailableModelEntry {
	provider: SupportedProvider;
	model: Model<any>;
}

export function listAuthenticatedAvailableModels(registry: ModelRegistry): AvailableModelEntry[] {
	const out: AvailableModelEntry[] = [];
	for (const model of registry.getAvailable()) {
		if (!SUPPORTED_PROVIDER_SET.has(model.provider)) continue;
		if (!registry.hasConfiguredAuth(model)) continue;
		out.push({ provider: model.provider as SupportedProvider, model });
	}
	return out;
}

export function findAuthenticatedModel(
	registry: ModelRegistry,
	policy: ModelPolicySnapshot,
): Model<any> | undefined {
	if (!SUPPORTED_PROVIDER_SET.has(policy.provider)) return undefined;
	const model = registry.find(policy.provider, policy.model);
	if (!model) return undefined;
	if (!registry.hasConfiguredAuth(model)) return undefined;
	return model;
}

export function prevalidatePolicyAgainstRegistry(
	registry: ModelRegistry,
	policy: ModelPolicySnapshot,
): void {
	const model = findAuthenticatedModel(registry, policy);
	if (!model) {
		throw new ModelPolicyApplyError(
			`Model unavailable or auth missing: ${policy.provider}/${policy.model}`,
		);
	}
	const available = getSupportedThinkingLevels(model);
	if (!available.includes(policy.reasoning)) {
		throw new ModelPolicyApplyError(
			`Reasoning ${policy.reasoning} unsupported for ${policy.provider}/${policy.model}`,
		);
	}
}

export interface ModelPolicyApplyTarget {
	setModel(model: Model<any>): Promise<boolean>;
	setThinkingLevel(level: ThinkingLevel): void;
	getThinkingLevel(): ThinkingLevel;
	getModel(): Model<any> | undefined;
}

export interface CapturedSessionModel {
	model?: Model<any>;
	thinkingLevel: ThinkingLevel;
}

export function captureSessionModel(target: ModelPolicyApplyTarget): CapturedSessionModel {
	const model = target.getModel();
	const captured: CapturedSessionModel = {
		thinkingLevel: target.getThinkingLevel(),
	};
	if (model !== undefined) {
		captured.model = model;
	}
	return captured;
}

export async function restoreSessionModel(
	target: ModelPolicyApplyTarget,
	captured: CapturedSessionModel,
): Promise<void> {
	if (captured.model) {
		await target.setModel(captured.model);
	}
	target.setThinkingLevel(captured.thinkingLevel);
}

/**
 * Parent-only apply with prevalidation before mutation and rollback on failure.
 * Pi setters may mutate shared defaults — never call on persistent Herdr workers.
 */
export async function applyModelPolicy(
	target: ModelPolicyApplyTarget,
	registry: ModelRegistry,
	policy: ModelPolicySnapshot,
): Promise<void> {
	const prior = captureSessionModel(target);

	prevalidatePolicyAgainstRegistry(registry, policy);
	const model = findAuthenticatedModel(registry, policy);
	if (!model) {
		throw new ModelPolicyApplyError(
			`Model unavailable or auth missing: ${policy.provider}/${policy.model}`,
		);
	}

	try {
		const ok = await target.setModel(model);
		if (!ok) {
			throw new ModelPolicyApplyError(`setModel rejected: ${policy.provider}/${policy.model}`);
		}
		const current = target.getModel();
		if (
			!current ||
			current.provider !== policy.provider ||
			current.id !== policy.model
		) {
			throw new ModelPolicyApplyError(
				`Model verification failed after setModel: expected ${policy.provider}/${policy.model}`,
			);
		}

		target.setThinkingLevel(policy.reasoning);
		const effective = target.getThinkingLevel();
		if (effective !== policy.reasoning) {
			throw new ModelPolicyApplyError(
				`Reasoning verification failed: requested ${policy.reasoning}, effective ${effective}`,
			);
		}
	} catch (error) {
		try {
			await restoreSessionModel(target, prior);
		} catch {
			// Best-effort rollback; original error is authoritative.
		}
		throw error;
	}
}

export interface SessionModelView {
	model?: Model<any>;
	thinkingLevel?: ThinkingLevel;
}

/** Derive verified concrete policy from live session after fence checks. */
export function verifiedPolicyFromSession(session: SessionModelView): ModelPolicySnapshot {
	const current = session.model;
	if (!current) {
		throw new ModelPolicyApplyError("Session model missing during policy verification");
	}
	const thinking = session.thinkingLevel ?? "off";
	return {
		provider: current.provider,
		model: current.id,
		reasoning: thinking,
	};
}

/** Worker verify-only: generation-bound concrete policy must match session before prompt. */
export function verifySessionModelPolicy(
	session: SessionModelView,
	expected: ModelPolicySnapshot,
): void {
	const current = session.model;
	if (!current || current.provider !== expected.provider || current.id !== expected.model) {
		throw new ModelPolicyApplyError(
			`Session model mismatch: expected ${expected.provider}/${expected.model}, got ${
				current ? `${current.provider}/${current.id}` : "none"
			}`,
		);
	}
	const thinking = session.thinkingLevel ?? "off";
	if (thinking !== expected.reasoning) {
		throw new ModelPolicyApplyError(
			`Session reasoning mismatch: expected ${expected.reasoning}, effective ${thinking}`,
		);
	}
}

export function cliArgsForPolicy(policy: ModelPolicySnapshot): string[] {
	return [
		"--provider",
		policy.provider,
		"--model",
		policy.model,
		"--thinking",
		policy.reasoning,
	];
}

export function assertWorkerPolicyCompatible(
	bound: ModelPolicySnapshot | undefined,
	requested: ModelPolicySnapshot | undefined,
): void {
	if (bound === undefined && requested === undefined) return;
	if (!policySnapshotsEqual(bound, requested)) {
		throw new WorkerPolicyMismatchError(WORKER_POLICY_MISMATCH_MESSAGE);
	}
}

/** @deprecated use resolveConcreteAssignmentPolicy */
export function resolveAssignmentPolicyField(
	role: string,
	config: MomoConfigV1 | undefined,
): ModelPolicySnapshot | undefined {
	return resolveConcreteAssignmentPolicy(role, config);
}

import type { ExtensionAPI, ExtensionCommandContext } from "@earendil-works/pi-coding-agent";
import { DynamicBorder } from "@earendil-works/pi-coding-agent";
import type { ThinkingLevel } from "@earendil-works/pi-agent-core";
import { getSupportedThinkingLevels } from "@earendil-works/pi-ai";
import { type SelectItem, SelectList, Text, Container } from "@earendil-works/pi-tui";
import {
	applyModelPolicy,
	captureSessionModel,
	effectivePolicyForScope,
	formatPolicyLine,
	listAuthenticatedAvailableModels,
	MOMO_POLICY_SCOPES,
	ModelPolicyValidationError,
	restoreSessionModel,
	THINKING_LEVELS,
	policySnapshotsEqual,
	validateMomoConfig,
	type ModelPolicySnapshot,
	type MomoConfigV1,
	type MomoPolicyScope,
} from "./model-policy.js";
import {
	MomoConfigError,
	MomoConfigLockError,
	momoConfigDir,
	persistMomoConfig,
	readMomoConfig,
	withMomoConfigLockAsync,
	type ConfigMutationResult,
} from "./momo-config.js";
import path from "node:path";

const LOGIN_GUIDANCE =
	"Start Pi, then run `/login` (or provider-specific login) to configure API credentials. " +
	"Momo never reads auth.json or stores credentials in momo/config.json.";

const WORKER_SCOPES = new Set<string>(["scout", "planner", "implementer", "reviewer"]);

const BUSY_PARENT_GUIDANCE =
	"Cannot change effective parent model policy while the agent is running. Wait for idle or restart Momo.";

const CLEAR_LEGACY_NO_BASELINE_GUIDANCE =
	"Cannot clear effective parent policy during an active session without a pre-configuration session baseline. Restart Momo to reset manually.";

type SessionBaselineState = {
	preFeatureSessionBaseline: ReturnType<typeof captureSessionModel> | undefined;
	preFeatureBaselineAttempted: boolean;
};

function capturePreFeatureBaselineIfLegacy(
	pi: ExtensionAPI,
	ctx: ExtensionCommandContext,
	baseline: SessionBaselineState,
): void {
	if (baseline.preFeatureBaselineAttempted) return;
	baseline.preFeatureBaselineAttempted = true;
	if (readMomoConfig() !== undefined) return;
	baseline.preFeatureSessionBaseline = captureSessionModel(sessionTarget(pi, ctx));
}

function parseScopeAndMode(args: string): {
	scope?: MomoPolicyScope;
	clear: boolean;
	unknownArgs: string[];
} {
	const tokens = args.trim().split(/\s+/).filter(Boolean);
	const clear = tokens.includes("clear");
	const known = new Set<string>([...MOMO_POLICY_SCOPES, "clear"]);
	const unknownArgs = tokens.filter((token) => !known.has(token));
	const scopeToken = tokens.find(
		(token) => token !== "clear" && (MOMO_POLICY_SCOPES as readonly string[]).includes(token),
	);
	return {
		...(scopeToken ? { scope: scopeToken as MomoPolicyScope } : {}),
		clear,
		unknownArgs,
	};
}

async function selectFromList<T extends string>(
	ctx: ExtensionCommandContext,
	title: string,
	items: SelectItem[],
): Promise<T | null> {
	if (!ctx.hasUI) {
		ctx.ui.notify("Model selector requires interactive UI (TUI/RPC).", "warning");
		return null;
	}
	return ctx.ui.custom<T | null>((tui, theme, _kb, done) => {
		const container = new Container();
		container.addChild(new DynamicBorder((str) => theme.fg("accent", str)));
		container.addChild(new Text(theme.fg("accent", theme.bold(title))));
		const selectList = new SelectList(items, Math.min(items.length, 12), {
			selectedPrefix: (text) => theme.fg("accent", text),
			selectedText: (text) => theme.fg("accent", text),
			description: (text) => theme.fg("muted", text),
			scrollInfo: (text) => theme.fg("dim", text),
			noMatch: (text) => theme.fg("warning", text),
		});
		selectList.onSelect = (item) => done(item.value as T);
		selectList.onCancel = () => done(null);
		container.addChild(selectList);
		container.addChild(new Text(theme.fg("dim", "↑↓ navigate • enter select • esc cancel")));
		container.addChild(new DynamicBorder((str) => theme.fg("accent", str)));
		return {
			render(width: number) {
				return container.render(width);
			},
			invalidate() {
				container.invalidate();
			},
			handleInput(data: string) {
				selectList.handleInput(data);
				tui.requestRender();
			},
		};
	});
}

async function pickScope(
	ctx: ExtensionCommandContext,
	explicit?: MomoPolicyScope,
): Promise<MomoPolicyScope | "__clear__" | null> {
	if (explicit) return explicit;
	const items: SelectItem[] = MOMO_POLICY_SCOPES.map((scope) => ({
		value: scope,
		label: scope,
		description: scope === "default" ? "Fallback for unset role scopes" : `Override for ${scope}`,
	}));
	items.push({
		value: "__clear__",
		label: "(clear scope override)",
		description: "Remove scope override from config",
	});
	return selectFromList<MomoPolicyScope | "__clear__">(ctx, "Momo model scope", items);
}

/** Pure selector — never mutates parent session for worker scopes or cancel. */
async function pickPolicy(ctx: ExtensionCommandContext): Promise<ModelPolicySnapshot | null> {
	if (typeof ctx.modelRegistry.refresh === "function") {
		try {
			await ctx.modelRegistry.refresh();
		} catch {
			// Best-effort catalog refresh before selector.
		}
	}

	const available = listAuthenticatedAvailableModels(ctx.modelRegistry);
	if (available.length === 0) {
		ctx.ui.notify(`No authenticated models available. ${LOGIN_GUIDANCE}`, "warning");
		return null;
	}

	const providers = [...new Set(available.map((entry) => entry.provider))].sort();
	const providerItems: SelectItem[] = providers.map((provider) => ({
		value: provider,
		label: provider,
		description: `${available.filter((e) => e.provider === provider).length} model(s)`,
	}));
	const provider = await selectFromList<string>(ctx, "Select provider", providerItems);
	if (!provider) return null;

	const models = available.filter((entry) => entry.provider === provider);
	const modelItems: SelectItem[] = models.map((entry) => ({
		value: entry.model.id,
		label: entry.model.id,
		description: entry.model.name ?? entry.model.id,
	}));
	const modelId = await selectFromList<string>(ctx, `Select model (${provider})`, modelItems);
	if (!modelId) return null;

	const model = ctx.modelRegistry.find(provider, modelId);
	if (!model) {
		ctx.ui.notify(`Model no longer available: ${provider}/${modelId}`, "warning");
		return null;
	}

	const levels = getSupportedThinkingLevels(model);
	const reasoningItems: SelectItem[] = levels.map((level) => ({
		value: level,
		label: level,
		description: (THINKING_LEVELS as readonly string[]).includes(level) ? "supported" : level,
	}));
	const reasoning = await selectFromList<ThinkingLevel>(ctx, "Select reasoning level", reasoningItems);
	if (!reasoning) return null;

	if (!levels.includes(reasoning)) {
		ctx.ui.notify(`Reasoning ${reasoning} unsupported for ${provider}/${modelId}`, "error");
		return null;
	}

	return { provider, model: modelId, reasoning };
}

function sessionTarget(pi: ExtensionAPI, ctx: ExtensionCommandContext) {
	return {
		setModel: (model: Parameters<ExtensionAPI["setModel"]>[0]) => pi.setModel(model),
		setThinkingLevel: (level: ThinkingLevel) => pi.setThinkingLevel(level),
		getThinkingLevel: () => pi.getThinkingLevel(),
		getModel: () => ctx.model,
	};
}

function effectiveParentFromConfig(config: MomoConfigV1 | undefined): ModelPolicySnapshot | undefined {
	if (!config) return undefined;
	return effectivePolicyForScope("parent", config);
}
function effectiveParentAfterMutation(
	scope: MomoPolicyScope,
	policy: ModelPolicySnapshot | undefined,
	previous: MomoConfigV1 | undefined,
): ModelPolicySnapshot | undefined {
	if (!previous && policy === undefined) return undefined;
	if (!previous) return policy;
	const simulated: MomoConfigV1 = {
		version: previous.version,
		policies: { ...previous.policies },
	};
	if (policy === undefined) {
		delete simulated.policies[scope];
	} else {
		simulated.policies[scope] = policy;
	}
	if (Object.keys(simulated.policies).length === 0) return undefined;
	return effectivePolicyForScope("parent", simulated);
}

function buildNextForScope(
	existing: MomoConfigV1 | undefined,
	scope: MomoPolicyScope,
	policy: ModelPolicySnapshot | undefined,
): MomoConfigV1 | null {
	const base: MomoConfigV1 = existing ?? { version: 1, policies: {} };
	const next: MomoConfigV1 = { version: 1, policies: { ...base.policies } };
	if (policy === undefined) {
		if (scope === "default") {
			const overrides = Object.keys(next.policies).filter((key) => key !== "default");
			if (overrides.length > 0) {
				throw new MomoConfigError(
					`Cannot clear default while overrides remain (${overrides.join(", ")}); clear overrides first`,
				);
			}
		}
		delete next.policies[scope];
	} else {
		if (scope !== "default" && !base.policies.default) {
			throw new MomoConfigError("Configure config.policies.default first");
		}
		next.policies[scope] = policy;
	}
	if (Object.keys(next.policies).length === 0) return null;
	try {
		return validateMomoConfig(next);
	} catch (error) {
		if (error instanceof ModelPolicyValidationError) {
			throw new MomoConfigError(error.message);
		}
		throw error;
	}
}

async function applyEffectiveParentTarget(
	pi: ExtensionAPI,
	ctx: ExtensionCommandContext,
	baseline: SessionBaselineState,
	targetPolicy: ModelPolicySnapshot | undefined,
): Promise<void> {
	const target = sessionTarget(pi, ctx);
	if (targetPolicy) {
		await applyModelPolicy(target, ctx.modelRegistry, targetPolicy);
		return;
	}
	if (baseline.preFeatureSessionBaseline?.model !== undefined) {
		await restoreSessionModel(target, baseline.preFeatureSessionBaseline);
		return;
	}
	throw new MomoConfigError(CLEAR_LEGACY_NO_BASELINE_GUIDANCE);
}

async function runConfigTransaction(
	pi: ExtensionAPI,
	ctx: ExtensionCommandContext,
	baseline: SessionBaselineState,
	options: {
		scope: MomoPolicyScope;
		policy: ModelPolicySnapshot | undefined;
	},
): Promise<ConfigMutationResult> {
	capturePreFeatureBaselineIfLegacy(pi, ctx, baseline);
	const target = sessionTarget(pi, ctx);
	const configDir = momoConfigDir();
	const configPath = path.join(configDir, "config.json");
	let rollbackCaptured: ReturnType<typeof captureSessionModel> | undefined;

	return withMomoConfigLockAsync(async () => {
		const previous = readMomoConfig({ configPath });
		const rawNext = buildNextForScope(previous, options.scope, options.policy);
		const oldEffective = effectiveParentFromConfig(previous);
		const newEffective = effectiveParentAfterMutation(
			options.scope,
			options.policy,
			previous,
		);
		const parentChanged = !policySnapshotsEqual(oldEffective, newEffective);

		if (parentChanged && !ctx.isIdle()) {
			throw new MomoConfigError(BUSY_PARENT_GUIDANCE);
		}

		if (
			parentChanged &&
			newEffective === undefined &&
			baseline.preFeatureSessionBaseline?.model === undefined
		) {
			throw new MomoConfigError(CLEAR_LEGACY_NO_BASELINE_GUIDANCE);
		}

		if (parentChanged) {
			rollbackCaptured = captureSessionModel(target);
			await applyEffectiveParentTarget(pi, ctx, baseline, newEffective);
		}

		try {
			const next = persistMomoConfig(configDir, rawNext);
			return { previous, next };
		} catch (persistError) {
			if (parentChanged && rollbackCaptured) {
				try {
					await restoreSessionModel(target, rollbackCaptured);
				} catch {
					// Persist error remains authoritative.
				}
			}
			throw persistError;
		}
	});
}

async function handleMomoModel(
	args: string,
	ctx: ExtensionCommandContext,
	pi: ExtensionAPI,
	baseline: SessionBaselineState,
): Promise<void> {
	const { scope: scopeArg, clear, unknownArgs } = parseScopeAndMode(args);
	if (unknownArgs.length > 0) {
		ctx.ui.notify(`Unknown arguments: ${unknownArgs.join(" ")}`, "error");
		return;
	}

	let scope = await pickScope(ctx, scopeArg);
	if (!scope) return;

	if (scope === "__clear__") {
		const target = await pickScope(ctx);
		if (!target || target === "__clear__") return;
		try {
			await runConfigTransaction(pi, ctx, baseline, {
				scope: target,
				policy: undefined,
			});
			ctx.ui.notify(`Cleared ${target} model policy override`, "info");
			if (WORKER_SCOPES.has(target)) {
				ctx.ui.notify(
					"Worker scope cleared. Existing panes keep their launch-bound policy until /momo-cleanup.",
					"info",
				);
			}
		} catch (error) {
			const message =
				error instanceof MomoConfigLockError || error instanceof MomoConfigError
					? error.message
					: error instanceof Error
						? error.message
						: String(error);
			ctx.ui.notify(`Failed to clear policy: ${message}`, "error");
		}
		return;
	}

	if (clear) {
		try {
			await runConfigTransaction(pi, ctx, baseline, {
				scope,
				policy: undefined,
			});
			ctx.ui.notify(`Cleared ${scope} model policy override`, "info");
			if (WORKER_SCOPES.has(scope)) {
				ctx.ui.notify(
					"Worker scope cleared. Existing panes keep their launch-bound policy until /momo-cleanup.",
					"info",
				);
			}
		} catch (error) {
			const message =
				error instanceof MomoConfigLockError || error instanceof MomoConfigError
					? error.message
					: error instanceof Error
						? error.message
						: String(error);
			ctx.ui.notify(`Failed to clear policy: ${message}`, "error");
		}
		return;
	}

	const policy = await pickPolicy(ctx);
	if (!policy) return;

	try {
		await runConfigTransaction(pi, ctx, baseline, {
			scope,
			policy,
		});
		ctx.ui.notify(
			`Saved ${scope} policy: ${policy.provider}/${policy.model} reasoning=${policy.reasoning}`,
			"info",
		);
		if (scope === "parent") {
			ctx.ui.notify(
				"Applied parent policy to this session. Worker policy changes require /momo-cleanup to recreate panes.",
				"info",
			);
		} else if (WORKER_SCOPES.has(scope)) {
			ctx.ui.notify(
				"Worker scope saved. Existing panes keep their launch-bound policy until /momo-cleanup.",
				"info",
			);
		}
	} catch (error) {
		const message =
			error instanceof MomoConfigLockError || error instanceof MomoConfigError
				? error.message
				: error instanceof Error
					? error.message
					: String(error);
		ctx.ui.notify(`Failed to save policy: ${message}`, "error");
	}
}

function handleMomoModels(_args: string, ctx: ExtensionCommandContext): void {
	const { unknownArgs } = parseScopeAndMode(_args);
	if (unknownArgs.length > 0) {
		ctx.ui.notify(`Unknown arguments: ${unknownArgs.join(" ")}`, "error");
		return;
	}

	let config;
	try {
		config = readMomoConfig();
	} catch (error) {
		const message =
			error instanceof MomoConfigError
				? error.message
				: error instanceof Error
					? error.message
					: String(error);
		ctx.ui.notify(`Config corrupt or unreadable: ${message}`, "error");
		return;
	}

	if (!config) {
		ctx.ui.notify(
			`No momo/config.json (legacy mode — Pi session defaults). ${LOGIN_GUIDANCE}`,
			"info",
		);
		return;
	}

	const lines = MOMO_POLICY_SCOPES.map((scope) =>
		formatPolicyLine(scope, effectivePolicyForScope(scope, config)),
	);
	lines.push("");
	lines.push(LOGIN_GUIDANCE);
	lines.push("Use /momo-model [scope] to set overrides; use 'clear' to remove.");
	lines.push("Cross-policy worker changes require /momo-cleanup to recreate panes.");
	ctx.ui.notify(lines.join("\n"), "info");
}

export function registerModelPolicyCommands(pi: ExtensionAPI): void {
	const baseline: SessionBaselineState = {
		preFeatureSessionBaseline: undefined,
		preFeatureBaselineAttempted: false,
	};

	pi.registerCommand("momo-model", {
		description: "Configure provider/model/reasoning for a Momo scope (authenticated models only)",
		handler: async (args, ctx) => handleMomoModel(args, ctx, pi, baseline),
	});

	pi.registerCommand("momo-models", {
		description: "Show effective Momo model policies (credential-free config)",
		handler: async (args, ctx) => handleMomoModels(args, ctx),
	});
}

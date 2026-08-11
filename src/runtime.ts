import path from "node:path";
import { mkdirSync } from "node:fs";
import {
  type AgentSessionRuntime,
  type CreateAgentSessionOptions,
  type CreateAgentSessionRuntimeFactory,
  InteractiveMode,
  SessionManager,
  createAgentSessionFromServices,
  createAgentSessionRuntime,
  createAgentSessionServices,
  getAgentDir,
} from "@earendil-works/pi-coding-agent";
import { getSupportedThinkingLevels } from "@earendil-works/pi-ai";
import {
  effectivePolicyForScope,
  findAuthenticatedModel,
} from "./config/model-policy.js";
import { readMomoConfigOrThrow } from "./config/momo-config.js";
import { createDelegateTool } from "./delegation/tool.js";
import { getModelPolicyExtensionPath } from "./paths.js";
import { MOMO_SYSTEM_PROMPT } from "./prompts.js";

export interface RunMomoOptions {
  cwd?: string;
  initialMessage?: string;
}

export async function createMomoRuntime(cwd: string): Promise<AgentSessionRuntime> {
  const resolvedCwd = path.resolve(cwd);
  const agentDir = getAgentDir();
  mkdirSync(path.join(agentDir, "sessions"), { recursive: true });

  const createRuntime: CreateAgentSessionRuntimeFactory = async ({
    cwd: sessionCwd,
    sessionManager,
    sessionStartEvent,
  }) => {
    const configSnapshot = readMomoConfigOrThrow();
    const parentPolicy = configSnapshot
      ? effectivePolicyForScope("parent", configSnapshot)
      : undefined;

    const services = await createAgentSessionServices({
      cwd: sessionCwd,
      agentDir,
      resourceLoaderOptions: {
        systemPromptOverride: () => MOMO_SYSTEM_PROMPT,
        additionalExtensionPaths: [getModelPolicyExtensionPath()],
      },
    });

    const modelRegistryAdapter = {
      getAvailable: () => services.modelRuntime.getAvailableSnapshot(),
      find: (provider: string, modelId: string) =>
        services.modelRuntime.getModel(provider, modelId),
      hasConfiguredAuth: (model: { provider: string }) =>
        services.modelRuntime.hasConfiguredAuth(model.provider),
    } as unknown as import("@earendil-works/pi-coding-agent").ModelRegistry;

    let sessionModel: CreateAgentSessionOptions["model"];
    let sessionThinking: CreateAgentSessionOptions["thinkingLevel"];
    if (parentPolicy) {
      const model = findAuthenticatedModel(modelRegistryAdapter, parentPolicy);
      if (!model) {
        throw new Error(
          `Parent model unavailable or auth missing: ${parentPolicy.provider}/${parentPolicy.model}`,
        );
      }
      const levels = getSupportedThinkingLevels(model);
      if (!levels.includes(parentPolicy.reasoning)) {
        throw new Error(
          `Parent reasoning ${parentPolicy.reasoning} unsupported for ${parentPolicy.provider}/${parentPolicy.model}`,
        );
      }
      sessionModel = model;
      sessionThinking = parentPolicy.reasoning;
    }

    const delegateTool = createDelegateTool({
      cwd: sessionCwd,
      modelRuntime: services.modelRuntime,
      settingsManager: services.settingsManager,
      agentsFiles: services.resourceLoader.getAgentsFiles().agentsFiles,
      readConfig: readMomoConfigOrThrow,
      getModelRegistry: () => modelRegistryAdapter,
      ...(sessionModel !== undefined ? { model: sessionModel } : {}),
      ...(sessionThinking !== undefined ? { thinkingLevel: sessionThinking } : {}),
    });
    const sessionResult = await createAgentSessionFromServices({
      services,
      sessionManager,
      ...(sessionStartEvent ? { sessionStartEvent } : {}),
      ...(sessionModel !== undefined ? { model: sessionModel } : {}),
      ...(sessionThinking !== undefined ? { thinkingLevel: sessionThinking } : {}),
      tools: ["read", "grep", "find", "ls", "delegate"],
      customTools: [delegateTool],
    });

    return {
      ...sessionResult,
      services,
      diagnostics: services.diagnostics,
    };
  };

  return createAgentSessionRuntime(createRuntime, {
    cwd: resolvedCwd,
    agentDir,
    sessionManager: SessionManager.create(resolvedCwd),
  });
}

export async function runMomo(options: RunMomoOptions = {}): Promise<void> {
  const runtime = await createMomoRuntime(options.cwd ?? process.cwd());

  try {
    const mode = new InteractiveMode(runtime, {
      ...(options.initialMessage ? { initialMessage: options.initialMessage } : {}),
      initialImages: [],
      initialMessages: [],
    });
    await mode.run();
  } finally {
    await runtime.dispose();
  }
}

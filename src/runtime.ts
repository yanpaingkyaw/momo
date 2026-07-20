import path from "node:path";
import { mkdirSync } from "node:fs";
import {
  type AgentSessionRuntime,
  type CreateAgentSessionRuntimeFactory,
  InteractiveMode,
  SessionManager,
  createAgentSessionFromServices,
  createAgentSessionRuntime,
  createAgentSessionServices,
  getAgentDir,
} from "@earendil-works/pi-coding-agent";
import { createDelegateTool } from "./delegation/tool.js";
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
    const services = await createAgentSessionServices({
      cwd: sessionCwd,
      agentDir,
      resourceLoaderOptions: {
        systemPromptOverride: () => MOMO_SYSTEM_PROMPT,
      },
    });
    const delegateTool = createDelegateTool({
      cwd: sessionCwd,
      modelRuntime: services.modelRuntime,
      settingsManager: services.settingsManager,
      agentsFiles: services.resourceLoader.getAgentsFiles().agentsFiles,
    });
    const sessionResult = await createAgentSessionFromServices({
      services,
      sessionManager,
      ...(sessionStartEvent ? { sessionStartEvent } : {}),
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

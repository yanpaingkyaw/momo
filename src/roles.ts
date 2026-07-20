import {
  IMPLEMENTER_SYSTEM_PROMPT,
  PLANNER_SYSTEM_PROMPT,
  REVIEWER_SYSTEM_PROMPT,
  SCOUT_SYSTEM_PROMPT,
} from "./prompts.js";

export type AgentName = "scout" | "planner" | "implementer" | "reviewer";

export interface AgentRole {
  readonly name: AgentName;
  readonly description: string;
  readonly systemPrompt: string;
  readonly tools: readonly string[];
  readonly canWrite: boolean;
}

const readOnlyTools = Object.freeze(["read", "grep", "find", "ls"] as const);

function defineRole(role: AgentRole): Readonly<AgentRole> {
  return Object.freeze({
    ...role,
    tools: Object.freeze([...role.tools]),
  });
}

export const AGENT_NAMES = Object.freeze([
  "scout",
  "planner",
  "implementer",
  "reviewer",
] as const satisfies readonly AgentName[]);

export const ROLES: Readonly<Record<AgentName, Readonly<AgentRole>>> =
  Object.freeze({
    scout: defineRole({
      name: "scout",
      description:
        "Locates relevant code, tests, ownership boundaries, and repository conventions.",
      systemPrompt: SCOUT_SYSTEM_PROMPT,
      tools: readOnlyTools,
      canWrite: false,
    }),
    planner: defineRole({
      name: "planner",
      description:
        "Turns a bounded goal and repository evidence into an implementable plan.",
      systemPrompt: PLANNER_SYSTEM_PROMPT,
      tools: readOnlyTools,
      canWrite: false,
    }),
    implementer: defineRole({
      name: "implementer",
      description: "Makes one coherent repository change and verifies it.",
      systemPrompt: IMPLEMENTER_SYSTEM_PROMPT,
      tools: ["read", "grep", "find", "ls", "bash", "edit", "write"],
      canWrite: true,
    }),
    reviewer: defineRole({
      name: "reviewer",
      description:
        "Independently reviews changes for defects, regressions, security risks, and test gaps.",
      systemPrompt: REVIEWER_SYSTEM_PROMPT,
      tools: ["read", "grep", "find", "ls", "workspace_diff"],
      canWrite: false,
    }),
  });

export const ROLE_LIST: readonly Readonly<AgentRole>[] = Object.freeze(
  AGENT_NAMES.map((name) => ROLES[name]),
);

export function isAgentName(value: string): value is AgentName {
  return Object.hasOwn(ROLES, value);
}

export function getRole(name: AgentName): Readonly<AgentRole> {
  return ROLES[name];
}

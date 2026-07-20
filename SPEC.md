# Momo Orchestrator Specification

Status: Implemented baseline; authenticated manual acceptance pending  
Version: 1.0.0  
Last updated: 2026-07-20

## 1. Purpose

Momo is an interactive software-engineering orchestrator built with the Pi SDK.
It runs in a terminal, understands the repository in which it was launched, and
delegates bounded work to isolated specialist agents.

Momo is responsible for deciding when delegation is useful, selecting the
appropriate specialist, coordinating dependencies between specialists, and
synthesizing their results for the user. Only one specialist may have mutation
capabilities at a time.

This document is the implementation contract for the first production-ready
version of Momo. Requirements using **must**, **must not**, **should**, and
**may** are normative.

## 2. Goals

Momo must:

1. Provide an executable named `momo`.
2. Reuse Pi's complete interactive terminal UI.
3. Use the user's existing Pi models, credentials, settings, skills, and
   repository context.
4. Preserve the main conversation between launches.
5. Delegate work to isolated scout, planner, implementer, and reviewer agents.
6. Support single, parallel, and sequential delegation.
7. Enforce a single-writer policy through tool permissions and scheduling.
8. Stream specialist progress into the parent Pi session.
9. Propagate failures and cancellation without leaking child processes or
   sessions.
10. Provide enough structured result data for Momo to synthesize an accurate
    final response.

## 3. Non-goals

Version 1 does not include:

- A web, desktop, or mobile interface.
- A network server or public API.
- Remote or distributed workers.
- Container, virtual-machine, or operating-system sandboxing.
- Git worktree creation or automatic branch management.
- Parallel agents with write or shell permissions.
- User-defined or repository-defined specialist roles.
- Per-role model selection.
- A workflow editor or persisted workflow definitions.
- Publishing the package to npm.
- Automatic commits, pushes, pull requests, or deployments.

## 4. Target User

The target user is a software developer working in a local repository who has
already configured Pi with at least one usable model. The user expects Momo to
inspect code, plan changes, implement changes, run verification, review the
result, and explain the outcome in the same terminal session.

## 5. Runtime Requirements

The implementation must use:

- Node.js 22.19.0 or newer.
- TypeScript in ECMAScript module mode.
- npm for dependency and script management.
- `@earendil-works/pi-coding-agent` compatible with version `0.80.10`.
- `typebox` for model-facing tool schemas.
- Vitest for automated tests.

The installed dependency versions must be recorded in `package-lock.json`.
Runtime code must not depend on TypeScript execution loaders; the published bin
entry must point to compiled JavaScript.

## 6. Terminology

- **Parent**: the persistent Momo `AgentSession` shown in the TUI.
- **Child**: an ephemeral `AgentSession` created for one delegated task.
- **Role**: a named child configuration defining purpose, prompt, and tools.
- **Writer**: a role with shell or file-mutation tools.
- **Read-only role**: a role without shell, edit, or write tools.
- **Delegation**: one invocation of Momo's custom `delegate` tool.
- **Task**: one role and instruction pair inside a delegation.
- **Chain**: tasks executed sequentially, with optional output passing.
- **Parallel group**: independent read-only tasks executed concurrently.

## 7. Command-line Interface

### 7.1 Command forms

Momo must support:

```text
momo
momo <initial task...>
momo --help
momo -h
momo --version
momo -v
```

`momo` without a task must open the interactive TUI with an empty editor.

All non-option arguments must be joined with a single space and supplied to
`InteractiveMode` as the initial message. Quoted shell arguments naturally
remain a single argument, but quoting must not be required.

Examples:

```bash
momo
momo fix the failing authentication tests
momo "review the cache implementation"
```

### 7.2 Working directory

The process working directory at startup is the repository root from Momo's
perspective. It must be resolved to an absolute path once and reused for the
parent runtime and all child sessions.

The model-facing delegation API must not accept a working-directory field.
Agents must not be able to redirect another child to a different repository.

### 7.3 Exit behavior

- `--help` and `--version` must write to standard output and exit with code `0`.
- An unknown option must write a concise error to standard error and exit with
  code `2`.
- Unrecoverable runtime construction or configuration failures must exit with
  code `1`.
- A missing configured model or provider credential is recoverable: Momo must
  open Pi's TUI so the user can use Pi's model and login flows. If an initial
  task cannot run before configuration, the TUI must show the prompt error and
  remain usable.
- Normal TUI exit must use code `0`.
- A user interrupt must be forwarded to active work before the process exits.

### 7.4 Branding

The executable, package description, help text, README, and system identity must
use the name **Momo**. Pi's existing TUI layout, editor, commands, controls, and
session interface remain unchanged. Momo must not claim that the underlying TUI
was independently implemented.

## 8. Package Structure

The implementation should use these module boundaries:

```text
src/
  cli.ts                 CLI parsing, startup, and process exit behavior
  runtime.ts             Parent Pi services, runtime, and InteractiveMode
  prompts.ts             Parent and role system prompts
  roles.ts               Built-in role registry and role types
  delegation/
    tool.ts              TypeBox schema and delegate tool definition
    runner.ts            Child session lifecycle and result collection
    scheduler.ts         Bounded parallel execution
    results.ts           Output extraction, usage, and truncation
    workspace-diff.ts    Fixed read-only reviewer diff tool
test/
  cli.test.ts
  roles.test.ts
  delegation.test.ts
  scheduler.test.ts
  results.test.ts
```

Modules may be combined when doing so reduces incidental complexity, but the
boundaries between CLI startup, parent runtime, role policy, scheduling, and
child execution must remain testable.

## 9. Parent Runtime

### 9.1 Construction

The parent must be constructed using these Pi SDK layers:

1. `createAgentSessionServices()` creates cwd-bound resources.
2. `createAgentSessionFromServices()` creates each active parent session.
3. `createAgentSessionRuntime()` owns session replacement operations.
4. `InteractiveMode` provides the terminal user interface.

The runtime factory must pass the same Momo custom tools and parent tool
allowlist whenever the active session is replaced. This ensures `/new`, resume,
fork, clone, and import flows retain Momo's behavior.

### 9.2 Persistence

The initial parent must use:

```ts
SessionManager.create(cwd)
```

Before calling `SessionManager.create(cwd)`, Momo must create the Pi session
parent directory `<agentDir>/sessions` recursively. Pi creates the project
session directory but currently expects this parent to exist.

Momo must use Pi's normal session storage location and format. Momo must not
invent a second conversation database.

Child sessions must use:

```ts
SessionManager.inMemory(cwd)
```

Child histories must not appear in the user's persistent Pi session list.

### 9.3 Configuration inheritance

The parent must use Pi's normal agent directory and resource discovery. It must
therefore inherit:

- Stored API keys and OAuth credentials.
- Environment-provided provider credentials.
- Custom model definitions.
- Global and project settings.
- `AGENTS.md` files.
- Skills and prompt templates.
- User-enabled extensions for the parent session.

Momo must not introduce mandatory Momo-specific API key variables.

### 9.4 Parent tool policy

The parent session must enable only:

```text
read, grep, find, ls, delegate
```

The parent must not receive `bash`, `edit`, or `write`. This makes delegation to
the implementer the only path for repository mutation initiated by Momo.

If parent extensions register additional tools, the runtime must apply an
explicit allowlist so those tools are not automatically exposed unless required
by Momo.

## 10. Parent System Behavior

The parent system prompt must establish the following policy:

1. The assistant's name is Momo.
2. Momo is responsible for the final answer and must not merely forward raw
   specialist output.
3. Momo may answer simple read-only questions directly.
4. Momo should delegate when a task requires broad discovery, a multi-file
   change, implementation, or independent review.
5. Momo should use scouts before planning when repository context is missing.
6. Parallel work is only appropriate for independent read-only investigations.
7. Any code mutation must be delegated to an implementer.
8. Only one implementer may run at a time.
9. The implementer must run relevant verification before declaring success.
10. Material changes should be reviewed by a reviewer after implementation.
11. Actionable reviewer findings should be sent to an implementer in a later,
    sequential delegation.
12. Momo must distinguish completed, failed, aborted, skipped, and unverified
    work in its response.
13. Momo must not expose hidden reasoning or ask children to expose it.
14. Momo must not claim that tests passed unless a child result reports that
    they ran successfully.

The prompt should encourage proportional delegation. A spelling fix does not
require a scout-plan-review pipeline, while a cross-module feature normally
does.

## 11. Built-in Roles

### 11.1 Shared role contract

```ts
export type AgentName =
  | "scout"
  | "planner"
  | "implementer"
  | "reviewer";

export interface AgentRole {
  name: AgentName;
  description: string;
  systemPrompt: string;
  tools: readonly string[];
  canWrite: boolean;
}
```

The registry must be immutable after startup. Role lookup must use the exact
lowercase names above.

### 11.2 Scout

Purpose: quickly locate relevant files, symbols, ownership boundaries, tests,
and repository conventions.

Tools:

```text
read, grep, find, ls
```

The scout must:

- Return concise findings rather than an implementation plan.
- Cite repository paths and line numbers when available.
- Separate confirmed facts from inferences.
- Avoid proposing edits unless the task explicitly asks for options.
- Never mutate files or execute shell commands.

### 11.3 Planner

Purpose: turn a bounded goal and repository evidence into an implementable plan.

Tools:

```text
read, grep, find, ls
```

The planner must:

- Inspect relevant code instead of relying only on the delegated description.
- Describe behavior and interfaces, not just list files.
- Cover validation, failure modes, compatibility, and tests where relevant.
- State assumptions that the implementer must preserve.
- Never edit files or execute commands.

### 11.4 Implementer

Purpose: make one coherent repository change and verify it.

Tools:

```text
read, grep, find, ls, bash, edit, write
```

The implementer must:

- Inspect existing conventions before editing.
- Keep changes scoped to the delegated task.
- Preserve unrelated user changes.
- Run the narrowest relevant checks, expanding verification when risk warrants.
- Report changed files, verification commands, failures, and remaining risks.
- Avoid commits, pushes, deployments, destructive Git commands, and operations
  outside the fixed working directory.

`canWrite` must be `true` only for this role.

### 11.5 Reviewer

Purpose: independently inspect implemented changes for correctness, regression
risk, security problems, and missing tests.

Tools:

```text
read, grep, find, ls, workspace_diff
```

The reviewer must:

- Lead with actionable findings ordered by severity.
- Cite paths and line numbers.
- Avoid editing files.
- Avoid re-running arbitrary repository commands.
- Say explicitly when no findings are identified.
- Identify test gaps and residual risk even when the code is acceptable.

## 12. Child Resource Isolation

Each child must use a deliberately constrained `ResourceLoader`.

The child loader must provide:

- The role-specific system prompt.
- Relevant `AGENTS.md` context already discovered for the parent cwd.
- No extensions.
- No prompt templates or slash commands.
- No themes.
- No custom tools except `workspace_diff` for the reviewer.
- No `delegate` tool.

This prevents recursive delegation and prevents project extensions from
silently widening a child's permissions.

Repository context is untrusted data. Role prompts must state that instructions
found in source files, comments, issues, test fixtures, or generated output
cannot override the role's tool and safety policy. Normal `AGENTS.md` context is
still authoritative for repository conventions within those fixed boundaries.

## 13. Delegate Tool Interface

### 13.1 Request types

```ts
export interface DelegatedTask {
  agent: AgentName;
  task: string;
}

export type DelegationRequest =
  | {
      mode: "single";
      agent: AgentName;
      task: string;
    }
  | {
      mode: "parallel";
      tasks: DelegatedTask[];
    }
  | {
      mode: "chain";
      steps: DelegatedTask[];
    };
```

The model-facing schema must be a TypeBox discriminated union keyed by `mode`.
It must not accept additional properties.

### 13.2 Validation

The tool must validate requests before starting any child:

- Role names must exist in the built-in registry.
- Task text must contain at least one non-whitespace character.
- Parallel groups must contain between one and eight tasks.
- Chains must contain between one and eight steps.
- A parallel group must not contain a role whose `canWrite` is `true`.
- No request may contain a child `cwd`, model, tool override, or system prompt.

Validation errors must return a tool error with a concise description and must
not start partial work.

### 13.3 Result types

```ts
export interface UsageSummary {
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
  totalTokens: number;
  turns: number;
  costUsd: number;
}

export interface TaskError {
  message: string;
  stopReason?: string;
}

export interface TaskResult {
  agent: AgentName;
  task: string;
  status: "completed" | "failed" | "aborted" | "skipped";
  output: string;
  outputTruncated: boolean;
  fullOutput?: string;
  usage: UsageSummary;
  error?: TaskError;
}

export interface DelegationResult {
  mode: DelegationRequest["mode"];
  status: "completed" | "partial" | "failed" | "aborted";
  results: TaskResult[];
  usage: UsageSummary;
}
```

The Pi tool result's text content must contain a readable summary for the parent
model. The complete structured `DelegationResult` must be stored in tool
details.

`fullOutput` must be present only when `outputTruncated` is true. It remains in
the in-memory tool details and must not be copied into model-visible text.

### 13.4 Status calculation

- `completed`: every requested task completed.
- `partial`: a parallel group contains both completed and failed or aborted
  tasks.
- `failed`: no task completed, validation failed, or a chain failed before its
  final step.
- `aborted`: the parent signal aborted the delegation before normal completion.

Skipped chain steps do not count as successful work.

## 14. Execution Modes

### 14.1 Single

Single mode creates exactly one child, waits for it, and returns one result.
Any role, including implementer, may run in single mode.

### 14.2 Parallel

Parallel mode is limited to read-only roles. It must:

- Accept no more than eight tasks.
- Run no more than four children concurrently.
- Start later tasks as capacity becomes available.
- Preserve request order in the returned `results` array.
- Allow already-running tasks to finish when an unrelated task fails.
- Report live counts for queued, running, completed, and failed tasks.

Concurrency must be implemented with an in-process bounded scheduler, not by
starting all promises and applying a display-only limit.

### 14.3 Chain

Chain mode must execute one step at a time. Before each step after the first,
every literal `{previous}` token in its task must be replaced with the complete
model-visible output of the preceding successful step.

If a task contains no `{previous}` token, it runs unchanged.

If a step fails or is aborted:

1. The chain must stop.
2. Remaining steps must not create sessions.
3. Remaining steps must receive `skipped` results.
4. The delegation must return `failed` or `aborted` as appropriate.

Implementer steps are allowed in a chain because execution is sequential.

## 15. Child Session Lifecycle

For each task, the runner must:

1. Resolve the role from the immutable registry.
2. Create a constrained child resource loader.
3. Create an in-memory child session using the shared `ModelRuntime` and parent
   settings.
4. Enable only the role's exact tool allowlist.
5. Subscribe to session events before prompting.
6. Forward safe progress events through the delegate tool update callback.
7. Call `session.prompt()` once with the delegated task.
8. Wait for the prompt and agent processing to finish.
9. Extract final text, usage, stop reason, and error information.
10. Unsubscribe and dispose the child in a `finally` block.

Children must never be reused across tasks. This guarantees isolated context
and avoids one specialist's instructions contaminating another specialist.

## 16. Progress Streaming

The delegate tool should emit updates for:

- Task queued.
- Child started.
- Child text progress, excluding hidden reasoning.
- Tool execution start and completion.
- Task completed, failed, or aborted.
- Aggregate parallel progress.

Updates must be throttled or coalesced when needed so token streaming does not
cause excessive TUI redraws. The final result must not depend on whether an
update callback was supplied.

Tool arguments may be summarized for display, but secrets and full environment
variables must never be rendered.

## 17. Output Extraction and Limits

The child runner must scan messages from newest to oldest and use the last
assistant text content as the task output. Empty text parts must be ignored.

A process that ends normally without assistant text must produce a failed
result with the message `Child completed without a final response`.

The text returned to the parent model for each task must be limited to 50 KiB,
measured with `Buffer.byteLength(text, "utf8")`. Truncation must not split a
UTF-8 code point. A suffix must state how many bytes were omitted.

The full untruncated output may remain in the in-memory tool details for the
duration of the parent call, but it must not be written to a separate file or
persistent child session.

## 18. Usage Accounting

Usage must be accumulated from completed assistant messages. Missing usage
fields count as zero.

For each task and delegation, record:

- Input tokens.
- Output tokens.
- Cache-read tokens.
- Cache-write tokens.
- Total tokens from the most recent assistant turn.
- Assistant turns.
- Total reported cost in US dollars.

Aggregate input, output, cache, turns, and cost fields are sums. Aggregate
`totalTokens` is the sum of each task's final context total and is labeled as an
aggregate metric rather than a single context-window size.

Usage reporting is informational and must not turn a successful task into a
failure when a provider omits usage data.

## 19. Workspace Diff Tool

The reviewer receives a fixed `workspace_diff` tool so it can inspect changes
without arbitrary shell access.

The tool accepts no model-provided arguments. It must run only these commands in
the fixed parent cwd:

```text
git status --short
git diff --no-ext-diff --no-color
git diff --cached --no-ext-diff --no-color
```

If the cwd is not a Git repository, it must return a non-fatal explanation and
the reviewer must continue using file reads. Command execution must use an
argument array with `shell: false`.

Diff output must use the same 50 KiB model-visible limit. Failure details must
not include environment variables.

## 20. Cancellation and Shutdown

The delegate tool's `AbortSignal` must control all active child sessions.

When aborted:

1. Stop scheduling queued parallel tasks.
2. Call `abort()` on every active child.
3. Await child shutdown with a bounded timeout.
4. Mark active tasks as aborted and unstarted tasks as skipped.
5. Dispose all created sessions.
6. Return an aborted tool result unless the parent process is terminating.

Signal listeners must be removed after completion. Repeated cancellation must
be idempotent.

The CLI must dispose the parent runtime in `finally` during normal exit and
handled startup failure.

## 21. Error Handling

Expected failures must become structured task or tool errors rather than
unhandled promise rejections.

The implementation must handle:

- No configured or authenticated model. This is recoverable through Pi's TUI
  login and model-selection flows and is not, by itself, a startup failure.
- Provider authentication failure.
- Rate limiting and provider errors.
- Unknown role names.
- Invalid delegation shapes.
- Child session creation failure.
- Child prompt failure.
- Child tool failure.
- Empty child response.
- Cancellation.
- Diff execution outside Git.
- Session disposal errors.

Primary errors must not be hidden by cleanup errors. Cleanup failures may be
reported as diagnostics while preserving the original task outcome.

Parallel failures must retain successful sibling results. Chain failures must
identify the failed step and skipped steps.

## 22. Security Boundaries

Momo provides agent-level capability separation, not an operating-system
sandbox.

The implementation must enforce these available boundaries:

- Only implementer sessions receive `bash`, `edit`, or `write`.
- Parallel mode rejects implementers.
- The parent cannot mutate files directly.
- Children cannot delegate recursively.
- Delegation cannot override cwd, model, tools, role prompt, or session storage.
- Reviewer commands are fixed and do not accept shell input.
- Child sessions do not load project extensions.
- Temporary secrets must not be logged or returned in progress output.

The documentation must clearly state that an implementer's shell commands run
with the same operating-system permissions as the Momo process.

## 23. Observability

The TUI must make delegation understandable without overwhelming the user.

Collapsed tool output should show:

- Mode and task count.
- Role name.
- Queued, running, completed, or failed state.
- Recent safe progress.
- Final usage summary.

Expanded tool output should additionally show:

- Full delegated task.
- Per-task final output.
- Tool calls with sanitized arguments.
- Error messages and stop reasons.
- Per-task usage.

Momo must not create a separate telemetry service in version 1.

## 24. Configuration

Version 1 intentionally has few Momo-specific settings.

Fixed defaults:

| Setting | Value |
|---|---:|
| Maximum tasks per parallel request | 8 |
| Maximum tasks per chain | 8 |
| Maximum parallel concurrency | 4 |
| Model-visible output per task | 50 KiB |
| Child abort wait timeout | 5 seconds |
| Parent session storage | Persistent Pi session |
| Child session storage | In memory |
| Child delegation | Disabled |

These values should be named constants and covered by tests. They are not CLI
flags in version 1.

## 25. Build and Development Scripts

`package.json` must provide:

```json
{
  "scripts": {
    "dev": "...",
    "build": "...",
    "typecheck": "...",
    "test": "...",
    "start": "..."
  },
  "bin": {
    "momo": "./dist/cli.js"
  }
}
```

Exact compiler commands may follow the installed Pi package conventions, but:

- `build` must produce runnable ESM JavaScript in `dist/`.
- `typecheck` must not emit files.
- `test` must run once and exit.
- The built CLI file must contain a Node shebang and be executable when packed.

`.gitignore` must exclude at least `node_modules/`, `dist/`, `coverage/`, local
environment files, logs, and editor-generated files. It must not exclude source,
tests, the lockfile, or `SPEC.md`.

## 26. Automated Test Requirements

### 26.1 Role tests

Tests must verify:

- All four roles exist exactly once.
- Only implementer has `canWrite: true`.
- Only implementer contains `bash`, `edit`, or `write`.
- Reviewer contains `workspace_diff`.
- No role contains `delegate`.

### 26.2 Validation tests

Tests must cover:

- Valid single request for each role.
- Empty and whitespace-only tasks.
- Unknown roles.
- Empty parallel and chain arrays.
- More than eight tasks.
- Implementer rejected in parallel mode.
- Unexpected fields such as cwd or model rejected.
- Validation completes before any child factory call.

### 26.3 Scheduler tests

Tests must prove:

- At most four tasks run simultaneously.
- Later tasks start as slots become available.
- Results preserve request order despite out-of-order completion.
- A failed task does not cancel healthy parallel siblings.
- Cancellation prevents queued tasks from starting.

### 26.4 Chain tests

Tests must cover:

- Sequential execution order.
- One and multiple `{previous}` replacements.
- A step without a placeholder.
- Implementer steps serialized with all other steps.
- Failure stops the chain.
- Remaining steps are marked skipped.
- Cancellation stops later steps.

### 26.5 Result tests

Tests must cover:

- Last assistant text extraction.
- Ignoring empty text parts.
- Failure on no final text.
- Usage aggregation with complete and missing fields.
- UTF-8 output exactly at, below, and above 50 KiB.
- Truncation does not split multi-byte characters.
- Completed, partial, failed, and aborted status calculation.

### 26.6 Lifecycle tests

Using an injected fake child-session factory, tests must prove:

- Subscription occurs before prompting.
- Unsubscription and disposal occur after success.
- Disposal occurs after creation, prompt, or extraction failure.
- Abort reaches every active session.
- Cleanup errors do not replace the primary error.
- Child sessions use in-memory session managers.
- Child sessions do not receive the delegate tool.

### 26.7 CLI tests

Compiled CLI smoke tests must verify:

- `momo --help` exits `0` and names Momo.
- `momo --version` exits `0` and matches package metadata.
- Unknown flags exit `2` with no stack trace.
- Positional arguments become one initial message.
- Startup failures exit `1` with a concise diagnostic.

Automated tests must not require network access or real provider credentials.

## 27. Manual Acceptance Scenarios

Before version 1 is considered complete, perform these tests with a configured
Pi account and a disposable Git fixture repository.

### 27.1 Interactive startup

1. Run `momo` in the fixture repository.
2. Confirm Pi's TUI opens.
3. Ask the assistant its name and role.
4. Confirm it responds as Momo.
5. Exit and resume the session.
6. Confirm conversation history persists.

### 27.2 Read-only parallel delegation

1. Ask Momo to inspect two independent subsystems.
2. Confirm it delegates parallel scout tasks.
3. Confirm no more than four child tasks run simultaneously.
4. Confirm progress for both tasks is visible.
5. Confirm no tracked file changes occur.

### 27.3 Implementation workflow

1. Ask Momo for a small, testable code change.
2. Confirm discovery or planning is proportional to the task.
3. Confirm exactly one implementer receives mutation tools.
4. Confirm the implementer changes the expected files.
5. Confirm the implementer runs relevant verification.
6. Confirm a reviewer can inspect the resulting diff without shell access.
7. Confirm Momo synthesizes changes, tests, review findings, and residual risk.

### 27.4 Failure handling

1. Delegate a task that causes a child failure.
2. Confirm the TUI remains usable.
3. Confirm Momo reports which task failed.
4. Confirm successful parallel sibling output is retained.
5. Confirm a failed chain does not run later steps.

### 27.5 Cancellation

1. Start a long-running delegation.
2. Press Ctrl+C while children are active.
3. Confirm active child sessions abort.
4. Confirm queued tasks never start.
5. Confirm no child process or session remains active.
6. Confirm Momo can accept another prompt or exit cleanly.

## 28. Definition of Done

Momo version 1 is complete only when:

1. Every normative behavior in this specification is implemented.
2. TypeScript type-checking succeeds with no errors.
3. All automated tests pass without network credentials.
4. The production build succeeds.
5. CLI smoke tests pass against compiled output.
6. All manual acceptance scenarios pass with a real configured model.
7. No read-only role can obtain shell or file-mutation tools through the
   delegate interface.
8. Cancellation and all failure paths dispose child sessions.
9. User-facing documentation explains setup, usage, role behavior, and the
   absence of operating-system sandboxing.

## 29. Deferred Enhancements

The following may be considered after version 1:

- User-defined roles loaded from trusted global configuration.
- Project roles with explicit trust confirmation.
- Per-role model and thinking-level selection.
- Git worktree isolation for parallel implementers.
- Configurable concurrency and output limits.
- A non-interactive print mode.
- JSON-RPC integration.
- Structured workflow presets.
- Cost budgets and task timeouts.
- Containerized implementers.
- Remote workers and distributed scheduling.

Deferred features must not weaken the version 1 permission boundaries when
introduced.

## 30. Verified Implementation Notes

The version 0.1.0 implementation established these SDK-specific details:

- The delegate schema uses `Type.Unsafe` to emit a JSON Schema string enum.
  This matches Pi's `StringEnum` wire shape without adding a direct dependency
  on the transitive `@earendil-works/pi-ai` package.
- A runner-level promise mutex serializes implementers across separate,
  concurrently requested delegations. Parallel request validation alone is not
  sufficient to enforce the single-writer invariant.
- When cancellation occurs before child startup, one affected task is marked
  `aborted` and later unstarted tasks are marked `skipped`. This preserves an
  aggregate `aborted` status while accurately describing work that never ran.
- Truncated task output is capped for model visibility while the complete text
  remains available as optional `fullOutput` in in-memory tool details.
- Parent startup creates the Pi sessions parent directory for a clean first-run
  environment. No Momo-specific session store is introduced.

As of 2026-07-20, type-checking, 42 automated tests, the production build,
compiled CLI help/version/error checks, and an SDK runtime construction smoke
test pass. Authenticated TUI execution and the manual scenarios in section 27
remain pending because this environment has no configured Pi credentials.

## 31. References

- Pi SDK documentation: <https://pi.dev/docs/latest/sdk>
- Pi subagent example:
  <https://github.com/earendil-works/pi/tree/main/packages/coding-agent/examples/extensions/subagent>
- Pi coding agent package:
  <https://www.npmjs.com/package/@earendil-works/pi-coding-agent>

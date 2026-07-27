# Momo Orchestrator Specification

Status: Baseline + Herdr pane-worker **implementation candidate** in package
0.2.0; authenticated §27 and live §32.11 Herdr acceptance still pending
Spec version: 1.1.0
Package implementation version: 0.2.0 (`package.json`)
Last updated: 2026-07-27

Implementation-status summary (does not weaken normative requirements below):

- Package `0.2.0` depends on `@earendil-works/pi-coding-agent@0.82.1`.
- Automated checks cover typecheck/tests/build/CLI smoke with mocked Herdr
  seams; they do **not** prove live Herdr compatibility.
- Authenticated manual acceptance in §27 remains incomplete.
- Architecture is recorded in `Architecture.md` (baseline + candidate §32 map).
- Known implementation gaps versus some presentation/redaction expectations
  are called out inline as **Implementation status** notes.
- Spec 1.1.0 §32 Herdr target is an **implementation candidate** (fail-closed
  preflight, execve Pi parent wrapper, persistent role-pane pool, IPC, writer
  lease with wait/serialize never-steal, retention/cleanup, result-aware
  relaunch reconciliation, in-process fallback/override). It is **not**
  shipped/live-compatible until §32.11 passes.
- Partial macOS operator evidence exists (parent prompt, scout E2E, four-role
  panes, implementer write, reviewer diff, active planner cancel, cleanup).
  Full cross-parent contention, crash recovery, and Linux remain pending.

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
   repository context for the parent session.
4. Preserve the main conversation between launches.
5. Delegate work to isolated scout, planner, implementer, and reviewer agents.
6. Support single, parallel, and sequential delegation.
7. Enforce a single-writer policy through tool permissions and scheduling.
8. Stream specialist progress into the parent Pi session.
9. Propagate failures and cancellation without leaking child sessions or
   residual active child work.
10. Provide enough structured result data for Momo to synthesize an accurate
    final response.

**Implementation status:** Specialist isolation is implemented with in-process
Pi child **sessions** (`SessionManager.inMemory`), not operating-system child
processes. Goal 9 still requires that cancelled or finished work does not leave
active child sessions behind.

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
- `@earendil-works/pi-coding-agent` / `pi` CLI version `0.82.1` (required).
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
  workspace-diff.test.ts
```

Modules may be combined when doing so reduces incidental complexity, but the
boundaries between CLI startup, parent runtime, role policy, scheduling, and
child execution must remain testable.

**Implementation status:** The tree above matches the 0.1.0 source layout,
including `test/workspace-diff.test.ts`.

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

These inheritance rules apply to the **parent** session only. Child sessions
must use the constrained loader in §12 and must not load parent skills,
extensions, prompt templates, slash commands, or themes. Children may receive
the `AGENTS.md` files already discovered for the parent cwd.

Momo must not introduce mandatory Momo-specific API key variables.

**Implementation status:** `src/runtime.ts` uses normal Pi services for the
parent. `src/delegation/runner.ts` `createChildResourceLoader` returns empty
extensions, skills, prompts, and themes while copying parent-discovered
`AGENTS.md` content.

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
- No skills.
- No prompt templates or slash commands.
- No themes.
- No custom tools except `workspace_diff` for the reviewer.
- No `delegate` tool.

This prevents recursive delegation and prevents project extensions or skills
from silently widening a child's permissions.

**Implementation status:** Satisfied by the explicit empty-resource child
loader in `src/delegation/runner.ts`. Children are sessions in the same Node
process as the parent; isolation is resource/tool/session isolation, not a
separate OS process.

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

Delegation aggregate status must be derived from per-task statuses after
execution (validation failures are tool errors and never produce a
`DelegationResult`):

1. If there are no task results, the delegation status is `failed`.
2. If every task status is `completed`, the delegation status is `completed`.
3. Else if the mode is `parallel` and at least one task `completed`, the
   delegation status is `partial` (remaining siblings may be failed, aborted,
   or skipped).
4. Else if any task status is `aborted`, the delegation status is `aborted`.
5. Else if any task is `skipped` and none failed, the delegation status is
   `aborted` (cancellation skipped queued work before failure).
6. Otherwise the delegation status is `failed`.

Skipped chain or parallel tasks do not count as successful work. A chain that
stops on a failed step therefore returns `failed` even when later steps are
`skipped`. A chain or parallel group stopped only by cancellation returns
`aborted`.

**Implementation status:** Implemented by `calculateDelegationStatus` in
`src/delegation/results.ts` and covered by `test/results.test.ts`.

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

Independent of parallel validation, implementer tasks must also be serialized
by a runner-level writer mutex so overlapping `delegate` calls cannot mutate
the repository concurrently.

**Implementation status:** `writerTail` in `src/delegation/runner.ts`
serializes `canWrite` tasks across concurrent `run()` invocations on the same
`DelegationRunner` instance. The mutex is process-local and per-runner; it does
not coordinate separate Momo processes.

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

For the in-process backend, child sessions must never be reused across tasks.
This guarantees isolated context and avoids one specialist's instructions
contaminating another specialist. The Herdr backend is the explicit exception:
it reuses the persistent role worker while resetting model-visible context per
assignment as specified in §32.3.

**Implementation status:** Outside Herdr, child workers are Pi `AgentSession`
instances created in the parent Node process and lifecycle dispose/abort applies
to those sessions. In Herdr mode, §32's persistent pane-worker lifecycle applies.

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

**Implementation status:**

- Text deltas are coalesced on an approximately 100ms cadence in
  `src/delegation/runner.ts`.
- Tool progress messages currently include the tool name only and do not
  forward tool arguments.
- Thinking/hidden-reasoning blocks are not used as final task output
  (`extractLastAssistantText`), but streamed `text_delta` progress is forwarded
  as plain text with **no dedicated secret-redaction filter**.
- The normative "secrets must never be rendered" requirement therefore remains
  only partially met: argument omission helps, but progress text is not
  scrubbed.

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

**Implementation status:** The bounded wait applies to each child's abort
promise and defaults to five seconds (`CHILD_ABORT_TIMEOUT_MS` in
`src/delegation/runner.ts`). There is **no** overall per-task or
per-delegation execution timeout in version 0.1.0; wall-clock task timeouts
remain deferred (§29). Cancellation therefore depends on cooperative Pi
session abort plus the five-second abort-promise wait, after which disposal
still runs.

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
- Child sessions do not load project extensions or skills.
- Temporary secrets must not be logged or returned in progress output.

The documentation must clearly state that an implementer's shell commands run
with the same operating-system permissions as the Momo process.

**Implementation status:** Extension/skill isolation and tool allowlists are
enforced in code. Progress secret redaction remains incomplete (§16). Writer
serialization is per-runner/process-local (§14.2).

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

**Implementation status:** Version 0.1.0 relies on **default Pi tool-result
rendering** plus delegate progress/`formatDelegationResult` text. Momo does
not implement a custom collapsed/expanded presentation layer. Whether Pi's
default rendering satisfies the collapsed/expanded checklist above is
**unverified**; authenticated UI acceptance remains pending (§27).

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
| Overall task / delegation timeout | None in v1 (deferred) |
| Parent session storage | Persistent Pi session |
| Child session storage | In memory |
| Child delegation | Disabled |

These values should be named constants and covered by tests. They are not CLI
flags in version 1.

**Implementation status:** Named constants exist for task caps, parallel
concurrency, 50 KiB output, and the five-second abort wait. There is still no
overall task timeout setting.

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

### 26.8 Workspace diff tests

Tests must cover the fixed reviewer Git tool:

- Exact command argument arrays.
- Non-Git cwd returns a non-fatal explanation.
- Output truncation uses the shared 50 KiB limit.

Automated tests must not require network access or real provider credentials.

**Implementation status (2026-07-26):** `npm test` passed 43/43 across the six
files listed in §8. Remaining coverage gaps relative to this section:

- Compiled CLI binary smoke checks (help/version/unknown-option) were verified
  outside Vitest on 2026-07-26; startup-failure exit `1` remains less
  thoroughly automated against a live Pi runtime failure.
- No automated test asserts active secret redaction of streamed progress text.
- No automated test asserts a custom collapsed/expanded TUI presentation.
- No authenticated end-to-end delegation test with a real model provider.

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
5. Confirm no child session remains active (specialists are in-process
   sessions, not separate OS processes).
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
10. Current-state architecture is documented in `Architecture.md`.
11. When claiming production-ready Herdr pane-worker support, §32 and its
    acceptance matrix are satisfied. Until §32.11 passes, §32 must be advertised
    only as an implementation candidate, not as production/live Herdr-ready behavior.

## 29. Deferred Enhancements

The following may be considered after version 1:

- User-defined roles loaded from trusted global configuration.
- Project roles with explicit trust confirmation.
- Per-role model and thinking-level selection.
- Git worktree isolation for parallel implementers as a **product runtime**
  feature (distinct from delivery-governance worktrees in §32.12).
- Configurable concurrency and output limits.
- A non-interactive print mode.
- JSON-RPC integration.
- Structured workflow presets.
- Cost budgets and task timeouts.
- Containerized implementers.
- Remote workers and distributed scheduling.
- Upstream Herdr native `momo` agent kind (until then §32 uses kind `pi` with
  display name Momo).

Deferred features must not weaken the version 1 permission boundaries when
introduced.

**Note:** §32 Herdr pane workers are an implementation candidate in package
`0.2.0` code; live §32.11 acceptance remains pending.

## 30. Verified Implementation Notes

The version 0.1.0 implementation established these SDK-specific details:

- Spec contract version is `1.1.0`; package metadata version is `0.2.0` with
  Pi SDK `0.82.1` and Herdr preflight targeting CLI `0.7.x` / protocol `17`.
  Do not treat spec and package versions as interchangeable.
- The delegate schema uses `Type.Unsafe` to emit a JSON Schema string enum.
  This matches Pi's `StringEnum` wire shape without adding a direct dependency
  on the transitive `@earendil-works/pi-ai` package.
- A runner-level promise mutex serializes implementers across separate,
  concurrently requested delegations on the same runner. The lock is
  process-local; parallel request validation alone is not sufficient.
- Children are ephemeral in-process Pi sessions with empty extensions/skills
  loaders. They are not OS child processes and do not inherit parent
  extensions.
- Aggregate delegation status follows `calculateDelegationStatus` (§13.4).
- When cancellation occurs before child startup, one affected task is marked
  `aborted` and later unstarted tasks are marked `skipped`. This preserves an
  aggregate `aborted` status while accurately describing work that never ran.
- Cancellation waits up to five seconds for each child's abort promise and does
  not impose an overall task timeout.
- Truncated task output is capped at 50 KiB for model visibility while the
  complete text remains available as optional `fullOutput` in in-memory tool
  details.
- Progress uses default Pi tool updates; custom collapsed/expanded presentation
  from §23 is unverified. Progress text is not actively redacted for secrets.
- Parent startup creates the Pi sessions parent directory for a clean first-run
  environment. No Momo-specific session store is introduced.
- Current Herdr observations (operator-run smoke executed and captured by Pi
  in the operator environment on 2026-07-26; authoritative for that run, but
  **not** covered by repository automated tests or a checked-in evidence
  artifact): Momo launched in a disposable Herdr pane (historical pane id
  `w1:p5`), loaded `herdr-agent-state.ts`, was reported as `agent: pi` with
  title `π - momo`, and `herdr agent prompt` failed with `agent_not_ready`
  because Momo was no longer considered the pane foreground process. This is
  a confirmed observed limitation from that smoke, not established Herdr
  compatibility and not a future design.

As of the 0.2.0 implementation pass, `npm run typecheck` passed, `npm test`
passed (including mocked Herdr/IPC/lease/launch coverage), `npm run build`
passed, and compiled CLI help/version/unknown-option smoke checks passed.
Authenticated TUI scenarios in section 27 and live Herdr acceptance in §32.11
remain incomplete and are not claimed here.

## 31. References

- Pi SDK documentation: <https://pi.dev/docs/latest/sdk>
- Pi subagent example:
  <https://github.com/earendil-works/pi/tree/main/packages/coding-agent/examples/extensions/subagent>
- Pi coding agent package:
  <https://www.npmjs.com/package/@earendil-works/pi-coding-agent>
- Herdr home: <https://herdr.dev>
- Herdr socket API docs: <https://herdr.dev/docs/socket-api/>
- Current-state and approved-target architecture: `Architecture.md`

## 32. Herdr Pane-Worker Target Contract (Approved; Implementation Candidate)

This section is normative for the user-approved Herdr target. Requirements use
**must**, **must not**, **should**, and **may** as elsewhere.

**Implementation status:** Implementation candidate in package `0.2.0` with
mocked unit and integration coverage. Live operator acceptance in §32.11 is
still incomplete; do not advertise production/live Herdr readiness until it
passes. When Herdr mode is not active (or `MOMO_BACKEND=inprocess`), Momo
continues to use the baseline in-process specialist backend.

Architecture companion: `Architecture.md` §13–§14 (ADR-009 through ADR-015).

### 32.1 Mode selection

Momo must select a specialist backend as follows:

1. If Herdr is not detected, Momo must use the in-process specialist backend.
2. If an explicit in-process override is set, Momo must use the in-process
   backend even when Herdr is detected.
3. If Herdr is detected and no in-process override is set, Momo must use the
   Herdr pane-worker backend for every accepted specialist task.
4. If Herdr is detected but the environment is incompatible or unusable for the
   pane-worker backend, Momo must **fail closed**: return a clear delegation
   error and must not silently fall back to in-process execution.

### 32.2 Parent process and identity

1. The `momo` executable must act as a thin wrapper that launches a canonical
   Pi foreground parent session suitable for Herdr agent detection.
2. The underlying Herdr agent kind must be `pi`.
3. The user-visible display name must be **Momo**, provided through a Momo
   parent Pi extension and Herdr metadata reporting.
4. An upstream native Herdr kind named `momo` is deferred and must not block
   this target.
5. Specialist pane creation must use no-focus allocation so the parent pane
   remains the user's controllable foreground agent whenever practical.

### 32.3 Persistent role-pane pool

For each accepted `scout`, `planner`, `implementer`, or `reviewer` task while
the Herdr pane-worker backend is active, Momo must:

1. Logically preallocate an assignment proxy (runner `prepareAllChildren` may
   still allocate proxies for parallel/chain). Prepared chain tails that never
   prompt must not enqueue or create panes.
2. Create or reuse exactly **one persistent Herdr pane per role** per pool key
   (canonical git root/cwd + Herdr workspace + socket/server identity; excludes
   parent pane id).
3. Create the physical pane **lazily** when the assignment prompt executes, not
   at prepare time.
4. When the role worker is busy, enqueue the assignment on a cross-parent FIFO
   queue with **no overflow panes**.
5. Launch/reuse a **full Pi TUI** worker constrained to the task role.
6. Reset model-visible context each assignment (Pi `context` event; latest
   assignment user message onward) without calling `ctx.newSession`.
7. Block interactive/RPC input on persistent workers even while idle.

Momo must not keep Herdr-mode specialists as invisible in-process-only sessions.
Momo must not create a new pane per task when a compatible same-role pool worker
already exists.

### 32.4 Concurrency and writer lease

While the Herdr pane-worker backend is active:

1. Momo must run at most four **active** read-only workers at a time.
2. Momo must run at most one implementer at a time across processes via a
   cross-process writer lease.
3. Queued tasks may own allocated panes while waiting, but must not become
   active in violation of (1) or (2).
4. Parallel delegation must continue to reject implementers at validation time.
5. The in-process runner mutex remains necessary for the in-process backend but
   is not sufficient alone for Herdr workers.

### 32.5 Role and tool restrictions

Herdr workers must enforce the same role tool allowlists as the baseline roles:

- scout/planner: `read`, `grep`, `find`, `ls`
- implementer: `read`, `grep`, `find`, `ls`, `bash`, `edit`, `write`
- reviewer: `read`, `grep`, `find`, `ls`, `workspace_diff`

Workers must not receive `delegate`. Workers must not load capability-widening
skills or extensions. A minimal Herdr/Momo reporter hook may load only if it
cannot add tools or widen permissions.

### 32.6 Authoritative IPC

1. Parent and worker must communicate through a private, versioned IPC schema
   for task input, heartbeats, progress, and final results.
2. IPC must be the authoritative channel for orchestration and `TaskResult`
   ingestion.
3. Momo must not scrape pane TTY output or `herdr pane read` to determine
   completion or to extract final results.
4. Pane TTY output is for humans and debugging only.

### 32.7 Herdr CLI invocation

1. Momo must invoke the `herdr` CLI with an argv array and must not use a
   shell for those invocations.
2. Momo must validate Herdr JSON responses before acting on identifiers, pane
   state, or errors.
3. Initial supported platforms are macOS and Linux.
4. Acceptance must record the aligned tested Pi SDK version and Herdr CLI /
   protocol version used.

### 32.8 Cancellation, heartbeat, crash, and uncertain write

1. Parent cancellation must stop scheduling, signal active workers, bound the
   wait for shutdown, and mark tasks `aborted` or `skipped` per baseline status
   rules.
2. Workers must renew an IPC heartbeat while running. A stale heartbeat must be
   treated as worker failure/unresponsiveness.
3. A crash or exit without a valid result record must not be reported as
   successful completion.
4. If an implementer held the writer lease and ends without a clean successful
   result after it may have mutated the repository, Momo must surface an
   **uncertain-write** condition to the parent/user and must not claim verified
   success.
5. Writer lease release semantics must be deterministic and tested for success,
   abort, crash, and uncertain-write paths.

### 32.9 Pane retention, cleanup, adoption, and legacy migration

1. Persistent role panes must remain until explicit cleanup.
2. Momo must not auto-close role panes on assignment completion.
3. `/momo-workers` must show role, state, current assignment, and queued count.
4. `/momo-cleanup` must close `idle`/`unhealthy` (clear control ephemerals, retain
   monotonic generation tombstone), refuse `busy`/`blocked`, and for `--force`
   uncertain must verify exact lease owner **before** close (refuse missing/
   mismatched; never treat `no_lease` as safe).
5. Parent relaunch may adopt a pool worker only when registry + v2 manifest +
   heartbeat freshness + Herdr identity match. Momo must not adopt arbitrary or
   legacy v1 workers.
6. One-time migration must close terminal/ready legacy pane-per-task duplicates
   safely; active/uncertain legacy panes must fail clearly for operator cleanup.
7. Parent shutdown must cancel only assignments owned by the current parent
   epoch and must not leave unmanaged worker processes running.
8. Ready timeout / start failure must generation-fence rollback to
   unhealthy/closable (no stale generation reuse).

### 32.10 Fallback summary

| Condition | Required backend |
|---|---|
| Herdr not detected | In-process |
| Explicit in-process override | In-process |
| Herdr detected and compatible | Persistent role-pane pool |
| Herdr detected but incompatible | Fail closed (error) |

### 32.11 Manual / live Herdr acceptance matrix

Before the Herdr target is considered implemented, operators must verify at
least the following with a real Herdr session and configured Pi model:

1. Parent launches as controllable Pi foreground with display name Momo.
2. `herdr agent prompt` against the parent succeeds while the parent is the
   pane foreground agent.
3. A single scout task opens a new pane with a full Pi TUI and live activity.
4. Parallel read-only tasks allocate panes immediately and never exceed four
   active read-only workers.
5. An implementer acquires the cross-process writer lease; a second implementer
   waits/serializes (never steals); parallel implementer validation still fails.
6. Progress and final results arrive over IPC; killing TTY readability does not
   corrupt authoritative completion if IPC remains intact.
7. Cancellation aborts active workers and skips queued work without orphan
   processes.
8. Crash and uncertain-write paths surface correctly for implementers.
9. Completed/failed/aborted panes remain until explicit cleanup.
10. Outside Herdr, specialists remain in-process.
11. Explicit in-process override works inside Herdr.
12. Incompatible Herdr detection fails closed with a clear error.
13. macOS and Linux smoke notes recorded with Pi SDK and Herdr versions.

Automated mocked-Herdr tests must cover IPC, lease, allocation, cancellation,
and fail-closed logic without requiring a live Herdr server. Live Herdr tests
may be env-gated and must not be required for credential-free CI.

### 32.12 Delivery governance

1. Implementation of §32 must occur in a separate development Git worktree /
   feature branch and be submitted through a GitHub pull request.
2. That worktree/PR process is **delivery governance only**.
3. Momo must not automatically create Git worktrees as a product runtime
   mechanism for Herdr specialists in this target.

### 32.13 Non-goals for this target

- Upstream Herdr native `momo` kind.
- Automatic product Git worktrees for specialist isolation.
- Windows support in the initial delivery.
- TTY scraping as an orchestration control plane.
- Silent in-process fallback when Herdr is detected but broken.

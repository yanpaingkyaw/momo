# Momo

Momo is an interactive software-engineering orchestrator built with the
[Pi SDK](https://pi.dev/docs/latest/sdk). It uses Pi's terminal interface and
delegates repository work to isolated scout, planner, implementer, and reviewer
specialists.

**Status:** package `0.2.0` is an **implementation candidate** for Herdr pane
workers. Mocked unit/integration tests cover the IPC/lease/CLI seams. Partial
macOS operator evidence exists (below); **full live Herdr acceptance
(`SPEC.md` §32.11) has not passed** and must not be assumed.

## Requirements

- Node.js 22.19 or newer
- npm
- A model and credentials configured for Pi
- Optional: [Herdr](https://herdr.dev) 0.7.5 for pane-visible specialists

Momo reads the normal Pi configuration under `~/.pi/agent`, including provider
credentials, model settings, and project context. Momo **never** reads or writes
`auth.json` or other credential material in its own config.

### Model policies (credential-free)

Optional global model/reasoning policies live at:

- `$XDG_CONFIG_HOME/momo/config.json` (fallback `~/.config/momo/config.json`)

Schema version `1` with scopes: `default`, `parent`, `scout`, `planner`,
`implementer`, `reviewer`. Each policy is `{ provider, model, reasoning }`.
Absent config preserves legacy Pi session defaults.

Supported provider IDs: `openai-codex`, `anthropic`, `openai`, `openrouter`,
`google`, `opencode`, `opencode-go`. **Cursor subscription is not supported.**

Commands (parent Herdr and in-process):

- `/momo-model [scope]` — staged selector over **authenticated** models only
  (`modelRegistry.getAvailable` / `find`; no probing, no `getProviderAuth`)
- `/momo-models` — effective policies + `/login` guidance
- `/momo-model [scope] clear` — remove a scope override

Precedence: role override → `default`; `parent` override → `default`. Config is read
once per delegation `run()`, then the immutable policy is passed to in-process or
Herdr assignment proxies before `prompt()`; it is never retained as a process-lifetime snapshot.
IPC capability **v3** requires **concrete** `modelPolicy` on queue/command/started/result
(legacy records omit both `capability` and `modelPolicy`; null is forbidden).

**Generation-bound workers:** each Herdr worker generation binds to an exact concrete
policy at **process launch** via `--provider`, `--model`, `--thinking`. Workers
**verify** live session (`ctx.model` + `ctx.thinkingLevel`) and store the verified
effective snapshot in `started.json` / `result.json` — never blind echo, never
`pi.setModel` on persistent panes. Early terminal paths carry requested policy with
`modelPolicyApplied: false`; completed success requires `modelPolicyApplied: true`
and matching started/result policies.

**Parent apply:** `/momo-model` parent/default changes refuse while the agent is
busy (`!ctx.isIdle()`), otherwise capture session, prevalidate/apply target, persist
under token-fenced config lock, rollback session on persist failure. Clearing to
legacy restores captured baseline when available. Worker-scope changes persist only;
existing panes keep launch-bound policy until `/momo-cleanup`.

### Provider auth matrix

| Provider | Auth path | Notes |
|---|---|---|
| OpenAI Codex (`openai-codex`) | Codex Plus/Pro via Pi `/login` | Supported |
| Anthropic (`anthropic`) | Claude Pro/Max OAuth or API key | Pro/Max third-party usage is billed per token, not plan quota |
| OpenAI (`openai`) | Direct API key | Supported |
| OpenRouter (`openrouter`) | OAuth credits / API key | Supported |
| Google (`google`) | Direct API key | Supported |
| OpenCode (`opencode`, `opencode-go`) | Provider keys via Pi | Supported |
| Cursor subscription | — | **Unsupported** (no token scraping) |

Start Momo/Pi, then run `/login` in its interactive input before `/momo-model`
when no authenticated models appear in the selector.

## Target version matrix (candidate)

| Component | Target / dependency |
|---|---|
| Momo package | `0.2.0` (implementation candidate) |
| `@earendil-works/pi-coding-agent` / `pi` CLI | `0.82.1` (required) |
| Herdr CLI | `0.7.5` (0.7.x line) |
| Herdr protocol | `17` |
| Platforms | macOS operator-smoked (partial); Linux not claimed tested |

## Setup

```bash
npm install
npm run build
npm link
```

Run Momo from the repository you want it to work on:

```bash
momo
momo fix the failing authentication tests
```

### Backend selection

| `MOMO_BACKEND` | Behavior |
|---|---|
| `auto` (default) | In-process outside Herdr; Herdr pane-workers inside Herdr |
| `inprocess` | Always use in-process specialists (even inside Herdr) |
| `herdr` | Require Herdr; fail closed if preflight fails |

Other useful variables:

- `MOMO_PI_BINARY` — optional override of the Pi executable. Outside Herdr this may
  point at a specific binary; **inside Herdr** it is rejected unless it
  realpath-equals the canonical PATH `pi` (version still must be `0.82.1`).
- `MOMO_HERDR_BINARY` — Herdr executable (default `herdr`)

Inside Herdr (`HERDR_ENV=1`), `momo` preflights Herdr (`HERDR_ENV`, socket/pane
environment values, platform, official Herdr Pi lifecycle extension, CLI 0.7.x,
protocol 17, Pi 0.82.1),
then replaces itself with canonical PATH `pi` via `process.execve`, loading:

- compiled Momo parent extension (`dist/extensions/parent.js`)
- installed Herdr Pi lifecycle extension (`~/.pi/agent/extensions/herdr-agent-state.ts`) when present

Underlying Herdr kind remains `pi`; display/rename is **Momo**. Do not expect a
first-class Herdr `momo` kind on 0.7.5.

Install the official Pi integration once:

```bash
herdr integration install pi
```

## Herdr specialists

When the Herdr backend is active, accepted specialist tasks use a **persistent
role-pane pool**:

1. Parallel tasks allocate assignment proxies concurrently (failures are
   structured `TaskResult`s). Single/chain allocate lazily one step at a time
   (chain tails that never prompt create no sessions/panes)
2. Physical pane/agent starts lazily on first `prompt()` for that role
3. Exactly one Momo-managed pane per role per pool key (canonical repo + Herdr
   workspace + socket); workers execute from the canonical root; busy same-role
   work FIFO-queues with no overflow panes
4. Model-visible context resets each assignment; transcript/pane persists
5. Progress/results use private versioned IPC (control heartbeat separate from
   assignment spools; never TTY scraping)

Concurrency: cross-role workers may run in parallel; same-role work serializes
on the shared pane; one cross-process implementer via a writer lease under
`~/.cache/momo/leases/` (reacquired per assignment).

Cleanup:

```text
/momo-workers
/momo-cleanup
/momo-cleanup --force   # supervised: UI confirm, then force-release only if lock owner matches
```

### Migrating away from duplicate pane-per-task panes

If you still have old `Momo implementer` / `Momo reviewer` panes from the
pane-per-task candidate:

1. Finish or abort any **active** legacy workers you still need.
2. Restart Momo parent in the same Herdr workspace — startup migrates terminal/
   ready legacy registry records by closing those panes (never adopts them into
   the pool). Active/uncertain legacy panes are refused and require manual close.
3. Confirm pool workers only appear after a real `prompt()` (lazy physical create);
   `/momo-workers` should show at most one row per role.
4. For uncertain pool workers, `/momo-cleanup --force` verifies the **exact**
   writer-lease owner first and refuses missing/mismatched leases (does **not**
   treat `no_lease` as safe). After pane close succeeds, `paneClosed` is
   persisted before agent-stop confirmation so a transient lookup can retry
   without re-closing; force-release failure retains `paneClosed` +
   `recoveryRequired`. Idle/unhealthy cleanup clears control ephemerals
   but retains the monotonic generation tombstone.
5. After cleanup/recreate, generations must advance (no stale g1 reuse).

### Operator evidence (macOS, partial — not §32.11)

Recorded against the earlier pane-per-task candidate; pool correction needs
re-smoke for reuse/FIFO/context isolation:

- Canonical parent prompt under Herdr
- Scout E2E / role panes / implementer write / reviewer diff / cancel / cleanup

**Automated coverage (credential-free):** sequential same-pane assignments + queued
claim, lock token-safe release / fail-closed stale locks (no automatic takeover),
durable claim recovery, result-write failure
stops queue, cancel→aborted, stale/missing heartbeat grace, generation
tombstone, lease-first uncertain cleanup refusal, epoch shutdown queue/claiming
cleanup. See `npm test`.

**Explicitly still pending live:** pool reuse across repeated implementer/reviewer
tasks under real Herdr, queued-task cancellation without active interrupt in a
live pane, cross-parent queue contention on a real socket, Linux. Process death
without `session_shutdown` cannot guarantee cancel IPC (matching-epoch queued
entries are removed, non-active claiming removed, exact active/claiming gets
cancel IPC when shutdown does run).

## Roles

| Role | Purpose | Capabilities |
|---|---|---|
| `scout` | Locate relevant code and conventions | Read-only repository tools |
| `planner` | Produce an implementation plan | Read-only repository tools |
| `implementer` | Change and verify code | Read, shell, edit, and write tools |
| `reviewer` | Review the current change | Read-only tools and a fixed Git diff tool |

## Troubleshooting

| Symptom | What to check |
|---|---|
| `Momo failed to start` under Herdr | `herdr --version` is 0.7.x; `herdr api schema --json` protocol 17; `HERDR_SOCKET_PATH` / `HERDR_PANE_ID` set |
| Parent not controllable / `agent_not_ready` | Focus the parent pane; ensure specialists were created with `--no-focus`; kind is `pi` with display Momo |
| Specialists invisible | Confirm `MOMO_BACKEND` is not `inprocess`; rebuild so `dist/extensions/*.js` exist |
| Writer contention | Another Momo implementer holds `~/.cache/momo/leases/*`; this worker waits (read-only) until lease release, cancel, or deadline — never steals |
| Cleanup refused | Active panes are never force-removed; uncertain force path needs UI confirm and matching lease ownerId |
| `MOMO_PI_BINARY` rejected under Herdr | Override must realpath-equal PATH `pi` at `0.82.1` |

Live Herdr acceptance remains operator-run (`SPEC.md` §32.11). Credential-free
automated tests use mocked Herdr clients; they are not a substitute for the live matrix.

## Development

```bash
npm run typecheck
npm test
npm run build
node dist/cli.js --help
```

Dependency audit: after `npm audit fix`, the lockfile reports 0 vulnerabilities on this
candidate snapshot. Re-run `npm audit` after dependency bumps; do not assume a
clean tree without checking.

Architecture: [Architecture.md](./Architecture.md)

Contract: [SPEC.md](./SPEC.md)

## Security

Momo enforces role-level capability boundaries: the parent and read-only roles
do not receive shell or mutation tools, children cannot delegate recursively,
and the reviewer can only run fixed Git status/diff commands.

This is not an operating-system sandbox. Implementer commands run with the same
filesystem and process permissions as the host user. Use Momo only in
repositories and environments where those permissions are appropriate.

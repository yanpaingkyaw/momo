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
credentials, model settings, and project context.

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

When the Herdr backend is active, every accepted specialist task:

1. Allocates a new Herdr pane immediately (including queued parallel/chain tasks)
2. Starts canonical Pi full TUI with the Momo worker extension
3. Streams live activity in that pane
4. Returns authoritative progress/results over private versioned file IPC (never TTY scraping)

Concurrency: at most four active read-only workers; one cross-process implementer
via a writer lease under `~/.cache/momo/leases/` (outside the git tree). A second
implementer **waits** (cancellation-aware, bounded) for that lease — it never steals.

Completed/failed/aborted panes are retained until explicit cleanup:

```text
/momo-workers
/momo-cleanup
/momo-cleanup --force   # supervised: UI confirm, then force-release only if lock owner matches
```

### Operator evidence (macOS, partial — not §32.11)

Recorded against this candidate:

- Canonical parent prompt under Herdr
- Scout E2E
- Four role panes opened
- Implementer exact `ok` newline write in a disposable fixture
- Reviewer `workspace_diff`
- Active planner cancellation via parent interrupt, with an aborted retained pane
- Retention / cleanup

**Explicitly still pending live:** queued-task cancellation, full cross-parent
lease contention, crash recovery, Linux. Process death without `session_shutdown`
cannot guarantee cancel IPC.

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

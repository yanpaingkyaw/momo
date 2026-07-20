# Momo

Momo is an interactive software-engineering orchestrator built with the
[Pi SDK](https://pi.dev/docs/latest/sdk). It uses Pi's terminal interface and
delegates repository work to isolated scout, planner, implementer, and reviewer
sessions.

## Requirements

- Node.js 22.19 or newer
- npm
- A model and credentials configured for Pi

Momo reads the normal Pi configuration under `~/.pi/agent`, including provider
credentials, model settings, and project context.

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

The main conversation uses Pi's persistent session storage. Delegated child
sessions are ephemeral.

## Roles

| Role | Purpose | Capabilities |
|---|---|---|
| `scout` | Locate relevant code and conventions | Read-only repository tools |
| `planner` | Produce an implementation plan | Read-only repository tools |
| `implementer` | Change and verify code | Read, shell, edit, and write tools |
| `reviewer` | Review the current change | Read-only tools and a fixed Git diff tool |

Momo can run up to four independent read-only tasks concurrently. Implementers
are rejected from parallel delegations, so mutation work is always serialized.

## Development

```bash
npm run typecheck
npm test
npm run build
node dist/cli.js --help
```

The complete behavior and acceptance contract is in [SPEC.md](./SPEC.md).

## Security

Momo enforces role-level capability boundaries: the parent and read-only roles
do not receive shell or mutation tools, children cannot delegate recursively,
and the reviewer can only run fixed Git status/diff commands.

This is not an operating-system sandbox. The implementer's commands run with
the same filesystem and process permissions as Momo. Use Momo only in
repositories and environments where those permissions are appropriate.

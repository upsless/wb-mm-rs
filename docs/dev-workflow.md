# Development Workflow

## Machines

Development happens on the laptop and desktop directly. A separate shared VM is
not required because both machines can reach the test WirenBoard host.

## Test Target

- Host: `wb.loc`
- MQTT and DBus access are expected to be reachable from both development
  machines.

## Git Workflow

- Main development repository: `upsless/wb-mm-rs`.
- Reference repository visible to the GitHub connector: `upsless/wb-mm-mqtt`.
- AbyssDiggers upstream reference: `AbyssDiggers/wb-mm-mqtt`.

Before starting work on a machine:

```bash
git pull --rebase
git status
```

After a coherent change:

```bash
git status
git diff
git push
```

## Local Context

Important project memory belongs in repository files, not in a single chat
session:

- `AGENTS.md`
- `docs/architecture.md`
- `docs/dev-workflow.md`
- `docs/reference-wb-mm.md`
- `.agents/skills/modemmanager-mqtt-review/SKILL.md`

This keeps the project recoverable from either development machine.

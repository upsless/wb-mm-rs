# Codex Handoff

Use this file to restore context when opening the project in a new workspace or
starting a new Codex chat.

## Project

- Repository: `upsless/wb-mm-rs`
- Purpose: clean Rust daemon for Wiren Board ModemManager integration.
- Reference fork available to GitHub connector: `upsless/wb-mm-mqtt`.
- Upstream reference: `AbyssDiggers/wb-mm-mqtt`.
- Test target: `wb.loc`; development machines can reach MQTT and DBus there.

## Current Direction

Build a focused daemon, not a general-purpose framework. The old Python project
is reference material for behavior, logs, mappings, and edge cases, but its
architecture should not be copied.

Planned async components:

- DBus handler: ModemManager discovery, DBus events, and method calls.
- MQTT handler: Wiren Board device/control creation, value publishing, user
  writes, cleanup, and Last Will setup.
- Dispatcher: business logic, state decisions, and routing commands between
  DBus and MQTT handlers.

## Important Decisions

- Before any commit intended to be pushed to GitHub, Codex should review this
  handoff file and update it if the commit changes project context, decisions,
  workflow, known issues, or next steps.
- Use current Wiren Board MQTT naming for new topics: lowercase words separated
  by underscores. Do not copy old names like `IsAvailable`, `ModemsCount`,
  `SignalQuality`, or `mm-modem-1` unless explicit compatibility is needed.
- Preserve old `wb-mm-mqtt` Last Will semantics: if the daemon dies,
  ModemManager must become unavailable in the UI/control model. The
  availability control is a daemon capability marker, not only a cached DBus
  value.
- Consider combining UI-visible availability with conventional
  `/devices/<device>/meta/error` reporting, but do not lose the Last Will
  behavior.
- Keep DBus/MQTT mappings compact and reviewable, similar in spirit to
  `mqtt_logics.py` and `dbus_logics.py`, without the old universal library
  structure.

## Known Reference Findings

- `wb-mm-mqtt` mostly follows the basic WB MQTT device/control model.
- It uses older topic naming style, so the new Rust daemon should modernize
  names.
- Python reference bug: `mqtt_delete_modem()` appears to call
  `mqtt_del_control(target, modem_mqtt_path, control)`, while
  `mqtt_del_control()` accepts only `(target, control_path)`. Do not copy this
  cleanup implementation; consider fixing it later in the reference fork.

## Key Files

- `AGENTS.md` - agent/project rules.
- `docs/architecture.md` - architecture notes.
- `docs/dev-workflow.md` - machine/Git workflow.
- `docs/reference-wb-mm.md` - reference project notes and findings.
- `.agents/skills/modemmanager-mqtt-review/SKILL.md` - local Codex skill.

## Next Likely Work

1. Rename/open workspace as `wb-mm-rs`.
2. Scaffold the Rust project.
3. Decide initial MQTT device/control mapping names.
4. Implement the first minimal slice:
   - MQTT device/control metadata for ModemManager availability;
   - MQTT Last Will unavailable state;
   - DBus connection to ModemManager on `wb.loc`;
   - dispatcher event for availability/version.

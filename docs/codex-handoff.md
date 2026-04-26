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
- MQTT lifecycle supervisor: MQTT is the primary lifecycle gate. If MQTT is
  disconnected, DBus work must be stopped completely until MQTT reconnects.

## Important Decisions

- Before any commit intended to be pushed to GitHub, Codex should review this
  handoff file and update it if the commit changes project context, decisions,
  workflow, known issues, or next steps.
- If the user says that a change should be committed only after confirmation,
  Codex must not commit or push until the user explicitly grants that
  permission in a later message.
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
- Production logs should be quiet after debugging: startup, shutdown, unhandled
  errors, and important unrecoverable conditions. Development logs should be
  very detailed, at least as useful as `wb-mm-mqtt` logs.
- On MQTT loss, stop DBus subscriptions/work and drop live runtime state. After
  MQTT reconnect, republish metadata and perform fresh DBus discovery.
- Stage 0 daemon startup uses `--dbus-address <ADDRESS>` for custom DBus
  connection. If the argument is not provided, use the system bus.
- Current scaffold uses `zbus` 5.x because the development DBus address relies
  on `unixexec:` transport (`ssh ... systemd-stdio-bridge`), which did not
  work in the earlier setup.
- The daemon now listens for both `SIGINT` and `SIGTERM` and shuts down MQTT
  and DBus loops gracefully.
- In a plain terminal, `Ctrl+C` works as expected. In VS Code CodeLLDB debug
  sessions, `Ctrl+C` in the debug terminal is unreliable and may kill the
  process before graceful shutdown logs appear.
- For VS Code debugging, the reliable shutdown path is `Shift+F5` / `Stop`
  with `gracefulShutdown: "SIGTERM"` in the local `.vscode/launch.json`.
  If graceful shutdown hangs, use `Stop` again to force termination.
- Stage 0 now captures ModemManager DBus state in three buckets:
  `Active`, `Inactive`, and `NotFound`.
- The initial state is logged after DBus connect, and further transitions are
  tracked through `org.freedesktop.DBus` `NameOwnerChanged` subscription for
  `org.freedesktop.ModemManager1`.
- Keep the daemon core compact in `main.rs` while it still reads cleanly from
  top to bottom. Split modules only when they gain an independent
  responsibility.
- Separate shared DBus/MQTT runtime code from concrete signal/topic mappings.
  Use lightweight `dbus/logics.rs` and `mqtt/logics.rs` style modules rather
  than a general-purpose framework.

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

1. Finish stage 0 scaffold refinement:
   - align log messages with useful `wb-mm-mqtt` reference wording where it
     fits;
   - keep local debug runner defaults for remote DBus access through
     `unixexec:path=ssh,argv1=-T,argv2=root@wb.loc,argv3=systemd-stdio-bridge`;
   - read and log more initial ModemManager data such as version and modem
     count;
   - add focused tests around startup and shutdown wiring where practical.
2. Implement stage 0.1:
   - DBus loop bus availability checks / health handling.
3. Implement stage 1:
   - MQTT + DBus + ModemManager device;
   - version and modem count controls;
   - correct MQTT updates on modem connect/disconnect;
   - correct behavior when ModemManager service is stopped, started, or removed
     on `wb.loc`;
   - focused unit tests.

## Roadmap

1. ModemManager device with version/modem count and service/modem change tests.
2. Per-modem device with basic characteristics, DBus/MQTT/daemon failure
   behavior, cleanup, Last Will checks, and unit tests.
3. SMS data: counts, last SMS time, SMS viewing in modem device, and unit tests.
4. MQTT-to-DBus user actions routed through dispatcher, with unit tests.

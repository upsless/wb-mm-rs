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
- Tresher: business logic, state decisions, and routing commands between
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
- A compact supervisor in `main.rs` now owns reconnect behavior:
  - MQTT is the top-level lifecycle gate;
  - DBus runs only while MQTT is connected;
  - DBus reconnect retries use fast and slow intervals;
  - MQTT reconnect retries use the same pattern with separate constants;
  - on DBus session failure, ModemManager is treated as `NotFound` until the
    bus connection returns;
  - on MQTT loss, DBus is stopped first, and after MQTT reconnect both
    subsystems start again from a clean slate.
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
- When ModemManager is `Active`, stage 0 also logs a small snapshot:
  `Version` and `modem_count`.
- After ModemManager service restart on `wb.loc`, the DBus name may become
  `Active` before the modem object list repopulates. The current stage-0 logic
  therefore:
  - arms `ObjectManager` watchers first;
  - logs the initial snapshot with the current `modem_count`;
  - later logs `ModemManager modem count changed: modem_count=...` when the
    modem object list catches up.
- Keep the daemon core compact in `main.rs` while it still reads cleanly from
  top to bottom. Split modules only when they gain an independent
  responsibility.
- Separate shared DBus/MQTT runtime code from concrete signal/topic mappings.
  Use lightweight `dbus/logics.rs` and `mqtt/logics.rs` style modules rather
  than a general-purpose framework.
- Stage 0.2 now includes an explicit event/command exchange path:
  - DBus emits manager-level events into a tresher;
  - the tresher keeps small last-published state and translates DBus events
    into MQTT commands;
  - the MQTT side is still a stub, but it now receives structured commands and
    logs their "execution" instead of logging only raw lifecycle.
- The current stage-0.2 manager-level DBus events are intentionally compact:
  `StatusChanged`, `Snapshot { version, modem_count }`, and
  `ModemCountChanged`.
- Current logging split for stage 0.2:
  - `info`: meaningful DBus-side ModemManager events and MQTT-side command
    execution results;
  - `debug`: tresher-internal event/command routing and lower-level
    lifecycle details.

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

1. Clean up and harden the new supervisor/reconnect stage:
   - add focused tests for reconnect backoff and lifecycle ordering where
     practical;
   - review whether reconnect log wording should be aligned even more closely
     with `wb-mm-mqtt`;
   - keep local debug runner defaults for remote DBus access through
     `unixexec:path=ssh,argv1=-T,argv2=root@wb.loc,argv3=systemd-stdio-bridge`.
2. Build out stage 0.2 from manager-level exchange to richer mappings:
   - add the next DBus event shapes needed for per-modem and SMS work;
   - expand tresher command routing beyond the ModemManager manager device;
   - keep the event/command types compact and reviewable, following the small
     manager-level shape borrowed from the python reference.
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
4. MQTT-to-DBus user actions routed through tresher, with unit tests.

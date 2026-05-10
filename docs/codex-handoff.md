# Codex Handoff

Use this file to restore context when opening the project in a new workspace or
starting a new Codex chat.

## Project

- Repository: `upsless/wb-mm-rs`
- Purpose: focused Rust daemon for Wiren Board ModemManager integration.
- Reference fork available to the GitHub connector: `upsless/wb-mm-mqtt`.
- Upstream reference: `AbyssDiggers/wb-mm-mqtt`.
- Test target: `wb.loc`; development machines can reach MQTT and DBus there.
- Canonical MQTT conventions reference:
  `https://github.com/wirenboard/conventions/blob/main/README.md`
- Wiren Board MQTT wiki reference:
  `https://wiki.wirenboard.com/wiki/MQTT`

## Direction

Build a focused daemon, not a general-purpose framework. The Python project is
reference material for behavior, logs, mappings, and edge cases, but its
architecture should not be copied.

The intended daemon has three async parts:

- DBus handler: ModemManager discovery, DBus events, and method calls.
- MQTT handler: Wiren Board device/control creation, value publishing, user
  writes, cleanup, and Last Will setup.
- Tresher: thin dispatcher/business layer routing commands between DBus and
  MQTT.

MQTT is the primary lifecycle gate. If MQTT is disconnected, DBus work should be
stopped and runtime state dropped until MQTT reconnects.

## Standing Decisions

- Before any commit intended to be pushed to GitHub, review this handoff and
  update it if the commit changes project context, decisions, workflow, known
  issues, or next steps.
- Read `docs/arcnotes.md` when resuming architecture work. It contains
  user-owned design notes that may not yet be reflected in implementation;
  notes must stay as exact numbered quotes unless the user explicitly asks for
  a summary or another form. Use Russian by default unless another language is
  requested.
- Do not commit or push after any "commit only if I confirm" style instruction
  until the user explicitly grants that permission later.
- Preserve old `wb-mm-mqtt` Last Will semantics: if the daemon dies,
  ModemManager must become unavailable in UI/control terms. The public MQTT
  `is_available` control is the user-facing trust marker and must become `0`
  when the daemon dies unexpectedly or when DBus says ModemManager is inactive
  or deleted.
- Use current Wiren Board naming style for new topics: lowercase words
  separated by underscores. Do not copy old names such as `IsAvailable`,
  `ModemsCount`, `SignalQuality`, or `mm-modem-1` unless compatibility is
  explicitly required.
- Do not change the MQTT topic schema unless the user explicitly asks.
- Keep DBus destination, path, interface, member, and error context explicit.
- Keep mapping modules compact and reviewable. Current naming:
  `src/dbus/schema.rs` for DBus/domain mapping helpers and
  `src/mqtt/schema.rs` for MQTT topic/control schema helpers.
- Production logs should stay quiet: startup, shutdown, important state
  transitions, and unrecoverable conditions. Debug logs can be more detailed.

## Current Architecture State

- `main.rs` owns the top-level supervisor only:
  - MQTT is the top-level lifecycle gate;
  - DBus runs only while MQTT is connected;
  - MQTT reconnect intervals still live in `main.rs`;
  - DBus reconnect intervals now live in `src/dbus.rs`;
  - on DBus session failure, current code maps loss to `ManagerDeleted` until
    the bus returns;
  - on MQTT loss, DBus is stopped first; after reconnect both subsystems start
    from a clean slate.
- The daemon listens for `SIGINT` and `SIGTERM` and shuts down MQTT and DBus
  loops gracefully.
- VS Code CodeLLDB note: `Ctrl+C` in the debug terminal is unreliable. The
  reliable shutdown path is `Shift+F5` / `Stop` with
  `gracefulShutdown: "SIGTERM"` in the local `.vscode/launch.json`.
- MQTT publishes retained WB device/control topics for one stable main device
  and per-modem devices, clears retained topics on normal shutdown, and sets
  Last Will on the top-level availability control.
- `ManagerStatus` is only `Active | Inactive`; DBus object disappearance
  is represented by `ManagerDeleted`.
- MQTT-facing modem numbering starts from `1` even when DBus modem ids start
  from `0`. DBus ids stay internal; MQTT device names are user-facing, e.g.
  `mm_modem_1`.
- DBus manager-level runtime is now split out of the outer loop:
  - `src/dbus/connection.rs` owns DBus connection setup, the top-level select
    loop, and shutdown/command-channel integration;
  - `src/dbus/runtime.rs` owns the DBus-specific `DbusRuntime`: the
    `org.freedesktop.DBus` proxy, ModemManager owner-change subscription, and
    the embedded `ManagerWatcher`;
  - `src/dbus/manager.rs` owns ModemManager-specific state and logic:
    manager presence, active manager streams, modem watcher collection, and
    manager-level command/event handling;
  - `src/dbus/modem.rs` owns modem/SMS watchers, modem/SMS DBus proxy work, and
    DBus commands for SMS refresh/delete;
  - `src/dbus/schema.rs` replaced the old `src/dbus/logics.rs` name and holds
    DBus/domain vocabulary, mappings, parsers, and log message helpers.
- MQTT runtime is similarly layered:
  - `src/mqtt.rs` owns one MQTT session lifecycle;
  - `src/mqtt/loop.rs` owns the low-level rumqtt event loop polling;
  - `src/mqtt/frontend.rs` owns MQTT-side command handling and user writes;
  - `src/mqtt/publish.rs` owns retained publish/cleanup helpers and publisher
    state;
  - `src/mqtt/state.rs` owns frontend state.

## Current Exchange Vocabulary

- DBus events:
  - `ManagerFound { version, modem_count }`
  - `ManagerUpdated(ManagerUpdate)`
  - `ManagerDeleted`
  - `ModemFound { modem_id, info: ModemInfo }`
  - `ModemUpdated { modem_id, update }`
  - `ModemDeleted { modem_id }`
  - `SmsInventorySnapshot { modem_id, sms_ids, initial_sms_snapshot }`
  - `SmsListChanged { modem_id, sms_ids }`
  - `SmsSnapshot { modem_id, snapshot }`
  - `SmsPropertyChanged { modem_id, update }`
  - `SmsDeleted { modem_id, sms_id }`
- MQTT commands mirror the DBus events but keep update-oriented names where the
  MQTT layer updates the frontend projection, for example
  `PublishSmsUpdate { modem_id, update }`.
- `SmsSnapshot` events/commands no longer carry a redundant outer `sms_id`;
  `snapshot.sms_id` is authoritative.
- DBus SMS property changes are modeled as:
  - `SmsUpdate { sms_id, property }`
  - `SmsPropertyChange::{IsReceived, Storage, Timestamp, Number, Text}`

`ModemInfo` is the shared domain description of a modem. It replaced the old
flat `ModemFound` field list and the MQTT-local `MqttModemFoundPayload`.

## MQTT State Model

MQTT runtime state lives in `src/mqtt/state.rs`.

- `MqttSessionState` owns session-level MQTT state:
  - ModemManager availability flag;
  - modem map;
  - reverse modem index map.
- `MqttPublisher` in `src/mqtt/publish.rs` owns MQTT publication state:
  - main device creation flag;
  - per-modem SMS control creation/subscription sets;
  - cached manager-level SMS count used to avoid duplicate publishes.
- `MqttModemState` owns one modem's MQTT state:
  - user-facing modem index;
  - optional `sms_state`.
- `MqttModemState::ensure_sms_state()` creates the SMS state explicitly when
  the first SMS inventory snapshot for that modem is handled.
- `MqttModemSmsState` owns the SMS selection model:
  - `sms_order: Vec<SmsId>`;
  - `picked_sms_index: u32` as a 1-based UI position;
  - `displayed_sms_id: Option<SmsId>` as the DBus id currently rendered in
    selected-SMS fields.

Unit tests for MQTT state live in `src/mqtt/state/tests.rs`, not inline inside
`state.rs`.

## SMS Behavior

- DBus starts a separate SMS inventory watcher only when modem state allows SMS
  inventory (`enabled` or later). This avoids the hot-plug burst where
  `Messages` changes before the modem reaches a stable usable state.
- DBus emits one `SmsInventorySnapshot` with the ordered `sms_ids` and an
  optional `initial_sms_snapshot`, then live `SmsListChanged` updates.
- Current implementation orders SMS ids from the modem `Messages` property by
  numeric short DBus id. This is now known to be wrong for arrival order:
  ModemManager/the modem may reuse the first free SMS number after deletion, so
  a newly received SMS can appear in an older numeric slot. Rework SMS list
  ordering according to `docs/arcnotes.md`: pull the receive timestamp for each
  SMS and sort by real receive time instead of DBus short id.
- Per-modem SMS controls are created lazily on the first SMS inventory command.
  Before that, SMS controls for the modem should not exist.
- Empty SMS inventory after initialization publishes:
  - `sms_count=0`;
  - `last_sms_dbus_id=null`;
  - `message_select` readonly with `min=1 max=1 value=1`;
  - selected-SMS fields as `null`/`0`;
  - `delete_message` readonly.
- The manager MQTT device publishes aggregate incoming-SMS count as `sms_count`.
  It does not publish a best-effort "last SMS timestamp".
- Each modem MQTT device publishes:
  - `sms_count`;
  - `last_sms_dbus_id`;
  - writable `message_select`;
  - readonly `displayed_sms_index`;
  - selected-SMS fields: `selected_sms_dbus_id`,
    `selected_sms_timestamp`, hidden `selected_sms_timestamp_unixtime`,
    `selected_sms_sender`, `selected_sms_is_received`, `selected_sms_text`,
    `selected_sms_storage`;
  - `delete_message` pushbutton for the currently displayed SMS.
- Timestamp controls are published in pairs: visible text timestamp and hidden
  readonly `unixtime` payload for machine consumers.
- Because DBus SMS numeric id is not a reliable "last received" criterion,
  revisit `last_sms_dbus_id` before the SMS sorting rewrite. The likely UI
  replacement is a visible "Last Received SMS Date" plus hidden unix-time
  control, matching the selected-SMS timestamp pair.

### SMS Selection Rules

- `SmsSnapshot` is accepted only when `snapshot.sms_id` equals the DBus id at
  `sms_order[picked_sms_index - 1]`.
- Accepting a snapshot records `displayed_sms_id = snapshot.sms_id`, publishes
  selected-SMS fields, publishes `displayed_sms_index`, and enables delete.
- Snapshots for any other SMS id are ignored by MQTT.
- Live `SmsUpdate` is applied to visible MQTT fields only when
  `displayed_sms_id == update.sms_id`.
- User writes to `message_select/on` update `picked_sms_index`, map the index to
  `sms_order[picked_sms_index - 1]`, and request a fresh snapshot only when the
  effective clamped index changes.
- User writes to `delete_message/on` delete `displayed_sms_id`, i.e. the SMS
  currently visible to the user. Ordinary DBus deletion events drive MQTT
  cleanup and reselection.
- When a new SMS list arrives:
  - update `sms_order`;
  - keep `picked_sms_index` as a positional user intent, clamped to the new
    list bounds;
  - compare the DBus id under the picked position before and after applying the
    new list;
  - request a new snapshot only when the picked DBus id changed and the new
    picked id exists;
  - publish updated `message_select` and `displayed_sms_index` from state.
- `displayed_sms_index` is now a real state-derived value: it is the current
  1-based position of `displayed_sms_id` inside `sms_order`, or `null` if the
  displayed SMS is no longer in the list.
- `displayed_sms_id` is changed only by accepting a snapshot. List changes and
  deletion handling may make it invalid for the current list, but they do not
  clear it directly.

## Recent Refactor Notes From 2026-04-30

- `src/mqtt/logics.rs` was renamed to `src/mqtt/schema.rs` because it contains
  MQTT schema/topic/control definitions, not runtime logic.
- MQTT session/modem/SMS state structs were moved into `src/mqtt/state.rs`.
- `MqttModemState { index, sms_state }` replaced the older flat
  `modem_sms_data` style.
- `MqttModemSmsState::apply_snapshot()` replaced separate
  `accepts_sms_snapshot()` / `accept_sms_snapshot()` methods.
- `handle_sms_list()` now handles both initial inventory lists and ordinary
  list changes. `SmsInventorySnapshot` only ensures modem/SMS state exists and
  passes the optional initial snapshot into this shared handler.
- The old duplicated list/inventory tail was removed: list application,
  optional initial snapshot application, delete read-only transition, manager
  SMS count sync, and snapshot request now happen through one path.
- `displayed_sms_index` publishing moved into the general modem SMS sync path
  so the control is updated when the displayed SMS stays the same but moves to
  a different list position.
- SMS list processing now treats `picked_sms_index` as positional intent:
  `MqttModemSmsState::apply_sms_order()` requests a fresh snapshot only when
  the picked DBus id changes after applying the new list.
- MQTT SMS handlers now share `finish_synced_sms_change()` for the common tail
  after modem SMS state has already been synced: make delete read-only while a
  new snapshot is pending, optionally sync manager SMS count, and request the
  needed snapshot.
- MQTT schema now keeps modem base controls and SMS controls in separate arrays
  instead of slicing one combined list with `MODEM_BASE_CONTROL_COUNT`.
- MQTT update/snapshot/sync paths that should not create devices now use a
  plain modem-index lookup. Device creation is owned by `ModemFound` via
  `handle_modem_found()`; initial SMS inventory for an unknown modem is ignored.
- MQTT state tracks `manager_available` and per-modem `is_active`. User writes
  to modem `/on` topics are ignored while ModemManager is unavailable or the
  target modem is inactive; WB readonly metadata is UI guidance, not backend
  protection.
- Tests were moved out of `state.rs` into `src/mqtt/state/tests.rs`.
- Latest local checks after these changes passed:
  - `cargo fmt --check`
  - `cargo clippy --all-targets --all-features`
  - `cargo test` (21 tests)

## Live Verification Notes

- Live DBus monitoring on `wb.loc` showed hot-plug SMS inventory arriving as a
  burst of `Messages` changes before `registered`: the modem appears with
  `Messages=[]`, later emits `Messages=[90]`, `Messages=[91,90]`, and so on,
  then transitions through `enabled` toward `registered`.
- Initial SMS snapshot and MQTT-to-DBus selection flow were verified earlier.
- Live SMS add/delete/change events still need verification on a working SIM;
  the current SIM was operator-blocked during previous testing.
- On the A7600-style modem, `AT+CPMS="ME","ME","ME"` made the modem reboot and
  then rediscover SMS received earlier today. Incoming SMS had been reported as
  `Mobile` storage, but recent messages still disappeared across modem reboot
  until this storage mode was explicitly selected. Need determine how to persist
  or apply this setting cleanly, preferably through ModemManager if available;
  otherwise consider a controlled AT command path.

## Known Reference Findings

- `wb-mm-mqtt` follows the basic WB MQTT device/control model but uses older
  naming style. The Rust daemon should modernize names unless compatibility is
  explicitly requested.
- Python reference bug: `mqtt_delete_modem()` appears to call
  `mqtt_del_control(target, modem_mqtt_path, control)`, while
  `mqtt_del_control()` accepts only `(target, control_path)`. Do not copy this
  cleanup implementation; consider fixing it later in the reference fork.

## Key Files

- `AGENTS.md` - agent/project rules.
- `docs/architecture.md` - architecture notes.
- `docs/arcnotes.md` - user-owned architecture notes. Read it during handoff
  recovery, but do not modify it unless the user explicitly asks. Notes are
  stored as numbered Russian notes by default; exact quotes are preferred
  unless the user asks for a summary.
- `docs/dev-workflow.md` - machine/Git workflow.
- `docs/reference-wb-mm.md` - reference project notes and findings.
- `src/exchange.rs` - DBus/MQTT/tresher event and command vocabulary.
- `src/dbus/schema.rs` - DBus/domain ids, snapshots, updates, parsers, and log
  message helpers.
- `src/dbus/connection.rs` - DBus connection, top-level select loop,
  and shutdown/command-channel integration.
- `src/dbus/runtime.rs` - DBus-specific runtime wrapper: DBus proxy,
  ModemManager owner/status stream, and delegation into manager logic.
- `src/dbus/manager.rs` - ModemManager-specific state, streams, activation,
  modem collection, and manager-level command/event handling.
- `src/dbus/modem.rs` - modem/SMS watchers, modem/SMS proxy setup, SMS
  inventory and tracked-SMS streams, and SMS refresh/delete DBus commands.
- `src/mqtt.rs` - MQTT session lifecycle, frontend startup, graceful stop, and
  command/shutdown integration.
- `src/mqtt/schema.rs` - MQTT device/control schema, topic builders, metadata,
  and log message helpers.
- `src/mqtt/state.rs` - MQTT session, modem, and SMS state machines.
- `src/mqtt/state/tests.rs` - unit tests for MQTT state behavior.
- `src/mqtt/loop.rs` - low-level rumqtt event loop polling and incoming
  publish forwarding.
- `src/mqtt/frontend.rs` - MQTT command handling, frontend/business decisions,
  user write parsing, and state orchestration.
- `src/mqtt/publish.rs` - MQTT retained publishing, cleanup, metadata sync, and
  publication-only state.
- `src/tresher.rs` - thin dispatcher between DBus and MQTT.
- `.agents/skills/modemmanager-mqtt-review/SKILL.md` - local Codex skill.

## Next Likely Work

1. Systematize `src/dbus/modem.rs` after the mechanical extraction:
   - separate proxy/stream initialization;
   - separate modem property change handling;
   - separate SMS inventory task start/stop/retarget logic.
2. Before mass SMS sorting changes, re-check the current initial inventory path:
   `handle_sms_list()` syncs modem SMS controls before applying
   `initial_sms_snapshot`, so it may briefly publish empty selected-SMS fields
   and then immediately publish the real snapshot. Decide whether to collapse
   this into one publish pass.
3. Rework SMS sorting/list payloads according to `docs/arcnotes.md`:
   - DBus SMS ids are not an arrival-order source because the modem can reuse
     freed numeric slots;
   - fetch/include each SMS receive timestamp when building list snapshots and
     list-change events;
   - keep MQTT picker semantics positional, but base positions on receive-time
     order rather than DBus short id order.
4. Replace or complement `last_sms_dbus_id` with "Last Received SMS Date" plus
   hidden unix-time, because max DBus id is not the last-arrival marker.
5. Figure out how to persist the SMS storage choice that worked manually:
   `AT+CPMS="ME","ME","ME"` caused the modem to rediscover today's SMS after a
   reboot-like transition, but the daemon should not rely on manual console
   state.
6. Continue reducing MQTT SMS handler complexity:
   - review `apply_sms_deleted`, `pick_modem_sms`, and `delete_picked_sms` for
     the same state/frontend split used in `handle_sms_list`;
   - consider clearer method names after behavior settles.
7. Re-check the stale `displayed_sms_id` policy after more cleanup:
   - current rule is "only accepted snapshots change it";
   - if the selected-SMS fields are cleared because the list is empty or the
     displayed SMS disappeared, decide whether an explicit state method should
     also set `displayed_sms_id=None`.
8. Verify live SMS add/delete/change behavior on a working SIM.
9. Add focused tests around reconnect/lifecycle ordering where practical.
10. Keep WB MQTT semantics and Last Will behavior intact while tightening topic
   metadata and UI details.

## Recent DBus Cleanup Notes

- `src/dbus/logics.rs` was renamed to `src/dbus/schema.rs`, matching the MQTT
  naming pattern: schema files hold compact vocabulary/mapping helpers rather
  than runtime logic.
- The DBus side now has an explicit two-level shape:
  - `DbusRuntime` in `src/dbus/runtime.rs` is the outer DBus orchestrator;
  - `ManagerWatcher` in `src/dbus/manager.rs` owns ModemManager-specific
    state, active streams, and manager-level behavior.
  The outer `src/dbus/connection.rs` still keeps DBus connection setup,
  shutdown/command integration, and the top-level select loop.
- Modem and SMS watcher logic was mechanically moved out of the old DBus
  top-level file into `src/dbus/modem.rs`. It is intentionally not yet
  internally split; next work should systematize modem vs SMS responsibilities
  inside/under that file.
- The old manager-level `SmsCommandRegistry` was removed. `RefreshSms` now
  routes directly to the right `ModemWatcher`, which tracks its own SMS command
  channel internally.
- DBus shutdown now clears active modem watchers via `ManagerWatcher::reset()`
  instead of relying on detached task bookkeeping in the outer loop.
- `wait_for_shutdown()` now lives in `src/shutdown.rs` and is shared by main,
  MQTT, DBus, and tresher.

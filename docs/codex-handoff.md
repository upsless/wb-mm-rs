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
  notes must stay as exact numbered quotes unless the user explicitly says
  otherwise.
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
  `src/dbus/logics.rs` for DBus/domain mapping helpers and
  `src/mqtt/schema.rs` for MQTT topic/control schema helpers.
- Production logs should stay quiet: startup, shutdown, important state
  transitions, and unrecoverable conditions. Debug logs can be more detailed.

## Current Architecture State

- `main.rs` owns compact reconnect supervision:
  - MQTT is the top-level lifecycle gate;
  - DBus runs only while MQTT is connected;
  - DBus and MQTT reconnect use fast/slow retry intervals;
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
- `ModemManagerStatus` is only `Active | Inactive`; DBus object disappearance
  is represented by `ManagerDeleted`.
- MQTT-facing modem numbering starts from `1` even when DBus modem ids start
  from `0`. DBus ids stay internal; MQTT device names are user-facing, e.g.
  `mm_modem_1`.

## Current Exchange Vocabulary

- DBus events:
  - `ManagerFound { version, modem_count }`
  - `ManagerUpdated(ManagerUpdate)`
  - `ManagerDeleted`
  - `ModemFound { modem_id, ...flat modem fields... }`
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
  - `SmsPropertyChange::{IsReceived, Timestamp, Number, Text}`

## MQTT State Model

MQTT runtime state lives in `src/mqtt/state.rs`.

- `MqttSessionState` owns session-level MQTT state:
  - main device creation flag;
  - modem map;
  - reverse modem index map;
  - per-modem SMS control creation/subscription sets;
  - cached manager-level SMS count.
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
- The authoritative SMS order comes from the modem `Messages` property after
  stripping `/org/freedesktop/ModemManager1/SMS/` and sorting by numeric short
  ids.
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
    `selected_sms_sender`, `selected_sms_is_received`, `selected_sms_text`;
  - `delete_message` pushbutton for the currently displayed SMS.
- Timestamp controls are published in pairs: visible text timestamp and hidden
  readonly `unixtime` payload for machine consumers.

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
  - if `displayed_sms_id` is still in the list, set `picked_sms_index` to that
    SMS position and do not request a new snapshot;
  - otherwise clamp the current `picked_sms_index`, compute the picked SMS id,
    and request a snapshot if that picked id differs from current
    `displayed_sms_id`;
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
- Tests were moved out of `state.rs` into `src/mqtt/state/tests.rs`.
- Latest local checks after these changes passed:
  - `cargo fmt --check`
  - `cargo clippy --all-targets --all-features`
  - `cargo test` (20 tests)

## Live Verification Notes

- Live DBus monitoring on `wb.loc` showed hot-plug SMS inventory arriving as a
  burst of `Messages` changes before `registered`: the modem appears with
  `Messages=[]`, later emits `Messages=[90]`, `Messages=[91,90]`, and so on,
  then transitions through `enabled` toward `registered`.
- Initial SMS snapshot and MQTT-to-DBus selection flow were verified earlier.
- Live SMS add/delete/change events still need verification on a working SIM;
  the current SIM was operator-blocked during previous testing.

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
  stored as exact numbered quotes.
- `docs/dev-workflow.md` - machine/Git workflow.
- `docs/reference-wb-mm.md` - reference project notes and findings.
- `src/exchange.rs` - DBus/MQTT/tresher event and command vocabulary.
- `src/dbus/logics.rs` - DBus/domain ids, snapshots, updates, parsers, and log
  message helpers.
- `src/dbus/loop.rs` - DBus runtime, ModemManager discovery, SMS watchers, and
  method calls.
- `src/mqtt/schema.rs` - MQTT device/control schema, topic builders, metadata,
  and log message helpers.
- `src/mqtt/state.rs` - MQTT session, modem, and SMS state machines.
- `src/mqtt/state/tests.rs` - unit tests for MQTT state behavior.
- `src/mqtt/loop.rs` - MQTT runtime, command handling, publishing, cleanup, and
  user write parsing.
- `src/tresher.rs` - thin dispatcher between DBus and MQTT.
- `.agents/skills/modemmanager-mqtt-review/SKILL.md` - local Codex skill.

## Next Likely Work

1. Continue reducing MQTT SMS handler complexity:
   - review `apply_sms_deleted`, `pick_modem_sms`, and `delete_picked_sms` for
     the same state/frontend split used in `handle_sms_list`;
   - consider clearer method names after behavior settles.
2. Re-check the stale `displayed_sms_id` policy after more cleanup:
   - current rule is "only accepted snapshots change it";
   - if the selected-SMS fields are cleared because the list is empty or the
     displayed SMS disappeared, decide whether an explicit state method should
     also set `displayed_sms_id=None`.
3. Verify live SMS add/delete/change behavior on a working SIM.
4. Add focused tests around reconnect/lifecycle ordering where practical.
5. Keep WB MQTT semantics and Last Will behavior intact while tightening topic
   metadata and UI details.

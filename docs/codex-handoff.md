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

The implemented daemon currently has three long-lived async subsystems plus a
shared vocabulary:

- DBus handler: ModemManager discovery, DBus events, and DBus method calls.
- Core handler: cross-subsystem routing and the future home for policy and
  command execution.
- MQTT handler: Wiren Board device/control creation, value publishing, user
  writes, cleanup, and Last Will setup.
- `src/domain.rs`: shared DBus->MQTT events, MQTT->DBus commands, and the
  neutral domain types used by both subsystems.
- `src/common.rs`: only small cross-runtime helpers such as
  `wait_for_shutdown()`.

Today DBus and MQTT already run as independent long-lived lifecycle loops.
MQTT no longer owns DBus task lifetime; instead, each fresh MQTT session asks
DBus for a resync so frontend state is rebuilt from a new DBus discovery pass.

That is still not the final architecture. The next major direction is to
introduce a Core runtime that keeps DBus-side command handling alive even
while MQTT is down and becomes the real policy/command owner between DBus and
MQTT.

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
  - DBus and MQTT are both started as independent long-lived tasks;
  - DBus reconnect intervals live in `src/dbus.rs`;
  - MQTT reconnect intervals live in `src/mqtt.rs`;
  - on DBus session failure, current code maps loss to `ManagerDeleted` until
    the bus returns;
  - on each fresh MQTT session start, MQTT asks DBus for a resync by
    restarting the current DBus session;
  - while MQTT is disconnected, the MQTT lifecycle loop drops queued DBus
    events instead of preserving a stale backlog.
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
  - `src/dbus/modem.rs` owns modem watchers, modem DBus proxy work, modem
    property streams, and modem snapshot/update emission;
  - `src/dbus/sms.rs` owns SMS inventory watching, single-SMS watching, and
    DBus commands for SMS refresh/delete;
  - `src/domain.rs` owns the shared daemon domain types and DBus/MQTT exchange
    enums;
  - `src/dbus/schema.rs` replaced the old `src/dbus/logics.rs` name and now
    holds DBus-specific mappings, parsers, signal specs, and ids/path helpers;
  - `src/dbus/logstrings.rs` centralizes DBus log targets and message text.
- MQTT runtime is similarly layered:
  - `src/mqtt.rs` now owns the long-lived MQTT reconnect loop plus
    session-level resync requests into DBus;
  - `src/mqtt/loop.rs` owns the low-level rumqtt event loop polling;
  - `src/mqtt/frontend.rs` owns MQTT-side DBus event handling, user writes,
    and command emission into Core;
  - `src/mqtt/publish.rs` owns retained publish/cleanup helpers and publisher
    state;
  - `src/mqtt/state.rs` owns frontend state.
- `src/dbus.rs` now owns the long-lived DBus reconnect loop plus explicit
  resync-triggered session restarts requested by MQTT.
- `src/core.rs` now exists as the first replacement for the old `tresher`
  idea:
  - DBus emits `DbusEvent` into Core;
  - Core forwards those events into the long-lived MQTT lifecycle;
  - MQTT sends `DbusCommand` into Core;
  - Core forwards those commands to the currently active DBus session sender.
  This Core layer is still intentionally small, but it is no longer purely
  transparent: it already revalidates outgoing `SendSms` requests and returns
  a failed outgoing-SMS event to MQTT when such a request should not reach
  DBus. It also now tracks per-modem incoming SMS inventory membership and
  automatically requests snapshots for newly observed SMS ids, while still
  forwarding all SMS-related traffic to MQTT unchanged.

## Planned Core-Centric Direction

The next architectural step is no longer "more MQTT controls first". It is a
lifecycle inversion:

- DBus + Core must become the always-on command plane;
- MQTT must become an optional projection/control adapter that may disconnect
  and reconnect independently.

The planned Core layer should own:

- operational configuration/state loading and persistence;
- authorization for outbound SMS/calls;
- handling of command SMS and future DTMF commands;
- audit logging for privileged operations.

## Current Core Command Slice

Core now owns the first real inbound SMS command:

- `#help`
- reply text: `Commands:\n#help [command]`

Current behavior:

- Core recognizes `#help` from incoming SMS snapshots;
- authorized senders are currently configured with repeated CLI flags:
  `--command-number <phone>`;
- Core queues an outbound SMS reply only for authorized senders;
- Core queues deletion of handled command SMS for both authorized and
  unauthorized `#help` attempts;
- with no `--command-number` values, `#help` is effectively disabled because
  no sender is authorized;
- Core suppresses `SmsSnapshot` forwarding for handled `#help` traffic, but
  list/inventory churn may still briefly reach MQTT before deletion lands.

### Number Lists

The old single whitelist idea is now split into two separate lists:

1. `command list`
   - numbers allowed to issue Core-level commands through SMS and later DTMF;
   - Core may also send SMS replies and place confirmation calls to these
     numbers;
   - this list should not be exposed through MQTT.
2. `send list`
   - numbers that MQTT-driven automation is allowed to use as outbound
     destinations;
   - this list may be visible in MQTT so scripts can discover valid targets.

The lists may overlap arbitrarily.

There must be one default number in each list:

- `default_command_number`
- `default_send_number`

If defaults are missing, the daemon may still start, but it should enter a
degraded mode where:

- Core command handling is disabled;
- outbound SMS/calls are rejected before any DBus action.

### Visibility and Routing Rules

- Ordinary user-facing incoming SMS continue to flow into MQTT.
- Core treats only `#...` traffic as a candidate for kernel-level commands.
- After `#`, Core matches the command name at the beginning of the SMS/DTMF
  payload.
- If the command name is known:
  - authorized numbers get execution or syntax/help feedback from Core;
  - unauthorized numbers produce an audit event and do not reach MQTT.
- If the command name is unknown, that `#...` traffic is not a Core command
  and may continue through ordinary MQTT-side policy.
- Future command calls / DTMF sessions should follow the same split.
- MQTT may optionally receive only a coarse incoming-controller status for
  command calls, not command payload/details.
- Outbound requests initiated from MQTT must be checked against the send list
  in Core even if the UI already disables invalid actions.
- Acceptance of ordinary non-command SMS/calls into MQTT should be a separate
  policy switch, independent from command recognition/authorization.

### Persistence and Logging

- No database is planned for the command/configuration layer.
- TOML is the current preferred direction for both static configuration and
  dynamic operational state.
- Command execution, authorization failures, and list-changing operations
  should be logged under a dedicated target such as `AUDIT` or `CORE`, with an
  optional separate audit log file.
- Command-list publication into MQTT should also be configurable:
  - completely disabled;
  - published but hidden;
  - published and visible as readonly controls.

## Current Shared Vocabulary

- `src/domain.rs` now holds the shared DBus/MQTT vocabulary and neutral domain
  types:
  - `DbusEvent` from DBus into Core and then into MQTT;
  - `DbusCommand` from MQTT into Core and then into DBus;
- `src/common.rs` now only keeps shared runtime helpers:
  - `wait_for_shutdown()` used by supervisor, DBus, and MQTT lifecycles.
- DBus events:
  - `ManagerFound { version, modem_count }`
  - `ManagerUpdated(ManagerUpdate)`
  - `ManagerDeleted`
  - `ModemFound { modem_id, info: ModemInfo }`
  - `ModemUpdated { modem_id, update }`
  - `ModemDeleted { modem_id }`
  - `SmsInventorySnapshot { modem_id, entries }`
  - `SmsListChanged { modem_id, entries }`
  - `SmsSnapshot { modem_id, snapshot }`
  - `SmsPropertyChanged { modem_id, update }`
  - `SmsDeleted { modem_id, sms_id }`
  - `OutgoingSmsUpdated { modem_id, info }`
- DBus commands:
  - `RefreshSms { modem_id, sms_id }`
  - `DeleteSms { modem_id, sms_id }`
  - `SendSms { modem_id, recipient, text }`
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
- `MqttModemSmsState` owns the SMS selection model:
  - `sms_order: Vec<SmsId>`;
  - `picked_sms_index: u32` as a 1-based UI position;
  - `last_published_sms_id: Option<SmsId>` as the DBus id whose snapshot was
    last accepted and published into selected-SMS fields.

Unit tests for MQTT state live in `src/mqtt/state/tests.rs`, not inline inside
`state.rs`.

## SMS Behavior

- DBus starts a separate SMS inventory watcher only when modem state allows SMS
  inventory (`enabled` or later). This avoids the hot-plug burst where
  `Messages` changes before the modem reaches a stable usable state.
- DBus emits one `SmsInventorySnapshot` with factual inventory `entries`
  (`sms_id + timestamp`), then live `SmsListChanged` updates with the same
  entry shape.
- DBus keeps lightweight per-modem inventory metadata so timestamp is queried
  only for newly added SMS ids and removed ids are dropped from that metadata
  cache.
- MQTT/frontend sorts SMS inventory by receive timestamp and uses DBus short id
  only as a tie-breaker. This replaced the old assumption that numeric DBus id
  order matched arrival order; the modem may reuse the first free SMS number
  after deletion, so a newly received SMS can appear in an older numeric slot.
- A worked-through architecture variant for that rewrite is:
  - DBus inventory emits factual SMS entries (at least `sms_id + timestamp`);
  - DBus keeps a lightweight per-modem inventory metadata cache so timestamp is
    queried only for newly added SMS ids, while removed ids are dropped from the
    cache;
  - MQTT/frontend performs UI ordering by `(timestamp, dbus_id)`;
  - `timestamp=None` is treated as oldest;
  - modem-level `last_received_sms_dbus_id` is derived from that ordered view.
  This remains the current low-risk evolution path.
- A more radical alternative is also under consideration:
  - DBus reads full `SmsSnapshot` data for every SMS during inventory
    initialization and keeps a per-modem truth cache;
  - MQTT consumes rich initial inventory plus incremental upsert/delete-style
    updates instead of requesting selection-time snapshots;
  - live cache maintenance could be implemented either with one
    `PropertiesChanged` watcher per SMS object or with one shared low-level
    `PropertiesChanged` signal stream for all SMS objects.
  This looks architecturally attractive, but it is a riskier subsystem rewrite
  and should be treated as a deliberate future refactor, not the next
  incremental change.
- Per-modem SMS controls are created lazily on the first SMS inventory command.
  Before that, SMS controls for the modem should not exist.
- Empty SMS inventory after initialization publishes:
  - `sms_count=0`;
  - `last_received_sms_dbus_id=null`;
  - `message_select` readonly with `min=1 max=1 value=1`;
  - selected-SMS fields as `null`/`0`;
  - `delete_message` readonly.
- The manager MQTT device publishes aggregate incoming-SMS count as `sms_count`.
  It does not publish a best-effort "last SMS timestamp".
- Each modem MQTT device publishes:
  - `sms_count`;
  - `last_received_sms_dbus_id`;
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
  `last_received_sms_dbus_id` must be chosen from receive-time ordering rather
  than from `max(dbus_id)`.

### SMS Selection Rules

- `SmsSnapshot` is accepted only when `snapshot.sms_id` equals the DBus id at
  `sms_order[picked_sms_index - 1]`.
- Accepting a snapshot records `last_published_sms_id = snapshot.sms_id`,
  publishes selected-SMS fields, publishes `displayed_sms_index`, and enables
  delete.
- Snapshots for any other SMS id are ignored by MQTT.
- Live `SmsUpdate` is applied to visible MQTT fields only when
  `last_published_sms_id == update.sms_id`.
- User writes to `message_select/on` update `picked_sms_index`, map the index to
  `sms_order[picked_sms_index - 1]`, and request a fresh snapshot only when the
  effective clamped index changes.
- User writes to `delete_message/on` delete `last_published_sms_id`, i.e. the
  SMS currently visible to the user. Ordinary DBus deletion events drive MQTT
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
  1-based position of `last_published_sms_id` inside `sms_order`, or `null` if
  the last published SMS is no longer in the list.
- `last_published_sms_id` is changed only by accepting a snapshot. List
  changes and deletion handling may make it invalid for the current list, but
  they do not clear it directly.

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
- `src/domain.rs` - shared DBus/MQTT event-command vocabulary and neutral
  domain types.
- `src/common.rs` - shared runtime helpers such as `wait_for_shutdown()`.
- `src/dbus/schema.rs` - DBus/domain ids, snapshots, updates, parsers, and
  mappings.
- `src/dbus/logstrings.rs` - DBus log target and DBus-side log message text.
- `src/dbus/connection.rs` - DBus connection, top-level select loop,
  and shutdown/command-channel integration.
- `src/dbus/runtime.rs` - DBus-specific runtime wrapper: DBus proxy,
  ModemManager owner/status stream, and delegation into manager logic.
- `src/dbus/manager.rs` - ModemManager-specific state, streams, activation,
  modem collection, and manager-level command/event handling.
- `src/dbus/modem.rs` - modem watchers, modem proxy setup, modem property
  streams, modem snapshot/update emission, and SMS inventory start/stop
  integration.
- `src/dbus/sms.rs` - SMS inventory watcher, single-SMS watcher, SMS queries,
  and SMS refresh/delete DBus commands.
- `src/mqtt.rs` - MQTT session lifecycle, frontend startup, graceful stop, DBus
  event intake from Core, DBus resync requests, graceful stop, and shutdown
  handling.
- `src/mqtt/schema.rs` - MQTT device/control schema, topic builders, metadata,
  and payload helpers.
- `src/mqtt/logstrings.rs` - MQTT log target and MQTT-side log message text.
- `src/mqtt/state.rs` - MQTT session, modem, and SMS state machines.
- `src/mqtt/state/tests.rs` - unit tests for MQTT state behavior.
- `src/mqtt/loop.rs` - low-level rumqtt event loop polling and incoming
  publish forwarding.
- `src/mqtt/frontend.rs` - DBus event handling, frontend/business decisions,
  user write parsing, command emission into Core, and state orchestration.
- `src/core.rs` - transparent Core pass-through loop between DBus and MQTT; the
  future home for whitelist policy and command SMS/DTMF handling.
- `src/mqtt/publish.rs` - MQTT retained publishing, cleanup, metadata sync, and
  publication-only state.
- `.agents/skills/modemmanager-mqtt-review/SKILL.md` - local Codex skill.

## Next Likely Work

1. Define the first persistent TOML model for:
   - `command list`;
   - `send list`;
   - `default_command_number`;
   - `default_send_number`;
   - degraded-mode semantics when defaults are absent.
2. Continue teaching the new Core layer real policy:
   - start using the new Core-owned incoming SMS snapshots for command
     recognition/parsing;
   - decide what must become Core-owned state before business rules land;
   - extend Core-side validation from current outgoing-SMS format checks toward
     whitelist-based outbound authorization.
3. Add the first information-only Core surface before role-management commands:
   - expose send-list facts to MQTT for script visibility;
   - keep command-list facts private to Core;
   - prepare audit logging target/file plumbing.
4. Observe the new timestamp-based inventory ordering on a live modem:
   - verify that first selection after startup now always comes through the
     ordinary `RefreshSms` path;
   - verify that deleting an SMS and then receiving a new SMS in the reused
     numeric slot still produces correct receive-time ordering.
5. Consider whether DBus should keep sending full inventory entry lists or move
   later to incremental add/remove inventory events once behavior is stable.
6. Revisit whether `last_received_sms_dbus_id` should remain the final user
   visible control, or whether it should later be complemented by a visible
   "Last Received SMS Date" plus hidden unix-time control.
7. Validate and polish the new modem-level outgoing SMS flow on real hardware:
   - per modem, writable compose controls now exist for recipient and text plus
     a `send_sms` trigger button;
   - readonly result controls now show `last_sent_sms_status`,
     `last_sent_sms_timestamp`, hidden unix-time, `last_sent_sms_recipient`,
     and a one-line `last_sent_sms_text`;
   - outgoing SMS handling is still intentionally separate from the current
     incoming inventory / picker model;
   - confirm that the ModemManager `Create` + `Send` path behaves correctly on
     the target modem and that `sending -> sent/failed` is sufficient for the
     first release.
8. After outgoing SMS, move on to incoming call signaling/handling.
9. Figure out how to persist the SMS storage choice that worked manually:
   `AT+CPMS="ME","ME","ME"` caused the modem to rediscover today's SMS after a
   reboot-like transition, but the daemon should not rely on manual console
   state.
10. Continue reducing MQTT SMS handler complexity:
   - review `apply_sms_deleted`, `pick_modem_sms`, and `delete_picked_sms` for
     the same state/frontend split used in `handle_sms_list`;
   - consider clearer method names after behavior settles.
11. Re-check the stale `last_published_sms_id` policy after more cleanup:
   - current rule is "only accepted snapshots change it";
   - if the selected-SMS fields are cleared because the list is empty or the
     displayed SMS disappeared, decide whether an explicit state method should
     also set `last_published_sms_id=None`.
12. Verify live SMS add/delete/change behavior on a working SIM.
13. Add focused tests around reconnect/lifecycle ordering where practical.
14. Keep WB MQTT semantics and Last Will behavior intact while tightening topic
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
- SMS logic has now been split further out of `src/dbus/modem.rs` into
  `src/dbus/sms.rs`, leaving modem property watching and SMS watching as
  separate modules under the DBus subtree.
- The old manager-level `SmsCommandRegistry` was removed. `RefreshSms` now
  routes directly to the right `ModemWatcher`, which tracks its own SMS command
  channel internally.
- DBus shutdown now clears active modem watchers via `ManagerWatcher::reset()`
  instead of relying on detached task bookkeeping in the outer loop.
- The old `tresher` layer is not coming back verbatim, but `src/core.rs` is now
  the new architectural seam for that class of work:
  - DBus emits `DbusEvent` into Core;
  - Core forwards into MQTT;
  - MQTT emits `DbusCommand` back into Core;
  - Core forwards to the currently active DBus session sender.
  At the moment it is still a small pass-through-oriented layer, not yet a full
  policy engine, but outgoing SMS revalidation has already moved there.
- SMS inventory timestamp caching intentionally remains in `src/dbus/sms.rs`:
  it is still coupled directly to the modem `Messages` property stream and the
  local DBus-side cache, so moving it into Core now would add another
  request/response seam without reducing complexity.
- `wait_for_shutdown()` now lives in `src/common.rs` and is shared by main,
  MQTT, and DBus lifecycle code.

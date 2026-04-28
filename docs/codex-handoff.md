# Codex Handoff

Use this file to restore context when opening the project in a new workspace or
starting a new Codex chat.

## Project

- Repository: `upsless/wb-mm-rs`
- Purpose: clean Rust daemon for Wiren Board ModemManager integration.
- Reference fork available to GitHub connector: `upsless/wb-mm-mqtt`.
- Upstream reference: `AbyssDiggers/wb-mm-mqtt`.
- Test target: `wb.loc`; development machines can reach MQTT and DBus there.
- Canonical MQTT device/control conventions reference:
  `https://github.com/wirenboard/conventions/blob/main/README.md`
- Wiren Board MQTT wiki reference for topic semantics and `/on` command flow:
  `https://wiki.wirenboard.com/wiki/MQTT`

## Current Direction

Build a focused daemon, not a general-purpose framework. The old Python project
is reference material for behavior, logs, mappings, and edge cases, but its
architecture should not be copied.

### Agreed Next Refactor

The next structural refactor is already agreed and should be treated as the
new target vocabulary:

- DBus/tresher speak only in domain terms:
  - `ManagerFound { version, modem_count }`
  - `ManagerUpdated(ManagerUpdate)`
  - `ManagerDeleted`
  - `ModemFound { modem_id, ...flat modem fields... }`
  - `ModemUpdated { modem_id, update }`
  - `ModemDeleted { modem_id }`
  - `Sms*` events stay as they are now
- `ManagerUpdate` should contain only real manager facts:
  - `Status(Active | Inactive)`
  - `Version(String)`
  - `ModemCount(usize)`
- `sms_count` for the top-level device is synthetic and must stay MQTT-side.
- `CreateMainDevice` / `DeleteMainDevice` are MQTT-local lifecycle actions
  only. They must not appear in `exchange.rs` or leak into DBus/tresher.
- `ManagerDeleted` is distinct from `ManagerUpdated(Status(Inactive))`:
  - `Inactive` means ModemManager is still known on DBus but not active;
  - `Deleted` means the ModemManager object disappeared from DBus entirely.
- The public MQTT projection should be:
  - one stable main device representing the gateway itself;
  - ModemManager state displayed inside that main device;
  - separate modem devices;
  - SMS controls inside modem devices.
- The top-level MQTT device should be presented as
  `ModemManager Gateway (MMG)` / `Шлюз ModemManager (MMG)`.
- Per-modem device titles should be `MMG Modem #N` / `Модем MMG №N`.
- Keep the existing MQTT topic schema unless the user explicitly asks to
  rename device/control ids. Current agreement covers titles and semantics,
  not a forced topic-path migration.

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
- Treat `wirenboard/conventions` README as the source of truth for MQTT
  device/control shape, metadata, control types, and compatibility details.
- Preserve old `wb-mm-mqtt` Last Will semantics: if the daemon dies,
  ModemManager must become unavailable in the UI/control model. The public
  MQTT `is_available` control is the single user-facing trust marker: it must
  become `0` both when the daemon dies unexpectedly and when ModemManager is
  inactive or deleted from DBus.
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
  - on DBus session failure, the current code maps the loss to
    `ManagerDeleted` until the bus connection returns;
  - on MQTT loss, DBus is stopped first, and after MQTT reconnect both
    subsystems start again from a clean slate.
- In a plain terminal, `Ctrl+C` works as expected. In VS Code CodeLLDB debug
  sessions, `Ctrl+C` in the debug terminal is unreliable and may kill the
  process before graceful shutdown logs appear.
- For VS Code debugging, the reliable shutdown path is `Shift+F5` / `Stop`
  with `gracefulShutdown: "SIGTERM"` in the local `.vscode/launch.json`.
  If graceful shutdown hangs, use `Stop` again to force termination.
- Stage 0 now distinguishes three manager situations:
  - `Active`
  - `Inactive`
  - deleted / missing from DBus as a separate event, not a status value
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
  - the tresher is now intended to stay a thin router and should not own
    MQTT-projection-only concepts such as the main MMG device lifecycle.
- The MQTT side is no longer a stub:
  - it connects to a real broker through `rumqttc`;
  - it publishes retained WB device/control topics for one stable main device
    (presented as `ModemManager Gateway (MMG)`) and for per-modem devices;
  - it clears those retained topics on normal shutdown;
  - it sets Last Will on the top-level availability control so unexpected
    daemon death still makes the service unavailable in UI/control terms.
- The current domain vocabulary is:
  - manager events: `ManagerFound { version, modem_count }`,
    `ManagerUpdated(ManagerUpdate)`, `ManagerDeleted`
  - modem events: `ModemFound { modem_id, ...flat fields... }`,
    `ModemUpdated { modem_id, update }`, `ModemDeleted`
  - SMS events stay inventory/snapshot/update/delete based
- `ModemManagerStatus` is now only `Active | Inactive`; disappearance of the
  DBus object is represented by `ManagerDeleted`.
- MQTT-only lifecycle operations such as creating or deleting the stable MMG
  device must stay inside the MQTT layer and must not leak into `exchange.rs`.
- SMS support is now partially implemented end-to-end:
  - DBus does not subscribe to `Modem.Messaging.Messages` during the initial
    modem snapshot; once modem state reaches `enabled` or later, it starts a
    separate SMS inventory watcher, subscribes to `Messages`, reads the full SMS
    list, and emits one `SmsInventorySnapshot` before live SMS changes;
  - the initial `SmsInventorySnapshot` now carries `sms_ids` plus one
    `initial_sms_snapshot`; when that snapshot matches the current
    `picked_sms_id`, MQTT commits it through the same snapshot-apply path as an
    ordinary `SmsSnapshot`, so the selected-SMS fields, picker value, and
    `delete_message` button stay in sync from the very first render; otherwise
    MQTT requests a fresh snapshot for the current target;
  - DBus now keeps only one tracked SMS watcher per modem. The initial
    `initial_sms_snapshot` or any later `RefreshSms` request retargets that
    watcher to the currently requested SMS;
  - modem MQTT devices publish only base modem controls at creation time; the
    per-modem SMS controls (`sms_count`, `last_sms_dbus_id`, `message_select`,
    selected-SMS fields, and `delete_message`) are created lazily on the first
    SMS-facing MQTT command produced from `SmsInventorySnapshot`;
  - the authoritative SMS order comes from the modem `Messages` property after
    stripping the constant `/org/freedesktop/ModemManager1/SMS/` prefix and
    sorting by the resulting numeric short ids;
  - SMS ownership cleanup is implemented: tresher is now a thin router for SMS
    facts and commands, while MQTT owns `sms_order`, per-modem target
    `picked_sms_id`, the last committed selected-SMS payload, and last-published
    SMS control values;
  - MQTT-side human selection numbering starts from `1`, and MQTT maps it back
    to the ordered DBus short-id list. `SmsId` is the stable identity;
    `message_select` is only the derived UI index;
  - when the SMS list changes, MQTT should keep the current `picked_sms_id` if
    that DBus id still exists, even if its UI index changes. If the picked id
    disappeared, MQTT should choose a replacement by preserving the old
    position, falling back to the new last SMS, or clearing selection if the
    list is empty, then request a fresh snapshot for the replacement;
  - empty SMS behavior after the first `SmsInventorySnapshot`: create the SMS
    controls, publish `last_sms_dbus_id=null`, `sms_count=0`, `message_select`
    as `readonly=true min=1 max=1 value=1`, selected-SMS fields as `null`/`0`,
    and make `delete_message` readonly because there is nothing to delete.
    Before the first SMS inventory snapshot, per-modem SMS controls should not
    exist at all;
  - manager MQTT device now publishes only aggregate incoming-SMS count
    (`sms_count`) and no longer publishes a best-effort "last SMS timestamp";
  - each modem device publishes `last_sms_dbus_id`, incoming-SMS count
    (`sms_count`), writable `message_select` without a visible title,
    selected-SMS fields (`selected_sms_dbus_id`, `selected_sms_timestamp`,
    hidden `selected_sms_timestamp_unixtime`, `selected_sms_sender`,
    `selected_sms_is_received`, `selected_sms_text`), and a
    `delete_message` pushbutton for the currently picked SMS;
  - modem `is_active` is `1` only when raw ModemManager state is `enabled` or
    later (`6..=11`); before that the modem device may exist but is not treated
    as active;
  - DBus and dispatcher still pass actual SMS timestamps as `OffsetDateTime`
    inside selected-SMS snapshots; MQTT formats visible text controls and
    publishes paired hidden `meta/type=unixtime` controls with integer unix
    time payloads. All `unixtime` controls are marked `hidden=true` in both
    JSON meta and compatibility meta subtopics;
  - MQTT publishes SMS-facing state changes immediately; hot-plug SMS inventory
    churn is handled by the DBus-side SMS bootstrap, not by MQTT-side batching;
  - user writes to `message_select/on` are handled by MQTT: it translates the
    user-facing index to a DBus `SmsId`, updates per-modem `picked_sms_id`, and
    requests a fresh `SmsSnapshot` through
    `MQTT -> Tresher -> DBUS -> Tresher -> MQTT`; selected-SMS fields are
    published first, and `message_select` is published last as the effective
    commit marker for the new selection;
  - `SmsSnapshot` or `SmsUpdated` for SMS other than the current
    `picked_sms_id` are ignored by MQTT after receipt; they may still retarget
    DBus-side tracking or satisfy a later request, but they do not immediately
    affect the visible selected-SMS controls;
  - user writes to `delete_message/on` delete the currently visible committed
    selected SMS id via
    `org.freedesktop.ModemManager1.Modem.Messaging.Delete(path)`; ordinary
    DBus deletion events then drive MQTT cleanup/reselection;
  - when `picked_sms_id` changes, MQTT immediately republishes
    `delete_message` as readonly; once the target `SmsSnapshot` commits, MQTT
    republishes the button as writable again, so delete availability follows
    the explicit picker transition flow instead of being derived from cached
    flags;
  - after the delete button is pressed, MQTT keeps it readonly until the
    currently displayed committed SMS `dbus_id` changes, so repeated deletes
    cannot target the same still-visible message.
- Live verification on `wb.loc` confirmed:
  - manager topics publish `sms_count`;
  - modem topics publish SMS count and selected-SMS fields;
  - after the short-id rewrite, the initial selected SMS is the first element
    of the ordered `Messages` list (`message_select=1`);
  - writing a new value to `/devices/mm_modem_1/controls/message_select/on`
    updates the selected-SMS MQTT topics.
- The current MQTT-owned SMS selection/commit model is compile- and
  unit-tested, but still needs a live retest on `wb.loc` after this handoff
  update.
- Live DBus monitoring on `wb.loc` confirmed that hot-plug SMS inventory arrives
  as a burst of `Messages` changes before `registered`: the modem appears with
  `Messages=[]`, later emits `Messages=[90]`, `Messages=[91,90]`, ...,
  `Messages=[116..90]`, then transitions through `enabled` toward
  `registered`. This is why SMS bootstrap is gated on `enabled` and published
  as a separate inventory snapshot.
- Live SMS add/delete/change events could not yet be tested against the modem
  because the current SIM is operator-blocked; only initial snapshot and
  MQTT-to-DBus selection flow are verified so far.
- MQTT-facing modem numbering now starts from `1` even if the DBus modem id is
  `0`. The daemon keeps the DBus id internally and maps it to user-facing WB
  device names such as `mm_modem_1`.
- Current logging split for stage 0.2:
  - `info`: meaningful DBus-side ModemManager events and MQTT-side command
    execution results;
  - `debug`: tresher-internal event/command routing and lower-level
    lifecycle details.
- Current log formatting uses explicit component targets, mirroring the python
  project's style more closely:
  `MAIN`, `DBUS`, `MQTT`, and `DISP`.
- SMS timestamp controls are published in pairs: visible text controls with
  payload like `YYYY-MM-DD HH:MM:SS`, and hidden read-only `unixtime` controls
  with integer unix time payloads for machine consumers.

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
     `unixexec:path=ssh,argv1=-T,argv2=-q,argv3=root@wb.loc,argv4=systemd-stdio-bridge`.
2. Build out stage 0.2 from manager-level exchange to richer mappings:
   - real MQTT publishing is in place, but topic/control semantics may still
     need small alignment passes against WB UI expectations;
   - keep the `is_available` / Last Will cleanup intact: only one public trust
     switch for ModemManager;
   - live SMS runtime events (new, deleted, partial-to-complete) still need
     verification on a working SIM;
   - keep the event/command types compact and reviewable as the DBus/MQTT
     surface grows.
3. Implement stage 1:
   - build from the now-working MQTT + DBus + ModemManager device baseline;
   - correct MQTT updates on modem connect/disconnect;
   - correct behavior when ModemManager service is stopped, started, or removed
     on `wb.loc`;
   - focused unit tests.

## Roadmap

1. ModemManager device with version/modem count and service/modem change tests.
2. Per-modem device with basic characteristics, DBus/MQTT/daemon failure
   behavior, cleanup, Last Will checks, and unit tests.
3. SMS data hardening: live runtime event verification, edge cases for partial
   messages, and unit tests.
4. MQTT-to-DBus user actions hardening and unit tests.

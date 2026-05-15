# Architecture Notes

## Goal

Build a specialized Rust daemon for WirenBoard devices that integrates
ModemManager with the standard WirenBoard MQTT device/control model.

The daemon should cover the practical use case:

- discover ModemManager and modems;
- publish state to MQTT;
- update state from DBus events;
- observe user control changes from MQTT;
- call DBus methods for requested actions;
- clean up created MQTT entities on shutdown.

## Non-Goal

Do not recreate the old `wb-mm-mqtt` universal library architecture. The old
project is valuable as a reference for behavior, logging style, DBus/MQTT
mapping ideas, and cleanup semantics, but not as a structural template.

## Main Async Components

### DBus Handler

- Connects to ModemManager.
- Performs initial discovery and state loading.
- Subscribes to DBus events.
- Executes DBus method calls requested by the MQTT frontend.
- Emits domain events directly into the MQTT session.

Current DBus implementation is split by responsibility:

- `src/dbus/connection.rs` owns the outer DBus lifecycle: connect to the bus, race
  connection and runtime work against shutdown, run the top-level event loop,
  and stop cleanly when the bus/session fails or shutdown is requested.
- `src/dbus/runtime.rs` owns the DBus-specific orchestration layer:
  `DbusRuntime` keeps the `org.freedesktop.DBus` proxy, the ModemManager
  owner-change stream, the DBus event sender, and the embedded
  `ManagerWatcher`.
- `src/dbus/manager.rs` owns ModemManager-specific runtime state and behavior:
  manager presence, active manager streams, discovered modem watchers, manager
  event handling, and manager-level DBus command routing.
- `src/dbus/modem.rs` owns modem watcher work: per-modem proxy setup, modem
  property streams, modem snapshot/update emission, and SMS watcher startup.
- `src/dbus/sms.rs` owns SMS watcher work: SMS inventory watching, single-SMS
  watching, and DBus SMS method calls.
- `src/dbus/schema.rs` owns DBus-specific vocabulary: object/interface names,
  signal specs, DBus path helpers, and DBus-side mappings.
- `src/dbus/logstrings.rs` owns DBus log targets and DBus-side log message
  text.

The outer loop deliberately remains in `connection.rs`: it is the process-level
control flow for the DBus subsystem. `DbusRuntime` provides the next
manager-related DBus event and handles it, but the outer loop still decides
when to stop for shutdown, closed command channel, or unrecoverable DBus
failure.

The modem and SMS watcher logic currently live together in `src/dbus/modem.rs`.
That file is intentionally a holding area after extracting the large block from
the old DBus top-level file; it can be split further into modem- and
SMS-specific modules once
the behavior boundary is clearer.

### MQTT Handler

- Creates WirenBoard devices and controls.
- Publishes initial metadata and values.
- Applies DBus events to the frontend projection and publishes the result.
- Observes user writes to writable controls.
- Emits DBus commands directly to the current DBus session sender.
- Removes or marks generated entities on daemon shutdown, according to the
  chosen WirenBoard behavior.
- Sets MQTT Last Will so that an unexpected daemon stop marks ModemManager as
  unavailable in the UI/control model.

Current MQTT implementation is also split by responsibility:

- `src/mqtt.rs` owns MQTT session lifecycle: option building, Last Will setup,
  frontend startup, graceful stop, and integration of the MQTT event loop with
  DBus event intake, DBus command watch updates, and shutdown handling.
- `src/mqtt/loop.rs` owns the low-level rumqtt event loop polling and forwards
  incoming publishes into the frontend pipeline.
- `src/mqtt/frontend.rs` owns MQTT-side DBus event handling, user-write
  processing, and direct DBus command emission.
- `src/mqtt/publish.rs` owns retained publish/cleanup helpers and
  publication-only state.
- `src/mqtt/state.rs` owns the frontend state model.

Current modem-level outgoing SMS support intentionally stays separate from the
incoming SMS picker model:

- MQTT exposes per-modem compose controls (`outgoing_sms_recipient`,
  `outgoing_sms_text`, `send_sms`) and readonly "last sent" controls above the
  incoming SMS controls only when the daemon is started with
  `--allow-outgoing-sms`;
- without `--allow-outgoing-sms`, outgoing SMS controls are not created at all
  and outgoing SMS status events are not projected into MQTT;
- DBus executes `Create` + `Send` on the modem messaging interface and emits
  outgoing SMS status updates back into MQTT;
- the first implementation treats outgoing SMS as a separate action/result
  channel rather than merging it into the incoming SMS inventory.

### Shared Vocabulary

- `src/domain.rs` owns the shared cross-subsystem domain vocabulary.
- `DbusEvent` flows from DBus into MQTT.
- `DbusCommand` flows from MQTT back into DBus.
- `src/common.rs` now stays deliberately small and holds only truly shared
  runtime helpers such as `wait_for_shutdown()`.

The current mental model is:

```text
DBus events -> MQTT frontend/state -> DBus commands
```

## SMS Ordering Options

SMS ordering is no longer allowed to rely on raw DBus SMS ids alone: the modem
may reuse a freed numeric SMS slot, so `max(dbus_id)` is not a reliable "most
recent SMS" criterion.

Two architecture variants are currently considered valid:

1. DBus-side full SMS snapshot cache:
   - DBus reads all fields for all SMS during modem inventory initialization;
   - DBus keeps an in-memory full snapshot and emits rich incremental updates;
   - MQTT consumes already complete SMS data and no longer needs selection-time
     snapshot requests.
2. DBus-side inventory facts, MQTT-side ordering:
   - DBus reads at least `sms_id` plus receive timestamp for every SMS in the
     inventory;
   - DBus keeps lightweight per-modem inventory metadata so timestamp is read
     only for newly added SMS ids, while removed ids are dropped from that
     metadata cache;
   - DBus sends those facts without imposing UI-specific order;
   - MQTT/frontend sorts inventory entries for presentation using
     `(timestamp, dbus_id)`, treating `timestamp=None` as oldest;
   - `last_received_sms_dbus_id` is then derived on the MQTT side from that
     receive-time ordering.

At the moment, variant 2 is the more developed design option because it keeps
UI ordering responsibility in MQTT while still giving the frontend enough facts
to sort correctly. Variant 1 remains open for later choice because it may offer
useful operational benefits despite a slower initial inventory load.

### Full-Cache Evolution Path

If the daemon moves to variant 1, the most promising shape is a DBus-side
truth cache per modem:

- initial inventory load reads full `SmsSnapshot` data for every SMS object;
- DBus stores `HashMap<SmsId, SmsSnapshot>` plus inventory membership/order
  metadata;
- MQTT receives initial full inventory plus incremental upsert/delete-style
  events;
- the current `RefreshSms` request/response path can then disappear or shrink
  drastically.

This is attractive because once inventory initialization already needs one
DBus round-trip per SMS object for timestamp-aware ordering, switching from
"fetch one field" to "fetch full snapshot" becomes much less expensive
architecturally, while removing a large amount of asynchronous selection-time
complexity from MQTT.

The risky part is live cache maintenance. Two sub-variants are worth keeping in
mind:

1. One `PropertiesChanged` subscription per SMS object:
   - simpler object-local update logic;
   - still only one watcher per SMS, not one watcher per property;
   - scales with the number of SMS objects.
2. One shared low-level `PropertiesChanged` signal stream for all SMS objects:
   - one central event loop updates the whole SMS cache;
   - avoids a per-SMS watcher set;
   - requires more manual DBus signal parsing and filtering.

This full-cache direction is promising but should be treated as a deliberate,
riskier architectural refactor rather than the next incremental patch. The
current low-risk evolution path remains variant 2:

- DBus emits inventory entries/facts;
- MQTT sorts them for UI use;
- MQTT requests a snapshot only for the currently needed SMS.

## Core Command Plane (Planned)

The daemon is evolving from a pure DBus<->MQTT bridge into a small control
core that can keep enforcing modem policy even when MQTT is unavailable.

The next architecture step is to add an explicit core layer with three
responsibilities:

- load and persist operational settings;
- enforce number-based policy for outgoing actions and remote commands;
- execute privileged SMS/DTMF command flows independently of MQTT health.

The intended shape is:

```text
DBus adapter <-> Core runtime <-> MQTT adapter
```

Where:

- DBus remains the source of truth for modem, SMS, call, and DTMF events;
- Core decides whether an event is a user-facing modem event or an internal
  command event;
- MQTT becomes an optional frontend that can disconnect without disabling the
  command plane.

### Number Lists

The current whitelist idea is being split into two independent lists:

1. `command list`
   - numbers allowed to control daemon behavior through SMS commands or future
     DTMF commands;
   - Core may send SMS replies and place confirmation calls to these numbers;
   - these numbers should not be exposed in MQTT.
2. `send list`
   - numbers that MQTT-driven automation is allowed to use as SMS/call
     destinations;
   - these numbers may be exposed through MQTT so automation scripts can see
     where user-driven outbound traffic is allowed.

The two lists may intersect arbitrarily. They are separate policy domains, not
different flags on one visible MQTT list.

Each list has one default number:

- `default_command_number`
- `default_send_number`

The defaults may point to the same physical phone number, but both concepts
must exist in configuration/state. If either default is missing, the daemon
may still start, but it should enter a degraded mode:

- command SMS/DTMF handling is disabled;
- outbound SMS/calls are rejected before any real DBus action.

### Command Traffic Handling

Incoming modem traffic is no longer assumed to be always user-facing.

Planned rule:

- ordinary incoming SMS continue through the current DBus -> MQTT projection;
- command SMS are intercepted by Core, authorized, executed, logged, and then
  deleted without reaching MQTT;
- future command calls / DTMF sessions are also handled at Core level;
- MQTT may optionally receive only a coarse status such as "incoming
  controller" for command calls, but not command payload/details.

This keeps the operational command channel alive even when the MQTT frontend is
down or intentionally unavailable.

### Persistent State

No database is planned for this configuration layer.

The preferred direction is:

- static configuration in TOML;
- dynamic operational state in TOML as well, or a very similar simple file;
- atomic rewrite on change.

This is intended to hold at least:

- command-list numbers;
- send-list numbers;
- their defaults;
- future provenance/role metadata needed for command-side list management.

### Audit Logging

Command execution, authorization failures, and list-changing operations should
be logged separately from ordinary device chatter.

Planned approach:

- use a dedicated log target such as `AUDIT` or `CORE`;
- optionally mirror that target to a separate configured log file;
- keep normal runtime logging quiet while still preserving an operator-visible
  audit trail.

## Lifecycle Model

### Current Implementation

MQTT is still the primary lifecycle gate in the implemented code. If MQTT is
unavailable, DBus work is fully stopped.

Runtime shape today:

```text
connect MQTT
  -> publish meta / set Last Will
  -> start DBus handler
  -> run until MQTT disconnect
  -> stop DBus handler
  -> drop live runtime state
  -> retry MQTT
```

Consequences today:

- When MQTT is disconnected, do not keep DBus subscriptions alive.
- Do not queue DBus events while MQTT is down.
- After MQTT reconnect, publish metadata again and perform fresh DBus discovery.
- If DBus is lost while MQTT is connected, keep MQTT alive, mark ModemManager as
  unavailable, retry DBus, and republish fresh state after DBus recovery.

The current top-level supervisor still lives in `main.rs`, but subsystem
lifecycle details now live one layer lower:

- `src/mqtt.rs::run_lifecycle()` owns one MQTT session from connect to stop;
- `src/dbus.rs::run_lifecycle()` owns DBus reconnect behavior while that MQTT
  session is alive.

### Planned Evolution

The current MQTT-gated lifecycle is no longer sufficient once command SMS and
DTMF must keep working without MQTT.

The planned direction is:

```text
load config/state
  -> start Core runtime
  -> start DBus adapter and keep it alive
  -> start MQTT adapter if available
  -> keep Core + DBus alive across MQTT loss/reconnect
  -> stop everything only on daemon shutdown or fatal Core failure
```

Consequences of the planned model:

- DBus becomes part of the always-on command plane, not a child of MQTT;
- MQTT reconnects no longer force DBus/Core teardown;
- command SMS/DTMF remain functional while MQTT is disconnected;
- MQTT becomes a projection/control adapter, not the owner of runtime truth;
- outbound actions requested from MQTT are still re-validated inside Core
  before DBus method calls.

The first lifecycle refactor should therefore aim at introducing an explicit
Core runtime and then flipping the supervision order from:

```text
MQTT -> DBus
```

to:

```text
Core + DBus -> optional MQTT
```

## Availability Semantics

The ModemManager availability control is not merely a cached DBus property. It
represents whether the daemon is alive and able to manage ModemManager, observe
new SMS, and execute modem-related actions.

The old `wb-mm-mqtt` project deliberately used MQTT Last Will to force this
availability state to false/unavailable when the daemon disconnects
unexpectedly. That behavior must be preserved. The exact new topic and payload
should be chosen deliberately:

- keep the UI-visible availability signal obvious;
- avoid leaving stale "available" state after daemon death;
- consider also publishing conventional `/meta/error` state if it helps
  consumers that follow WirenBoard conventions strictly.

## Mapping Files

The project should preserve the useful idea from `mqtt_logics.py` and
`dbus_logics.py`: bindings between DBus entities and MQTT devices/controls
should live in compact, easy-to-review mapping definitions.

Current Rust naming uses `schema.rs` for these compact vocabularies:

- `src/domain.rs` for shared domain ids, snapshots, updates, and DBus/MQTT
  exchange enums;
- `src/dbus/schema.rs` for DBus-specific constants, signal specs, and path
  helpers;
- `src/mqtt/schema.rs` for MQTT topic/control schema and payload helpers.

Logging text now lives alongside each subsystem in dedicated files:

- `src/dbus/logstrings.rs` for DBus-side log messages and log target names;
- `src/mqtt/logstrings.rs` for MQTT-side log messages and log target names.

Prefer typed data structures or small declarative config over ad hoc string
manipulation.

## Logging

Use `tracing` for structured logs.

Production logging should be quiet after the daemon is debugged:

- startup;
- clean shutdown;
- unhandled errors;
- important unrecoverable conditions.

Development logging should be very detailed, at least as useful as
`wb-mm-mqtt` logs:

- MQTT connect/disconnect/reconnect;
- DBus connect/disconnect/reconnect;
- device/control creation and cleanup;
- Last Will setup;
- DBus discovery and property/event handling;
- frontend state decisions and emitted DBus commands.

The logging level should be configurable so normal production operation does
not produce chatty traces, while development can enable full trace/debug output.

## MQTT Naming

Use current WirenBoard naming conventions for new topics:

- device and control topic names should be lowercase;
- separate words with underscores;
- avoid punctuation and special characters;
- do not carry over old names such as `mm-modem-1`, `IsAvailable`,
  `ModemsCount`, or `SignalQuality` unless an explicit compatibility mode is
  added.

The new daemon should probably expose names shaped like `modemmanager`,
`mm_modem_1`, `is_available`, `modems_count`, and `signal_quality`, with final
names chosen as part of the mapping design.

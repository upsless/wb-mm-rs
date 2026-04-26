# Architecture Notes

## Goal

Build a specialized Rust daemon for Wiren Board devices that integrates
ModemManager with the standard Wiren Board MQTT device/control model.

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
- Executes DBus method calls requested by the dispatcher.
- Emits domain events to the dispatcher.

### MQTT Handler

- Creates Wiren Board devices and controls.
- Publishes initial metadata and values.
- Publishes value updates from dispatcher commands.
- Observes user writes to writable controls.
- Emits user actions to the dispatcher.
- Removes or marks generated entities on daemon shutdown, according to the
  chosen Wiren Board behavior.
- Sets MQTT Last Will so that an unexpected daemon stop marks ModemManager as
  unavailable in the UI/control model.

### Dispatcher

- Owns high-level daemon state.
- Receives events from DBus and MQTT handlers.
- Applies business rules.
- Sends commands to DBus and MQTT handlers.

The initial mental model is:

```text
DBus events + MQTT user actions -> dispatcher state -> DBus/MQTT commands
```

## Lifecycle Model

MQTT is the primary lifecycle gate. If MQTT is unavailable, the daemon is not
useful: there is nowhere to publish state and nowhere to receive user commands
from. In this state, DBus work must be fully stopped.

Runtime shape:

```text
connect MQTT
  -> publish meta / set Last Will
  -> start dispatcher
  -> start DBus handler
  -> run until MQTT disconnect
  -> stop DBus handler
  -> stop dispatcher runtime session
  -> drop live runtime state
  -> retry MQTT
```

Consequences:

- When MQTT is disconnected, do not keep DBus subscriptions alive.
- Do not queue DBus events while MQTT is down.
- After MQTT reconnect, publish metadata again and perform fresh DBus discovery.
- If DBus is lost while MQTT is connected, keep MQTT alive, mark ModemManager as
  unavailable, retry DBus, and republish fresh state after DBus recovery.

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
  consumers that follow Wiren Board conventions strictly.

## Mapping Files

The project should preserve the useful idea from `mqtt_logics.py` and
`dbus_logics.py`: bindings between DBus entities and MQTT devices/controls
should live in compact, easy-to-review mapping definitions.

The exact Rust representation is still open. Prefer typed data structures or
small declarative config over ad hoc string manipulation.

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
- dispatcher decisions and emitted commands.

The logging level should be configurable so normal production operation does
not produce chatty traces, while development can enable full trace/debug output.

## MQTT Naming

Use current Wiren Board naming conventions for new topics:

- device and control topic names should be lowercase;
- separate words with underscores;
- avoid punctuation and special characters;
- do not carry over old names such as `mm-modem-1`, `IsAvailable`,
  `ModemsCount`, or `SignalQuality` unless an explicit compatibility mode is
  added.

The new daemon should probably expose names shaped like `modemmanager`,
`mm_modem_1`, `is_available`, `modems_count`, and `signal_quality`, with final
names chosen as part of the mapping design.

## Implementation Roadmap

### Stage 1: ModemManager Device

- MQTT + DBus + ModemManager device only.
- Publish ModemManager version and modem count.
- Verify correct MQTT updates when:
  - a modem is disconnected or connected;
  - ModemManager service is stopped, started, or removed on the Wiren Board.
- Add focused unit tests.

### Stage 2: Modem Device

- Add per-modem MQTT device with basic modem characteristics.
- Verify behavior when DBus fails.
- Verify behavior when MQTT fails: DBus work must stop completely until MQTT
  returns.
- Verify daemon shutdown behavior:
  - normal shutdown cleans up devices/controls correctly;
  - daemon crash triggers correct Last Will availability state.
- Add focused unit tests.

### Stage 3: SMS Data

- Add SMS summary data:
  - total SMS count;
  - last SMS time in relevant devices.
- Add SMS viewing in the modem device.
- Add focused unit tests.

### Stage 4: MQTT-To-DBus Actions

- Add user-initiated control writes from MQTT.
- Route user actions through the dispatcher into DBus method calls.
- Add focused unit tests.

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

---
name: modemmanager-mqtt-review
description: Use for reviewing or implementing the Rust WirenBoard ModemManager daemon, especially tasks involving ModemManager DBus, WirenBoard MQTT devices/controls, reference behavior from wb-mm-mqtt, logging, and daemon architecture.
---

# ModemManager MQTT Review

## Purpose

Review and implement changes for a focused Rust daemon that bridges
ModemManager DBus state/actions to WirenBoard MQTT devices and controls.

Use `upsless/wb-mm-mqtt` as reference code only. Preserve useful behavior and
mapping ideas, but do not copy its general-purpose library architecture.

## Trigger Notes

Use this skill not only for feature work, but also when the user asks for a
code review of "strange", "itchy", or suspicious implementation details. In
that review mode, prefer an architectural smell pass before hunting for style
nits.

## Rules

- Preserve WirenBoard MQTT topic schema unless explicitly requested.
- DBus service destination is `org.freedesktop.ModemManager1`.
- ObjectManager path is `/org/freedesktop/ModemManager1`.
- Prefer `zbus` typed proxies where they keep the code clearer.
- Keep DBus destination, path, interface, member, and error context explicit.
- Keep mappings compact and reviewable, similar in spirit to the old
  `mqtt_logics.py` and `dbus_logics.py`.
- After Rust edits run `cargo fmt --check`, `cargo clippy`, and `cargo test`
  where applicable.
- Never read `.env` or deployment secrets.

## Smell Checklist

When the user asks to "check the code for strange things" or similar, start by
looking for these specific smells:

- functions or variables that have only one real call site and do not buy
  readability;
- functions with too many similar parameters that are good candidates for a
  struct, config object, or method receiver;
- "bags of functions" imported from modules where a small struct or owned
  surface would be clearer than function-by-function wiring;
- `if` or `match` branches that are functionally impossible or only exist
  because the data shape is awkward;
- constants scattered through the file instead of grouped near the top.

Treat the first three as readability and ownership smells, and the fourth as a
possible modeling bug, not just a style issue.

## Architecture Reminder

The intended daemon has three async parts:

- DBus handler: discovery, event handling, and method calls.
- MQTT handler: WirenBoard devices/controls, value updates, user writes, and
  cleanup.
- Dispatcher: business logic and high-level command routing.

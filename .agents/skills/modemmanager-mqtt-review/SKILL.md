---
name: modemmanager-mqtt-review
description: Use for reviewing or implementing the Rust Wiren Board ModemManager daemon, especially tasks involving ModemManager DBus, Wiren Board MQTT devices/controls, reference behavior from wb-mm-mqtt, logging, and daemon architecture.
---

# ModemManager MQTT Review

## Purpose

Review and implement changes for a focused Rust daemon that bridges
ModemManager DBus state/actions to Wiren Board MQTT devices and controls.

Use `upsless/wb-mm-mqtt` as reference code only. Preserve useful behavior and
mapping ideas, but do not copy its general-purpose library architecture.

## Rules

- Preserve Wiren Board MQTT topic schema unless explicitly requested.
- DBus service destination is `org.freedesktop.ModemManager1`.
- ObjectManager path is `/org/freedesktop/ModemManager1`.
- Prefer `zbus` typed proxies where they keep the code clearer.
- Keep DBus destination, path, interface, member, and error context explicit.
- Keep mappings compact and reviewable, similar in spirit to the old
  `mqtt_logics.py` and `dbus_logics.py`.
- After Rust edits run `cargo fmt --check`, `cargo clippy`, and `cargo test`
  where applicable.
- Never read `.env` or deployment secrets.

## Architecture Reminder

The intended daemon has three async parts:

- DBus handler: discovery, event handling, and method calls.
- MQTT handler: Wiren Board devices/controls, value updates, user writes, and
  cleanup.
- Dispatcher: business logic and high-level command routing.

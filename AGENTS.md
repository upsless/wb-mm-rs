# Project Agent Notes

This repository is the clean starting point for a Rust daemon for Wiren Board
ModemManager integration.

## Operating Rules

- Keep the implementation focused on the target daemon. Do not recreate a
  general-purpose framework from the reference project.
- Treat `upsless/wb-mm-mqtt` as reference code only.
- Do not read `.env`, secrets, keys, tokens, or private deployment files.
- Prefer small, reviewable diffs.
- Before broad refactors, explain the planned file-level changes.
- After Rust code edits, run `cargo fmt --check`, `cargo clippy`, and
  `cargo test` where applicable.
- Do not change the Wiren Board MQTT topic schema unless explicitly requested.
- For DBus code, preserve explicit destination, path, interface, and error
  context.

## Repository Topology

- New project: this repository, planned as `upsless/wb-mm-rs`.
- Reference fork available to the GitHub connector: `upsless/wb-mm-mqtt`.
- Upstream reference owned by AbyssDiggers: `AbyssDiggers/wb-mm-mqtt`.
- The OpenAI GitHub connector currently should use only the `upsless` fork, not
  the AbyssDiggers organization repository.

## Development Target

- Test Wiren Board host: `wb.loc`.
- Development machines can access DBus and MQTT on `wb.loc` directly.
- Project state is synchronized through GitHub, not a shared VM.

## Architecture Sketch

The intended daemon has three async parts:

- DBus backend: initial discovery, DBus event handling, ModemManager method
  calls.
- Wiren Board MQTT frontend: device/control creation, initial value publishing,
  user control change observation, and cleanup on shutdown.
- Dispatcher/business logic: receives events, owns high-level state decisions,
  and sends commands to DBus or MQTT handlers.

Reference mappings from the old project should be captured as compact
configuration or mapping files, similar in spirit to `mqtt_logics.py` and
`dbus_logics.py`, but without carrying over the old universal-library design.

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
- For new MQTT devices and controls, use current Wiren Board naming style:
  lowercase words separated by underscores. Do not copy old CamelCase control
  names or hyphenated device names from `wb-mm-mqtt` unless compatibility is
  explicitly required.
- For DBus code, preserve explicit destination, path, interface, and error
  context.
- Preserve the old project's Last Will semantics: if the daemon disappears,
  ModemManager must be treated as unavailable for UI/control purposes. The
  MQTT availability control is a daemon capability marker, not just a cached
  DBus value.

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

Important reference behavior: the old project uses MQTT Last Will to force the
ModemManager availability control into an unavailable state when the daemon
dies. Keep this behavior in the new design, even if the exact topic/payload is
reworked to better fit current Wiren Board conventions.

Reference mappings from the old project should be captured as compact
configuration or mapping files, similar in spirit to `mqtt_logics.py` and
`dbus_logics.py`, but without carrying over the old universal-library design.

Known reference bug: `wb-mm-mqtt` modem cleanup appears to call
`mqtt_del_control()` with the wrong argument count in `mqtt_delete_modem()`.
Do not copy that implementation; keep it as a possible upstream/fork fix.

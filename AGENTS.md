# Project Agent Notes

This repository is the clean starting point for a Rust daemon for WirenBoard
ModemManager integration.

## Operating Rules

- Keep the implementation focused on the target daemon. Do not recreate a
  general-purpose framework from the reference project.
- Treat `upsless/wb-mm-mqtt` as reference code only.
- Do not read `.env`, secrets, keys, tokens, or private deployment files.
- Prefer small, reviewable diffs.
- When the user is discussing or investigating a problem rather than directly
  asking for implementation, first explain the likely cause and propose the
  intended fix. Do not edit code until the user explicitly approves the fix.
- If an external development or diagnostic tool that materially improves
  productivity or accuracy is missing, stop the investigation and ask the user
  to install it before building a custom workaround, unless the user explicitly
  asks for a workaround. The request must include the tool name, a short
  description of what it does, links to fuller documentation, available
  alternatives, and the drawbacks of those alternatives. Example: if `rg`
  (ripgrep, https://github.com/BurntSushi/ripgrep) is missing, explain that it
  is a fast recursive code search tool that respects `.gitignore`; `grep` is an
  available fallback, but it is slower and needs more manual filtering in large
  repositories. If the user refuses to install a requested tool, record that
  refusal in `docs/codex-handoff.md` and avoid requesting the same tool again.
- Before broad refactors, explain the planned file-level changes.
- If the user says that a change should be committed only after confirmation
  (for example, "commit only if I confirm" or "if I confirm, add it for future
  agents"), do not commit or push until the user explicitly grants that
  permission in a later message.
- Before any commit intended to be pushed to GitHub, review
  `docs/codex-handoff.md` and update it if the commit changes project context,
  decisions, workflow, known issues, or next steps.
- Do not modify `docs/arcnotes.md` unless the user explicitly asks to change
  that file. When adding notes there, preserve the user's text as an exact
  quote in the same language unless the user explicitly asks for a summary or
  another form. Add notes sequentially with an ordinal number, do not rewrite
  previous notes, and use Russian by default unless the user specifies another
  language.
- After Rust code edits, run `cargo fmt --check`, `cargo clippy`, and
  `cargo test` where applicable.
- Do not change the WirenBoard MQTT topic schema unless explicitly requested.
- For new MQTT devices and controls, use current WirenBoard naming style:
  lowercase words separated by underscores. Do not copy old CamelCase control
  names or hyphenated device names from `wb-mm-mqtt` unless compatibility is
  explicitly required.
- For DBus code, preserve explicit destination, path, interface, and error
  context.
- Preserve the old project's Last Will semantics: if the daemon disappears,
  ModemManager must be treated as unavailable for UI/control purposes. The
  public MQTT `is_available` control is the user-facing trust marker: it must
  go to `0` both when DBus says ModemManager data is not trustworthy and when
  the daemon disappears unexpectedly via Last Will.
- For Rust code that uses async/Tokio or other non-obvious "Rusty"
  constructions, prefer adding concise rustdoc comments on public items and
  short inline comments around non-obvious control flow. Write them so a
  threads-first reader can follow the code without reverse-engineering every
  async primitive.

## Repository Topology

- New project: this repository, planned as `upsless/wb-mm-rs`.
- Reference fork available to the GitHub connector: `upsless/wb-mm-mqtt`.
- Upstream reference owned by AbyssDiggers: `AbyssDiggers/wb-mm-mqtt`.
- The OpenAI GitHub connector currently should use only the `upsless` fork, not
  the AbyssDiggers organization repository.

## Development Target

- Test WirenBoard host: `wb.loc`.
- Development machines can access DBus and MQTT on `wb.loc` directly.
- Project state is synchronized through GitHub, not a shared VM.

## Architecture Sketch

The intended daemon has three async parts:

- DBus backend: initial discovery, DBus event handling, ModemManager method
  calls.
- WirenBoard MQTT frontend: device/control creation, initial value publishing,
  user control change observation, and cleanup on shutdown.
- Tresher/business logic: receives events, owns high-level state decisions,
  and sends commands to DBus or MQTT handlers.

Important reference behavior: the old project uses MQTT Last Will to force the
ModemManager availability control into an unavailable state when the daemon
dies. Keep this behavior in the new design, even if the exact topic/payload is
reworked to better fit current WirenBoard conventions.

Reference mappings from the old project should be captured as compact
configuration or mapping files, similar in spirit to `mqtt_logics.py` and
`dbus_logics.py`, but without carrying over the old universal-library design.

Known reference bug: `wb-mm-mqtt` modem cleanup appears to call
`mqtt_del_control()` with the wrong argument count in `mqtt_delete_modem()`.
Do not copy that implementation; keep it as a possible upstream/fork fix.

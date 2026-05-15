# wb-mm-mqtt Reference Notes

Reference repositories:

- Connector-accessible fork: `upsless/wb-mm-mqtt`
- Upstream: `AbyssDiggers/wb-mm-mqtt`

The reference project is useful for:

- logging style;
- WirenBoard MQTT device/control creation behavior;
- primary metadata/value population;
- update behavior from DBus;
- cleanup behavior on daemon shutdown;
- MQTT Last Will behavior for daemon failure;
- compact DBus and MQTT mapping concepts from files such as `mqtt_logics.py`
  and `dbus_logics.py`.

The reference project should not be copied structurally. It became difficult to
maintain because a set of very general libraries grew around a narrow practical
task. The new daemon should stay specialized and explicit.

Open questions to resolve from the reference code:

- exact MQTT topics and retained/non-retained behavior for generated entities;
- cleanup semantics expected by WirenBoard;
- DBus event coverage needed for modem discovery, SMS, USSD, and calls;
- logging structure worth preserving;
- compact representation for DBus-to-MQTT bindings in Rust.

Known issue in the Python implementation:

- `mqtt_delete_modem()` appears to call `mqtt_del_control(target,
  modem_mqtt_path, control)`, but `mqtt_del_control()` accepts only
  `(target, control_path)`. Modem deletion may therefore fail and leave retained
  topics behind. Do not copy this cleanup implementation into Rust; consider
  fixing it later in the reference fork if useful.

Naming note:

- `wb-mm-mqtt` uses old-style topic/control names such as `mm-modem-1`,
  `IsAvailable`, `ModemsCount`, and `SignalQuality`.
- The new Rust daemon should use current WirenBoard conventions:
  lowercase names with underscores and no punctuation/special characters,
  unless explicit backward compatibility is required.

## Important Reference Behavior: Last Will Availability

In `wb-mm-mqtt`, daemon death is intentionally surfaced through MQTT Last Will:
if the daemon disappears, ModemManager is no longer available for management,
new SMS observation, or modem operations from the WirenBoard UI. The old code
therefore uses Last Will on the ModemManager availability control instead of
treating availability as a normal last-known-good sensor value.

This is a project-specific operational rule, not an accidental conventions
violation. Future implementations should preserve the behavior: stale
`available=true` state after daemon death is worse than losing the last cached
availability value.

For the Rust daemon, decide the exact representation explicitly. A likely shape:

- publish a retained availability control for the UI;
- set Last Will to the unavailable value for that control;
- optionally also publish `/devices/<device>/meta/error` to satisfy consumers
  that expect conventional WirenBoard error topics.

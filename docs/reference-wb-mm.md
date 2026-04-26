# wb-mm-mqtt Reference Notes

Reference repositories:

- Connector-accessible fork: `upsless/wb-mm-mqtt`
- Upstream: `AbyssDiggers/wb-mm-mqtt`

The reference project is useful for:

- logging style;
- Wiren Board MQTT device/control creation behavior;
- primary metadata/value population;
- update behavior from DBus;
- cleanup behavior on daemon shutdown;
- compact DBus and MQTT mapping concepts from files such as `mqtt_logics.py`
  and `dbus_logics.py`.

The reference project should not be copied structurally. It became difficult to
maintain because a set of very general libraries grew around a narrow practical
task. The new daemon should stay specialized and explicit.

Open questions to resolve from the reference code:

- exact MQTT topics and retained/non-retained behavior for generated entities;
- cleanup semantics expected by Wiren Board;
- DBus event coverage needed for modem discovery, SMS, USSD, and calls;
- logging structure worth preserving;
- compact representation for DBus-to-MQTT bindings in Rust.

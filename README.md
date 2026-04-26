# wb-mm-rs

Clean starting point for a Rust daemon that exposes ModemManager state and
actions to Wiren Board MQTT devices and controls.

The old `wb-mm-mqtt` project is used only as reference material. This project
will keep the final daemon narrow: detect modems and messages on a Wiren Board
device, publish state to MQTT, and route user actions back to ModemManager over
DBus.

Current repository contents are intentionally documentation-only. Rust code will
be added after the initial architecture and workflow are agreed.

use crate::dbus::{ModemId, ModemManagerStatus, ModemSnapshot, ModemUpdate};

/// Stage-0.2 events emitted by the DBus side into the tresher.
///
/// The manager-level part stays intentionally small, following the old python
/// project where ModemManager itself had only a few event shapes and most of
/// the detail lived in the payload. The modem-level part mirrors that idea but
/// uses typed Rust structs instead of a kwargs bag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbusEvent {
    StatusChanged(ModemManagerStatus),
    Snapshot {
        version: String,
        modem_count: usize,
    },
    ModemCountChanged {
        modem_count: usize,
    },
    ModemFound {
        modem_id: ModemId,
    },
    ModemSnapshot {
        modem_id: ModemId,
        snapshot: ModemSnapshot,
    },
    ModemUpdated {
        modem_id: ModemId,
        update: ModemUpdate,
    },
    ModemDeleted {
        modem_id: ModemId,
    },
}

/// Commands produced by the tresher and consumed by the MQTT side.
///
/// MQTT is still a stub, so for now these commands only turn into structured
/// logs. The shape is already close to what a real MQTT frontend will need.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttCommand {
    EnsureModemManagerDevice,
    PublishModemManagerStatus(ModemManagerStatus),
    PublishModemManagerVersion(String),
    PublishModemManagerModemCount(usize),
    EnsureModemDevice {
        modem_id: ModemId,
    },
    PublishModemSnapshot {
        modem_id: ModemId,
        snapshot: ModemSnapshot,
    },
    PublishModemUpdate {
        modem_id: ModemId,
        update: ModemUpdate,
    },
    DeleteModemDevice {
        modem_id: ModemId,
    },
}

use crate::dbus::ModemManagerStatus;

/// Stage-0.2 events emitted by the DBus side into the dispatcher.
///
/// We keep the manager-level event set intentionally small for now, following
/// the old python project where ModemManager itself had only a few event
/// shapes and most of the detail lived in the payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbusEvent {
    StatusChanged(ModemManagerStatus),
    Snapshot { version: String, modem_count: usize },
    ModemCountChanged { modem_count: usize },
}

/// Commands produced by the dispatcher and consumed by the MQTT side.
///
/// MQTT is still a stub, so for now these commands only turn into structured
/// logs. The shape is already close to what a real MQTT frontend will need.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttCommand {
    EnsureModemManagerDevice,
    PublishModemManagerStatus(ModemManagerStatus),
    PublishModemManagerVersion(String),
    PublishModemManagerModemCount(usize),
}

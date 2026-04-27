use crate::dbus::{
    ModemId, ModemManagerStatus, ModemSnapshot, ModemUpdate, SmsId, SmsSnapshot, SmsUpdate,
};

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
    SmsSnapshot {
        modem_id: ModemId,
        sms_id: SmsId,
        snapshot: SmsSnapshot,
    },
    SmsListChanged {
        modem_id: ModemId,
        sms_ids: Vec<SmsId>,
    },
    SmsUpdated {
        modem_id: ModemId,
        sms_id: SmsId,
        update: SmsUpdate,
    },
    SmsDeleted {
        modem_id: ModemId,
        sms_id: SmsId,
    },
    SelectedSmsSnapshot {
        modem_id: ModemId,
        sms_id: SmsId,
        snapshot: SmsSnapshot,
    },
}

/// Commands produced by the tresher and consumed by the MQTT side.
///
/// The MQTT frontend now executes these commands against a real broker, but
/// the command set is still intentionally compact: only the currently modeled
/// ModemManager and per-modem controls are covered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttCommand {
    EnsureModemManagerDevice,
    PublishModemManagerStatus(ModemManagerStatus),
    PublishModemManagerVersion(String),
    PublishModemManagerModemCount(usize),
    PublishModemManagerSmsCount(usize),
    PublishModemManagerLastSms(Option<i64>),
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
    PublishModemSmsCount {
        modem_id: ModemId,
        sms_count: usize,
    },
    PublishModemSmsSelection {
        modem_id: ModemId,
        selected_index: Option<u32>,
        max_index: u32,
        writable: bool,
    },
    PublishSelectedSms {
        modem_id: ModemId,
        snapshot: Option<SmsSnapshot>,
    },
    DeleteModemDevice {
        modem_id: ModemId,
    },
}

/// MQTT-originated events that need business-logic decisions before touching
/// DBus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttEvent {
    SelectModemSms {
        modem_id: ModemId,
        selected_index: u32,
    },
}

/// DBus-side actions requested by business logic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbusCommand {
    RefreshSelectedSms { modem_id: ModemId, sms_id: SmsId },
}

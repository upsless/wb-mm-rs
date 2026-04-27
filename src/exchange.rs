use time::OffsetDateTime;

use crate::dbus::{
    ModemId, ModemManagerStatus, ModemSnapshot, ModemUpdate, SmsId, SmsSnapshot, SmsUpdate,
};

/// Events emitted by the DBus loop and consumed by the dispatcher.
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

/// Commands emitted by the dispatcher and executed by the MQTT loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttCommand {
    EnsureModemManagerDevice,
    PublishModemManagerStatus(ModemManagerStatus),
    PublishModemManagerVersion(String),
    PublishModemManagerModemCount(usize),
    PublishModemManagerSmsCount(usize),
    PublishModemManagerLastSms(Option<OffsetDateTime>),
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
    PublishModemLastSms {
        modem_id: ModemId,
        last_sms_timestamp: Option<OffsetDateTime>,
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

/// MQTT writes that need dispatcher validation before DBus calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttEvent {
    SelectModemSms {
        modem_id: ModemId,
        selected_index: u32,
    },
}

/// Commands emitted by the dispatcher and executed by the DBus loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbusCommand {
    RefreshSelectedSms { modem_id: ModemId, sms_id: SmsId },
}

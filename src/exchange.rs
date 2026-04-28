use crate::dbus::{ManagerUpdate, ModemId, ModemUpdate, SmsId, SmsSnapshot, SmsUpdate};

/// Events emitted by the DBus loop and consumed by the dispatcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbusEvent {
    ManagerFound {
        version: String,
        modem_count: usize,
    },
    ManagerUpdated(ManagerUpdate),
    ManagerDeleted,
    ModemFound {
        modem_id: ModemId,
        is_active: bool,
        model: Option<String>,
        revision: Option<String>,
        state: Option<String>,
        primary_sim_slot: Option<u32>,
        operator_name: Option<String>,
        signal_quality: Option<u32>,
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
    SmsInventorySnapshot {
        modem_id: ModemId,
        sms_ids: Vec<SmsId>,
        initial_sms_snapshot: Option<SmsSnapshot>,
    },
}

/// Commands emitted by the dispatcher and executed by the MQTT loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttCommand {
    ManagerFound {
        version: String,
        modem_count: usize,
    },
    ManagerUpdated(ManagerUpdate),
    ManagerDeleted,
    ModemFound {
        modem_id: ModemId,
        is_active: bool,
        model: Option<String>,
        revision: Option<String>,
        state: Option<String>,
        primary_sim_slot: Option<u32>,
        operator_name: Option<String>,
        signal_quality: Option<u32>,
    },
    ModemUpdated {
        modem_id: ModemId,
        update: ModemUpdate,
    },
    PublishSmsInventorySnapshot {
        modem_id: ModemId,
        sms_ids: Vec<SmsId>,
        initial_sms_snapshot: Option<SmsSnapshot>,
    },
    PublishSmsList {
        modem_id: ModemId,
        sms_ids: Vec<SmsId>,
    },
    PublishSmsSnapshot {
        modem_id: ModemId,
        sms_id: SmsId,
        snapshot: SmsSnapshot,
    },
    PublishSmsUpdate {
        modem_id: ModemId,
        sms_id: SmsId,
        update: SmsUpdate,
    },
    PublishSmsDeleted {
        modem_id: ModemId,
        sms_id: SmsId,
    },
    ModemDeleted {
        modem_id: ModemId,
    },
}

/// MQTT writes that need dispatcher validation before DBus calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttEvent {
    RequestSmsSnapshot { modem_id: ModemId, sms_id: SmsId },
    DeleteSms { modem_id: ModemId, sms_id: SmsId },
}

/// Commands emitted by the dispatcher and executed by the DBus loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbusCommand {
    RefreshSms { modem_id: ModemId, sms_id: SmsId },
    DeleteSms { modem_id: ModemId, sms_id: SmsId },
}

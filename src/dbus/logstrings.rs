use super::schema::{
    DbusSignalSpec, ManagerStatus, ManagerUpdate, ModemId, ModemUpdate, SmsId, SmsUpdate,
};
use crate::domain::OutgoingSmsInfo;

pub const LOG_TARGET: &str = "DBUS";

pub const DBUS_CONNECTED_MESSAGE: &str = "Connection established";
pub const DBUS_STOPPED_BEFORE_CONNECT_MESSAGE: &str = "Stopped before connection was established";
pub const DBUS_STOPPED_MESSAGE: &str = "Connection closed";

pub fn dbus_signal_stream_closed_message(signal: DbusSignalSpec) -> String {
    format!(
        "Signal stream closed: {} ({} {} {}.{})",
        signal.id, signal.bus_name, signal.path, signal.interface, signal.member
    )
}

pub fn modemmanager_status_message(status: ManagerStatus) -> &'static str {
    match status {
        ManagerStatus::Active => "ModemManager found on DBus and Active",
        ManagerStatus::Inactive => "ModemManager found on DBus and Inactive",
    }
}

pub fn manager_found_message(version: &str, modem_count: usize) -> String {
    format!("ModemManager data: version={version}, modem_count={modem_count}")
}

pub fn manager_deleted_message() -> &'static str {
    "ModemManager deleted from DBus"
}

pub fn manager_update_message(update: &ManagerUpdate) -> String {
    format!("ModemManager changed: {}", update.summary())
}

pub fn modem_deleted_message(modem_id: &ModemId) -> String {
    format!("Modem {} deleted from DBus", modem_id.0)
}

pub fn modem_update_message(modem_id: &ModemId, update: &ModemUpdate) -> String {
    format!("Modem {} changed: {}", modem_id.0, update.summary())
}

pub fn sms_inventory_snapshot_message(modem_id: &ModemId, sms_count: usize) -> String {
    format!("Modem {} SMS inventory: sms_count={sms_count}", modem_id.0)
}

pub fn sms_inventory_changed_message(
    modem_id: &ModemId,
    old_sms_count: usize,
    new_sms_count: usize,
    added_sms_ids: &[SmsId],
    removed_sms_ids: &[SmsId],
) -> String {
    let added = format_sms_id_list(added_sms_ids);
    let removed = format_sms_id_list(removed_sms_ids);
    format!(
        "Modem {} SMS inventory changed: sms_count={old_sms_count}->{new_sms_count} added={added} removed={removed}",
        modem_id.0
    )
}

pub fn sms_property_changed_message(modem_id: &ModemId, update: &SmsUpdate) -> String {
    format!(
        "Modem {} SMS {} changed: {}",
        modem_id.0,
        update.sms_id.0,
        update.summary()
    )
}

pub fn sms_deleted_message(modem_id: &ModemId, sms_id: &SmsId) -> String {
    format!("Modem {} SMS {} deleted from DBus", modem_id.0, sms_id.0)
}

pub fn outgoing_sms_update_message(modem_id: &ModemId, info: &OutgoingSmsInfo) -> String {
    format!(
        "Modem {} outgoing SMS changed: {}",
        modem_id.0,
        info.summary()
    )
}

pub fn sms_signal_stream_closed_message(signal_id: &str, object_path: &str) -> String {
    format!("Signal stream closed: {signal_id} ({object_path})")
}

fn format_sms_id_list(sms_ids: &[SmsId]) -> String {
    let ids = sms_ids
        .iter()
        .map(|sms_id| format!("#{}", sms_id.0))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{ids}]")
}

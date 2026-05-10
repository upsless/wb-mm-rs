pub const LOG_TARGET: &str = "MQTT";

pub fn mqtt_connected_message() -> &'static str {
    "Connection established"
}

pub fn mqtt_stopped_message() -> &'static str {
    "Loop stopped"
}

pub fn mqtt_publish_mm_availability_message(is_available: &str) -> String {
    format!("Update ModemManager: is_available={is_available}")
}

pub fn mqtt_publish_mm_version_message(version: &str) -> String {
    format!("Update ModemManager: version={version}")
}

pub fn mqtt_publish_mm_modem_count_message(modem_count: usize) -> String {
    format!("Update ModemManager: modem_count={modem_count}")
}

pub fn mqtt_publish_mm_sms_count_message(sms_count: usize) -> String {
    format!("Update ModemManager: sms_count={sms_count}")
}

pub fn mqtt_publish_modem_snapshot_message(
    modem_index: u32,
    dbus_modem_id: &str,
    snapshot: &str,
) -> String {
    format!("Update modem snapshot: modem={modem_index} dbus_modem_id={dbus_modem_id} {snapshot}")
}

pub fn mqtt_publish_modem_update_message(
    modem_index: u32,
    dbus_modem_id: &str,
    update: &str,
) -> String {
    format!("Update modem update: modem={modem_index} dbus_modem_id={dbus_modem_id} {update}")
}

pub fn mqtt_publish_modem_sms_count_message(
    modem_index: u32,
    dbus_modem_id: &str,
    sms_count: usize,
) -> String {
    format!(
        "Update modem sms_count: modem={modem_index} dbus_modem_id={dbus_modem_id} sms_count={sms_count}"
    )
}

pub fn mqtt_publish_message_select_control_message(
    modem_index: u32,
    dbus_modem_id: &str,
    picked_index: Option<u32>,
    max_index: u32,
    writable: bool,
) -> String {
    format!(
        "Update modem message_select: modem={modem_index} dbus_modem_id={dbus_modem_id} picked_index={} max_index={max_index} writable={writable}",
        picked_index
            .map(|value| value.to_string())
            .unwrap_or_else(|| "None".to_string()),
    )
}

pub fn mqtt_publish_picked_sms_message(
    modem_index: u32,
    dbus_modem_id: &str,
    snapshot_summary: Option<&str>,
) -> String {
    match snapshot_summary {
        Some(snapshot_summary) => format!(
            "Update picked SMS: modem={modem_index} dbus_modem_id={dbus_modem_id} {snapshot_summary}"
        ),
        None => {
            format!("Update picked SMS: modem={modem_index} dbus_modem_id={dbus_modem_id} None")
        }
    }
}

pub fn mqtt_delete_modem_device_message(modem_index: u32, dbus_modem_id: &str) -> String {
    format!("Delete modem device: modem={modem_index} dbus_modem_id={dbus_modem_id}")
}

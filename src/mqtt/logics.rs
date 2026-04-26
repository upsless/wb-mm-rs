pub fn mqtt_connected_message() -> &'static str {
    "MQTT connection established"
}

pub fn mqtt_stopped_message() -> &'static str {
    "MQTT loop stopped"
}

pub fn mqtt_ensure_mm_device_message() -> &'static str {
    "MQTT command: ensure ModemManager device"
}

pub fn mqtt_publish_mm_status_message(status: &str) -> String {
    format!("MQTT command: publish ModemManager status={status}")
}

pub fn mqtt_publish_mm_version_message(version: &str) -> String {
    format!("MQTT command: publish ModemManager version={version}")
}

pub fn mqtt_publish_mm_modem_count_message(modem_count: usize) -> String {
    format!("MQTT command: publish ModemManager modem_count={modem_count}")
}

pub fn mqtt_ensure_modem_device_message(modem_id: &str) -> String {
    format!("MQTT command: ensure modem device modem_id={modem_id}")
}

pub fn mqtt_publish_modem_snapshot_message(modem_id: &str, snapshot: &str) -> String {
    format!("MQTT command: publish modem snapshot modem_id={modem_id} {snapshot}")
}

pub fn mqtt_publish_modem_update_message(modem_id: &str, update: &str) -> String {
    format!("MQTT command: publish modem update modem_id={modem_id} {update}")
}

pub fn mqtt_delete_modem_device_message(modem_id: &str) -> String {
    format!("MQTT command: delete modem device modem_id={modem_id}")
}

/// Well-known DBus service name used by ModemManager.
pub const MM_BUS_NAME: &str = "org.freedesktop.ModemManager1";

/// Small, reviewable state model for stage 0.
///
/// We intentionally keep this separate from the async runtime code so the
/// policy ("how we interpret DBus facts") stays easy to test and read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModemManagerStatus {
    Active,
    Inactive,
    NotFound,
}

pub fn dbus_connected_message() -> &'static str {
    "DBus connection established"
}

pub fn dbus_stopped_before_connect_message() -> &'static str {
    "DBus loop stopped before connection was established"
}

pub fn dbus_stopped_message() -> &'static str {
    "DBus connection closed"
}

pub fn dbus_name_owner_stream_closed_message() -> &'static str {
    "ModemManager DBus owner change stream closed"
}

pub fn modemmanager_status_message(status: ModemManagerStatus) -> &'static str {
    match status {
        ModemManagerStatus::Active => "ModemManager found on DBus and Active",
        ModemManagerStatus::Inactive => "ModemManager found on DBus and Inactive",
        ModemManagerStatus::NotFound => "ModemManager not found on DBus",
    }
}

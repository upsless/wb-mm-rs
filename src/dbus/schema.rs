pub use crate::domain::{
    ManagerStatus, ManagerUpdate, ModemId, ModemInfo, ModemUpdate, SmsId, SmsPropertyChange,
    SmsSnapshot, SmsUpdate,
};

/// Well-known DBus service name used by ModemManager.
pub const DBUS_BUS_NAME: &str = "org.freedesktop.DBus";
pub const DBUS_OBJ_PATH: &str = "/org/freedesktop/DBus";
pub const DBUS_INTERFACE: &str = "org.freedesktop.DBus";
pub const DBUS_PROPERTIES_INTERFACE: &str = "org.freedesktop.DBus.Properties";
pub const DBUS_OBJECT_MANAGER_INTERFACE: &str = "org.freedesktop.DBus.ObjectManager";

pub const MM_BUS_NAME: &str = "org.freedesktop.ModemManager1";
pub const MM_OBJ_PATH: &str = "/org/freedesktop/ModemManager1";
pub const MM_INTERFACE: &str = "org.freedesktop.ModemManager1";
pub const MM_MODEM_INTERFACE: &str = "org.freedesktop.ModemManager1.Modem";
pub const MM_MODEM_MESSAGING_INTERFACE: &str = "org.freedesktop.ModemManager1.Modem.Messaging";
pub const MM_SIM_INTERFACE: &str = "org.freedesktop.ModemManager1.Sim";
pub const MM_SMS_INTERFACE: &str = "org.freedesktop.ModemManager1.Sms";

/// Small, explicit description of a watched DBus signal.
///
/// Keeping this data near the runtime-independent mappings makes it easier to
/// scale later when we start adding modem- and SMS-level signal sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DbusSignalSpec {
    pub id: &'static str,
    pub bus_name: &'static str,
    pub path: &'static str,
    pub interface: &'static str,
    pub member: &'static str,
}

pub const MM_NAME_OWNER_CHANGED_SIGNAL: DbusSignalSpec = DbusSignalSpec {
    id: "mm_name_owner_changed",
    bus_name: DBUS_BUS_NAME,
    path: DBUS_OBJ_PATH,
    interface: DBUS_INTERFACE,
    member: "NameOwnerChanged",
};

pub const MM_VERSION_CHANGED_SIGNAL: DbusSignalSpec = DbusSignalSpec {
    id: "mm_version_changed",
    bus_name: MM_BUS_NAME,
    path: MM_OBJ_PATH,
    interface: DBUS_PROPERTIES_INTERFACE,
    member: "PropertiesChanged",
};

pub const MM_INTERFACES_ADDED_SIGNAL: DbusSignalSpec = DbusSignalSpec {
    id: "mm_interfaces_added",
    bus_name: MM_BUS_NAME,
    path: MM_OBJ_PATH,
    interface: DBUS_OBJECT_MANAGER_INTERFACE,
    member: "InterfacesAdded",
};

pub const MM_INTERFACES_REMOVED_SIGNAL: DbusSignalSpec = DbusSignalSpec {
    id: "mm_interfaces_removed",
    bus_name: MM_BUS_NAME,
    path: MM_OBJ_PATH,
    interface: DBUS_OBJECT_MANAGER_INTERFACE,
    member: "InterfacesRemoved",
};

pub fn modem_id_from_path(path: &str) -> Option<ModemId> {
    let modem_prefix = format!("{MM_OBJ_PATH}/Modem/");
    path.strip_prefix(&modem_prefix)
        .map(|suffix| ModemId(suffix.to_string()))
}

pub fn modem_path_from_id(modem_id: &ModemId) -> String {
    format!("{MM_OBJ_PATH}/Modem/{}", modem_id.0)
}

pub fn sms_id_from_path(path: &str) -> Option<SmsId> {
    let sms_prefix = format!("{MM_OBJ_PATH}/SMS/");
    path.strip_prefix(&sms_prefix)
        .map(|suffix| SmsId(suffix.to_string()))
}

pub fn sms_path_from_id(sms_id: &SmsId) -> String {
    format!("{MM_OBJ_PATH}/SMS/{}", sms_id.0)
}

pub fn modem_state_name(state: i32) -> &'static str {
    match state {
        -1 => "failed",
        0 => "unknown",
        1 => "initializing",
        2 => "locked",
        3 => "disabled",
        4 => "disabling",
        5 => "enabling",
        6 => "enabled",
        7 => "searching",
        8 => "registered",
        9 => "disconnecting",
        10 => "connecting",
        11 => "connected",
        _ => "unknown",
    }
}

pub fn modem_state_is_active(state: i32) -> bool {
    matches!(state, 6..=11)
}

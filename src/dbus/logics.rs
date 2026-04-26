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

/// Initial ModemManager data we want to see at stage 0 before building any
/// MQTT-facing state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModemManagerSnapshot {
    pub version: String,
    pub modem_count: usize,
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

pub fn dbus_signal_stream_closed_message(signal: DbusSignalSpec) -> String {
    format!(
        "ModemManager DBus signal stream closed: {} ({} {} {}.{})",
        signal.id, signal.bus_name, signal.path, signal.interface, signal.member
    )
}

pub fn modemmanager_status_message(status: ModemManagerStatus) -> &'static str {
    match status {
        ModemManagerStatus::Active => "ModemManager found on DBus and Active",
        ModemManagerStatus::Inactive => "ModemManager found on DBus and Inactive",
        ModemManagerStatus::NotFound => "ModemManager not found on DBus",
    }
}

pub fn modemmanager_snapshot_message(snapshot: &ModemManagerSnapshot) -> String {
    format!(
        "ModemManager data: version={}, modem_count={}",
        snapshot.version, snapshot.modem_count
    )
}

pub fn modemmanager_modem_count_changed_message(modem_count: usize) -> String {
    format!("ModemManager modem count changed: modem_count={modem_count}")
}

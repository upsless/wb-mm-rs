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
pub const MM_SIM_INTERFACE: &str = "org.freedesktop.ModemManager1.Sim";

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

/// Compact typed modem identifier derived from the DBus object path suffix.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModemId(pub String);

/// Full modem snapshot we want to route through the tresher before SMS work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModemSnapshot {
    pub is_active: bool,
    pub model: Option<String>,
    pub revision: Option<String>,
    pub state: Option<String>,
    pub primary_sim_slot: Option<u32>,
    pub operator_name: Option<String>,
    pub signal_quality: Option<u32>,
}

impl ModemSnapshot {
    pub fn summary(&self) -> String {
        format!(
            "is_active={}, model={}, revision={}, state={}, primary_sim_slot={}, operator_name={}, signal_quality={}",
            self.is_active,
            format_option_string(self.model.as_deref()),
            format_option_string(self.revision.as_deref()),
            format_option_string(self.state.as_deref()),
            format_option_u32(self.primary_sim_slot),
            format_option_string(self.operator_name.as_deref()),
            format_option_u32(self.signal_quality),
        )
    }
}

/// Single-field modem update used for property-change driven events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModemUpdate {
    Model(String),
    Revision(String),
    State(Option<String>),
    PrimarySimSlot(u32),
    OperatorName(Option<String>),
    SignalQuality(Option<u32>),
}

impl ModemUpdate {
    pub fn summary(&self) -> String {
        match self {
            ModemUpdate::Model(value) => format!("model={value}"),
            ModemUpdate::Revision(value) => format!("revision={value}"),
            ModemUpdate::State(value) => {
                format!("state={}", format_option_string(value.as_deref()))
            }
            ModemUpdate::PrimarySimSlot(value) => format!("primary_sim_slot={value}"),
            ModemUpdate::OperatorName(value) => {
                format!("operator_name={}", format_option_string(value.as_deref()))
            }
            ModemUpdate::SignalQuality(value) => {
                format!("signal_quality={}", format_option_u32(*value))
            }
        }
    }
}

fn format_option_string(value: Option<&str>) -> String {
    value.unwrap_or("None").to_string()
}

fn format_option_u32(value: Option<u32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "None".to_string())
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

pub fn modem_id_from_path(path: &str) -> Option<ModemId> {
    let modem_prefix = format!("{MM_OBJ_PATH}/Modem/");
    path.strip_prefix(&modem_prefix)
        .map(|suffix| ModemId(suffix.to_string()))
}

pub fn modem_path_from_id(modem_id: &ModemId) -> String {
    format!("{MM_OBJ_PATH}/Modem/{}", modem_id.0)
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

pub fn modem_found_message(modem_id: &ModemId) -> String {
    format!("Modem {} found on DBus", modem_id.0)
}

pub fn modem_deleted_message(modem_id: &ModemId) -> String {
    format!("Modem {} deleted from DBus", modem_id.0)
}

pub fn modem_snapshot_message(modem_id: &ModemId, snapshot: &ModemSnapshot) -> String {
    format!("Modem {} data: {}", modem_id.0, snapshot.summary())
}

pub fn modem_update_message(modem_id: &ModemId, update: &ModemUpdate) -> String {
    format!("Modem {} changed: {}", modem_id.0, update.summary())
}

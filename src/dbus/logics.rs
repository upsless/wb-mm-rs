use time::{OffsetDateTime, format_description::well_known::Iso8601};

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

pub const MM_SMS_STATE_CHANGED_SIGNAL_ID: &str = "mm_sms_state_changed";
pub const MM_SMS_TEXT_CHANGED_SIGNAL_ID: &str = "mm_sms_text_changed";
pub const MM_SMS_TIMESTAMP_CHANGED_SIGNAL_ID: &str = "mm_sms_timestamp_changed";
pub const MM_SMS_NUMBER_CHANGED_SIGNAL_ID: &str = "mm_sms_number_changed";
pub const MM_SMS_STORAGE_CHANGED_SIGNAL_ID: &str = "mm_sms_storage_changed";

/// DBus availability state derived from the ModemManager service name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModemManagerStatus {
    Active,
    Inactive,
}

/// Compact typed modem identifier derived from the DBus object path suffix.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModemId(pub String);

/// Compact typed SMS identifier derived from the DBus object path suffix.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SmsId(pub String);

/// Single manager-property update emitted from live DBus observations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagerUpdate {
    Status(ModemManagerStatus),
    Version(String),
    ModemCount(usize),
}

impl ManagerUpdate {
    pub fn summary(&self) -> String {
        match self {
            ManagerUpdate::Status(value) => format!("status={}", modemmanager_status_name(*value)),
            ManagerUpdate::Version(value) => format!("version={value}"),
            ManagerUpdate::ModemCount(value) => format!("modem_count={value}"),
        }
    }
}

/// Single modem-property update emitted from live DBus property changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModemUpdate {
    IsActive(bool),
    Model(String),
    Revision(String),
    State(Option<String>),
    PrimarySimSlot(u32),
    OperatorName(Option<String>),
    OwnNumbers(Vec<String>),
    SignalQuality(Option<u32>),
}

impl ModemUpdate {
    pub fn summary(&self) -> String {
        match self {
            ModemUpdate::IsActive(value) => format!("is_active={value}"),
            ModemUpdate::Model(value) => format!("model={value}"),
            ModemUpdate::Revision(value) => format!("revision={value}"),
            ModemUpdate::State(value) => {
                format!("state={}", format_option_string(value.as_deref()))
            }
            ModemUpdate::PrimarySimSlot(value) => format!("primary_sim_slot={value}"),
            ModemUpdate::OperatorName(value) => {
                format!("operator_name={}", format_option_string(value.as_deref()))
            }
            ModemUpdate::OwnNumbers(value) => format!("own_numbers={}", format_string_array(value)),
            ModemUpdate::SignalQuality(value) => {
                format!("signal_quality={}", format_option_u32(*value))
            }
        }
    }
}

/// Full SMS snapshot for one incoming ModemManager SMS object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SmsSnapshot {
    pub sms_id: SmsId,
    pub is_received: bool,
    pub storage: String,
    pub timestamp: Option<OffsetDateTime>,
    pub number: Option<String>,
    pub text: Option<String>,
}

impl SmsSnapshot {
    pub fn summary(&self) -> String {
        format!(
            "sms_id={}, is_received={}, storage={}, timestamp={}, sender={}, text={}",
            self.sms_id.0,
            self.is_received,
            self.storage,
            format_option_timestamp(self.timestamp),
            format_option_string(self.number.as_deref()),
            format_text_summary(self.text.as_deref()),
        )
    }
}

/// SMS update emitted toward the frontend projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SmsUpdate {
    pub sms_id: SmsId,
    pub property: SmsPropertyChange,
}

impl SmsUpdate {
    pub fn summary(&self) -> String {
        self.property.summary()
    }
}

/// Single SMS property change observed from live DBus property changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SmsPropertyChange {
    IsReceived(bool),
    Storage(String),
    Timestamp(Option<OffsetDateTime>),
    Number(Option<String>),
    Text(Option<String>),
}

impl SmsPropertyChange {
    pub fn summary(&self) -> String {
        match self {
            SmsPropertyChange::IsReceived(value) => format!("is_received={value}"),
            SmsPropertyChange::Storage(value) => format!("storage={value}"),
            SmsPropertyChange::Timestamp(value) => {
                format!("timestamp={}", format_option_timestamp(*value))
            }
            SmsPropertyChange::Number(value) => {
                format!("sender={}", format_option_string(value.as_deref()))
            }
            SmsPropertyChange::Text(value) => {
                format!("text={}", format_text_summary(value.as_deref()))
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

fn format_string_array(values: &[String]) -> String {
    format!("[{}]", values.join(","))
}

fn format_option_timestamp(value: Option<OffsetDateTime>) -> String {
    value
        .map(|value| value.unix_timestamp().to_string())
        .unwrap_or_else(|| "None".to_string())
}

fn format_text_summary(value: Option<&str>) -> String {
    match value {
        Some(value) => format!("len:{}", value.chars().count()),
        None => "None".to_string(),
    }
}

pub fn dbus_connected_message() -> &'static str {
    "Connection established"
}

pub fn dbus_stopped_before_connect_message() -> &'static str {
    "Stopped before connection was established"
}

pub fn dbus_stopped_message() -> &'static str {
    "Connection closed"
}

pub fn dbus_signal_stream_closed_message(signal: DbusSignalSpec) -> String {
    format!(
        "Signal stream closed: {} ({} {} {}.{})",
        signal.id, signal.bus_name, signal.path, signal.interface, signal.member
    )
}

pub fn modemmanager_status_message(status: ModemManagerStatus) -> &'static str {
    match status {
        ModemManagerStatus::Active => "ModemManager found on DBus and Active",
        ModemManagerStatus::Inactive => "ModemManager found on DBus and Inactive",
    }
}

pub fn modemmanager_status_name(status: ModemManagerStatus) -> &'static str {
    match status {
        ModemManagerStatus::Active => "active",
        ModemManagerStatus::Inactive => "inactive",
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

pub fn modem_state_allows_sms_inventory(state: i32) -> bool {
    modem_state_is_active(state)
}

pub fn modem_state_is_active(state: i32) -> bool {
    matches!(state, 6..=11)
}

pub fn sms_storage_name(storage: u32) -> &'static str {
    match storage {
        0 => "unknown",
        1 => "SIM",
        2 => "Mobile",
        3 => "SIM + Mobile",
        4 => "Status",
        5 => "Broadcast",
        6 => "Terminal",
        _ => "unknown",
    }
}

pub fn modem_deleted_message(modem_id: &ModemId) -> String {
    format!("Modem {} deleted from DBus", modem_id.0)
}

pub fn modem_update_message(modem_id: &ModemId, update: &ModemUpdate) -> String {
    format!("Modem {} changed: {}", modem_id.0, update.summary())
}

pub fn sms_inventory_snapshot_message(
    modem_id: &ModemId,
    sms_count: usize,
    initial_sms_id: Option<&SmsId>,
) -> String {
    format!(
        "Modem {} SMS inventory: sms_count={sms_count}, initial_sms={}",
        modem_id.0,
        initial_sms_id
            .map(|sms_id| sms_id.0.as_str())
            .unwrap_or("None"),
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

pub fn sms_signal_stream_closed_message(signal_id: &str, object_path: &str) -> String {
    format!("Signal stream closed: {signal_id} ({object_path})")
}

pub fn is_incoming_sms_pdu(pdu_type: u32) -> bool {
    matches!(pdu_type, 1 | 32)
}

pub fn sms_is_received(state: u32) -> bool {
    state == 3
}

pub fn parse_sms_timestamp(timestamp: &str) -> Option<OffsetDateTime> {
    let trimmed = timestamp.trim();
    if trimmed.is_empty() {
        return None;
    }

    OffsetDateTime::parse(trimmed, &Iso8601::DEFAULT).ok()
}

pub fn format_timestamp_for_wb(value: OffsetDateTime) -> String {
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second(),
    )
}

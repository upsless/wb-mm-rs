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

/// DBus availability state derived from the ModemManager service name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModemManagerStatus {
    Active,
    Inactive,
    NotFound,
}

/// Manager-level snapshot read after connecting to ModemManager.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModemManagerSnapshot {
    pub version: String,
    pub modem_count: usize,
}

/// Compact typed modem identifier derived from the DBus object path suffix.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModemId(pub String);

/// Compact typed SMS identifier derived from the DBus object path suffix.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SmsId(pub String);

/// Full modem snapshot read before subscribing to live modem changes.
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

/// Single modem-property update emitted from live DBus property changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModemUpdate {
    IsActive(bool),
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
            ModemUpdate::SignalQuality(value) => {
                format!("signal_quality={}", format_option_u32(*value))
            }
        }
    }
}

/// Full SMS snapshot for one incoming ModemManager SMS object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SmsSnapshot {
    pub is_received: bool,
    pub timestamp: Option<OffsetDateTime>,
    pub number: Option<String>,
    pub text: Option<String>,
}

impl SmsSnapshot {
    pub fn summary(&self) -> String {
        format!(
            "is_received={}, timestamp={}, sender={}, text={}",
            self.is_received,
            format_option_timestamp(self.timestamp),
            format_option_string(self.number.as_deref()),
            format_text_summary(self.text.as_deref()),
        )
    }
}

/// Single SMS-property update emitted from live DBus property changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SmsUpdate {
    IsReceived(bool),
    Timestamp(Option<OffsetDateTime>),
    Number(Option<String>),
    Text(Option<String>),
}

impl SmsUpdate {
    pub fn summary(&self) -> String {
        match self {
            SmsUpdate::IsReceived(value) => format!("is_received={value}"),
            SmsUpdate::Timestamp(value) => format!("timestamp={}", format_option_timestamp(*value)),
            SmsUpdate::Number(value) => {
                format!("sender={}", format_option_string(value.as_deref()))
            }
            SmsUpdate::Text(value) => format!("text={}", format_text_summary(value.as_deref())),
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

pub fn sms_snapshot_message(modem_id: &ModemId, sms_id: &SmsId, snapshot: &SmsSnapshot) -> String {
    format!(
        "Modem {} SMS {} data: {}",
        modem_id.0,
        sms_id.0,
        snapshot.summary()
    )
}

pub fn sms_inventory_snapshot_message(
    modem_id: &ModemId,
    sms_count: usize,
    last_sms_timestamp: Option<OffsetDateTime>,
) -> String {
    format!(
        "Modem {} SMS inventory: sms_count={sms_count}, last_sms={}",
        modem_id.0,
        format_option_timestamp(last_sms_timestamp),
    )
}

pub fn sms_update_message(modem_id: &ModemId, sms_id: &SmsId, update: &SmsUpdate) -> String {
    format!(
        "Modem {} SMS {} changed: {}",
        modem_id.0,
        sms_id.0,
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

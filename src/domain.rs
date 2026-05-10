use time::OffsetDateTime;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagerStatus {
    Active,
    Inactive,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModemId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SmsId(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagerUpdate {
    Status(ManagerStatus),
    Version(String),
    ModemCount(usize),
}

impl ManagerUpdate {
    pub fn summary(&self) -> String {
        match self {
            ManagerUpdate::Status(ManagerStatus::Active) => "status=active".to_string(),
            ManagerUpdate::Status(ManagerStatus::Inactive) => "status=inactive".to_string(),
            ManagerUpdate::Version(value) => format!("version={value}"),
            ManagerUpdate::ModemCount(value) => format!("modem_count={value}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModemInfo {
    pub is_active: bool,
    pub model: Option<String>,
    pub revision: Option<String>,
    pub state: Option<String>,
    pub primary_sim_slot: Option<u32>,
    pub operator_name: Option<String>,
    pub own_numbers: Vec<String>,
    pub signal_quality: Option<u32>,
}

impl ModemInfo {
    pub fn summary(&self) -> String {
        format!(
            "is_active={}, model={}, revision={}, state={}, primary_sim_slot={}, operator_name={}, own_numbers={}, signal_quality={}",
            self.is_active,
            self.model.as_deref().unwrap_or("None"),
            self.revision.as_deref().unwrap_or("None"),
            self.state.as_deref().unwrap_or("None"),
            self.primary_sim_slot
                .map(|value| value.to_string())
                .unwrap_or_else(|| "None".to_string()),
            self.operator_name.as_deref().unwrap_or("None"),
            format_string_array(&self.own_numbers),
            self.signal_quality
                .map(|value| value.to_string())
                .unwrap_or_else(|| "None".to_string()),
        )
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SmsInventoryEntry {
    pub sms_id: SmsId,
    pub timestamp: Option<OffsetDateTime>,
}

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
        info: ModemInfo,
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
        snapshot: SmsSnapshot,
    },
    SmsListChanged {
        modem_id: ModemId,
        entries: Vec<SmsInventoryEntry>,
    },
    SmsPropertyChanged {
        modem_id: ModemId,
        update: SmsUpdate,
    },
    SmsDeleted {
        modem_id: ModemId,
        sms_id: SmsId,
    },
    SmsInventorySnapshot {
        modem_id: ModemId,
        entries: Vec<SmsInventoryEntry>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbusCommand {
    RefreshSms { modem_id: ModemId, sms_id: SmsId },
    DeleteSms { modem_id: ModemId, sms_id: SmsId },
}

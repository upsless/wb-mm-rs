pub const MQTT_DRIVER_NAME: &str = "wb-mm-mqtt";
pub const MM_DEVICE_NAME: &str = "modemmanager";
pub const MM_MODEM_DEVICE_PREFIX: &str = "mm_modem_";

pub const MM_CONTROL_IS_AVAILABLE: &str = "is_available";
pub const MM_CONTROL_MANAGER_STATUS: &str = "modemmanager_status";
pub const MM_CONTROL_VERSION: &str = "version";
pub const MM_CONTROL_MODEM_COUNT: &str = "modem_count";
pub const MM_CONTROL_SMS_COUNT: &str = "sms_count";

pub const MODEM_CONTROL_IS_ACTIVE: &str = "is_active";
pub const MODEM_CONTROL_MODEL: &str = "model";
pub const MODEM_CONTROL_REVISION: &str = "revision";
pub const MODEM_CONTROL_STATE: &str = "state";
pub const MODEM_CONTROL_PRIMARY_SIM_SLOT: &str = "primary_sim_slot";
pub const MODEM_CONTROL_OPERATOR_NAME: &str = "operator_name";
pub const MODEM_CONTROL_OWN_NUMBERS: &str = "own_numbers";
pub const MODEM_CONTROL_SIGNAL_QUALITY: &str = "signal_quality";
pub const MODEM_CONTROL_LAST_SENT_SMS_RECIPIENT: &str = "last_sent_sms_recipient";
pub const MODEM_CONTROL_LAST_SENT_SMS_TEXT: &str = "last_sent_sms_text";
pub const MODEM_CONTROL_LAST_SENT_SMS_STATUS: &str = "last_sent_sms_status";
pub const MODEM_CONTROL_LAST_SENT_SMS_TIMESTAMP: &str = "last_sent_sms_timestamp";
pub const MODEM_CONTROL_LAST_SENT_SMS_TIMESTAMP_UNIXTIME: &str = "last_sent_sms_timestamp_unixtime";
pub const MODEM_CONTROL_OUTGOING_SMS_RECIPIENT: &str = "outgoing_sms_recipient";
pub const MODEM_CONTROL_OUTGOING_SMS_TEXT: &str = "outgoing_sms_text";
pub const MODEM_CONTROL_CHECK_PHONE_FORMAT: &str = "check_phone_format";
pub const MODEM_CONTROL_SEND_SMS: &str = "send_sms";
pub const MODEM_CONTROL_DISPLAYED_SMS_INDEX: &str = "displayed_sms_index";
pub const MODEM_CONTROL_SMS_COUNT: &str = "sms_count";
pub const MODEM_CONTROL_LAST_RECEIVED_SMS_DBUS_ID: &str = "last_received_sms_dbus_id";
pub const MODEM_CONTROL_MESSAGE_SELECT: &str = "message_select";
pub const MODEM_CONTROL_SELECTED_SMS_DBUS_ID: &str = "selected_sms_dbus_id";
pub const MODEM_CONTROL_SELECTED_SMS_TIMESTAMP: &str = "selected_sms_timestamp";
pub const MODEM_CONTROL_SELECTED_SMS_TIMESTAMP_UNIXTIME: &str = "selected_sms_timestamp_unixtime";
pub const MODEM_CONTROL_SELECTED_SMS_SENDER: &str = "selected_sms_sender";
pub const MODEM_CONTROL_SELECTED_SMS_STORAGE: &str = "selected_sms_storage";
pub const MODEM_CONTROL_SELECTED_SMS_TEXT: &str = "selected_sms_text";
pub const MODEM_CONTROL_SELECTED_SMS_IS_RECEIVED: &str = "selected_sms_is_received";
pub const MODEM_CONTROL_DELETE_MESSAGE: &str = "delete_message";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlSpec {
    pub name: &'static str,
    pub title_en: &'static str,
    pub title_ru: &'static str,
    pub order: u32,
    pub control_type: &'static str,
    pub readonly: bool,
    pub hidden: bool,
    pub units: Option<&'static str>,
    pub min: Option<u32>,
    pub max: Option<u32>,
}

const MM_CONTROL_SPECS: [ControlSpec; 5] = [
    ControlSpec {
        name: MM_CONTROL_IS_AVAILABLE,
        title_en: "Available",
        title_ru: "Доступен",
        order: 0,
        control_type: "switch",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MM_CONTROL_MANAGER_STATUS,
        title_en: "ModemManager (DBus)",
        title_ru: "ModemManager (DBus)",
        order: 1,
        control_type: "text",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MM_CONTROL_VERSION,
        title_en: "Version",
        title_ru: "Версия",
        order: 2,
        control_type: "text",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MM_CONTROL_MODEM_COUNT,
        title_en: "Modems count",
        title_ru: "Количество модемов",
        order: 3,
        control_type: "value",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MM_CONTROL_SMS_COUNT,
        title_en: "Incoming SMS",
        title_ru: "Входящие СМС",
        order: 4,
        control_type: "value",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
];

const MODEM_BASE_CONTROL_SPECS: [ControlSpec; 8] = [
    ControlSpec {
        name: MODEM_CONTROL_IS_ACTIVE,
        title_en: "Active",
        title_ru: "Активен",
        order: 10,
        control_type: "switch",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_MODEL,
        title_en: "Model",
        title_ru: "Модель",
        order: 11,
        control_type: "text",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_REVISION,
        title_en: "Revision",
        title_ru: "Ревизия",
        order: 12,
        control_type: "text",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_STATE,
        title_en: "State",
        title_ru: "Статус",
        order: 13,
        control_type: "text",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_PRIMARY_SIM_SLOT,
        title_en: "Primary SIM",
        title_ru: "Основная SIM",
        order: 14,
        control_type: "value",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_OPERATOR_NAME,
        title_en: "Operator",
        title_ru: "Оператор",
        order: 15,
        control_type: "text",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_OWN_NUMBERS,
        title_en: "Numbers",
        title_ru: "Номера",
        order: 16,
        control_type: "text",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_SIGNAL_QUALITY,
        title_en: "Signal quality",
        title_ru: "Уровень сигнала",
        order: 17,
        control_type: "value",
        readonly: true,
        hidden: false,
        units: Some("%"),
        min: None,
        max: None,
    },
];

const MODEM_OUTGOING_SMS_CONTROL_SPECS: [ControlSpec; 9] = [
    ControlSpec {
        name: MODEM_CONTROL_LAST_SENT_SMS_STATUS,
        title_en: "Sent SMS status",
        title_ru: "Статус отправки СМС",
        order: 18,
        control_type: "text",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_LAST_SENT_SMS_TIMESTAMP,
        title_en: "Sent SMS date",
        title_ru: "Дата отправки СМС",
        order: 19,
        control_type: "text",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_LAST_SENT_SMS_TIMESTAMP_UNIXTIME,
        title_en: "Sent SMS date unix time",
        title_ru: "Дата отправки СМС unix time",
        order: 20,
        control_type: "unixtime",
        readonly: true,
        hidden: true,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_LAST_SENT_SMS_RECIPIENT,
        title_en: "Sent SMS recipient",
        title_ru: "Получатель СМС",
        order: 21,
        control_type: "text",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_LAST_SENT_SMS_TEXT,
        title_en: "Sent SMS text",
        title_ru: "Текст отправл. СМС",
        order: 22,
        control_type: "text",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_OUTGOING_SMS_RECIPIENT,
        title_en: "Recipient phone #",
        title_ru: "Номер получателя",
        order: 23,
        control_type: "text",
        readonly: false,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_OUTGOING_SMS_TEXT,
        title_en: "Outgoing SMS text",
        title_ru: "Текст исходящей СМС",
        order: 24,
        control_type: "text",
        readonly: false,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_CHECK_PHONE_FORMAT,
        title_en: "Check phone format",
        title_ru: "Проверять формат номера",
        order: 25,
        control_type: "switch",
        readonly: false,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_SEND_SMS,
        title_en: "Send SMS",
        title_ru: "Отправить СМС",
        order: 26,
        control_type: "pushbutton",
        readonly: false,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
];

const MODEM_SMS_CONTROL_SPECS: [ControlSpec; 12] = [
    ControlSpec {
        name: MODEM_CONTROL_DISPLAYED_SMS_INDEX,
        title_en: "Displayed SMS, #",
        title_ru: "Отображаемая СМС, №",
        order: 32,
        control_type: "value",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_LAST_RECEIVED_SMS_DBUS_ID,
        title_en: "Last incoming SMS DBus#",
        title_ru: "Последняя входящая СМС, DBus#",
        order: 27,
        control_type: "text",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_SMS_COUNT,
        title_en: "Incoming SMS",
        title_ru: "Всего входящих СМС",
        order: 28,
        control_type: "value",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_MESSAGE_SELECT,
        title_en: "Incoming SMS pick:",
        title_ru: "Выбор входящей СМС:",
        order: 29,
        control_type: "range",
        readonly: true,
        hidden: false,
        units: None,
        min: Some(1),
        max: Some(1),
    },
    ControlSpec {
        name: MODEM_CONTROL_SELECTED_SMS_DBUS_ID,
        title_en: "SMS DBus#",
        title_ru: "СМС DBus#",
        order: 34,
        control_type: "text",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_SELECTED_SMS_TIMESTAMP,
        title_en: "SMS timestamp",
        title_ru: "Дата получения СМС",
        order: 30,
        control_type: "text",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_SELECTED_SMS_TIMESTAMP_UNIXTIME,
        title_en: "SMS timestamp unix time",
        title_ru: "Дата получения СМС unix time",
        order: 31,
        control_type: "unixtime",
        readonly: true,
        hidden: true,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_SELECTED_SMS_SENDER,
        title_en: "SMS sender",
        title_ru: "Отправитель СМС",
        order: 33,
        control_type: "text",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_SELECTED_SMS_TEXT,
        title_en: "SMS text",
        title_ru: "Текст СМС",
        order: 37,
        control_type: "text",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_SELECTED_SMS_IS_RECEIVED,
        title_en: "SMS received fully",
        title_ru: "СМС получена полностью",
        order: 36,
        control_type: "switch",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_SELECTED_SMS_STORAGE,
        title_en: "SMS storage",
        title_ru: "Хранилище СМС",
        order: 35,
        control_type: "text",
        readonly: true,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
    ControlSpec {
        name: MODEM_CONTROL_DELETE_MESSAGE,
        title_en: "Delete current SMS",
        title_ru: "Удалить текущую СМС",
        order: 38,
        control_type: "pushbutton",
        readonly: false,
        hidden: false,
        units: None,
        min: None,
        max: None,
    },
];

pub fn manager_control_specs() -> &'static [ControlSpec] {
    &MM_CONTROL_SPECS
}

pub fn modem_base_control_specs() -> &'static [ControlSpec] {
    &MODEM_BASE_CONTROL_SPECS
}

pub fn modem_outgoing_sms_control_specs() -> &'static [ControlSpec] {
    &MODEM_OUTGOING_SMS_CONTROL_SPECS
}

pub fn modem_sms_control_specs() -> &'static [ControlSpec] {
    &MODEM_SMS_CONTROL_SPECS
}

pub fn dynamic_message_select_spec(readonly: bool, max: u32) -> ControlSpec {
    let base = MODEM_SMS_CONTROL_SPECS
        .iter()
        .find(|spec| spec.name == MODEM_CONTROL_MESSAGE_SELECT)
        .expect("message_select control spec exists");

    ControlSpec {
        readonly,
        max: Some(max.max(1)),
        ..*base
    }
}

pub fn dynamic_delete_message_spec(readonly: bool) -> ControlSpec {
    let base = MODEM_SMS_CONTROL_SPECS
        .iter()
        .find(|spec| spec.name == MODEM_CONTROL_DELETE_MESSAGE)
        .expect("delete_message control spec exists");

    ControlSpec { readonly, ..*base }
}

pub fn dynamic_outgoing_sms_spec(control_name: &'static str, readonly: bool) -> ControlSpec {
    let base = MODEM_OUTGOING_SMS_CONTROL_SPECS
        .iter()
        .find(|spec| spec.name == control_name)
        .expect("outgoing SMS control spec exists");

    ControlSpec { readonly, ..*base }
}

pub(super) fn modemmanager_is_available(status: crate::domain::ManagerStatus) -> bool {
    matches!(status, crate::domain::ManagerStatus::Active)
}

pub(super) fn manager_status_payload(status: Option<crate::domain::ManagerStatus>) -> &'static str {
    match status {
        Some(crate::domain::ManagerStatus::Active) => "active",
        Some(crate::domain::ManagerStatus::Inactive) => "inactive",
        None => "not_found_on_dbus",
    }
}

pub(super) fn format_timestamp_for_wb(value: time::OffsetDateTime) -> String {
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

pub fn mm_availability_topic() -> String {
    control_value_topic(MM_DEVICE_NAME, MM_CONTROL_IS_AVAILABLE)
}

pub fn device_name_for_modem(modem_index: u32) -> String {
    format!("{MM_MODEM_DEVICE_PREFIX}{modem_index}")
}

pub fn device_title_payload(title_en: &str, title_ru: &str) -> String {
    format!(r#"{{"driver":"{MQTT_DRIVER_NAME}","title":{{"en":"{title_en}","ru":"{title_ru}"}}}}"#)
}

pub fn manager_device_title_payload() -> String {
    device_title_payload("ModemManager Gateway (MMG)", "Шлюз ModemManager (MMG)")
}

pub fn modem_device_title_payload(modem_index: u32, dbus_modem_id: &str) -> String {
    device_title_payload(
        &format!("MMG Modem #{modem_index} (DBus #{dbus_modem_id})"),
        &format!("Модем MMG №{modem_index} (DBus #{dbus_modem_id})"),
    )
}

pub fn control_meta_payload(spec: &ControlSpec) -> String {
    let mut fields = vec![
        format!(
            r#""title":{{"en":"{}","ru":"{}"}}"#,
            spec.title_en, spec.title_ru
        ),
        format!(r#""order":{}"#, spec.order),
        format!(r#""type":"{}""#, spec.control_type),
        format!(r#""readonly":{}"#, bool_payload(spec.readonly)),
    ];

    if let Some(units) = spec.units {
        fields.push(format!(r#""units":"{units}""#));
    }

    if let Some(min) = spec.min {
        fields.push(format!(r#""min":{min}"#));
    }

    if let Some(max) = spec.max {
        fields.push(format!(r#""max":{max}"#));
    }

    if spec.hidden {
        fields.push(r#""hidden":true"#.to_string());
    }

    format!("{{{}}}", fields.join(","))
}

pub fn control_meta_leaf_payloads(spec: &ControlSpec) -> Vec<(&'static str, String)> {
    let mut fields = vec![
        ("type", spec.control_type.to_string()),
        ("order", spec.order.to_string()),
        ("readonly", bool_payload(spec.readonly).to_string()),
    ];

    if let Some(units) = spec.units {
        fields.push(("units", units.to_string()));
    }

    if let Some(min) = spec.min {
        fields.push(("min", min.to_string()));
    }

    if let Some(max) = spec.max {
        fields.push(("max", max.to_string()));
    }

    if spec.hidden {
        fields.push(("hidden", bool_payload(true).to_string()));
    }

    fields
}

pub fn string_array_payload(values: &[String]) -> String {
    serde_json::to_string(values).unwrap_or_else(|_| "[]".to_string())
}

pub fn single_line_payload(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            '\r' | '\n' | '\t' => ' ',
            _ => ch,
        })
        .collect::<String>()
}

pub fn device_meta_topic(device_name: &str) -> String {
    format!("/devices/{device_name}/meta")
}

pub fn control_value_topic(device_name: &str, control_name: &str) -> String {
    format!("/devices/{device_name}/controls/{control_name}")
}

pub fn control_meta_topic(device_name: &str, control_name: &str) -> String {
    format!("{}/meta", control_value_topic(device_name, control_name))
}

pub fn control_meta_leaf_topic(device_name: &str, control_name: &str, field: &str) -> String {
    format!(
        "{}/meta/{field}",
        control_value_topic(device_name, control_name)
    )
}

pub fn control_on_topic(device_name: &str, control_name: &str) -> String {
    format!("{}/on", control_value_topic(device_name, control_name))
}

pub fn bool_payload(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

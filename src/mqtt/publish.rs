use anyhow::{Context, Result};
use rumqttc::{AsyncClient, QoS};
use std::collections::HashSet;
use time::OffsetDateTime;
use tracing::info;

use crate::dbus::{
    ManagerStatus, ModemId, ModemInfo, ModemUpdate, SmsPropertyChange, SmsSnapshot, SmsUpdate,
};
use crate::mqtt::frontend::{manager_status_payload, modemmanager_is_available};
use crate::mqtt::r#loop::LOG_TARGET;
use crate::mqtt::schema::{self, ControlSpec};
use crate::mqtt::state::{MqttModemSmsState, max_message_select_index};

pub(super) trait IntoMqttPayload {
    fn into_mqtt_payload(self) -> String;
}

struct MqttNull;

#[derive(Clone, Copy)]
pub(super) struct WbSwitch(bool);

pub(super) struct MqttPublisher {
    client: AsyncClient,
    state: MqttPublishState,
}

#[derive(Default)]
struct MqttPublishState {
    main_device_created: bool,
    modem_sms_controls_created: HashSet<u32>,
    subscribed_modem_sms_controls: HashSet<u32>,
    last_manager_sms_count: Option<usize>,
}

pub(super) struct UnavailableModemPublishState {
    pub(super) modem_id: ModemId,
    pub(super) modem_index: u32,
    pub(super) sms_control_state: Option<(u32, u32)>,
}

impl WbSwitch {
    pub(super) fn as_str(self) -> &'static str {
        if self.0 { "1" } else { "0" }
    }
}

impl IntoMqttPayload for MqttNull {
    fn into_mqtt_payload(self) -> String {
        "null".to_string()
    }
}

impl IntoMqttPayload for WbSwitch {
    fn into_mqtt_payload(self) -> String {
        self.as_str().to_string()
    }
}

impl IntoMqttPayload for &[String] {
    fn into_mqtt_payload(self) -> String {
        schema::string_array_payload(self)
    }
}

impl IntoMqttPayload for &str {
    fn into_mqtt_payload(self) -> String {
        self.to_string()
    }
}

impl IntoMqttPayload for String {
    fn into_mqtt_payload(self) -> String {
        self
    }
}

impl IntoMqttPayload for &String {
    fn into_mqtt_payload(self) -> String {
        self.to_string()
    }
}

impl IntoMqttPayload for u32 {
    fn into_mqtt_payload(self) -> String {
        self.to_string()
    }
}

impl IntoMqttPayload for usize {
    fn into_mqtt_payload(self) -> String {
        self.to_string()
    }
}

impl IntoMqttPayload for i64 {
    fn into_mqtt_payload(self) -> String {
        self.to_string()
    }
}

impl<T> IntoMqttPayload for Option<T>
where
    T: IntoMqttPayload,
{
    fn into_mqtt_payload(self) -> String {
        match self {
            Some(value) => value.into_mqtt_payload(),
            None => MqttNull.into_mqtt_payload(),
        }
    }
}

impl MqttPublisher {
    pub(super) fn new(client: AsyncClient) -> Self {
        Self {
            client,
            state: MqttPublishState::default(),
        }
    }

    pub(super) async fn disconnect(&self) -> Result<()> {
        self.client
            .disconnect()
            .await
            .context("failed to disconnect MQTT client")
    }

    pub(super) async fn ensure_main_device(&mut self) -> Result<()> {
        if self.state.main_device_created {
            return Ok(());
        }

        self.publish_main_structure().await?;
        self.publish_control(
            schema::MM_DEVICE_NAME,
            schema::MM_CONTROL_IS_AVAILABLE,
            switch_payload(false),
        )
        .await?;
        self.publish_control(
            schema::MM_DEVICE_NAME,
            schema::MM_CONTROL_MANAGER_STATUS,
            manager_status_payload(None),
        )
        .await?;
        self.publish_control(schema::MM_DEVICE_NAME, schema::MM_CONTROL_VERSION, MqttNull)
            .await?;
        self.publish_control(
            schema::MM_DEVICE_NAME,
            schema::MM_CONTROL_MODEM_COUNT,
            MqttNull,
        )
        .await?;
        self.publish_control(schema::MM_DEVICE_NAME, schema::MM_CONTROL_SMS_COUNT, 0usize)
            .await?;

        self.state.last_manager_sms_count = Some(0);
        self.state.main_device_created = true;
        Ok(())
    }

    pub(super) async fn publish_manager_found(
        &self,
        version: &str,
        modem_count: usize,
    ) -> Result<()> {
        self.publish_control(schema::MM_DEVICE_NAME, schema::MM_CONTROL_VERSION, version)
            .await?;
        self.publish_control(
            schema::MM_DEVICE_NAME,
            schema::MM_CONTROL_MODEM_COUNT,
            modem_count,
        )
        .await?;

        info!(
            target: LOG_TARGET,
            "Update main device manager data: version={version} modem_count={modem_count}"
        );

        Ok(())
    }

    pub(super) async fn publish_manager_status(&self, status: Option<ManagerStatus>) -> Result<()> {
        let is_available = switch_payload(status.is_some_and(modemmanager_is_available));
        let manager_status = manager_status_payload(status);

        self.publish_control(
            schema::MM_DEVICE_NAME,
            schema::MM_CONTROL_IS_AVAILABLE,
            is_available,
        )
        .await?;
        self.publish_control(
            schema::MM_DEVICE_NAME,
            schema::MM_CONTROL_MANAGER_STATUS,
            manager_status,
        )
        .await?;

        info!(
            target: LOG_TARGET,
            "{}",
            schema::mqtt_publish_mm_availability_message(is_available.as_str())
        );
        info!(
            target: LOG_TARGET,
            "Update main device manager status: status={manager_status}"
        );

        Ok(())
    }

    pub(super) async fn publish_manager_version(&self, version: &str) -> Result<()> {
        self.publish_control(schema::MM_DEVICE_NAME, schema::MM_CONTROL_VERSION, version)
            .await?;

        info!(
            target: LOG_TARGET,
            "{}",
            schema::mqtt_publish_mm_version_message(version)
        );

        Ok(())
    }

    pub(super) async fn publish_manager_modem_count(&self, modem_count: usize) -> Result<()> {
        self.publish_control(
            schema::MM_DEVICE_NAME,
            schema::MM_CONTROL_MODEM_COUNT,
            modem_count,
        )
        .await?;

        info!(
            target: LOG_TARGET,
            "{}",
            schema::mqtt_publish_mm_modem_count_message(modem_count)
        );

        Ok(())
    }

    pub(super) async fn publish_empty_modem(&self, modem_index: u32) -> Result<()> {
        let device_name = schema::device_name_for_modem(modem_index);
        self.publish_control(
            &device_name,
            schema::MODEM_CONTROL_IS_ACTIVE,
            switch_payload(false),
        )
        .await?;
        self.publish_control(&device_name, schema::MODEM_CONTROL_MODEL, MqttNull)
            .await?;
        self.publish_control(&device_name, schema::MODEM_CONTROL_REVISION, MqttNull)
            .await?;
        self.publish_control(&device_name, schema::MODEM_CONTROL_STATE, MqttNull)
            .await?;
        self.publish_control(
            &device_name,
            schema::MODEM_CONTROL_PRIMARY_SIM_SLOT,
            MqttNull,
        )
        .await?;
        self.publish_control(&device_name, schema::MODEM_CONTROL_OPERATOR_NAME, MqttNull)
            .await?;
        self.publish_control(
            &device_name,
            schema::MODEM_CONTROL_OWN_NUMBERS,
            &[] as &[String],
        )
        .await?;
        self.publish_control(&device_name, schema::MODEM_CONTROL_SIGNAL_QUALITY, MqttNull)
            .await?;

        Ok(())
    }

    pub(super) async fn ensure_modem_sms_controls(
        &mut self,
        modem_id: &ModemId,
        modem_index: u32,
    ) -> Result<()> {
        if self.state.modem_sms_controls_created.contains(&modem_index) {
            return Ok(());
        }

        self.subscribe_to_modem_sms_controls(modem_index).await?;
        self.publish_modem_sms_structure(modem_index).await?;

        let device_name = schema::device_name_for_modem(modem_index);
        self.publish_control(
            &device_name,
            schema::MODEM_CONTROL_DISPLAYED_SMS_INDEX,
            MqttNull,
        )
        .await?;
        self.publish_control(
            &device_name,
            schema::MODEM_CONTROL_LAST_SMS_DBUS_ID,
            MqttNull,
        )
        .await?;
        self.publish_control(&device_name, schema::MODEM_CONTROL_SMS_COUNT, 0usize)
            .await?;
        self.publish_message_select_control(modem_id, modem_index, None, 1, false)
            .await?;
        self.publish_picked_sms(modem_id, modem_index, None).await?;
        self.publish_delete_message_control(modem_index, false)
            .await?;

        self.state.modem_sms_controls_created.insert(modem_index);
        Ok(())
    }

    async fn subscribe_to_modem_sms_controls(&mut self, modem_index: u32) -> Result<()> {
        if self
            .state
            .subscribed_modem_sms_controls
            .contains(&modem_index)
        {
            return Ok(());
        }

        let device_name = schema::device_name_for_modem(modem_index);
        for control_name in [
            schema::MODEM_CONTROL_MESSAGE_SELECT,
            schema::MODEM_CONTROL_DELETE_MESSAGE,
        ] {
            let topic = schema::control_on_topic(&device_name, control_name);
            self.client
                .subscribe(topic.clone(), QoS::AtMostOnce)
                .await
                .with_context(|| format!("failed to subscribe to MQTT topic `{topic}`"))?;
        }

        self.state.subscribed_modem_sms_controls.insert(modem_index);
        Ok(())
    }

    async fn publish_main_structure(&self) -> Result<()> {
        self.publish_retained(
            schema::device_meta_topic(schema::MM_DEVICE_NAME),
            schema::manager_device_title_payload(),
        )
        .await?;

        for spec in schema::manager_control_specs() {
            self.publish_control_metadata(schema::MM_DEVICE_NAME, spec)
                .await?;
        }

        Ok(())
    }

    pub(super) async fn publish_modem_structure(
        &self,
        modem_index: u32,
        modem_id: &ModemId,
    ) -> Result<()> {
        let device_name = schema::device_name_for_modem(modem_index);
        self.publish_retained(
            schema::device_meta_topic(&device_name),
            schema::modem_device_title_payload(modem_index, &modem_id.0),
        )
        .await?;

        for spec in schema::modem_base_control_specs() {
            self.publish_control_metadata(&device_name, spec).await?;
        }

        Ok(())
    }

    async fn publish_modem_sms_structure(&self, modem_index: u32) -> Result<()> {
        let device_name = schema::device_name_for_modem(modem_index);
        for spec in schema::modem_sms_control_specs() {
            self.publish_control_metadata(&device_name, spec).await?;
        }

        Ok(())
    }

    pub(super) async fn publish_modem_found(
        &self,
        modem_id: &ModemId,
        modem_index: u32,
        info: &ModemInfo,
    ) -> Result<()> {
        let device_name = schema::device_name_for_modem(modem_index);

        self.publish_control(
            &device_name,
            schema::MODEM_CONTROL_IS_ACTIVE,
            switch_payload(info.is_active),
        )
        .await?;
        self.publish_control(
            &device_name,
            schema::MODEM_CONTROL_MODEL,
            info.model.as_deref(),
        )
        .await?;
        self.publish_control(
            &device_name,
            schema::MODEM_CONTROL_REVISION,
            info.revision.as_deref(),
        )
        .await?;
        self.publish_control(
            &device_name,
            schema::MODEM_CONTROL_STATE,
            info.state.as_deref(),
        )
        .await?;
        self.publish_control(
            &device_name,
            schema::MODEM_CONTROL_PRIMARY_SIM_SLOT,
            info.primary_sim_slot,
        )
        .await?;
        self.publish_control(
            &device_name,
            schema::MODEM_CONTROL_OPERATOR_NAME,
            info.operator_name.as_deref(),
        )
        .await?;
        self.publish_control(
            &device_name,
            schema::MODEM_CONTROL_OWN_NUMBERS,
            info.own_numbers.as_slice(),
        )
        .await?;
        self.publish_control(
            &device_name,
            schema::MODEM_CONTROL_SIGNAL_QUALITY,
            info.signal_quality,
        )
        .await?;

        info!(
            target: LOG_TARGET,
            "{}",
            schema::mqtt_publish_modem_snapshot_message(
                modem_index,
                &modem_id.0,
                &info.summary(),
            )
        );

        Ok(())
    }

    pub(super) async fn publish_modem_update(
        &self,
        modem_id: &ModemId,
        modem_index: u32,
        update: &ModemUpdate,
    ) -> Result<()> {
        let device_name = schema::device_name_for_modem(modem_index);

        match update {
            ModemUpdate::IsActive(value) => {
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_IS_ACTIVE,
                    switch_payload(*value),
                )
                .await?;
            }
            ModemUpdate::Model(value) => {
                self.publish_control(&device_name, schema::MODEM_CONTROL_MODEL, value)
                    .await?;
            }
            ModemUpdate::Revision(value) => {
                self.publish_control(&device_name, schema::MODEM_CONTROL_REVISION, value)
                    .await?;
            }
            ModemUpdate::State(value) => {
                self.publish_control(&device_name, schema::MODEM_CONTROL_STATE, value.as_deref())
                    .await?;
            }
            ModemUpdate::PrimarySimSlot(value) => {
                self.publish_control(&device_name, schema::MODEM_CONTROL_PRIMARY_SIM_SLOT, *value)
                    .await?;
            }
            ModemUpdate::OperatorName(value) => {
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_OPERATOR_NAME,
                    value.as_deref(),
                )
                .await?;
            }
            ModemUpdate::OwnNumbers(value) => {
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_OWN_NUMBERS,
                    value.as_slice(),
                )
                .await?;
            }
            ModemUpdate::SignalQuality(value) => {
                self.publish_control(&device_name, schema::MODEM_CONTROL_SIGNAL_QUALITY, *value)
                    .await?;
            }
        }

        info!(
            target: LOG_TARGET,
            "{}",
            schema::mqtt_publish_modem_update_message(
                modem_index,
                &modem_id.0,
                &update.summary(),
            )
        );

        Ok(())
    }

    pub(super) async fn publish_modems_unavailable(
        &self,
        modems: Vec<UnavailableModemPublishState>,
    ) -> Result<()> {
        for modem in modems {
            let modem_index = modem.modem_index;
            let device_name = schema::device_name_for_modem(modem_index);
            self.publish_control(
                &device_name,
                schema::MODEM_CONTROL_IS_ACTIVE,
                switch_payload(false),
            )
            .await?;

            if self.state.modem_sms_controls_created.contains(&modem_index) {
                let (picked_sms_index, max_index) = modem.sms_control_state.unwrap_or((1, 1));
                self.publish_message_select_control(
                    &modem.modem_id,
                    modem_index,
                    Some(picked_sms_index),
                    max_index,
                    false,
                )
                .await?;
                self.publish_delete_message_control(modem_index, false)
                    .await?;
            }
        }

        Ok(())
    }

    pub(super) async fn sync_modem_sms_state(
        &mut self,
        modem_id: &ModemId,
        modem_index: u32,
        modem_sms: &MqttModemSmsState,
    ) -> Result<()> {
        self.ensure_modem_sms_controls(modem_id, modem_index)
            .await?;
        let sms_count = modem_sms.sms_count();
        let last_sms_id = modem_sms.last_sms_id().cloned();
        let picked_sms_index = modem_sms.picked_sms_index();
        let displayed_sms_index = modem_sms.displayed_sms_index();
        let max_index = max_message_select_index(sms_count);
        let message_select_writable = sms_count > 0;
        let has_displayed_sms = displayed_sms_index.is_some();

        let device_name = schema::device_name_for_modem(modem_index);

        self.publish_modem_sms_count(modem_id, modem_index, sms_count)
            .await?;

        self.publish_control(
            &device_name,
            schema::MODEM_CONTROL_LAST_SMS_DBUS_ID,
            last_sms_id.as_ref().map(|sms_id| sms_id.0.as_str()),
        )
        .await?;

        self.publish_message_select_control(
            modem_id,
            modem_index,
            Some(picked_sms_index),
            max_index,
            message_select_writable,
        )
        .await?;

        if let Some(displayed_sms_index) = displayed_sms_index {
            self.publish_control(
                &device_name,
                schema::MODEM_CONTROL_DISPLAYED_SMS_INDEX,
                displayed_sms_index,
            )
            .await?;
        } else {
            self.publish_control(
                &device_name,
                schema::MODEM_CONTROL_DISPLAYED_SMS_INDEX,
                MqttNull,
            )
            .await?;
            self.publish_picked_sms(modem_id, modem_index, None).await?;
        }

        self.publish_delete_message_control(modem_index, has_displayed_sms)
            .await?;

        Ok(())
    }

    pub(super) async fn sync_main_sms_state(&mut self, sms_count: usize) -> Result<()> {
        self.ensure_main_device().await?;

        if self.state.last_manager_sms_count != Some(sms_count) {
            self.publish_main_sms_count(sms_count).await?;
            self.state.last_manager_sms_count = Some(sms_count);
        }

        Ok(())
    }

    async fn publish_modem_sms_count(
        &self,
        modem_id: &ModemId,
        modem_index: u32,
        sms_count: usize,
    ) -> Result<()> {
        let device_name = schema::device_name_for_modem(modem_index);
        self.publish_control(&device_name, schema::MODEM_CONTROL_SMS_COUNT, sms_count)
            .await?;

        info!(
            target: LOG_TARGET,
            "{}",
            schema::mqtt_publish_modem_sms_count_message(modem_index, &modem_id.0, sms_count)
        );

        Ok(())
    }

    async fn publish_main_sms_count(&self, sms_count: usize) -> Result<()> {
        self.publish_control(
            schema::MM_DEVICE_NAME,
            schema::MM_CONTROL_SMS_COUNT,
            sms_count,
        )
        .await?;

        info!(
            target: LOG_TARGET,
            "{}",
            schema::mqtt_publish_mm_sms_count_message(sms_count)
        );

        Ok(())
    }

    pub(super) async fn publish_message_select_control(
        &self,
        modem_id: &ModemId,
        modem_index: u32,
        picked_index: Option<u32>,
        max_index: u32,
        writable: bool,
    ) -> Result<()> {
        let device_name = schema::device_name_for_modem(modem_index);
        let spec = schema::dynamic_message_select_spec(!writable, max_index);
        self.publish_control_metadata(&device_name, &spec).await?;

        match picked_index {
            Some(picked_index) => {
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_MESSAGE_SELECT,
                    picked_index,
                )
                .await?;
            }
            None => {
                self.publish_control(&device_name, schema::MODEM_CONTROL_MESSAGE_SELECT, 1u32)
                    .await?;
            }
        }

        info!(
            target: LOG_TARGET,
            "{}",
            schema::mqtt_publish_message_select_control_message(
                modem_index,
                &modem_id.0,
                picked_index,
                max_index,
                writable,
            )
        );

        Ok(())
    }

    pub(super) async fn publish_delete_message_control(
        &self,
        modem_index: u32,
        writable: bool,
    ) -> Result<()> {
        let device_name = schema::device_name_for_modem(modem_index);
        let spec = schema::dynamic_delete_message_spec(!writable);
        self.publish_control_metadata(&device_name, &spec).await?;
        self.publish_control(&device_name, schema::MODEM_CONTROL_DELETE_MESSAGE, "0")
            .await
    }

    pub(super) async fn set_delete_message_writable(
        &mut self,
        modem_id: &ModemId,
        modem_index: u32,
        writable: bool,
    ) -> Result<()> {
        self.ensure_modem_sms_controls(modem_id, modem_index)
            .await?;
        self.publish_delete_message_control(modem_index, writable)
            .await
    }

    pub(super) async fn publish_picked_sms(
        &self,
        modem_id: &ModemId,
        modem_index: u32,
        snapshot: Option<&SmsSnapshot>,
    ) -> Result<()> {
        let device_name = schema::device_name_for_modem(modem_index);

        match snapshot {
            Some(snapshot) => {
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_DBUS_ID,
                    &snapshot.sms_id.0,
                )
                .await?;
                self.publish_optional_timestamp_controls(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_TIMESTAMP,
                    schema::MODEM_CONTROL_SELECTED_SMS_TIMESTAMP_UNIXTIME,
                    snapshot.timestamp,
                )
                .await?;
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_SENDER,
                    snapshot.number.as_deref(),
                )
                .await?;
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_TEXT,
                    snapshot.text.as_deref(),
                )
                .await?;
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_IS_RECEIVED,
                    switch_payload(snapshot.is_received),
                )
                .await?;
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_STORAGE,
                    &snapshot.storage,
                )
                .await?;
            }
            None => {
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_DBUS_ID,
                    MqttNull,
                )
                .await?;
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_TIMESTAMP,
                    MqttNull,
                )
                .await?;
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_TIMESTAMP_UNIXTIME,
                    MqttNull,
                )
                .await?;
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_SENDER,
                    MqttNull,
                )
                .await?;
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_TEXT,
                    MqttNull,
                )
                .await?;
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_IS_RECEIVED,
                    switch_payload(false),
                )
                .await?;
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_STORAGE,
                    MqttNull,
                )
                .await?;
            }
        }

        info!(
            target: LOG_TARGET,
            "{}",
            schema::mqtt_publish_picked_sms_message(
                modem_index,
                &modem_id.0,
                snapshot.map(SmsSnapshot::summary).as_deref(),
            )
        );

        Ok(())
    }

    pub(super) async fn publish_sms_update(
        &self,
        modem_index: u32,
        update: &SmsUpdate,
    ) -> Result<()> {
        let device_name = schema::device_name_for_modem(modem_index);

        match &update.property {
            SmsPropertyChange::IsReceived(value) => {
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_IS_RECEIVED,
                    switch_payload(*value),
                )
                .await
            }
            SmsPropertyChange::Storage(value) => {
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_STORAGE,
                    value,
                )
                .await
            }
            SmsPropertyChange::Timestamp(value) => {
                self.publish_optional_timestamp_controls(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_TIMESTAMP,
                    schema::MODEM_CONTROL_SELECTED_SMS_TIMESTAMP_UNIXTIME,
                    *value,
                )
                .await
            }
            SmsPropertyChange::Number(value) => {
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_SENDER,
                    value.as_deref(),
                )
                .await
            }
            SmsPropertyChange::Text(value) => {
                self.publish_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_TEXT,
                    value.as_deref(),
                )
                .await
            }
        }
    }

    pub(super) async fn cleanup_session(&mut self, modem_indices: Vec<u32>) -> Result<()> {
        for modem_index in modem_indices {
            self.cleanup_modem_device(modem_index).await?;
        }

        if self.state.main_device_created {
            self.cleanup_main_device().await?;
        }

        self.state = MqttPublishState::default();
        Ok(())
    }

    async fn cleanup_main_device(&self) -> Result<()> {
        self.cleanup_device(schema::MM_DEVICE_NAME, schema::manager_control_specs())
            .await
    }

    pub(super) async fn cleanup_modem_device(&mut self, modem_index: u32) -> Result<()> {
        let device_name = schema::device_name_for_modem(modem_index);
        for spec in schema::modem_base_control_specs() {
            self.cleanup_control(&device_name, spec).await?;
        }
        for spec in schema::modem_sms_control_specs() {
            self.cleanup_control(&device_name, spec).await?;
        }

        self.state.modem_sms_controls_created.remove(&modem_index);
        self.state
            .subscribed_modem_sms_controls
            .remove(&modem_index);
        self.unpublish_retained(schema::device_meta_topic(&device_name))
            .await
    }

    async fn cleanup_device(&self, device_name: &str, control_specs: &[ControlSpec]) -> Result<()> {
        for spec in control_specs {
            self.cleanup_control(device_name, spec).await?;
        }

        self.unpublish_retained(schema::device_meta_topic(device_name))
            .await?;
        Ok(())
    }

    async fn publish_control_metadata(&self, device_name: &str, spec: &ControlSpec) -> Result<()> {
        self.publish_retained(
            schema::control_meta_topic(device_name, spec.name),
            schema::control_meta_payload(spec),
        )
        .await?;

        for (field, payload) in schema::control_meta_leaf_payloads(spec) {
            self.publish_retained(
                schema::control_meta_leaf_topic(device_name, spec.name, field),
                payload,
            )
            .await?;
        }

        Ok(())
    }

    pub(super) async fn publish_control(
        &self,
        device_name: &str,
        control_name: &str,
        payload: impl IntoMqttPayload,
    ) -> Result<()> {
        self.publish_retained(
            schema::control_value_topic(device_name, control_name),
            payload.into_mqtt_payload(),
        )
        .await
    }

    async fn publish_optional_timestamp_controls(
        &self,
        device_name: &str,
        text_control_name: &str,
        unixtime_control_name: &str,
        value: Option<OffsetDateTime>,
    ) -> Result<()> {
        let text_payload = value.map(format_timestamp_for_wb);
        let unixtime_payload = value.map(|value| value.unix_timestamp());

        self.publish_control(device_name, text_control_name, text_payload)
            .await?;
        self.publish_control(device_name, unixtime_control_name, unixtime_payload)
            .await
    }

    async fn cleanup_control(&self, device_name: &str, spec: &ControlSpec) -> Result<()> {
        self.unpublish_retained(schema::control_meta_topic(device_name, spec.name))
            .await?;

        for (field, _) in schema::control_meta_leaf_payloads(spec) {
            self.unpublish_retained(schema::control_meta_leaf_topic(
                device_name,
                spec.name,
                field,
            ))
            .await?;
        }

        self.unpublish_retained(schema::control_on_topic(device_name, spec.name))
            .await?;
        self.unpublish_retained(schema::control_value_topic(device_name, spec.name))
            .await?;

        Ok(())
    }

    async fn publish_retained(
        &self,
        topic: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Result<()> {
        let topic = topic.into();
        self.client
            .publish(topic.clone(), QoS::AtMostOnce, true, payload)
            .await
            .with_context(|| format!("failed to publish retained MQTT topic `{topic}`"))
    }

    async fn unpublish_retained(&self, topic: impl Into<String>) -> Result<()> {
        let topic = topic.into();
        self.client
            .publish(topic.clone(), QoS::AtLeastOnce, true, Vec::<u8>::new())
            .await
            .with_context(|| format!("failed to clear retained MQTT topic `{topic}`"))
    }
}

pub(super) fn switch_payload(value: bool) -> WbSwitch {
    WbSwitch(value)
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

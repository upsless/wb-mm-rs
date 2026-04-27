use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rumqttc::{AsyncClient, Event, LastWill, MqttOptions, Packet, Publish, QoS, Transport};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::dbus::{self, ModemId, ModemManagerStatus, ModemSnapshot, ModemUpdate, SmsSnapshot};
use crate::exchange::{MqttCommand, MqttEvent};
use crate::mqtt::logics::{self, ControlSpec};

const LOG_TARGET: &str = "MQTT";
const DEFAULT_MQTT_ADDRESS: &str = "unix:///var/run/mosquitto/mosquitto.sock";
const DEFAULT_MQTT_PORT: u16 = 1883;
const MQTT_CLIENT_ID_PREFIX: &str = "wb-mm-mqtt";
const MQTT_KEEP_ALIVE: Duration = Duration::from_secs(60);
const MQTT_REQUEST_QUEUE_CAPACITY: usize = 16;
const MQTT_INCOMING_CHANNEL_CAPACITY: usize = 32;

/// MQTT lifecycle loop with a real broker connection, retained publishes and
/// incoming `/on` command handling.
pub async fn run(
    mqtt_address: Option<String>,
    mut shutdown_rx: watch::Receiver<bool>,
    mut command_rx: mpsc::Receiver<MqttCommand>,
    mqtt_event_tx: mpsc::Sender<MqttEvent>,
) -> Result<()> {
    let mqtt_options = build_mqtt_options(mqtt_address.as_deref())?;
    let (client, eventloop) = AsyncClient::new(mqtt_options, MQTT_REQUEST_QUEUE_CAPACITY);
    let mut frontend = MqttFrontend::new(client.clone());
    let (eventloop_stop_tx, eventloop_stop_rx) = watch::channel(false);
    let (incoming_publish_tx, mut incoming_publish_rx) =
        mpsc::channel(MQTT_INCOMING_CHANNEL_CAPACITY);
    let mut eventloop_task = tokio::spawn(run_eventloop(
        eventloop_stop_rx,
        eventloop,
        incoming_publish_tx,
    ));

    loop {
        tokio::select! {
            result = wait_for_shutdown(&mut shutdown_rx) => {
                result?;
                frontend.stop(&eventloop_stop_tx, &mut eventloop_task).await?;
                break;
            }
            maybe_command = command_rx.recv() => {
                let Some(command) = maybe_command else {
                    frontend.stop(&eventloop_stop_tx, &mut eventloop_task).await?;
                    break;
                };
                frontend.handle_command(command).await?;
            }
            maybe_publish = incoming_publish_rx.recv() => {
                let Some(publish) = maybe_publish else {
                    return Ok(());
                };
                frontend
                    .handle_incoming_publish(publish, &mqtt_event_tx)
                    .await?;
            }
            result = &mut eventloop_task => {
                return eventloop_result(result);
            }
        }
    }

    debug!(target: LOG_TARGET, "{}", logics::mqtt_stopped_message());

    Ok(())
}

struct MqttFrontend {
    client: AsyncClient,
    state: MqttSessionState,
}

impl MqttFrontend {
    fn new(client: AsyncClient) -> Self {
        Self {
            client,
            state: MqttSessionState::default(),
        }
    }

    async fn stop(
        &mut self,
        eventloop_stop_tx: &watch::Sender<bool>,
        eventloop_task: &mut JoinHandle<Result<()>>,
    ) -> Result<()> {
        self.cleanup_session().await?;
        let _ = eventloop_stop_tx.send(true);
        let _ = self.client.disconnect().await;
        eventloop_result(eventloop_task.await)
    }

    async fn handle_command(&mut self, command: MqttCommand) -> Result<()> {
        match command {
            MqttCommand::EnsureModemManagerDevice => {
                self.ensure_manager_device().await?;
                info!(target: LOG_TARGET, "{}", logics::mqtt_ensure_mm_device_message());
            }
            MqttCommand::PublishModemManagerStatus(status) => {
                self.ensure_manager_device().await?;
                self.publish_text_control(
                    logics::MM_DEVICE_NAME,
                    logics::MM_CONTROL_STATUS,
                    modemmanager_status_name(status),
                )
                .await?;
                info!(
                    target: LOG_TARGET,
                    "{}",
                    logics::mqtt_publish_mm_status_message(modemmanager_status_name(status))
                );
            }
            MqttCommand::PublishModemManagerVersion(version) => {
                self.ensure_manager_device().await?;
                self.publish_text_control(
                    logics::MM_DEVICE_NAME,
                    logics::MM_CONTROL_VERSION,
                    &version,
                )
                .await?;
                info!(target: LOG_TARGET, "{}", logics::mqtt_publish_mm_version_message(&version));
            }
            MqttCommand::PublishModemManagerModemCount(modem_count) => {
                self.ensure_manager_device().await?;
                self.publish_number_control(
                    logics::MM_DEVICE_NAME,
                    logics::MM_CONTROL_MODEM_COUNT,
                    modem_count,
                )
                .await?;
                info!(
                    target: LOG_TARGET,
                    "{}",
                    logics::mqtt_publish_mm_modem_count_message(modem_count)
                );
            }
            MqttCommand::PublishModemManagerSmsCount(sms_count) => {
                self.ensure_manager_device().await?;
                self.publish_number_control(
                    logics::MM_DEVICE_NAME,
                    logics::MM_CONTROL_SMS_COUNT,
                    sms_count,
                )
                .await?;
                info!(
                    target: LOG_TARGET,
                    "{}",
                    logics::mqtt_publish_mm_sms_count_message(sms_count)
                );
            }
            MqttCommand::PublishModemManagerLastSms(last_sms) => {
                self.ensure_manager_device().await?;
                self.publish_optional_timestamp_text_control(
                    logics::MM_DEVICE_NAME,
                    logics::MM_CONTROL_LAST_SMS,
                    last_sms,
                )
                .await?;
                info!(
                    target: LOG_TARGET,
                    "{}",
                    logics::mqtt_publish_mm_last_sms_message(last_sms)
                );
            }
            MqttCommand::EnsureModemDevice { modem_id } => {
                let modem_index = self.ensure_modem_device(&modem_id).await?;
                info!(
                    target: LOG_TARGET,
                    "{}",
                    logics::mqtt_ensure_modem_device_message(modem_index, &modem_id.0)
                );
            }
            MqttCommand::PublishModemSnapshot { modem_id, snapshot } => {
                let modem_index = self.ensure_modem_device(&modem_id).await?;
                self.publish_modem_snapshot(modem_index, &snapshot).await?;
                info!(
                    target: LOG_TARGET,
                    "{}",
                    logics::mqtt_publish_modem_snapshot_message(
                        modem_index,
                        &modem_id.0,
                        &snapshot.summary(),
                    )
                );
            }
            MqttCommand::PublishModemUpdate { modem_id, update } => {
                let modem_index = self.ensure_modem_device(&modem_id).await?;
                self.publish_modem_update(modem_index, &update).await?;
                info!(
                    target: LOG_TARGET,
                    "{}",
                    logics::mqtt_publish_modem_update_message(
                        modem_index,
                        &modem_id.0,
                        &update.summary(),
                    )
                );
            }
            MqttCommand::PublishModemSmsCount {
                modem_id,
                sms_count,
            } => {
                let modem_index = self.ensure_modem_device(&modem_id).await?;
                let device_name = logics::device_name_for_modem(modem_index);
                self.publish_number_control(
                    &device_name,
                    logics::MODEM_CONTROL_SMS_COUNT,
                    sms_count,
                )
                .await?;
                info!(
                    target: LOG_TARGET,
                    "{}",
                    logics::mqtt_publish_modem_sms_count_message(modem_index, &modem_id.0, sms_count)
                );
            }
            MqttCommand::PublishModemSmsSelection {
                modem_id,
                selected_index,
                max_index,
                writable,
            } => {
                let modem_index = self.ensure_modem_device(&modem_id).await?;
                self.publish_modem_sms_selection(modem_index, selected_index, max_index, writable)
                    .await?;
                info!(
                    target: LOG_TARGET,
                    "{}",
                    logics::mqtt_publish_modem_sms_selection_message(
                        modem_index,
                        &modem_id.0,
                        selected_index,
                        max_index,
                        writable,
                    )
                );
            }
            MqttCommand::PublishSelectedSms { modem_id, snapshot } => {
                let modem_index = self.ensure_modem_device(&modem_id).await?;
                self.publish_selected_sms(modem_index, snapshot.as_ref())
                    .await?;
                info!(
                    target: LOG_TARGET,
                    "{}",
                    logics::mqtt_publish_selected_sms_message(
                        modem_index,
                        &modem_id.0,
                        snapshot.as_ref().map(SmsSnapshot::summary).as_deref(),
                    )
                );
            }
            MqttCommand::DeleteModemDevice { modem_id } => {
                if let Some(modem_index) = self.state.remove_modem_index(&modem_id) {
                    self.cleanup_modem_device(modem_index).await?;
                    info!(
                        target: LOG_TARGET,
                        "{}",
                        logics::mqtt_delete_modem_device_message(modem_index, &modem_id.0)
                    );
                }
            }
        }

        Ok(())
    }

    async fn handle_incoming_publish(
        &self,
        publish: Publish,
        mqtt_event_tx: &mpsc::Sender<MqttEvent>,
    ) -> Result<()> {
        let Some(modem_index) = parse_message_select_topic(&publish.topic) else {
            return Ok(());
        };
        let Some(modem_id) = self.state.modem_id_for_index(modem_index).cloned() else {
            return Ok(());
        };
        // WB writable controls are driven through the `/on` topic. Here we
        // translate the user-facing modem index back into the internal DBus
        // modem id before handing control to the tresher.
        let Ok(payload) = std::str::from_utf8(&publish.payload) else {
            debug!(
                target: LOG_TARGET,
                "Ignoring non-UTF8 message_select payload in topic `{}`",
                publish.topic
            );
            return Ok(());
        };
        let payload = payload.trim();
        let Ok(selected_index) = payload.parse::<u32>() else {
            debug!(
                target: LOG_TARGET,
                "Ignoring invalid message_select payload `{payload}` in topic `{}`",
                publish.topic
            );
            return Ok(());
        };

        if mqtt_event_tx
            .send(MqttEvent::SelectModemSms {
                modem_id,
                selected_index,
            })
            .await
            .is_err()
        {
            debug!(target: LOG_TARGET, "MQTT event channel closed while sending");
        }

        Ok(())
    }

    async fn ensure_manager_device(&mut self) -> Result<()> {
        if self.state.manager_device_created {
            return Ok(());
        }

        self.publish_retained(
            logics::device_meta_topic(logics::MM_DEVICE_NAME),
            logics::manager_device_title_payload(),
        )
        .await?;

        for spec in logics::manager_control_specs() {
            self.publish_control_metadata(logics::MM_DEVICE_NAME, spec)
                .await?;
        }

        self.publish_text_control(logics::MM_DEVICE_NAME, logics::MM_CONTROL_IS_AVAILABLE, "1")
            .await?;
        self.publish_null_control(logics::MM_DEVICE_NAME, logics::MM_CONTROL_STATUS)
            .await?;
        self.publish_null_control(logics::MM_DEVICE_NAME, logics::MM_CONTROL_VERSION)
            .await?;
        self.publish_null_control(logics::MM_DEVICE_NAME, logics::MM_CONTROL_MODEM_COUNT)
            .await?;
        self.publish_number_control(logics::MM_DEVICE_NAME, logics::MM_CONTROL_SMS_COUNT, 0)
            .await?;
        self.publish_null_control(logics::MM_DEVICE_NAME, logics::MM_CONTROL_LAST_SMS)
            .await?;

        self.state.manager_device_created = true;
        Ok(())
    }

    async fn ensure_modem_device(&mut self, modem_id: &ModemId) -> Result<u32> {
        let (modem_index, created_now) = self.state.ensure_modem_index(modem_id);
        if !created_now {
            return Ok(modem_index);
        }

        let device_name = logics::device_name_for_modem(modem_index);
        self.publish_retained(
            logics::device_meta_topic(&device_name),
            logics::modem_device_title_payload(modem_index, &modem_id.0),
        )
        .await?;

        for spec in logics::modem_control_specs() {
            self.publish_control_metadata(&device_name, spec).await?;
        }

        self.subscribe_to_message_select(modem_index).await?;
        self.publish_text_control(&device_name, logics::MODEM_CONTROL_IS_ACTIVE, "0")
            .await?;
        self.publish_null_control(&device_name, logics::MODEM_CONTROL_MODEL)
            .await?;
        self.publish_null_control(&device_name, logics::MODEM_CONTROL_REVISION)
            .await?;
        self.publish_null_control(&device_name, logics::MODEM_CONTROL_STATE)
            .await?;
        self.publish_null_control(&device_name, logics::MODEM_CONTROL_PRIMARY_SIM_SLOT)
            .await?;
        self.publish_null_control(&device_name, logics::MODEM_CONTROL_OPERATOR_NAME)
            .await?;
        self.publish_null_control(&device_name, logics::MODEM_CONTROL_SIGNAL_QUALITY)
            .await?;
        self.publish_number_control(&device_name, logics::MODEM_CONTROL_SMS_COUNT, 0)
            .await?;
        self.publish_modem_sms_selection(modem_index, None, 1, false)
            .await?;
        self.publish_selected_sms(modem_index, None).await?;

        Ok(modem_index)
    }

    async fn subscribe_to_message_select(&mut self, modem_index: u32) -> Result<()> {
        if !self.state.subscribed_message_select.insert(modem_index) {
            return Ok(());
        }

        let device_name = logics::device_name_for_modem(modem_index);
        let topic = logics::control_on_topic(&device_name, logics::MODEM_CONTROL_MESSAGE_SELECT);
        self.client
            .subscribe(topic.clone(), QoS::AtMostOnce)
            .await
            .with_context(|| format!("failed to subscribe to MQTT topic `{topic}`"))
    }

    async fn publish_modem_snapshot(
        &self,
        modem_index: u32,
        snapshot: &ModemSnapshot,
    ) -> Result<()> {
        let device_name = logics::device_name_for_modem(modem_index);

        self.publish_text_control(
            &device_name,
            logics::MODEM_CONTROL_IS_ACTIVE,
            switch_payload(snapshot.is_active),
        )
        .await?;
        self.publish_optional_text_control(
            &device_name,
            logics::MODEM_CONTROL_MODEL,
            snapshot.model.as_deref(),
        )
        .await?;
        self.publish_optional_text_control(
            &device_name,
            logics::MODEM_CONTROL_REVISION,
            snapshot.revision.as_deref(),
        )
        .await?;
        self.publish_optional_text_control(
            &device_name,
            logics::MODEM_CONTROL_STATE,
            snapshot.state.as_deref(),
        )
        .await?;
        self.publish_optional_number_control(
            &device_name,
            logics::MODEM_CONTROL_PRIMARY_SIM_SLOT,
            snapshot.primary_sim_slot,
        )
        .await?;
        self.publish_optional_text_control(
            &device_name,
            logics::MODEM_CONTROL_OPERATOR_NAME,
            snapshot.operator_name.as_deref(),
        )
        .await?;
        self.publish_optional_number_control(
            &device_name,
            logics::MODEM_CONTROL_SIGNAL_QUALITY,
            snapshot.signal_quality,
        )
        .await?;

        Ok(())
    }

    async fn publish_modem_update(&self, modem_index: u32, update: &ModemUpdate) -> Result<()> {
        let device_name = logics::device_name_for_modem(modem_index);

        match update {
            ModemUpdate::Model(value) => {
                self.publish_text_control(&device_name, logics::MODEM_CONTROL_MODEL, value)
                    .await?;
            }
            ModemUpdate::Revision(value) => {
                self.publish_text_control(&device_name, logics::MODEM_CONTROL_REVISION, value)
                    .await?;
            }
            ModemUpdate::State(value) => {
                self.publish_optional_text_control(
                    &device_name,
                    logics::MODEM_CONTROL_STATE,
                    value.as_deref(),
                )
                .await?;
            }
            ModemUpdate::PrimarySimSlot(value) => {
                self.publish_number_control(
                    &device_name,
                    logics::MODEM_CONTROL_PRIMARY_SIM_SLOT,
                    *value,
                )
                .await?;
            }
            ModemUpdate::OperatorName(value) => {
                self.publish_optional_text_control(
                    &device_name,
                    logics::MODEM_CONTROL_OPERATOR_NAME,
                    value.as_deref(),
                )
                .await?;
            }
            ModemUpdate::SignalQuality(value) => {
                self.publish_optional_number_control(
                    &device_name,
                    logics::MODEM_CONTROL_SIGNAL_QUALITY,
                    *value,
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn publish_modem_sms_selection(
        &self,
        modem_index: u32,
        selected_index: Option<u32>,
        max_index: u32,
        writable: bool,
    ) -> Result<()> {
        let device_name = logics::device_name_for_modem(modem_index);
        let spec = logics::dynamic_message_select_spec(!writable, max_index);
        self.publish_control_metadata(&device_name, &spec).await?;

        match selected_index {
            Some(selected_index) => {
                self.publish_number_control(
                    &device_name,
                    logics::MODEM_CONTROL_MESSAGE_SELECT,
                    selected_index,
                )
                .await?;
            }
            None => {
                self.publish_number_control(&device_name, logics::MODEM_CONTROL_MESSAGE_SELECT, 1)
                    .await?;
            }
        }

        Ok(())
    }

    async fn publish_selected_sms(
        &self,
        modem_index: u32,
        snapshot: Option<&SmsSnapshot>,
    ) -> Result<()> {
        let device_name = logics::device_name_for_modem(modem_index);

        match snapshot {
            Some(snapshot) => {
                self.publish_optional_timestamp_text_control(
                    &device_name,
                    logics::MODEM_CONTROL_SELECTED_SMS_TIMESTAMP,
                    snapshot.timestamp,
                )
                .await?;
                self.publish_optional_text_control(
                    &device_name,
                    logics::MODEM_CONTROL_SELECTED_SMS_SENDER,
                    snapshot.number.as_deref(),
                )
                .await?;
                self.publish_optional_text_control(
                    &device_name,
                    logics::MODEM_CONTROL_SELECTED_SMS_TEXT,
                    snapshot.text.as_deref(),
                )
                .await?;
                self.publish_text_control(
                    &device_name,
                    logics::MODEM_CONTROL_SELECTED_SMS_IS_RECEIVED,
                    switch_payload(snapshot.is_received),
                )
                .await?;
            }
            None => {
                self.publish_null_control(
                    &device_name,
                    logics::MODEM_CONTROL_SELECTED_SMS_TIMESTAMP,
                )
                .await?;
                self.publish_null_control(&device_name, logics::MODEM_CONTROL_SELECTED_SMS_SENDER)
                    .await?;
                self.publish_null_control(&device_name, logics::MODEM_CONTROL_SELECTED_SMS_TEXT)
                    .await?;
                self.publish_text_control(
                    &device_name,
                    logics::MODEM_CONTROL_SELECTED_SMS_IS_RECEIVED,
                    "0",
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn cleanup_session(&mut self) -> Result<()> {
        let modem_entries: Vec<_> = self
            .state
            .modem_indices
            .iter()
            .map(|(modem_id, modem_index)| (modem_id.clone(), *modem_index))
            .collect();

        for (_, modem_index) in modem_entries {
            self.cleanup_modem_device(modem_index).await?;
        }

        if self.state.manager_device_created {
            self.cleanup_manager_device().await?;
        }

        self.state = MqttSessionState::default();
        Ok(())
    }

    async fn cleanup_manager_device(&self) -> Result<()> {
        self.cleanup_device(logics::MM_DEVICE_NAME, logics::manager_control_specs())
            .await
    }

    async fn cleanup_modem_device(&self, modem_index: u32) -> Result<()> {
        let device_name = logics::device_name_for_modem(modem_index);
        self.cleanup_device(&device_name, logics::modem_control_specs())
            .await
    }

    async fn cleanup_device(&self, device_name: &str, control_specs: &[ControlSpec]) -> Result<()> {
        for spec in control_specs {
            self.unpublish_control(device_name, spec.name).await?;
        }

        self.unpublish_retained(logics::device_meta_topic(device_name))
            .await?;
        Ok(())
    }

    async fn publish_control_metadata(&self, device_name: &str, spec: &ControlSpec) -> Result<()> {
        self.publish_retained(
            logics::control_meta_topic(device_name, spec.name),
            logics::control_meta_payload(spec),
        )
        .await?;

        for (field, payload) in logics::control_meta_leaf_payloads(spec) {
            self.publish_retained(
                logics::control_meta_leaf_topic(device_name, spec.name, field),
                payload,
            )
            .await?;
        }

        Ok(())
    }

    async fn publish_text_control(
        &self,
        device_name: &str,
        control_name: &str,
        payload: &str,
    ) -> Result<()> {
        self.publish_retained(
            logics::control_value_topic(device_name, control_name),
            payload,
        )
        .await
    }

    async fn publish_null_control(&self, device_name: &str, control_name: &str) -> Result<()> {
        self.publish_text_control(device_name, control_name, "null")
            .await
    }

    async fn publish_number_control(
        &self,
        device_name: &str,
        control_name: &str,
        value: impl ToString,
    ) -> Result<()> {
        self.publish_retained(
            logics::control_value_topic(device_name, control_name),
            value.to_string(),
        )
        .await
    }

    async fn publish_optional_timestamp_text_control(
        &self,
        device_name: &str,
        control_name: &str,
        value: Option<i64>,
    ) -> Result<()> {
        match value.and_then(dbus::format_unix_timestamp_for_wb) {
            Some(value) => {
                self.publish_text_control(device_name, control_name, &value)
                    .await
            }
            None => self.publish_null_control(device_name, control_name).await,
        }
    }

    async fn publish_optional_text_control(
        &self,
        device_name: &str,
        control_name: &str,
        value: Option<&str>,
    ) -> Result<()> {
        match value {
            Some(value) => {
                self.publish_text_control(device_name, control_name, value)
                    .await
            }
            None => self.publish_null_control(device_name, control_name).await,
        }
    }

    async fn publish_optional_number_control(
        &self,
        device_name: &str,
        control_name: &str,
        value: Option<u32>,
    ) -> Result<()> {
        match value {
            Some(value) => {
                self.publish_number_control(device_name, control_name, value)
                    .await
            }
            None => self.publish_null_control(device_name, control_name).await,
        }
    }

    async fn unpublish_control(&self, device_name: &str, control_name: &str) -> Result<()> {
        self.unpublish_retained(logics::control_meta_topic(device_name, control_name))
            .await?;

        for (field, _) in logics::control_meta_leaf_payloads(
            control_spec_by_name(control_name).expect("unknown control spec"),
        ) {
            self.unpublish_retained(logics::control_meta_leaf_topic(
                device_name,
                control_name,
                field,
            ))
            .await?;
        }

        self.unpublish_retained(logics::control_on_topic(device_name, control_name))
            .await?;
        self.unpublish_retained(logics::control_value_topic(device_name, control_name))
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
            .publish(topic.clone(), QoS::AtMostOnce, true, Vec::<u8>::new())
            .await
            .with_context(|| format!("failed to clear retained MQTT topic `{topic}`"))
    }
}

#[derive(Debug, Default)]
struct MqttSessionState {
    manager_device_created: bool,
    modem_indices: HashMap<ModemId, u32>,
    reverse_modem_indices: HashMap<u32, ModemId>,
    subscribed_message_select: HashSet<u32>,
}

impl MqttSessionState {
    fn ensure_modem_index(&mut self, modem_id: &ModemId) -> (u32, bool) {
        if let Some(modem_index) = self.modem_indices.get(modem_id) {
            return (*modem_index, false);
        }

        let mut candidate = 1;
        while self.modem_indices.values().any(|value| *value == candidate) {
            candidate += 1;
        }

        self.modem_indices.insert(modem_id.clone(), candidate);
        self.reverse_modem_indices
            .insert(candidate, modem_id.clone());
        (candidate, true)
    }

    fn remove_modem_index(&mut self, modem_id: &ModemId) -> Option<u32> {
        let modem_index = self.modem_indices.remove(modem_id)?;
        self.reverse_modem_indices.remove(&modem_index);
        self.subscribed_message_select.remove(&modem_index);
        Some(modem_index)
    }

    fn modem_id_for_index(&self, modem_index: u32) -> Option<&ModemId> {
        self.reverse_modem_indices.get(&modem_index)
    }
}

fn build_mqtt_options(mqtt_address: Option<&str>) -> Result<MqttOptions> {
    let mqtt_address = mqtt_address.unwrap_or(DEFAULT_MQTT_ADDRESS);
    let client_id = format!("{MQTT_CLIENT_ID_PREFIX}-{}", std::process::id());

    let mut mqtt_options = match parse_mqtt_endpoint(mqtt_address)? {
        MqttEndpoint::Unix { path } => {
            let mut options = MqttOptions::new(client_id, path, DEFAULT_MQTT_PORT);
            options.set_transport(Transport::unix());
            options
        }
        MqttEndpoint::Tcp { host, port } => MqttOptions::new(client_id, host, port),
    };

    mqtt_options.set_keep_alive(MQTT_KEEP_ALIVE);
    mqtt_options.set_last_will(LastWill::new(
        logics::mm_availability_topic(),
        Vec::<u8>::new(),
        QoS::AtMostOnce,
        true,
    ));

    Ok(mqtt_options)
}

async fn run_eventloop(
    stop_rx: watch::Receiver<bool>,
    mut eventloop: rumqttc::EventLoop,
    incoming_publish_tx: mpsc::Sender<Publish>,
) -> Result<()> {
    let mut connected = false;
    let stop_rx = stop_rx;

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                if !connected {
                    connected = true;
                    debug!(target: LOG_TARGET, "{}", logics::mqtt_connected_message());
                }
            }
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                if incoming_publish_tx.send(publish).await.is_err() {
                    return Ok(());
                }
            }
            Ok(Event::Outgoing(rumqttc::Outgoing::Disconnect)) if *stop_rx.borrow() => {
                return Ok(());
            }
            Ok(_) => {}
            Err(_) if *stop_rx.borrow() => {
                return Ok(());
            }
            Err(err) => {
                return Err(err).context("failed to poll MQTT event loop");
            }
        }
    }
}

fn eventloop_result(result: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    result.map_err(|error| anyhow::anyhow!("MQTT event loop task join failed: {error}"))?
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MqttEndpoint {
    Unix { path: String },
    Tcp { host: String, port: u16 },
}

fn parse_mqtt_endpoint(mqtt_address: &str) -> Result<MqttEndpoint> {
    let (scheme, remainder) = mqtt_address
        .split_once("://")
        .with_context(|| format!("invalid MQTT address `{mqtt_address}`: missing scheme"))?;

    match scheme {
        "unix" => {
            if remainder.is_empty() {
                bail!("invalid MQTT address `{mqtt_address}`: empty unix socket path");
            }

            Ok(MqttEndpoint::Unix {
                path: remainder.to_string(),
            })
        }
        "tcp" | "mqtt" | "mqtt-tcp" => {
            let broker = remainder
                .split('/')
                .next()
                .filter(|broker| !broker.is_empty())
                .with_context(|| format!("invalid MQTT address `{mqtt_address}`: empty broker"))?;

            let (host, port) = match broker.rsplit_once(':') {
                Some((host, port)) if !host.is_empty() => (
                    host.to_string(),
                    port.parse::<u16>().with_context(|| {
                        format!("invalid MQTT address `{mqtt_address}`: bad port `{port}`")
                    })?,
                ),
                _ => (broker.to_string(), DEFAULT_MQTT_PORT),
            };

            Ok(MqttEndpoint::Tcp { host, port })
        }
        _ => bail!(
            "unsupported MQTT address scheme `{scheme}` in `{mqtt_address}`; supported schemes are unix://, tcp://, mqtt:// and mqtt-tcp://"
        ),
    }
}

fn control_spec_by_name(control_name: &str) -> Option<&'static ControlSpec> {
    logics::manager_control_specs()
        .iter()
        .chain(logics::modem_control_specs().iter())
        .find(|spec| spec.name == control_name)
}

fn parse_message_select_topic(topic: &str) -> Option<u32> {
    let prefix = format!("/devices/{}", logics::MM_MODEM_DEVICE_PREFIX);
    let suffix = format!("/controls/{}/on", logics::MODEM_CONTROL_MESSAGE_SELECT);
    let modem_index = topic.strip_prefix(&prefix)?.strip_suffix(&suffix)?;
    modem_index.parse::<u32>().ok()
}

fn modemmanager_status_name(status: ModemManagerStatus) -> &'static str {
    match status {
        ModemManagerStatus::Active => "active",
        ModemManagerStatus::Inactive => "inactive",
        ModemManagerStatus::NotFound => "not_found",
    }
}

fn switch_payload(value: bool) -> &'static str {
    if value { "1" } else { "0" }
}

async fn wait_for_shutdown(shutdown_rx: &mut watch::Receiver<bool>) -> Result<()> {
    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        if shutdown_rx.changed().await.is_err() {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MqttEndpoint, MqttSessionState, parse_message_select_topic, parse_mqtt_endpoint};
    use crate::dbus::ModemId;

    #[test]
    fn parses_unix_endpoint() {
        let endpoint = parse_mqtt_endpoint("unix:///var/run/mosquitto/mosquitto.sock").unwrap();
        assert_eq!(
            endpoint,
            MqttEndpoint::Unix {
                path: "/var/run/mosquitto/mosquitto.sock".to_string(),
            }
        );
    }

    #[test]
    fn parses_tcp_endpoint_with_default_port() {
        let endpoint = parse_mqtt_endpoint("tcp://wb.loc").unwrap();
        assert_eq!(
            endpoint,
            MqttEndpoint::Tcp {
                host: "wb.loc".to_string(),
                port: 1883,
            }
        );
    }

    #[test]
    fn modem_indices_start_from_one_and_reuse_gaps() {
        let mut state = MqttSessionState::default();

        let (first, first_created) = state.ensure_modem_index(&ModemId("0".to_string()));
        let (second, second_created) = state.ensure_modem_index(&ModemId("1".to_string()));
        let _ = state.remove_modem_index(&ModemId("0".to_string()));
        let (reused, reused_created) = state.ensure_modem_index(&ModemId("2".to_string()));

        assert_eq!((first, first_created), (1, true));
        assert_eq!((second, second_created), (2, true));
        assert_eq!((reused, reused_created), (1, true));
    }

    #[test]
    fn parses_message_select_topic() {
        assert_eq!(
            parse_message_select_topic("/devices/mm_modem_3/controls/message_select/on"),
            Some(3)
        );
        assert_eq!(
            parse_message_select_topic("/devices/mm_modem_3/controls/model/on"),
            None
        );
    }
}

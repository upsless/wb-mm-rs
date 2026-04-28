use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rumqttc::{AsyncClient, Event, LastWill, MqttOptions, Packet, Publish, QoS, Transport};
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, info};

use crate::dbus::{
    self, ModemId, ModemManagerStatus, ModemSnapshot, ModemUpdate, SmsId, SmsSnapshot, SmsUpdate,
};
use crate::exchange::{MqttCommand, MqttEvent};
use crate::mqtt::logics::{self, ControlSpec};

const LOG_TARGET: &str = "MQTT";
const DEFAULT_MQTT_ADDRESS: &str = "unix:///var/run/mosquitto/mosquitto.sock";
const DEFAULT_MQTT_PORT: u16 = 1883;
const MQTT_CLIENT_ID_PREFIX: &str = "wb-mm-mqtt";
const MQTT_KEEP_ALIVE: Duration = Duration::from_secs(60);
const MQTT_REQUEST_QUEUE_CAPACITY: usize = 16;
const MQTT_INCOMING_CHANNEL_CAPACITY: usize = 32;
const MQTT_GRACEFUL_CLEANUP_FLUSH_DELAY: Duration = Duration::from_millis(500);

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
                frontend.handle_command(command, &mqtt_event_tx).await?;
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
        sleep(MQTT_GRACEFUL_CLEANUP_FLUSH_DELAY).await;
        let _ = eventloop_stop_tx.send(true);
        let _ = self.client.disconnect().await;
        eventloop_result(eventloop_task.await)
    }

    async fn handle_command(
        &mut self,
        command: MqttCommand,
        mqtt_event_tx: &mpsc::Sender<MqttEvent>,
    ) -> Result<()> {
        match command {
            MqttCommand::EnsureModemManagerDevice => {
                self.ensure_manager_device().await?;
                info!(target: LOG_TARGET, "{}", logics::mqtt_ensure_mm_device_message());
            }
            MqttCommand::PublishModemManagerStatus(status) => {
                self.ensure_manager_device().await?;
                self.publish_text_control(
                    logics::MM_DEVICE_NAME,
                    logics::MM_CONTROL_IS_AVAILABLE,
                    switch_payload(modemmanager_is_available(status)),
                )
                .await?;
                info!(
                    target: LOG_TARGET,
                    "{}",
                    logics::mqtt_publish_mm_availability_message(switch_payload(
                        modemmanager_is_available(status)
                    ))
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
            MqttCommand::PublishSmsInventorySnapshot {
                modem_id,
                sms_ids,
                last_sms_timestamp,
            } => {
                self.apply_sms_inventory_snapshot(
                    modem_id,
                    sms_ids,
                    last_sms_timestamp,
                    mqtt_event_tx,
                )
                .await?;
            }
            MqttCommand::PublishSmsList { modem_id, sms_ids } => {
                self.apply_sms_list(modem_id, sms_ids, mqtt_event_tx)
                    .await?;
            }
            MqttCommand::PublishSmsSnapshot {
                modem_id,
                sms_id,
                snapshot,
            } => {
                self.apply_sms_snapshot(modem_id, sms_id, snapshot).await?;
            }
            MqttCommand::PublishSmsUpdate {
                modem_id,
                sms_id,
                update,
            } => {
                self.apply_sms_update(modem_id, sms_id, update).await?;
            }
            MqttCommand::PublishSmsDeleted { modem_id, sms_id } => {
                self.apply_sms_deleted(modem_id, sms_id, mqtt_event_tx)
                    .await?;
            }
            MqttCommand::DeleteModemDevice { modem_id } => {
                if let Some(modem_index) = self.state.remove_modem_index(&modem_id) {
                    self.cleanup_modem_device(modem_index).await?;
                    self.sync_manager_sms_state().await?;
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
        &mut self,
        publish: Publish,
        mqtt_event_tx: &mpsc::Sender<MqttEvent>,
    ) -> Result<()> {
        if let Some(modem_index) = parse_message_select_topic(&publish.topic) {
            let Some(modem_id) = self.state.modem_id_for_index(modem_index).cloned() else {
                return Ok(());
            };
            // WB writable controls are driven through the `/on` topic. The
            // frontend owns the user-facing SMS index and translates it back to
            // the DBus SMS id before asking DBus for fresh data.
            let Ok(payload) = std::str::from_utf8(&publish.payload) else {
                debug!(
                    target: LOG_TARGET,
                    "Ignoring non-UTF8 message_select payload in topic `{}`",
                    publish.topic
                );
                return Ok(());
            };
            let payload = payload.trim();
            let Ok(picked_index) = payload.parse::<u32>() else {
                debug!(
                    target: LOG_TARGET,
                    "Ignoring invalid message_select payload `{payload}` in topic `{}`",
                    publish.topic
                );
                return Ok(());
            };

            self.pick_modem_sms(modem_id, picked_index, mqtt_event_tx)
                .await?;

            return Ok(());
        }

        if let Some(modem_index) = parse_delete_picked_sms_topic(&publish.topic) {
            let Some(modem_id) = self.state.modem_id_for_index(modem_index).cloned() else {
                return Ok(());
            };

            self.delete_picked_sms(modem_id, mqtt_event_tx).await?;
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

        self.publish_text_control(logics::MM_DEVICE_NAME, logics::MM_CONTROL_IS_AVAILABLE, "0")
            .await?;
        self.publish_null_control(logics::MM_DEVICE_NAME, logics::MM_CONTROL_VERSION)
            .await?;
        self.publish_null_control(logics::MM_DEVICE_NAME, logics::MM_CONTROL_MODEM_COUNT)
            .await?;
        self.publish_number_control(logics::MM_DEVICE_NAME, logics::MM_CONTROL_SMS_COUNT, 0)
            .await?;
        self.publish_null_control(logics::MM_DEVICE_NAME, logics::MM_CONTROL_LAST_SMS)
            .await?;
        self.publish_null_control(logics::MM_DEVICE_NAME, logics::MM_CONTROL_LAST_SMS_UNIXTIME)
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

        for spec in logics::modem_base_control_specs() {
            self.publish_control_metadata(&device_name, spec).await?;
        }

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

        Ok(modem_index)
    }

    async fn ensure_modem_sms_controls(&mut self, modem_index: u32) -> Result<()> {
        if !self.state.modem_sms_controls_created.insert(modem_index) {
            return Ok(());
        }

        self.subscribe_to_modem_sms_controls(modem_index).await?;

        let device_name = logics::device_name_for_modem(modem_index);
        for spec in logics::modem_sms_control_specs() {
            self.publish_control_metadata(&device_name, spec).await?;
        }

        self.publish_number_control(&device_name, logics::MODEM_CONTROL_SMS_COUNT, 0)
            .await?;
        self.publish_null_control(&device_name, logics::MODEM_CONTROL_LAST_SMS)
            .await?;
        self.publish_null_control(&device_name, logics::MODEM_CONTROL_LAST_SMS_UNIXTIME)
            .await?;
        self.publish_message_select_control(modem_index, None, 1, false)
            .await?;
        self.publish_picked_sms(modem_index, None).await?;
        self.publish_delete_message_control(modem_index, false)
            .await
    }

    async fn subscribe_to_modem_sms_controls(&mut self, modem_index: u32) -> Result<()> {
        if !self.state.subscribed_modem_sms_controls.insert(modem_index) {
            return Ok(());
        }

        let device_name = logics::device_name_for_modem(modem_index);
        for control_name in [
            logics::MODEM_CONTROL_MESSAGE_SELECT,
            logics::MODEM_CONTROL_DELETE_MESSAGE,
        ] {
            let topic = logics::control_on_topic(&device_name, control_name);
            self.client
                .subscribe(topic.clone(), QoS::AtMostOnce)
                .await
                .with_context(|| format!("failed to subscribe to MQTT topic `{topic}`"))?;
        }

        Ok(())
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
            ModemUpdate::IsActive(value) => {
                self.publish_text_control(
                    &device_name,
                    logics::MODEM_CONTROL_IS_ACTIVE,
                    switch_payload(*value),
                )
                .await?;
            }
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

    async fn apply_sms_inventory_snapshot(
        &mut self,
        modem_id: ModemId,
        sms_ids: Vec<SmsId>,
        last_sms_timestamp: Option<OffsetDateTime>,
        mqtt_event_tx: &mpsc::Sender<MqttEvent>,
    ) -> Result<()> {
        self.ensure_modem_device(&modem_id).await?;

        let request_sms_id = {
            let modem_sms = self.state.modem_sms.entry(modem_id.clone()).or_default();
            let request_sms_id = apply_sms_order(modem_sms, sms_ids);
            modem_sms.last_sms_timestamp = last_sms_timestamp;
            request_sms_id
        };

        self.sync_modem_sms_state(&modem_id).await?;
        self.sync_manager_sms_state().await?;
        if let Some(sms_id) = request_sms_id {
            request_sms_snapshot(mqtt_event_tx, modem_id, sms_id).await;
        }

        Ok(())
    }

    async fn apply_sms_list(
        &mut self,
        modem_id: ModemId,
        sms_ids: Vec<SmsId>,
        mqtt_event_tx: &mpsc::Sender<MqttEvent>,
    ) -> Result<()> {
        let request_sms_id = {
            let Some(modem_sms) = self.state.modem_sms.get_mut(&modem_id) else {
                return Ok(());
            };
            let previous_order = modem_sms.sms_order.clone();
            let previous_last_sms_timestamp = modem_sms.last_sms_timestamp;
            let new_last_sms_id = sms_ids.last().cloned();
            let last_sms_is_new = new_last_sms_id
                .as_ref()
                .is_some_and(|sms_id| !previous_order.contains(sms_id));

            let request_sms_id = apply_sms_order(modem_sms, sms_ids);
            refresh_modem_last_sms_from_cache(modem_sms);
            if modem_sms.last_sms_timestamp.is_none() && last_sms_is_new {
                modem_sms.last_sms_timestamp = previous_last_sms_timestamp;
            }
            request_sms_id
        };

        self.sync_modem_sms_state(&modem_id).await?;
        self.sync_manager_sms_state().await?;
        if let Some(sms_id) = request_sms_id {
            request_sms_snapshot(mqtt_event_tx, modem_id, sms_id).await;
        }

        Ok(())
    }

    async fn apply_sms_snapshot(
        &mut self,
        modem_id: ModemId,
        sms_id: SmsId,
        snapshot: SmsSnapshot,
    ) -> Result<()> {
        let Some(modem_sms) = self.state.modem_sms.get_mut(&modem_id) else {
            return Ok(());
        };
        if !modem_sms.sms_order.contains(&sms_id) {
            return Ok(());
        }

        modem_sms
            .sms_snapshots
            .insert(sms_id.clone(), snapshot.clone());
        if modem_sms.sms_order.last() == Some(&sms_id) {
            modem_sms.last_sms_timestamp = snapshot.timestamp;
        }

        self.sync_modem_sms_state(&modem_id).await?;
        self.sync_manager_sms_state().await
    }

    async fn apply_sms_update(
        &mut self,
        modem_id: ModemId,
        sms_id: SmsId,
        update: SmsUpdate,
    ) -> Result<()> {
        let Some(modem_sms) = self.state.modem_sms.get_mut(&modem_id) else {
            return Ok(());
        };
        if !modem_sms.sms_order.contains(&sms_id) {
            return Ok(());
        }

        match modem_sms.sms_snapshots.get_mut(&sms_id) {
            Some(snapshot) => apply_sms_update_to_snapshot(snapshot, update),
            None => {
                if let SmsUpdate::Timestamp(timestamp) = update
                    && modem_sms.sms_order.last() == Some(&sms_id)
                {
                    modem_sms.last_sms_timestamp = timestamp;
                }
            }
        }
        if let Some(snapshot) = modem_sms.sms_snapshots.get(&sms_id)
            && modem_sms.sms_order.last() == Some(&sms_id)
        {
            modem_sms.last_sms_timestamp = snapshot.timestamp;
        }

        self.sync_modem_sms_state(&modem_id).await?;
        self.sync_manager_sms_state().await
    }

    async fn apply_sms_deleted(
        &mut self,
        modem_id: ModemId,
        sms_id: SmsId,
        mqtt_event_tx: &mpsc::Sender<MqttEvent>,
    ) -> Result<()> {
        let request_sms_id = {
            let Some(modem_sms) = self.state.modem_sms.get_mut(&modem_id) else {
                return Ok(());
            };
            modem_sms.sms_snapshots.remove(&sms_id);
            if !modem_sms.sms_order.contains(&sms_id) {
                None
            } else {
                let sms_ids = modem_sms
                    .sms_order
                    .iter()
                    .filter(|current_sms_id| *current_sms_id != &sms_id)
                    .cloned()
                    .collect();
                let request_sms_id = apply_sms_order(modem_sms, sms_ids);
                refresh_modem_last_sms_from_cache(modem_sms);
                request_sms_id
            }
        };

        self.sync_modem_sms_state(&modem_id).await?;
        self.sync_manager_sms_state().await?;
        if let Some(sms_id) = request_sms_id {
            request_sms_snapshot(mqtt_event_tx, modem_id, sms_id).await;
        }

        Ok(())
    }

    async fn pick_modem_sms(
        &mut self,
        modem_id: ModemId,
        picked_index: u32,
        mqtt_event_tx: &mpsc::Sender<MqttEvent>,
    ) -> Result<()> {
        let request_sms_id = if let Some(picked_index) = picked_index.checked_sub(1) {
            self.state
                .modem_sms
                .get_mut(&modem_id)
                .and_then(|modem_sms| {
                    let sms_id = modem_sms.sms_order.get(picked_index as usize).cloned()?;
                    modem_sms.picked_sms_id = Some(sms_id.clone());
                    Some(sms_id)
                })
        } else {
            None
        };

        self.sync_modem_sms_state(&modem_id).await?;
        if let Some(sms_id) = request_sms_id {
            request_sms_snapshot(mqtt_event_tx, modem_id, sms_id).await;
        }
        Ok(())
    }

    async fn delete_picked_sms(
        &mut self,
        modem_id: ModemId,
        mqtt_event_tx: &mpsc::Sender<MqttEvent>,
    ) -> Result<()> {
        let Some(sms_id) = self.state.modem_sms.get(&modem_id).and_then(|modem_sms| {
            let sms_id = modem_sms.picked_sms_id.as_ref()?;
            modem_sms
                .sms_snapshots
                .contains_key(sms_id)
                .then_some(sms_id.clone())
        }) else {
            return Ok(());
        };

        send_mqtt_event(mqtt_event_tx, MqttEvent::DeleteSms { modem_id, sms_id }).await;
        Ok(())
    }

    async fn sync_modem_sms_state(&mut self, modem_id: &ModemId) -> Result<()> {
        let modem_index = self.ensure_modem_device(modem_id).await?;
        self.ensure_modem_sms_controls(modem_index).await?;
        let Some(publish_state) = self.collect_modem_sms_publish_state(modem_id) else {
            return Ok(());
        };

        let device_name = logics::device_name_for_modem(modem_index);

        if self
            .state
            .modem_sms
            .get(modem_id)
            .is_none_or(|modem_sms| modem_sms.last_sms_count != Some(publish_state.sms_count))
        {
            self.publish_number_control(
                &device_name,
                logics::MODEM_CONTROL_SMS_COUNT,
                publish_state.sms_count,
            )
            .await?;
            if let Some(modem_sms) = self.state.modem_sms.get_mut(modem_id) {
                modem_sms.last_sms_count = Some(publish_state.sms_count);
            }
            info!(
                target: LOG_TARGET,
                "{}",
                logics::mqtt_publish_modem_sms_count_message(
                    modem_index,
                    &modem_id.0,
                    publish_state.sms_count,
                )
            );
        }

        if self.state.modem_sms.get(modem_id).is_none_or(|modem_sms| {
            modem_sms.last_published_last_sms != Some(publish_state.last_sms_timestamp)
        }) {
            self.publish_optional_timestamp_controls(
                &device_name,
                logics::MODEM_CONTROL_LAST_SMS,
                logics::MODEM_CONTROL_LAST_SMS_UNIXTIME,
                publish_state.last_sms_timestamp,
            )
            .await?;
            if let Some(modem_sms) = self.state.modem_sms.get_mut(modem_id) {
                modem_sms.last_published_last_sms = Some(publish_state.last_sms_timestamp);
            }
        }

        if self.state.modem_sms.get(modem_id).is_none_or(|modem_sms| {
            modem_sms.last_picked_index != Some(publish_state.picked_index)
                || modem_sms.last_picked_max_index != Some(publish_state.max_index)
                || modem_sms.last_picked_writable != Some(publish_state.message_writable)
        }) {
            self.publish_message_select_control(
                modem_index,
                publish_state.picked_index,
                publish_state.max_index,
                publish_state.message_writable,
            )
            .await?;
            if let Some(modem_sms) = self.state.modem_sms.get_mut(modem_id) {
                modem_sms.last_picked_index = Some(publish_state.picked_index);
                modem_sms.last_picked_max_index = Some(publish_state.max_index);
                modem_sms.last_picked_writable = Some(publish_state.message_writable);
            }
            info!(
                target: LOG_TARGET,
                "{}",
                logics::mqtt_publish_message_select_control_message(
                    modem_index,
                    &modem_id.0,
                    publish_state.picked_index,
                    publish_state.max_index,
                    publish_state.message_writable,
                )
            );
        }

        if self.state.modem_sms.get(modem_id).is_none_or(|modem_sms| {
            modem_sms.last_picked_snapshot != Some(publish_state.picked_snapshot.clone())
        }) {
            self.publish_picked_sms(modem_index, publish_state.picked_snapshot.as_ref())
                .await?;
            if let Some(modem_sms) = self.state.modem_sms.get_mut(modem_id) {
                modem_sms.last_picked_snapshot = Some(publish_state.picked_snapshot.clone());
            }
            info!(
                target: LOG_TARGET,
                "{}",
                logics::mqtt_publish_picked_sms_message(
                    modem_index,
                    &modem_id.0,
                    publish_state
                        .picked_snapshot
                        .as_ref()
                        .map(SmsSnapshot::summary)
                        .as_deref(),
                )
            );
        }

        if self.state.modem_sms.get(modem_id).is_none_or(|modem_sms| {
            modem_sms.last_delete_writable != Some(publish_state.delete_writable)
        }) {
            self.publish_delete_message_control(modem_index, publish_state.delete_writable)
                .await?;
            if let Some(modem_sms) = self.state.modem_sms.get_mut(modem_id) {
                modem_sms.last_delete_writable = Some(publish_state.delete_writable);
            }
        }

        Ok(())
    }

    fn collect_modem_sms_publish_state(
        &self,
        modem_id: &ModemId,
    ) -> Option<MqttModemSmsPublishState> {
        let modem_sms = self.state.modem_sms.get(modem_id)?;
        let sms_count = modem_sms.sms_order.len();
        let max_index = max_sms_index(sms_count);
        let picked_index = modem_sms
            .picked_sms_id
            .as_ref()
            .and_then(|picked_sms_id| {
                modem_sms
                    .sms_order
                    .iter()
                    .position(|sms_id| sms_id == picked_sms_id)
            })
            .and_then(|index| u32::try_from(index + 1).ok());
        let picked_snapshot = modem_sms
            .picked_sms_id
            .as_ref()
            .and_then(|sms_id| modem_sms.sms_snapshots.get(sms_id))
            .cloned();

        Some(MqttModemSmsPublishState {
            sms_count,
            last_sms_timestamp: modem_sms.last_sms_timestamp,
            picked_index,
            max_index,
            message_writable: sms_count > 0,
            delete_writable: picked_snapshot.is_some(),
            picked_snapshot,
        })
    }

    async fn sync_manager_sms_state(&mut self) -> Result<()> {
        self.ensure_manager_device().await?;
        let sms_count = self
            .state
            .modem_sms
            .values()
            .map(|modem_sms| modem_sms.sms_order.len())
            .sum::<usize>();
        let last_sms = self
            .state
            .modem_sms
            .values()
            .filter_map(|modem_sms| modem_sms.last_sms_timestamp)
            .max();

        if self.state.last_manager_sms_count != Some(sms_count) {
            self.publish_number_control(
                logics::MM_DEVICE_NAME,
                logics::MM_CONTROL_SMS_COUNT,
                sms_count,
            )
            .await?;
            self.state.last_manager_sms_count = Some(sms_count);
            info!(
                target: LOG_TARGET,
                "{}",
                logics::mqtt_publish_mm_sms_count_message(sms_count)
            );
        }

        if self.state.last_manager_last_sms != Some(last_sms) {
            self.publish_optional_timestamp_controls(
                logics::MM_DEVICE_NAME,
                logics::MM_CONTROL_LAST_SMS,
                logics::MM_CONTROL_LAST_SMS_UNIXTIME,
                last_sms,
            )
            .await?;
            self.state.last_manager_last_sms = Some(last_sms);
            info!(
                target: LOG_TARGET,
                "{}",
                logics::mqtt_publish_mm_last_sms_message(last_sms)
            );
        }

        Ok(())
    }

    async fn publish_message_select_control(
        &self,
        modem_index: u32,
        picked_index: Option<u32>,
        max_index: u32,
        writable: bool,
    ) -> Result<()> {
        let device_name = logics::device_name_for_modem(modem_index);
        let spec = logics::dynamic_message_select_spec(!writable, max_index);
        self.publish_control_metadata(&device_name, &spec).await?;

        match picked_index {
            Some(picked_index) => {
                self.publish_number_control(
                    &device_name,
                    logics::MODEM_CONTROL_MESSAGE_SELECT,
                    picked_index,
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

    async fn publish_delete_message_control(&self, modem_index: u32, writable: bool) -> Result<()> {
        let device_name = logics::device_name_for_modem(modem_index);
        let spec = logics::dynamic_delete_message_spec(!writable);
        self.publish_control_metadata(&device_name, &spec).await?;
        self.publish_text_control(&device_name, logics::MODEM_CONTROL_DELETE_MESSAGE, "0")
            .await
    }

    async fn publish_picked_sms(
        &self,
        modem_index: u32,
        snapshot: Option<&SmsSnapshot>,
    ) -> Result<()> {
        let device_name = logics::device_name_for_modem(modem_index);

        match snapshot {
            Some(snapshot) => {
                self.publish_optional_timestamp_controls(
                    &device_name,
                    logics::MODEM_CONTROL_SELECTED_SMS_TIMESTAMP,
                    logics::MODEM_CONTROL_SELECTED_SMS_TIMESTAMP_UNIXTIME,
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
                self.publish_null_control(
                    &device_name,
                    logics::MODEM_CONTROL_SELECTED_SMS_TIMESTAMP_UNIXTIME,
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
            self.cleanup_control(device_name, spec).await?;
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

    async fn publish_optional_timestamp_controls(
        &self,
        device_name: &str,
        text_control_name: &str,
        unixtime_control_name: &str,
        value: Option<OffsetDateTime>,
    ) -> Result<()> {
        match value {
            Some(value) => {
                self.publish_text_control(
                    device_name,
                    text_control_name,
                    &dbus::format_timestamp_for_wb(value),
                )
                .await?;
                self.publish_number_control(
                    device_name,
                    unixtime_control_name,
                    value.unix_timestamp(),
                )
                .await
            }
            None => {
                self.publish_null_control(device_name, text_control_name)
                    .await?;
                self.publish_null_control(device_name, unixtime_control_name)
                    .await
            }
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

    async fn cleanup_control(&self, device_name: &str, spec: &ControlSpec) -> Result<()> {
        self.unpublish_retained(logics::control_meta_topic(device_name, spec.name))
            .await?;

        for (field, _) in logics::control_meta_leaf_payloads(spec) {
            self.unpublish_retained(logics::control_meta_leaf_topic(
                device_name,
                spec.name,
                field,
            ))
            .await?;
        }

        self.unpublish_retained(logics::control_on_topic(device_name, spec.name))
            .await?;
        self.unpublish_retained(logics::control_value_topic(device_name, spec.name))
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

#[derive(Debug, Default)]
struct MqttSessionState {
    manager_device_created: bool,
    modem_indices: HashMap<ModemId, u32>,
    reverse_modem_indices: HashMap<u32, ModemId>,
    modem_sms_controls_created: HashSet<u32>,
    subscribed_modem_sms_controls: HashSet<u32>,
    modem_sms: HashMap<ModemId, MqttModemSmsState>,
    last_manager_sms_count: Option<usize>,
    last_manager_last_sms: Option<Option<OffsetDateTime>>,
}

#[derive(Debug, Default)]
struct MqttModemSmsState {
    sms_order: Vec<SmsId>,
    sms_snapshots: HashMap<SmsId, SmsSnapshot>,
    picked_sms_id: Option<SmsId>,
    last_sms_timestamp: Option<OffsetDateTime>,
    last_sms_count: Option<usize>,
    last_published_last_sms: Option<Option<OffsetDateTime>>,
    last_picked_index: Option<Option<u32>>,
    last_picked_max_index: Option<u32>,
    last_picked_writable: Option<bool>,
    last_delete_writable: Option<bool>,
    last_picked_snapshot: Option<Option<SmsSnapshot>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MqttModemSmsPublishState {
    sms_count: usize,
    last_sms_timestamp: Option<OffsetDateTime>,
    picked_index: Option<u32>,
    max_index: u32,
    message_writable: bool,
    delete_writable: bool,
    picked_snapshot: Option<SmsSnapshot>,
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
        self.modem_sms_controls_created.remove(&modem_index);
        self.subscribed_modem_sms_controls.remove(&modem_index);
        self.modem_sms.remove(modem_id);
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
    // If the daemon dies unexpectedly, the only user-facing trust marker must
    // flip to unavailable without waiting for any explicit cleanup path.
    mqtt_options.set_last_will(LastWill::new(
        logics::mm_availability_topic(),
        switch_payload(false),
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

fn parse_message_select_topic(topic: &str) -> Option<u32> {
    parse_modem_control_on_topic(topic, logics::MODEM_CONTROL_MESSAGE_SELECT)
}

fn parse_delete_picked_sms_topic(topic: &str) -> Option<u32> {
    parse_modem_control_on_topic(topic, logics::MODEM_CONTROL_DELETE_MESSAGE)
}

fn parse_modem_control_on_topic(topic: &str, control_name: &str) -> Option<u32> {
    let prefix = format!("/devices/{}", logics::MM_MODEM_DEVICE_PREFIX);
    let suffix = format!("/controls/{control_name}/on");
    let modem_index = topic.strip_prefix(&prefix)?.strip_suffix(&suffix)?;
    modem_index.parse::<u32>().ok()
}

fn modemmanager_is_available(status: ModemManagerStatus) -> bool {
    matches!(status, ModemManagerStatus::Active)
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

async fn send_mqtt_event(mqtt_event_tx: &mpsc::Sender<MqttEvent>, event: MqttEvent) {
    if mqtt_event_tx.send(event).await.is_err() {
        debug!(target: LOG_TARGET, "MQTT event channel closed while sending");
    }
}

async fn request_sms_snapshot(
    mqtt_event_tx: &mpsc::Sender<MqttEvent>,
    modem_id: ModemId,
    sms_id: SmsId,
) {
    send_mqtt_event(
        mqtt_event_tx,
        MqttEvent::RequestSmsSnapshot { modem_id, sms_id },
    )
    .await;
}

fn apply_sms_order(modem_sms: &mut MqttModemSmsState, sms_ids: Vec<SmsId>) -> Option<SmsId> {
    let old_picked_sms_id = modem_sms.picked_sms_id.clone();
    let old_picked_index = old_picked_sms_id
        .as_ref()
        .and_then(|picked_sms_id| {
            modem_sms
                .sms_order
                .iter()
                .position(|sms_id| sms_id == picked_sms_id)
        })
        .unwrap_or(0);

    modem_sms.sms_order = sms_ids;
    let sms_id_set: HashSet<_> = modem_sms.sms_order.iter().cloned().collect();
    modem_sms
        .sms_snapshots
        .retain(|sms_id, _| sms_id_set.contains(sms_id));

    let picked_sms_id = match old_picked_sms_id {
        Some(old_picked_sms_id) if modem_sms.sms_order.contains(&old_picked_sms_id) => {
            Some(old_picked_sms_id)
        }
        _ if modem_sms.sms_order.is_empty() => None,
        _ => {
            let picked_index = old_picked_index.min(modem_sms.sms_order.len() - 1);
            Some(modem_sms.sms_order[picked_index].clone())
        }
    };

    let changed = modem_sms.picked_sms_id != picked_sms_id;
    modem_sms.picked_sms_id = picked_sms_id.clone();
    changed.then_some(picked_sms_id).flatten()
}

fn refresh_modem_last_sms_from_cache(modem_sms: &mut MqttModemSmsState) {
    modem_sms.last_sms_timestamp = modem_sms
        .sms_order
        .last()
        .and_then(|sms_id| modem_sms.sms_snapshots.get(sms_id))
        .and_then(|snapshot| snapshot.timestamp);
}

fn apply_sms_update_to_snapshot(snapshot: &mut SmsSnapshot, update: SmsUpdate) {
    match update {
        SmsUpdate::IsReceived(value) => snapshot.is_received = value,
        SmsUpdate::Timestamp(value) => snapshot.timestamp = value,
        SmsUpdate::Number(value) => snapshot.number = value,
        SmsUpdate::Text(value) => snapshot.text = value,
    }
}

fn max_sms_index(sms_count: usize) -> u32 {
    u32::try_from(sms_count).unwrap_or(u32::MAX).max(1)
}

#[cfg(test)]
mod tests {
    use super::{
        MqttEndpoint, MqttModemSmsState, MqttSessionState, apply_sms_order,
        parse_delete_picked_sms_topic, parse_message_select_topic, parse_mqtt_endpoint,
    };
    use crate::dbus::{ModemId, SmsId};

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

    #[test]
    fn parses_delete_picked_sms_topic() {
        assert_eq!(
            parse_delete_picked_sms_topic("/devices/mm_modem_3/controls/delete_message/on"),
            Some(3)
        );
        assert_eq!(
            parse_delete_picked_sms_topic("/devices/mm_modem_3/controls/message_select/on"),
            None
        );
    }

    #[test]
    fn sms_order_keeps_picked_sms_by_dbus_id() {
        let mut state = MqttModemSmsState {
            sms_order: sms_ids(&["144", "145", "146"]),
            picked_sms_id: Some(SmsId("145".to_string())),
            ..Default::default()
        };

        let request_sms_id = apply_sms_order(&mut state, sms_ids(&["145", "146"]));

        assert_eq!(state.picked_sms_id, Some(SmsId("145".to_string())));
        assert_eq!(request_sms_id, None);
    }

    #[test]
    fn sms_order_preserves_position_when_picked_sms_disappears() {
        let mut state = MqttModemSmsState {
            sms_order: sms_ids(&["144", "145", "146"]),
            picked_sms_id: Some(SmsId("145".to_string())),
            ..Default::default()
        };

        let request_sms_id = apply_sms_order(&mut state, sms_ids(&["144", "146"]));

        assert_eq!(state.picked_sms_id, Some(SmsId("146".to_string())));
        assert_eq!(request_sms_id, Some(SmsId("146".to_string())));
    }

    #[test]
    fn sms_order_clears_empty_selection() {
        let mut state = MqttModemSmsState {
            sms_order: sms_ids(&["144"]),
            picked_sms_id: Some(SmsId("144".to_string())),
            ..Default::default()
        };

        let request_sms_id = apply_sms_order(&mut state, Vec::new());

        assert_eq!(state.picked_sms_id, None);
        assert_eq!(request_sms_id, None);
    }

    fn sms_ids(values: &[&str]) -> Vec<SmsId> {
        values
            .iter()
            .map(|value| SmsId((*value).to_string()))
            .collect()
    }
}

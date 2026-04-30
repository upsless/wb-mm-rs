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
    self, ManagerUpdate, ModemId, ModemManagerStatus, ModemUpdate, SmsId, SmsPropertyChange,
    SmsSnapshot, SmsUpdate,
};
use crate::exchange::{MqttCommand, MqttEvent};
use crate::mqtt::schema::{self, ControlSpec};
use crate::mqtt::state::{MqttModemSmsState, MqttSessionState, max_message_select_index};

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
    frontend.ensure_main_device().await?;

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

    debug!(target: LOG_TARGET, "{}", schema::mqtt_stopped_message());

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
            MqttCommand::ManagerFound {
                version,
                modem_count,
            } => {
                self.ensure_main_device().await?;
                self.publish_text_control(
                    schema::MM_DEVICE_NAME,
                    schema::MM_CONTROL_VERSION,
                    &version,
                )
                .await?;
                self.publish_number_control(
                    schema::MM_DEVICE_NAME,
                    schema::MM_CONTROL_MODEM_COUNT,
                    modem_count,
                )
                .await?;
                info!(
                    target: LOG_TARGET,
                    "Update main device manager data: version={version} modem_count={modem_count}"
                );
            }
            MqttCommand::ManagerUpdated(update) => {
                self.ensure_main_device().await?;
                match update {
                    ManagerUpdate::Status(status) => {
                        let is_available = switch_payload(modemmanager_is_available(status));
                        self.publish_text_control(
                            schema::MM_DEVICE_NAME,
                            schema::MM_CONTROL_IS_AVAILABLE,
                            is_available,
                        )
                        .await?;
                        self.publish_text_control(
                            schema::MM_DEVICE_NAME,
                            schema::MM_CONTROL_MANAGER_STATUS,
                            manager_status_payload(Some(status)),
                        )
                        .await?;
                        info!(
                            target: LOG_TARGET,
                            "{}",
                            schema::mqtt_publish_mm_availability_message(is_available)
                        );
                        info!(
                            target: LOG_TARGET,
                            "Update main device manager status: status={}",
                            manager_status_payload(Some(status))
                        );
                    }
                    ManagerUpdate::Version(version) => {
                        self.publish_text_control(
                            schema::MM_DEVICE_NAME,
                            schema::MM_CONTROL_VERSION,
                            &version,
                        )
                        .await?;
                        info!(
                            target: LOG_TARGET,
                            "{}",
                            schema::mqtt_publish_mm_version_message(&version)
                        );
                    }
                    ManagerUpdate::ModemCount(modem_count) => {
                        self.publish_number_control(
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
                    }
                }
            }
            MqttCommand::ManagerDeleted => {
                self.ensure_main_device().await?;
                let is_available = switch_payload(false);
                self.publish_text_control(
                    schema::MM_DEVICE_NAME,
                    schema::MM_CONTROL_IS_AVAILABLE,
                    is_available,
                )
                .await?;
                self.publish_text_control(
                    schema::MM_DEVICE_NAME,
                    schema::MM_CONTROL_MANAGER_STATUS,
                    manager_status_payload(None),
                )
                .await?;
                info!(
                    target: LOG_TARGET,
                    "{}",
                    schema::mqtt_publish_mm_availability_message(is_available)
                );
                info!(target: LOG_TARGET, "Update main device manager status: status={}", manager_status_payload(None));
            }
            MqttCommand::ModemFound {
                modem_id,
                is_active,
                model,
                revision,
                state,
                primary_sim_slot,
                operator_name,
                signal_quality,
            } => {
                let found = MqttModemFoundPayload {
                    is_active,
                    model,
                    revision,
                    state,
                    primary_sim_slot,
                    operator_name,
                    signal_quality,
                };
                let modem_index = self.ensure_modem_device(&modem_id).await?;
                self.publish_modem_found(modem_index, &found).await?;
                info!(
                    target: LOG_TARGET,
                    "{}",
                    schema::mqtt_publish_modem_snapshot_message(
                        modem_index,
                        &modem_id.0,
                        &found.summary(),
                    )
                );
            }
            MqttCommand::ModemUpdated { modem_id, update } => {
                let modem_index = self.ensure_modem_device(&modem_id).await?;
                self.publish_modem_update(modem_index, &update).await?;
                info!(
                    target: LOG_TARGET,
                    "{}",
                    schema::mqtt_publish_modem_update_message(
                        modem_index,
                        &modem_id.0,
                        &update.summary(),
                    )
                );
            }
            MqttCommand::PublishSmsInventorySnapshot {
                modem_id,
                sms_ids,
                initial_sms_snapshot,
            } => {
                self.ensure_modem_device(&modem_id).await?;
                let Some(modem) = self.state.modems.get_mut(&modem_id) else {
                    return Ok(());
                };
                modem.ensure_sms_state();
                self.handle_sms_list(modem_id, sms_ids, initial_sms_snapshot, mqtt_event_tx)
                    .await?;
            }
            MqttCommand::PublishSmsList { modem_id, sms_ids } => {
                self.handle_sms_list(modem_id, sms_ids, None, mqtt_event_tx)
                    .await?;
            }
            MqttCommand::PublishSmsSnapshot { modem_id, snapshot } => {
                self.handle_sms_snapshot(modem_id, snapshot).await?;
            }
            MqttCommand::PublishSmsUpdate { modem_id, update } => {
                self.handle_sms_update(modem_id, update).await?;
            }
            MqttCommand::PublishSmsDeleted { modem_id, sms_id } => {
                self.apply_sms_deleted(modem_id, sms_id, mqtt_event_tx)
                    .await?;
            }
            MqttCommand::ModemDeleted { modem_id } => {
                if let Some(modem_index) = self.state.remove_modem_index(&modem_id) {
                    self.cleanup_modem_device(modem_index).await?;
                    self.sync_main_sms_state().await?;
                    info!(
                        target: LOG_TARGET,
                        "{}",
                        schema::mqtt_delete_modem_device_message(modem_index, &modem_id.0)
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

    async fn ensure_main_device(&mut self) -> Result<()> {
        if self.state.main_device_created {
            return Ok(());
        }

        self.publish_retained(
            schema::device_meta_topic(schema::MM_DEVICE_NAME),
            schema::manager_device_title_payload(),
        )
        .await?;

        for spec in schema::manager_control_specs() {
            self.publish_control_metadata(schema::MM_DEVICE_NAME, spec)
                .await?;
        }

        self.publish_text_control(schema::MM_DEVICE_NAME, schema::MM_CONTROL_IS_AVAILABLE, "0")
            .await?;
        self.publish_text_control(
            schema::MM_DEVICE_NAME,
            schema::MM_CONTROL_MANAGER_STATUS,
            manager_status_payload(None),
        )
        .await?;
        self.publish_null_control(schema::MM_DEVICE_NAME, schema::MM_CONTROL_VERSION)
            .await?;
        self.publish_null_control(schema::MM_DEVICE_NAME, schema::MM_CONTROL_MODEM_COUNT)
            .await?;
        self.publish_number_control(schema::MM_DEVICE_NAME, schema::MM_CONTROL_SMS_COUNT, 0)
            .await?;

        self.state.main_device_created = true;
        Ok(())
    }

    async fn ensure_modem_device(&mut self, modem_id: &ModemId) -> Result<u32> {
        let (modem_index, created_now) = self.state.ensure_modem_index(modem_id);
        if !created_now {
            return Ok(modem_index);
        }

        let device_name = schema::device_name_for_modem(modem_index);
        self.publish_retained(
            schema::device_meta_topic(&device_name),
            schema::modem_device_title_payload(modem_index, &modem_id.0),
        )
        .await?;

        for spec in schema::modem_base_control_specs() {
            self.publish_control_metadata(&device_name, spec).await?;
        }

        self.publish_text_control(&device_name, schema::MODEM_CONTROL_IS_ACTIVE, "0")
            .await?;
        self.publish_null_control(&device_name, schema::MODEM_CONTROL_MODEL)
            .await?;
        self.publish_null_control(&device_name, schema::MODEM_CONTROL_REVISION)
            .await?;
        self.publish_null_control(&device_name, schema::MODEM_CONTROL_STATE)
            .await?;
        self.publish_null_control(&device_name, schema::MODEM_CONTROL_PRIMARY_SIM_SLOT)
            .await?;
        self.publish_null_control(&device_name, schema::MODEM_CONTROL_OPERATOR_NAME)
            .await?;
        self.publish_null_control(&device_name, schema::MODEM_CONTROL_SIGNAL_QUALITY)
            .await?;

        Ok(modem_index)
    }

    async fn ensure_modem_sms_controls(&mut self, modem_index: u32) -> Result<()> {
        if !self.state.modem_sms_controls_created.insert(modem_index) {
            return Ok(());
        }

        self.subscribe_to_modem_sms_controls(modem_index).await?;

        let device_name = schema::device_name_for_modem(modem_index);
        for spec in schema::modem_sms_control_specs() {
            self.publish_control_metadata(&device_name, spec).await?;
        }

        self.publish_null_control(&device_name, schema::MODEM_CONTROL_DISPLAYED_SMS_INDEX)
            .await?;
        self.publish_null_control(&device_name, schema::MODEM_CONTROL_LAST_SMS_DBUS_ID)
            .await?;
        self.publish_number_control(&device_name, schema::MODEM_CONTROL_SMS_COUNT, 0)
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

        Ok(())
    }

    async fn publish_modem_found(
        &self,
        modem_index: u32,
        found: &MqttModemFoundPayload,
    ) -> Result<()> {
        let device_name = schema::device_name_for_modem(modem_index);

        self.publish_text_control(
            &device_name,
            schema::MODEM_CONTROL_IS_ACTIVE,
            switch_payload(found.is_active),
        )
        .await?;
        self.publish_optional_text_control(
            &device_name,
            schema::MODEM_CONTROL_MODEL,
            found.model.as_deref(),
        )
        .await?;
        self.publish_optional_text_control(
            &device_name,
            schema::MODEM_CONTROL_REVISION,
            found.revision.as_deref(),
        )
        .await?;
        self.publish_optional_text_control(
            &device_name,
            schema::MODEM_CONTROL_STATE,
            found.state.as_deref(),
        )
        .await?;
        self.publish_optional_number_control(
            &device_name,
            schema::MODEM_CONTROL_PRIMARY_SIM_SLOT,
            found.primary_sim_slot,
        )
        .await?;
        self.publish_optional_text_control(
            &device_name,
            schema::MODEM_CONTROL_OPERATOR_NAME,
            found.operator_name.as_deref(),
        )
        .await?;
        self.publish_optional_number_control(
            &device_name,
            schema::MODEM_CONTROL_SIGNAL_QUALITY,
            found.signal_quality,
        )
        .await?;

        Ok(())
    }

    async fn publish_modem_update(&self, modem_index: u32, update: &ModemUpdate) -> Result<()> {
        let device_name = schema::device_name_for_modem(modem_index);

        match update {
            ModemUpdate::IsActive(value) => {
                self.publish_text_control(
                    &device_name,
                    schema::MODEM_CONTROL_IS_ACTIVE,
                    switch_payload(*value),
                )
                .await?;
            }
            ModemUpdate::Model(value) => {
                self.publish_text_control(&device_name, schema::MODEM_CONTROL_MODEL, value)
                    .await?;
            }
            ModemUpdate::Revision(value) => {
                self.publish_text_control(&device_name, schema::MODEM_CONTROL_REVISION, value)
                    .await?;
            }
            ModemUpdate::State(value) => {
                self.publish_optional_text_control(
                    &device_name,
                    schema::MODEM_CONTROL_STATE,
                    value.as_deref(),
                )
                .await?;
            }
            ModemUpdate::PrimarySimSlot(value) => {
                self.publish_number_control(
                    &device_name,
                    schema::MODEM_CONTROL_PRIMARY_SIM_SLOT,
                    *value,
                )
                .await?;
            }
            ModemUpdate::OperatorName(value) => {
                self.publish_optional_text_control(
                    &device_name,
                    schema::MODEM_CONTROL_OPERATOR_NAME,
                    value.as_deref(),
                )
                .await?;
            }
            ModemUpdate::SignalQuality(value) => {
                self.publish_optional_number_control(
                    &device_name,
                    schema::MODEM_CONTROL_SIGNAL_QUALITY,
                    *value,
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn handle_sms_list(
        &mut self,
        modem_id: ModemId,
        sms_ids: Vec<SmsId>,
        initial_sms_snapshot: Option<SmsSnapshot>,
        mqtt_event_tx: &mpsc::Sender<MqttEvent>,
    ) -> Result<()> {
        let picked_sms_id = {
            let Some(modem_sms) = self
                .state
                .modems
                .get_mut(&modem_id)
                .and_then(|modem| modem.sms_state.as_mut())
            else {
                return Ok(());
            };
            modem_sms.apply_sms_order(sms_ids)
        };

        self.sync_modem_sms_state(&modem_id).await?;
        if let Some(snapshot) = initial_sms_snapshot {
            self.handle_sms_snapshot(modem_id.clone(), snapshot).await?;
        }

        let request_sms_id = picked_sms_id.filter(|picked_sms_id| {
            self.state
                .modems
                .get(&modem_id)
                .and_then(|modem| modem.sms_state.as_ref())
                .and_then(MqttModemSmsState::displayed_sms_id)
                != Some(picked_sms_id)
        });
        if request_sms_id.is_some() {
            self.set_delete_message_writable(&modem_id, false).await?;
        }
        self.sync_main_sms_state().await?;
        if let Some(sms_id) = request_sms_id {
            request_sms_snapshot(mqtt_event_tx, modem_id, sms_id).await;
        }

        Ok(())
    }

    async fn handle_sms_snapshot(
        &mut self,
        modem_id: ModemId,
        snapshot: SmsSnapshot,
    ) -> Result<()> {
        let Some((modem_index, updated_sms_index)) = ({
            let Some(modem) = self.state.modems.get_mut(&modem_id) else {
                return Ok(());
            };
            let Some(modem_sms_state) = modem.sms_state.as_mut() else {
                return Ok(());
            };
            modem_sms_state
                .apply_snapshot(&snapshot)
                .map(|updated_sms_index| (modem.index, updated_sms_index))
        }) else {
            return Ok(());
        };
        let device_name = schema::device_name_for_modem(modem_index);

        self.publish_picked_sms(modem_index, Some(&snapshot))
            .await?;
        self.publish_number_control(
            &device_name,
            schema::MODEM_CONTROL_DISPLAYED_SMS_INDEX,
            updated_sms_index,
        )
        .await?;
        self.set_delete_message_writable(&modem_id, true).await?;

        info!(
            target: LOG_TARGET,
            "{}",
            schema::mqtt_publish_picked_sms_message(
                modem_index,
                &modem_id.0,
                Some(snapshot.summary()).as_deref(),
            )
        );

        Ok(())
    }

    async fn handle_sms_update(&mut self, modem_id: ModemId, update: SmsUpdate) -> Result<()> {
        let Some(modem) = self.state.modems.get(&modem_id) else {
            return Ok(());
        };

        let Some(modem_sms_state) = modem.sms_state.as_ref() else {
            return Ok(());
        };
        if modem_sms_state.displayed_sms_id() != Some(&update.sms_id) {
            return Ok(());
        };

        self.publish_sms_update(modem.index, &update).await
    }

    async fn apply_sms_deleted(
        &mut self,
        modem_id: ModemId,
        sms_id: SmsId,
        mqtt_event_tx: &mpsc::Sender<MqttEvent>,
    ) -> Result<()> {
        let request_sms_id = {
            let Some(modem_sms) = self
                .state
                .modems
                .get_mut(&modem_id)
                .and_then(|modem| modem.sms_state.as_mut())
            else {
                return Ok(());
            };
            modem_sms.remove_sms(&sms_id)
        };

        self.sync_modem_sms_state(&modem_id).await?;
        if request_sms_id.is_some() {
            self.set_delete_message_writable(&modem_id, false).await?;
        }
        self.sync_main_sms_state().await?;
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
        let request_sms_id = self
            .state
            .modems
            .get_mut(&modem_id)
            .and_then(|modem| modem.sms_state.as_mut())
            .and_then(|modem_sms| modem_sms.update_picked_sms_index(picked_index));

        self.sync_modem_sms_state(&modem_id).await?;
        if request_sms_id.is_some() {
            self.set_delete_message_writable(&modem_id, false).await?;
        }
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
        let Some(sms_id) = self
            .state
            .modems
            .get(&modem_id)
            .and_then(|modem| modem.sms_state.as_ref())
            .and_then(MqttModemSmsState::delete_message)
        else {
            return Ok(());
        };

        self.sync_modem_sms_state(&modem_id).await?;
        self.set_delete_message_writable(&modem_id, false).await?;
        send_mqtt_event(mqtt_event_tx, MqttEvent::DeleteSms { modem_id, sms_id }).await;
        Ok(())
    }

    async fn sync_modem_sms_state(&mut self, modem_id: &ModemId) -> Result<()> {
        let modem_index = self.ensure_modem_device(modem_id).await?;
        self.ensure_modem_sms_controls(modem_index).await?;
        let Some(modem_sms) = self
            .state
            .modems
            .get(modem_id)
            .and_then(|modem| modem.sms_state.as_ref())
        else {
            return Ok(());
        };
        let sms_count = modem_sms.sms_count();
        let last_sms_id = modem_sms.last_sms_id().cloned();
        let picked_sms_index = modem_sms.picked_sms_index();
        let displayed_sms_index = modem_sms.displayed_sms_index();
        let max_index = max_message_select_index(sms_count);
        let message_select_writable = sms_count > 0;
        let has_displayed_sms = displayed_sms_index.is_some();

        let device_name = schema::device_name_for_modem(modem_index);

        self.publish_number_control(&device_name, schema::MODEM_CONTROL_SMS_COUNT, sms_count)
            .await?;
        info!(
            target: LOG_TARGET,
            "{}",
            schema::mqtt_publish_modem_sms_count_message(modem_index, &modem_id.0, sms_count)
        );

        self.publish_optional_text_control(
            &device_name,
            schema::MODEM_CONTROL_LAST_SMS_DBUS_ID,
            last_sms_id.as_ref().map(|sms_id| sms_id.0.as_str()),
        )
        .await?;

        self.publish_message_select_control(
            modem_index,
            Some(picked_sms_index),
            max_index,
            message_select_writable,
        )
        .await?;
        info!(
            target: LOG_TARGET,
            "{}",
            schema::mqtt_publish_message_select_control_message(
                modem_index,
                &modem_id.0,
                Some(picked_sms_index),
                max_index,
                message_select_writable,
            )
        );

        if let Some(displayed_sms_index) = displayed_sms_index {
            self.publish_number_control(
                &device_name,
                schema::MODEM_CONTROL_DISPLAYED_SMS_INDEX,
                displayed_sms_index,
            )
            .await?;
        } else {
            self.publish_null_control(&device_name, schema::MODEM_CONTROL_DISPLAYED_SMS_INDEX)
                .await?;
            self.publish_picked_sms(modem_index, None).await?;
        }

        self.publish_delete_message_control(modem_index, has_displayed_sms)
            .await?;

        Ok(())
    }

    async fn sync_main_sms_state(&mut self) -> Result<()> {
        self.ensure_main_device().await?;
        let sms_count = self
            .state
            .modems
            .values()
            .filter_map(|modem| modem.sms_state.as_ref())
            .map(MqttModemSmsState::sms_count)
            .sum::<usize>();

        if self.state.last_manager_sms_count != Some(sms_count) {
            self.publish_number_control(
                schema::MM_DEVICE_NAME,
                schema::MM_CONTROL_SMS_COUNT,
                sms_count,
            )
            .await?;
            self.state.last_manager_sms_count = Some(sms_count);
            info!(
                target: LOG_TARGET,
                "{}",
                schema::mqtt_publish_mm_sms_count_message(sms_count)
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
        let device_name = schema::device_name_for_modem(modem_index);
        let spec = schema::dynamic_message_select_spec(!writable, max_index);
        self.publish_control_metadata(&device_name, &spec).await?;

        match picked_index {
            Some(picked_index) => {
                self.publish_number_control(
                    &device_name,
                    schema::MODEM_CONTROL_MESSAGE_SELECT,
                    picked_index,
                )
                .await?;
            }
            None => {
                self.publish_number_control(&device_name, schema::MODEM_CONTROL_MESSAGE_SELECT, 1)
                    .await?;
            }
        }

        Ok(())
    }

    async fn publish_delete_message_control(&self, modem_index: u32, writable: bool) -> Result<()> {
        let device_name = schema::device_name_for_modem(modem_index);
        let spec = schema::dynamic_delete_message_spec(!writable);
        self.publish_control_metadata(&device_name, &spec).await?;
        self.publish_text_control(&device_name, schema::MODEM_CONTROL_DELETE_MESSAGE, "0")
            .await
    }

    async fn set_delete_message_writable(
        &mut self,
        modem_id: &ModemId,
        writable: bool,
    ) -> Result<()> {
        let modem_index = self.ensure_modem_device(modem_id).await?;
        self.ensure_modem_sms_controls(modem_index).await?;
        self.publish_delete_message_control(modem_index, writable)
            .await
    }

    async fn publish_picked_sms(
        &self,
        modem_index: u32,
        snapshot: Option<&SmsSnapshot>,
    ) -> Result<()> {
        let device_name = schema::device_name_for_modem(modem_index);

        match snapshot {
            Some(snapshot) => {
                self.publish_text_control(
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
                self.publish_optional_text_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_SENDER,
                    snapshot.number.as_deref(),
                )
                .await?;
                self.publish_optional_text_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_TEXT,
                    snapshot.text.as_deref(),
                )
                .await?;
                self.publish_text_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_IS_RECEIVED,
                    switch_payload(snapshot.is_received),
                )
                .await?;
            }
            None => {
                self.publish_null_control(&device_name, schema::MODEM_CONTROL_SELECTED_SMS_DBUS_ID)
                    .await?;
                self.publish_null_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_TIMESTAMP,
                )
                .await?;
                self.publish_null_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_TIMESTAMP_UNIXTIME,
                )
                .await?;
                self.publish_null_control(&device_name, schema::MODEM_CONTROL_SELECTED_SMS_SENDER)
                    .await?;
                self.publish_null_control(&device_name, schema::MODEM_CONTROL_SELECTED_SMS_TEXT)
                    .await?;
                self.publish_text_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_IS_RECEIVED,
                    "0",
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn publish_sms_update(&self, modem_index: u32, update: &SmsUpdate) -> Result<()> {
        let device_name = schema::device_name_for_modem(modem_index);

        match &update.property {
            SmsPropertyChange::IsReceived(value) => {
                self.publish_text_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_IS_RECEIVED,
                    switch_payload(*value),
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
                self.publish_optional_text_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_SENDER,
                    value.as_deref(),
                )
                .await
            }
            SmsPropertyChange::Text(value) => {
                self.publish_optional_text_control(
                    &device_name,
                    schema::MODEM_CONTROL_SELECTED_SMS_TEXT,
                    value.as_deref(),
                )
                .await
            }
        }
    }

    async fn cleanup_session(&mut self) -> Result<()> {
        let modem_indices: Vec<_> = self
            .state
            .modems
            .values()
            .map(|modem| modem.index)
            .collect();

        for modem_index in modem_indices {
            self.cleanup_modem_device(modem_index).await?;
        }

        if self.state.main_device_created {
            self.cleanup_main_device().await?;
        }

        self.state = MqttSessionState::default();
        Ok(())
    }

    async fn cleanup_main_device(&self) -> Result<()> {
        self.cleanup_device(schema::MM_DEVICE_NAME, schema::manager_control_specs())
            .await
    }

    async fn cleanup_modem_device(&self, modem_index: u32) -> Result<()> {
        let device_name = schema::device_name_for_modem(modem_index);
        self.cleanup_device(&device_name, schema::modem_control_specs())
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

    async fn publish_text_control(
        &self,
        device_name: &str,
        control_name: &str,
        payload: &str,
    ) -> Result<()> {
        self.publish_retained(
            schema::control_value_topic(device_name, control_name),
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
            schema::control_value_topic(device_name, control_name),
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct MqttModemFoundPayload {
    is_active: bool,
    model: Option<String>,
    revision: Option<String>,
    state: Option<String>,
    primary_sim_slot: Option<u32>,
    operator_name: Option<String>,
    signal_quality: Option<u32>,
}

impl MqttModemFoundPayload {
    fn summary(&self) -> String {
        format_modem_found_summary(
            self.is_active,
            self.model.as_deref(),
            self.revision.as_deref(),
            self.state.as_deref(),
            self.primary_sim_slot,
            self.operator_name.as_deref(),
            self.signal_quality,
        )
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
        schema::mm_availability_topic(),
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
                    debug!(target: LOG_TARGET, "{}", schema::mqtt_connected_message());
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
    parse_modem_control_on_topic(topic, schema::MODEM_CONTROL_MESSAGE_SELECT)
}

fn parse_delete_picked_sms_topic(topic: &str) -> Option<u32> {
    parse_modem_control_on_topic(topic, schema::MODEM_CONTROL_DELETE_MESSAGE)
}

fn parse_modem_control_on_topic(topic: &str, control_name: &str) -> Option<u32> {
    let prefix = format!("/devices/{}", schema::MM_MODEM_DEVICE_PREFIX);
    let suffix = format!("/controls/{control_name}/on");
    let modem_index = topic.strip_prefix(&prefix)?.strip_suffix(&suffix)?;
    modem_index.parse::<u32>().ok()
}

fn modemmanager_is_available(status: ModemManagerStatus) -> bool {
    matches!(status, ModemManagerStatus::Active)
}

fn manager_status_payload(status: Option<ModemManagerStatus>) -> &'static str {
    match status {
        Some(ModemManagerStatus::Active) => "active",
        Some(ModemManagerStatus::Inactive) => "inactive",
        None => "not_found_on_dbus",
    }
}

fn format_modem_found_summary(
    is_active: bool,
    model: Option<&str>,
    revision: Option<&str>,
    state: Option<&str>,
    primary_sim_slot: Option<u32>,
    operator_name: Option<&str>,
    signal_quality: Option<u32>,
) -> String {
    format!(
        "is_active={}, model={}, revision={}, state={}, primary_sim_slot={}, operator_name={}, signal_quality={}",
        is_active,
        model.unwrap_or("None"),
        revision.unwrap_or("None"),
        state.unwrap_or("None"),
        primary_sim_slot
            .map(|value| value.to_string())
            .unwrap_or_else(|| "None".to_string()),
        operator_name.unwrap_or("None"),
        signal_quality
            .map(|value| value.to_string())
            .unwrap_or_else(|| "None".to_string()),
    )
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

#[cfg(test)]
mod tests {
    use super::{
        MqttEndpoint, parse_delete_picked_sms_topic, parse_message_select_topic,
        parse_mqtt_endpoint,
    };

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
}

use anyhow::Result;
use rumqttc::{AsyncClient, Publish};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, info};

use crate::dbus::{
    ManagerUpdate, ModemId, ModemInfo, ManagerStatus, ModemUpdate, SmsId, SmsSnapshot,
    SmsUpdate,
};
use crate::exchange::{MqttCommand, MqttEvent};
use crate::mqtt::r#loop::{LOG_TARGET, MQTT_GRACEFUL_CLEANUP_FLUSH_DELAY, eventloop_result};
use crate::mqtt::publish::{MqttPublisher, UnavailableModemPublishState};
use crate::mqtt::schema;
use crate::mqtt::state::{MqttModemSmsState, MqttSessionState, max_message_select_index};

pub(super) struct MqttFrontend {
    publisher: MqttPublisher,
    pub(super) state: MqttSessionState,
}

impl MqttFrontend {
    pub(super) fn new(client: AsyncClient) -> Self {
        Self {
            publisher: MqttPublisher::new(client),
            state: MqttSessionState::default(),
        }
    }

    pub(super) async fn ensure_main_device(&mut self) -> Result<()> {
        self.publisher.ensure_main_device().await
    }

    pub(super) async fn stop(
        &mut self,
        eventloop_stop_tx: &watch::Sender<bool>,
        eventloop_task: &mut JoinHandle<Result<()>>,
    ) -> Result<()> {
        self.cleanup_session().await?;
        sleep(MQTT_GRACEFUL_CLEANUP_FLUSH_DELAY).await;
        let _ = eventloop_stop_tx.send(true);
        let _ = self.publisher.disconnect().await;
        eventloop_result(eventloop_task.await)
    }

    pub(super) async fn handle_command(
        &mut self,
        command: MqttCommand,
        mqtt_event_tx: &mpsc::Sender<MqttEvent>,
    ) -> Result<()> {
        match command {
            MqttCommand::ManagerFound {
                version,
                modem_count,
            } => {
                self.handle_manager_found(version, modem_count).await?;
            }
            MqttCommand::ManagerUpdated(update) => {
                self.handle_manager_update(update).await?;
            }
            MqttCommand::ManagerDeleted => {
                self.handle_manager_deleted().await?;
            }
            MqttCommand::ModemFound { modem_id, info } => {
                self.handle_modem_found(modem_id, info).await?;
            }
            MqttCommand::ModemUpdated { modem_id, update } => {
                self.handle_modem_update(modem_id, update).await?;
            }
            MqttCommand::PublishSmsInventorySnapshot {
                modem_id,
                sms_ids,
                initial_sms_snapshot,
            } => {
                self.handle_sms_inventory_snapshot(
                    modem_id,
                    sms_ids,
                    initial_sms_snapshot,
                    mqtt_event_tx,
                )
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
                self.handle_modem_deleted(modem_id).await?;
            }
        }

        Ok(())
    }

    pub(super) async fn handle_incoming_publish(
        &mut self,
        publish: Publish,
        mqtt_event_tx: &mpsc::Sender<MqttEvent>,
    ) -> Result<()> {
        if let Some(modem_index) = parse_message_select_topic(&publish.topic) {
            let Some(modem_id) = self.state.modem_id_for_index(modem_index).cloned() else {
                return Ok(());
            };
            if !self.accepts_modem_user_write(&modem_id, schema::MODEM_CONTROL_MESSAGE_SELECT) {
                return Ok(());
            }
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
            if !self.accepts_modem_user_write(&modem_id, schema::MODEM_CONTROL_DELETE_MESSAGE) {
                return Ok(());
            }

            self.delete_picked_sms(modem_id, mqtt_event_tx).await?;
        }

        Ok(())
    }

    fn accepts_modem_user_write(&self, modem_id: &ModemId, control_name: &str) -> bool {
        if !self.state.manager_available {
            debug!(
                target: LOG_TARGET,
                "Ignoring {control_name} write while ModemManager is unavailable"
            );
            return false;
        }

        if !self.state.modem_is_active(modem_id) {
            debug!(
                target: LOG_TARGET,
                "Ignoring {control_name} write for inactive modem {}",
                modem_id.0
            );
            return false;
        }

        true
    }

    async fn handle_manager_found(&mut self, version: String, modem_count: usize) -> Result<()> {
        self.state.manager_available = true;
        self.publisher.ensure_main_device().await?;
        self.publisher
            .publish_manager_found(&version, modem_count)
            .await?;
        Ok(())
    }

    async fn handle_manager_update(&mut self, update: ManagerUpdate) -> Result<()> {
        self.publisher.ensure_main_device().await?;
        match update {
            ManagerUpdate::Status(status) => {
                let manager_available = modemmanager_is_available(status);
                self.state.manager_available = manager_available;
                self.publisher.publish_manager_status(Some(status)).await?;
                if !manager_available {
                    self.publish_modems_unavailable().await?;
                }
            }
            ManagerUpdate::Version(version) => {
                self.publisher.publish_manager_version(&version).await?;
            }
            ManagerUpdate::ModemCount(modem_count) => {
                self.publisher
                    .publish_manager_modem_count(modem_count)
                    .await?;
            }
        }
        Ok(())
    }

    async fn handle_manager_deleted(&mut self) -> Result<()> {
        self.state.manager_available = false;
        self.publisher.ensure_main_device().await?;
        self.publisher.publish_manager_status(None).await?;
        self.publish_modems_unavailable().await?;
        Ok(())
    }

    async fn handle_modem_found(&mut self, modem_id: ModemId, info: ModemInfo) -> Result<()> {
        let (modem_index, created_now) = self.state.ensure_modem_index(&modem_id);
        self.state.set_modem_active(&modem_id, info.is_active);
        if created_now {
            self.publisher
                .publish_modem_structure(modem_index, &modem_id)
                .await?;
            self.publisher.publish_empty_modem(modem_index).await?;
        }

        self.publisher
            .publish_modem_found(&modem_id, modem_index, &info)
            .await?;

        Ok(())
    }

    async fn handle_modem_update(&mut self, modem_id: ModemId, update: ModemUpdate) -> Result<()> {
        let Some(modem_index) = self.state.modem_index(&modem_id) else {
            return Ok(());
        };
        if let ModemUpdate::IsActive(is_active) = &update {
            self.state.set_modem_active(&modem_id, *is_active);
        }
        self.publisher
            .publish_modem_update(&modem_id, modem_index, &update)
            .await
    }

    async fn handle_modem_deleted(&mut self, modem_id: ModemId) -> Result<()> {
        let Some(modem_index) = self.state.remove_modem_index(&modem_id) else {
            return Ok(());
        };

        self.publisher.cleanup_modem_device(modem_index).await?;
        self.sync_main_sms_state().await?;
        info!(
            target: LOG_TARGET,
            "{}",
            schema::mqtt_delete_modem_device_message(modem_index, &modem_id.0)
        );
        Ok(())
    }

    async fn handle_sms_inventory_snapshot(
        &mut self,
        modem_id: ModemId,
        sms_ids: Vec<SmsId>,
        initial_sms_snapshot: Option<SmsSnapshot>,
        mqtt_event_tx: &mpsc::Sender<MqttEvent>,
    ) -> Result<()> {
        let Some(modem) = self.state.modems.get_mut(&modem_id) else {
            return Ok(());
        };
        modem.ensure_sms_state();
        self.handle_sms_list(modem_id, sms_ids, initial_sms_snapshot, mqtt_event_tx)
            .await
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
        self.finish_synced_sms_change(modem_id, request_sms_id, true, mqtt_event_tx)
            .await
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

        self.publisher
            .ensure_modem_sms_controls(&modem_id, modem_index)
            .await?;
        let device_name = schema::device_name_for_modem(modem_index);
        self.publisher
            .publish_picked_sms(&modem_id, modem_index, Some(&snapshot))
            .await?;
        self.publisher
            .publish_control(
                &device_name,
                schema::MODEM_CONTROL_DISPLAYED_SMS_INDEX,
                updated_sms_index,
            )
            .await?;
        self.publisher
            .publish_delete_message_control(modem_index, true)
            .await?;

        Ok(())
    }

    async fn handle_sms_update(&mut self, modem_id: ModemId, update: SmsUpdate) -> Result<()> {
        let Some(modem_index) = ({
            let Some(modem) = self.state.modems.get(&modem_id) else {
                return Ok(());
            };

            let Some(modem_sms_state) = modem.sms_state.as_ref() else {
                return Ok(());
            };
            if modem_sms_state.displayed_sms_id() != Some(&update.sms_id) {
                return Ok(());
            };
            Some(modem.index)
        }) else {
            return Ok(());
        };

        self.publisher
            .ensure_modem_sms_controls(&modem_id, modem_index)
            .await?;
        self.publisher
            .publish_sms_update(modem_index, &update)
            .await
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
        self.finish_synced_sms_change(modem_id, request_sms_id, true, mqtt_event_tx)
            .await
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
        self.finish_synced_sms_change(modem_id, request_sms_id, false, mqtt_event_tx)
            .await
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

        self.set_delete_message_writable(&modem_id, false).await?;
        send_mqtt_event(mqtt_event_tx, MqttEvent::DeleteSms { modem_id, sms_id }).await;
        Ok(())
    }

    async fn finish_synced_sms_change(
        &mut self,
        modem_id: ModemId,
        request_sms_id: Option<SmsId>,
        sync_main_sms_state: bool,
        mqtt_event_tx: &mpsc::Sender<MqttEvent>,
    ) -> Result<()> {
        if request_sms_id.is_some() {
            self.set_delete_message_writable(&modem_id, false).await?;
        }
        if sync_main_sms_state {
            self.sync_main_sms_state().await?;
        }
        if let Some(sms_id) = request_sms_id {
            request_sms_snapshot(mqtt_event_tx, modem_id, sms_id).await;
        }

        Ok(())
    }

    async fn cleanup_session(&mut self) -> Result<()> {
        let modem_indices = self
            .state
            .modems
            .values()
            .map(|modem| modem.index)
            .collect::<Vec<_>>();

        self.publisher.cleanup_session(modem_indices).await?;
        self.state = MqttSessionState::default();
        Ok(())
    }

    async fn publish_modems_unavailable(&self) -> Result<()> {
        let modems = self
            .state
            .modems
            .iter()
            .map(|(modem_id, modem)| {
                let sms_control_state = modem.sms_state.as_ref().map(|sms_state| {
                    (
                        sms_state.picked_sms_index(),
                        max_message_select_index(sms_state.sms_count()),
                    )
                });

                UnavailableModemPublishState {
                    modem_id: modem_id.clone(),
                    modem_index: modem.index,
                    sms_control_state,
                }
            })
            .collect::<Vec<_>>();

        self.publisher.publish_modems_unavailable(modems).await
    }

    async fn sync_modem_sms_state(&mut self, modem_id: &ModemId) -> Result<()> {
        let Some(modem) = self.state.modems.get(modem_id) else {
            return Ok(());
        };
        let Some(modem_sms) = modem.sms_state.as_ref() else {
            return Ok(());
        };

        self.publisher
            .sync_modem_sms_state(modem_id, modem.index, modem_sms)
            .await
    }

    async fn sync_main_sms_state(&mut self) -> Result<()> {
        let sms_count = self
            .state
            .modems
            .values()
            .filter_map(|modem| modem.sms_state.as_ref())
            .map(MqttModemSmsState::sms_count)
            .sum::<usize>();

        self.publisher.sync_main_sms_state(sms_count).await
    }

    async fn set_delete_message_writable(
        &mut self,
        modem_id: &ModemId,
        writable: bool,
    ) -> Result<()> {
        let Some(modem_index) = self.state.modem_index(modem_id) else {
            return Ok(());
        };

        self.publisher
            .set_delete_message_writable(modem_id, modem_index, writable)
            .await
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

pub(super) fn modemmanager_is_available(status: ManagerStatus) -> bool {
    matches!(status, ManagerStatus::Active)
}

pub(super) fn manager_status_payload(status: Option<ManagerStatus>) -> &'static str {
    match status {
        Some(ManagerStatus::Active) => "active",
        Some(ManagerStatus::Inactive) => "inactive",
        None => "not_found_on_dbus",
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
    use super::{parse_delete_picked_sms_topic, parse_message_select_topic};

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

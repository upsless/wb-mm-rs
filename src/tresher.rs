use std::collections::HashSet;

use anyhow::Result;
use tokio::sync::{mpsc, watch};
use tracing::debug;

use crate::dbus::{ModemId, ModemManagerStatus};
use crate::exchange::{DbusCommand, DbusEvent, MqttCommand, MqttEvent};

const LOG_TARGET: &str = "DISP";

/// Routes DBus and MQTT events, keeps manager/modem announcement state, and
/// emits commands for the side that must act next.
pub async fn run(
    mut shutdown_rx: watch::Receiver<bool>,
    mut dbus_event_rx: mpsc::Receiver<DbusEvent>,
    mut mqtt_event_rx: mpsc::Receiver<MqttEvent>,
    mqtt_command_tx: mpsc::Sender<MqttCommand>,
    dbus_command_tx_rx: watch::Receiver<Option<mpsc::Sender<DbusCommand>>>,
) -> Result<()> {
    let mut state = TresherState::default();
    let mut dbus_command_tx_rx = dbus_command_tx_rx;

    loop {
        tokio::select! {
            result = wait_for_shutdown(&mut shutdown_rx) => {
                result?;
                break;
            }
            maybe_event = dbus_event_rx.recv() => {
                let Some(event) = maybe_event else {
                    break;
                };

                debug!(target: LOG_TARGET, "Received DBus event: {event:?}");
                route_dbus_event(event, &mut state, &mqtt_command_tx).await?;
            }
            maybe_event = mqtt_event_rx.recv() => {
                let Some(event) = maybe_event else {
                    break;
                };
                // The DBus command sender is replaced on every DBus reconnect,
                // so MQTT-originated writes use whichever DBus session is live
                // right now.
                let current_dbus_command_tx = dbus_command_tx_rx.borrow().clone();

                debug!(target: LOG_TARGET, "Received MQTT event: {event:?}");
                route_mqtt_event(event, current_dbus_command_tx.as_ref()).await?;
            }
            changed = dbus_command_tx_rx.changed() => {
                if changed.is_err() {
                    break;
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug, Default)]
struct TresherState {
    device_announced: bool,
    last_status: Option<ModemManagerStatus>,
    last_version: Option<String>,
    last_modem_count: Option<usize>,
    modem_devices: HashSet<ModemId>,
}

async fn route_dbus_event(
    event: DbusEvent,
    state: &mut TresherState,
    mqtt_command_tx: &mpsc::Sender<MqttCommand>,
) -> Result<()> {
    ensure_manager_device(state, mqtt_command_tx).await?;

    match event {
        DbusEvent::StatusChanged(status) => {
            if state.last_status != Some(status) {
                send_mqtt_command(
                    mqtt_command_tx,
                    MqttCommand::PublishModemManagerStatus(status),
                )
                .await?;
                state.last_status = Some(status);
            }
        }
        DbusEvent::Snapshot {
            version,
            modem_count,
        } => {
            if state.last_status != Some(ModemManagerStatus::Active) {
                send_mqtt_command(
                    mqtt_command_tx,
                    MqttCommand::PublishModemManagerStatus(ModemManagerStatus::Active),
                )
                .await?;
                state.last_status = Some(ModemManagerStatus::Active);
            }

            if state.last_version.as_ref() != Some(&version) {
                send_mqtt_command(
                    mqtt_command_tx,
                    MqttCommand::PublishModemManagerVersion(version.clone()),
                )
                .await?;
                state.last_version = Some(version);
            }

            if state.last_modem_count != Some(modem_count) {
                send_mqtt_command(
                    mqtt_command_tx,
                    MqttCommand::PublishModemManagerModemCount(modem_count),
                )
                .await?;
                state.last_modem_count = Some(modem_count);
            }
        }
        DbusEvent::ModemCountChanged { modem_count } => {
            if state.last_modem_count != Some(modem_count) {
                send_mqtt_command(
                    mqtt_command_tx,
                    MqttCommand::PublishModemManagerModemCount(modem_count),
                )
                .await?;
                state.last_modem_count = Some(modem_count);
            }
        }
        DbusEvent::ModemFound { modem_id } => {
            ensure_modem_device(state, mqtt_command_tx, &modem_id).await?;
        }
        DbusEvent::ModemSnapshot { modem_id, snapshot } => {
            ensure_modem_device(state, mqtt_command_tx, &modem_id).await?;
            send_mqtt_command(
                mqtt_command_tx,
                MqttCommand::PublishModemSnapshot { modem_id, snapshot },
            )
            .await?;
        }
        DbusEvent::ModemUpdated { modem_id, update } => {
            ensure_modem_device(state, mqtt_command_tx, &modem_id).await?;
            send_mqtt_command(
                mqtt_command_tx,
                MqttCommand::PublishModemUpdate { modem_id, update },
            )
            .await?;
        }
        DbusEvent::ModemDeleted { modem_id } => {
            send_mqtt_command(
                mqtt_command_tx,
                MqttCommand::DeleteModemDevice {
                    modem_id: modem_id.clone(),
                },
            )
            .await?;
            state.modem_devices.remove(&modem_id);
        }
        DbusEvent::SmsInventorySnapshot {
            modem_id,
            sms_ids,
            last_sms_timestamp,
        } => {
            ensure_modem_device(state, mqtt_command_tx, &modem_id).await?;
            send_mqtt_command(
                mqtt_command_tx,
                MqttCommand::PublishSmsInventorySnapshot {
                    modem_id,
                    sms_ids,
                    last_sms_timestamp,
                },
            )
            .await?;
        }
        DbusEvent::SmsListChanged { modem_id, sms_ids } => {
            ensure_modem_device(state, mqtt_command_tx, &modem_id).await?;
            send_mqtt_command(
                mqtt_command_tx,
                MqttCommand::PublishSmsList { modem_id, sms_ids },
            )
            .await?;
        }
        DbusEvent::SmsSnapshot {
            modem_id,
            sms_id,
            snapshot,
        } => {
            ensure_modem_device(state, mqtt_command_tx, &modem_id).await?;
            send_mqtt_command(
                mqtt_command_tx,
                MqttCommand::PublishSmsSnapshot {
                    modem_id,
                    sms_id,
                    snapshot,
                },
            )
            .await?;
        }
        DbusEvent::SmsUpdated {
            modem_id,
            sms_id,
            update,
        } => {
            ensure_modem_device(state, mqtt_command_tx, &modem_id).await?;
            send_mqtt_command(
                mqtt_command_tx,
                MqttCommand::PublishSmsUpdate {
                    modem_id,
                    sms_id,
                    update,
                },
            )
            .await?;
        }
        DbusEvent::SmsDeleted { modem_id, sms_id } => {
            ensure_modem_device(state, mqtt_command_tx, &modem_id).await?;
            send_mqtt_command(
                mqtt_command_tx,
                MqttCommand::PublishSmsDeleted { modem_id, sms_id },
            )
            .await?;
        }
    }

    Ok(())
}

async fn route_mqtt_event(
    event: MqttEvent,
    dbus_command_tx: Option<&mpsc::Sender<DbusCommand>>,
) -> Result<()> {
    let Some(dbus_command_tx) = dbus_command_tx else {
        return Ok(());
    };

    match event {
        MqttEvent::RequestSmsSnapshot { modem_id, sms_id } => {
            send_dbus_command(
                dbus_command_tx,
                DbusCommand::RefreshSms { modem_id, sms_id },
            )
            .await?;
        }
        MqttEvent::DeleteSms { modem_id, sms_id } => {
            send_dbus_command(dbus_command_tx, DbusCommand::DeleteSms { modem_id, sms_id }).await?;
        }
    }

    Ok(())
}

async fn ensure_manager_device(
    state: &mut TresherState,
    mqtt_command_tx: &mpsc::Sender<MqttCommand>,
) -> Result<()> {
    if !state.device_announced {
        send_mqtt_command(mqtt_command_tx, MqttCommand::EnsureModemManagerDevice).await?;
        state.device_announced = true;
    }

    Ok(())
}

async fn ensure_modem_device(
    state: &mut TresherState,
    mqtt_command_tx: &mpsc::Sender<MqttCommand>,
    modem_id: &ModemId,
) -> Result<()> {
    if state.modem_devices.insert(modem_id.clone()) {
        send_mqtt_command(
            mqtt_command_tx,
            MqttCommand::EnsureModemDevice {
                modem_id: modem_id.clone(),
            },
        )
        .await?;
    }

    Ok(())
}

async fn send_mqtt_command(
    mqtt_command_tx: &mpsc::Sender<MqttCommand>,
    command: MqttCommand,
) -> Result<()> {
    debug!(target: LOG_TARGET, "Queued MQTT command: {command:?}");
    if mqtt_command_tx.send(command).await.is_err() {
        debug!(target: LOG_TARGET, "MQTT command channel closed while sending");
    }

    Ok(())
}

async fn send_dbus_command(
    dbus_command_tx: &mpsc::Sender<DbusCommand>,
    command: DbusCommand,
) -> Result<()> {
    debug!(target: LOG_TARGET, "Queued DBus command: {command:?}");
    if dbus_command_tx.send(command).await.is_err() {
        debug!(target: LOG_TARGET, "DBus command channel closed while sending");
    }

    Ok(())
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

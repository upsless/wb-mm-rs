use std::collections::HashSet;

use anyhow::Result;
use tokio::sync::{mpsc, watch};
use tracing::debug;

use crate::dbus::{ModemId, ModemManagerStatus};
use crate::exchange::{DbusEvent, MqttCommand};

const LOG_TARGET: &str = "DISP";

/// Minimal tresher that translates DBus events into MQTT commands.
///
/// This stays deliberately stateful but small: we remember only the last
/// manager values we published so the MQTT side receives clean, intentional
/// commands instead of every intermediate DBus detail.
pub async fn run(
    mut shutdown_rx: watch::Receiver<bool>,
    mut dbus_event_rx: mpsc::Receiver<DbusEvent>,
    mqtt_command_tx: mpsc::Sender<MqttCommand>,
) -> Result<()> {
    let mut state = TresherState::default();

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
                route_event(event, &mut state, &mqtt_command_tx).await?;
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

async fn route_event(
    event: DbusEvent,
    state: &mut TresherState,
    mqtt_command_tx: &mpsc::Sender<MqttCommand>,
) -> Result<()> {
    ensure_device(state, mqtt_command_tx).await?;

    match event {
        DbusEvent::StatusChanged(status) => {
            if state.last_status != Some(status) {
                send_command(
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
                send_command(
                    mqtt_command_tx,
                    MqttCommand::PublishModemManagerStatus(ModemManagerStatus::Active),
                )
                .await?;
                state.last_status = Some(ModemManagerStatus::Active);
            }

            if state.last_version.as_ref() != Some(&version) {
                send_command(
                    mqtt_command_tx,
                    MqttCommand::PublishModemManagerVersion(version.clone()),
                )
                .await?;
                state.last_version = Some(version);
            }

            if state.last_modem_count != Some(modem_count) {
                send_command(
                    mqtt_command_tx,
                    MqttCommand::PublishModemManagerModemCount(modem_count),
                )
                .await?;
                state.last_modem_count = Some(modem_count);
            }
        }
        DbusEvent::ModemCountChanged { modem_count } => {
            if state.last_modem_count != Some(modem_count) {
                send_command(
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
            send_command(
                mqtt_command_tx,
                MqttCommand::PublishModemSnapshot { modem_id, snapshot },
            )
            .await?;
        }
        DbusEvent::ModemUpdated { modem_id, update } => {
            ensure_modem_device(state, mqtt_command_tx, &modem_id).await?;
            send_command(
                mqtt_command_tx,
                MqttCommand::PublishModemUpdate { modem_id, update },
            )
            .await?;
        }
        DbusEvent::ModemDeleted { modem_id } => {
            send_command(
                mqtt_command_tx,
                MqttCommand::DeleteModemDevice {
                    modem_id: modem_id.clone(),
                },
            )
            .await?;
            state.modem_devices.remove(&modem_id);
        }
    }

    Ok(())
}

async fn ensure_device(
    state: &mut TresherState,
    mqtt_command_tx: &mpsc::Sender<MqttCommand>,
) -> Result<()> {
    if !state.device_announced {
        send_command(mqtt_command_tx, MqttCommand::EnsureModemManagerDevice).await?;
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
        send_command(
            mqtt_command_tx,
            MqttCommand::EnsureModemDevice {
                modem_id: modem_id.clone(),
            },
        )
        .await?;
    }

    Ok(())
}

async fn send_command(
    mqtt_command_tx: &mpsc::Sender<MqttCommand>,
    command: MqttCommand,
) -> Result<()> {
    debug!(target: LOG_TARGET, "Queued MQTT command: {command:?}");
    if mqtt_command_tx.send(command).await.is_err() {
        debug!(target: LOG_TARGET, "MQTT command channel closed while sending");
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

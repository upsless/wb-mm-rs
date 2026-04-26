use anyhow::Result;
use tokio::sync::{mpsc, watch};
use tracing::debug;

use crate::dbus::ModemManagerStatus;
use crate::exchange::{DbusEvent, MqttCommand};

/// Minimal "hammer mill" that translates DBus events into MQTT commands.
///
/// This stays deliberately stateful but small: we remember only the last
/// manager values we published so the MQTT side receives clean, intentional
/// commands instead of every intermediate DBus detail.
pub async fn run(
    mut shutdown_rx: watch::Receiver<bool>,
    mut dbus_event_rx: mpsc::Receiver<DbusEvent>,
    mqtt_command_tx: mpsc::Sender<MqttCommand>,
) -> Result<()> {
    let mut state = DispatchState::default();

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

                debug!("Dispatcher received DBus event: {event:?}");
                dispatch_event(event, &mut state, &mqtt_command_tx).await?;
            }
        }
    }

    Ok(())
}

#[derive(Debug, Default)]
struct DispatchState {
    device_announced: bool,
    last_status: Option<ModemManagerStatus>,
    last_version: Option<String>,
    last_modem_count: Option<usize>,
}

async fn dispatch_event(
    event: DbusEvent,
    state: &mut DispatchState,
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
    }

    Ok(())
}

async fn ensure_device(
    state: &mut DispatchState,
    mqtt_command_tx: &mpsc::Sender<MqttCommand>,
) -> Result<()> {
    if !state.device_announced {
        send_command(mqtt_command_tx, MqttCommand::EnsureModemManagerDevice).await?;
        state.device_announced = true;
    }

    Ok(())
}

async fn send_command(
    mqtt_command_tx: &mpsc::Sender<MqttCommand>,
    command: MqttCommand,
) -> Result<()> {
    debug!("Dispatcher queued MQTT command: {command:?}");
    if mqtt_command_tx.send(command).await.is_err() {
        debug!("MQTT command channel closed while dispatcher was sending");
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

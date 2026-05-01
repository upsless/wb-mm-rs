use anyhow::Result;
use tokio::sync::{mpsc, watch};
use tracing::debug;

use crate::exchange::{DbusCommand, DbusEvent, MqttCommand, MqttEvent};

const LOG_TARGET: &str = "DISP";

/// Routes DBus and MQTT events and emits commands/messages for the side that
/// has to act next.
pub async fn run(
    mut shutdown_rx: watch::Receiver<bool>,
    mut dbus_event_rx: mpsc::Receiver<DbusEvent>,
    mut mqtt_event_rx: mpsc::Receiver<MqttEvent>,
    mqtt_message_tx: mpsc::Sender<MqttCommand>,
    actual_dbus_command_rx: watch::Receiver<Option<mpsc::Sender<DbusCommand>>>,
) -> Result<()> {
    let mut actual_dbus_command_rx = actual_dbus_command_rx;

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
                route_dbus_event(event, &mqtt_message_tx).await?;
            }
            maybe_event = mqtt_event_rx.recv() => {
                let Some(event) = maybe_event else {
                    break;
                };
                let current_dbus_command_tx = actual_dbus_command_rx.borrow().clone();

                debug!(target: LOG_TARGET, "Received MQTT event: {event:?}");
                route_mqtt_event(event, current_dbus_command_tx.as_ref()).await?;
            }
            changed = actual_dbus_command_rx.changed() => {
                if changed.is_err() {
                    break;
                }
            }
        }
    }

    Ok(())
}
async fn route_dbus_event(
    event: DbusEvent,
    mqtt_message_tx: &mpsc::Sender<MqttCommand>,
) -> Result<()> {
    match event {
        DbusEvent::ManagerFound {
            version,
            modem_count,
        } => {
            send_to_mqtt(
                mqtt_message_tx,
                MqttCommand::ManagerFound {
                    version,
                    modem_count,
                },
            )
            .await?;
        }
        DbusEvent::ManagerUpdated(update) => {
            send_to_mqtt(mqtt_message_tx, MqttCommand::ManagerUpdated(update)).await?;
        }
        DbusEvent::ManagerDeleted => {
            send_to_mqtt(mqtt_message_tx, MqttCommand::ManagerDeleted).await?;
        }
        DbusEvent::ModemFound {
            modem_id,
            is_active,
            model,
            revision,
            state,
            primary_sim_slot,
            operator_name,
            own_numbers,
            signal_quality,
        } => {
            send_to_mqtt(
                mqtt_message_tx,
                MqttCommand::ModemFound {
                    modem_id,
                    is_active,
                    model,
                    revision,
                    state,
                    primary_sim_slot,
                    operator_name,
                    own_numbers,
                    signal_quality,
                },
            )
            .await?;
        }
        DbusEvent::ModemUpdated { modem_id, update } => {
            send_to_mqtt(
                mqtt_message_tx,
                MqttCommand::ModemUpdated { modem_id, update },
            )
            .await?;
        }
        DbusEvent::ModemDeleted { modem_id } => {
            send_to_mqtt(mqtt_message_tx, MqttCommand::ModemDeleted { modem_id }).await?;
        }
        DbusEvent::SmsInventorySnapshot {
            modem_id,
            sms_ids,
            initial_sms_snapshot,
        } => {
            send_to_mqtt(
                mqtt_message_tx,
                MqttCommand::PublishSmsInventorySnapshot {
                    modem_id,
                    sms_ids,
                    initial_sms_snapshot,
                },
            )
            .await?;
        }
        DbusEvent::SmsListChanged { modem_id, sms_ids } => {
            send_to_mqtt(
                mqtt_message_tx,
                MqttCommand::PublishSmsList { modem_id, sms_ids },
            )
            .await?;
        }
        DbusEvent::SmsSnapshot { modem_id, snapshot } => {
            send_to_mqtt(
                mqtt_message_tx,
                MqttCommand::PublishSmsSnapshot { modem_id, snapshot },
            )
            .await?;
        }
        DbusEvent::SmsPropertyChanged { modem_id, update } => {
            send_to_mqtt(
                mqtt_message_tx,
                MqttCommand::PublishSmsUpdate { modem_id, update },
            )
            .await?;
        }
        DbusEvent::SmsDeleted { modem_id, sms_id } => {
            send_to_mqtt(
                mqtt_message_tx,
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
            send_to_dbus(
                dbus_command_tx,
                DbusCommand::RefreshSms { modem_id, sms_id },
            )
            .await?;
        }
        MqttEvent::DeleteSms { modem_id, sms_id } => {
            send_to_dbus(dbus_command_tx, DbusCommand::DeleteSms { modem_id, sms_id }).await?;
        }
    }

    Ok(())
}

async fn send_to_mqtt(
    mqtt_message_tx: &mpsc::Sender<MqttCommand>,
    command: MqttCommand,
) -> Result<()> {
    debug!(target: LOG_TARGET, "Sending message to MQTT: {command:?}");
    if mqtt_message_tx.send(command).await.is_err() {
        debug!(target: LOG_TARGET, "MQTT command channel closed while sending");
    }

    Ok(())
}

async fn send_to_dbus(
    dbus_command_tx: &mpsc::Sender<DbusCommand>,
    command: DbusCommand,
) -> Result<()> {
    debug!(target: LOG_TARGET, "Sending command to DBus: {command:?}");
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

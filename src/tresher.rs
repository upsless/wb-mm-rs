use std::collections::{HashMap, HashSet};

use anyhow::Result;
use tokio::sync::{mpsc, watch};
use tracing::debug;

use crate::dbus::{ModemId, ModemManagerStatus, SmsId, SmsSnapshot, SmsUpdate};
use crate::exchange::{DbusCommand, DbusEvent, MqttCommand, MqttEvent};

const LOG_TARGET: &str = "DISP";

/// Stateful business-logic bridge between DBus and MQTT.
///
/// The tresher keeps just enough cached state to:
/// - aggregate manager-level values like total SMS count;
/// - remember per-modem device numbering and selected SMS;
/// - route user-originated MQTT selection changes back into DBus.
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
                // The DBus command sender is replaced on every DBus reconnect,
                // so we snapshot the current sender before awaiting.
                let current_dbus_command_tx = dbus_command_tx_rx.borrow().clone();

                debug!(target: LOG_TARGET, "Received DBus event: {event:?}");
                route_dbus_event(
                    event,
                    &mut state,
                    &mqtt_command_tx,
                    current_dbus_command_tx.as_ref(),
                ).await?;
            }
            maybe_event = mqtt_event_rx.recv() => {
                let Some(event) = maybe_event else {
                    break;
                };
                // Same idea for MQTT-originated writes: selection events should
                // use whichever DBus session is live right now.
                let current_dbus_command_tx = dbus_command_tx_rx.borrow().clone();

                debug!(target: LOG_TARGET, "Received MQTT event: {event:?}");
                route_mqtt_event(
                    event,
                    &mut state,
                    &mqtt_command_tx,
                    current_dbus_command_tx.as_ref(),
                ).await?;
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
    last_manager_sms_count: Option<usize>,
    last_manager_last_sms: Option<Option<i64>>,
    modem_devices: HashSet<ModemId>,
    modems: HashMap<ModemId, ModemState>,
}

#[derive(Debug, Default)]
struct ModemState {
    sms: HashMap<SmsId, SmsSnapshot>,
    sms_order: Vec<SmsId>,
    selected_sms_id: Option<SmsId>,
    last_sms_count: Option<usize>,
    last_selected_index: Option<Option<u32>>,
    last_selected_max_index: Option<u32>,
    last_selected_writable: Option<bool>,
    last_selected_snapshot: Option<Option<SmsSnapshot>>,
}

async fn route_dbus_event(
    event: DbusEvent,
    state: &mut TresherState,
    mqtt_command_tx: &mpsc::Sender<MqttCommand>,
    dbus_command_tx: Option<&mpsc::Sender<DbusCommand>>,
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
            ensure_modem_state(state, &modem_id);
            sync_modem_sms_state(state, mqtt_command_tx, &modem_id).await?;
            sync_manager_sms_state(state, mqtt_command_tx).await?;
        }
        DbusEvent::ModemSnapshot { modem_id, snapshot } => {
            ensure_modem_device(state, mqtt_command_tx, &modem_id).await?;
            ensure_modem_state(state, &modem_id);
            send_mqtt_command(
                mqtt_command_tx,
                MqttCommand::PublishModemSnapshot { modem_id, snapshot },
            )
            .await?;
        }
        DbusEvent::ModemUpdated { modem_id, update } => {
            ensure_modem_device(state, mqtt_command_tx, &modem_id).await?;
            ensure_modem_state(state, &modem_id);
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
            state.modems.remove(&modem_id);
            sync_manager_sms_state(state, mqtt_command_tx).await?;
        }
        DbusEvent::SmsSnapshot {
            modem_id,
            sms_id,
            snapshot,
        } => {
            ensure_modem_device(state, mqtt_command_tx, &modem_id).await?;
            let (selected_before, selection_missing) = {
                let modem_state = ensure_modem_state(state, &modem_id);
                modem_state.sms.insert(sms_id.clone(), snapshot);
                (
                    modem_state.selected_sms_id.as_ref() == Some(&sms_id),
                    modem_state.selected_sms_id.is_none(),
                )
            };
            sync_modem_sms_state(state, mqtt_command_tx, &modem_id).await?;
            sync_manager_sms_state(state, mqtt_command_tx).await?;

            if selected_before {
                publish_selected_sms(state, mqtt_command_tx, &modem_id).await?;
            } else if selection_missing {
                ensure_selected_sms(state, mqtt_command_tx, &modem_id, dbus_command_tx).await?;
            }
        }
        DbusEvent::SmsListChanged { modem_id, sms_ids } => {
            let should_refresh_selected_sms = {
                let modem_state = ensure_modem_state(state, &modem_id);
                let had_any_sms_snapshots = !modem_state.sms.is_empty();
                modem_state.sms_order = sms_ids;

                if modem_state
                    .selected_sms_id
                    .as_ref()
                    .is_some_and(|sms_id| !modem_state.sms_order.contains(sms_id))
                {
                    modem_state.selected_sms_id = None;
                }

                had_any_sms_snapshots
            };

            sync_modem_sms_state(state, mqtt_command_tx, &modem_id).await?;
            sync_manager_sms_state(state, mqtt_command_tx).await?;
            if should_refresh_selected_sms {
                ensure_selected_sms(state, mqtt_command_tx, &modem_id, dbus_command_tx).await?;
            }
        }
        DbusEvent::SmsUpdated {
            modem_id,
            sms_id,
            update,
        } => {
            let selected_now = {
                let modem_state = ensure_modem_state(state, &modem_id);
                let Some(snapshot) = modem_state.sms.get_mut(&sms_id) else {
                    return Ok(());
                };
                apply_sms_update(snapshot, &update);
                modem_state.selected_sms_id.as_ref() == Some(&sms_id)
            };
            sync_modem_sms_state(state, mqtt_command_tx, &modem_id).await?;
            sync_manager_sms_state(state, mqtt_command_tx).await?;

            if selected_now {
                publish_selected_sms(state, mqtt_command_tx, &modem_id).await?;
            }
        }
        DbusEvent::SmsDeleted { modem_id, sms_id } => {
            let modem_state = ensure_modem_state(state, &modem_id);
            modem_state.sms.remove(&sms_id);
            if modem_state.selected_sms_id.as_ref() == Some(&sms_id) {
                modem_state.selected_sms_id = None;
            }

            sync_modem_sms_state(state, mqtt_command_tx, &modem_id).await?;
            sync_manager_sms_state(state, mqtt_command_tx).await?;
            ensure_selected_sms(state, mqtt_command_tx, &modem_id, dbus_command_tx).await?;
        }
        DbusEvent::SelectedSmsSnapshot {
            modem_id,
            sms_id,
            snapshot,
        } => {
            let modem_state = ensure_modem_state(state, &modem_id);
            modem_state.sms.insert(sms_id.clone(), snapshot);

            if modem_state.selected_sms_id.as_ref() == Some(&sms_id) {
                publish_selected_sms(state, mqtt_command_tx, &modem_id).await?;
            }
        }
    }

    Ok(())
}

async fn route_mqtt_event(
    event: MqttEvent,
    state: &mut TresherState,
    mqtt_command_tx: &mpsc::Sender<MqttCommand>,
    dbus_command_tx: Option<&mpsc::Sender<DbusCommand>>,
) -> Result<()> {
    match event {
        MqttEvent::SelectModemSms {
            modem_id,
            selected_index,
        } => {
            let modem_state = ensure_modem_state(state, &modem_id);
            let Some(sms_id) = modem_state
                .sms_order
                .get(selected_index.saturating_sub(1) as usize)
                .cloned()
            else {
                return Ok(());
            };

            modem_state.selected_sms_id = Some(sms_id);
            sync_modem_sms_state(state, mqtt_command_tx, &modem_id).await?;
            request_selected_sms_refresh(state, mqtt_command_tx, &modem_id, dbus_command_tx)
                .await?;
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

fn ensure_modem_state<'a>(state: &'a mut TresherState, modem_id: &ModemId) -> &'a mut ModemState {
    state.modems.entry(modem_id.clone()).or_default()
}

async fn sync_manager_sms_state(
    state: &mut TresherState,
    mqtt_command_tx: &mpsc::Sender<MqttCommand>,
) -> Result<()> {
    let sms_count = state
        .modems
        .values()
        .map(|modem_state| modem_state.sms_order.len())
        .sum::<usize>();
    let last_sms = state
        .modems
        .values()
        .flat_map(|modem_state| modem_state.sms.values())
        .filter_map(|snapshot| snapshot.timestamp)
        .max();

    if state.last_manager_sms_count != Some(sms_count) {
        send_mqtt_command(
            mqtt_command_tx,
            MqttCommand::PublishModemManagerSmsCount(sms_count),
        )
        .await?;
        state.last_manager_sms_count = Some(sms_count);
    }

    if state.last_manager_last_sms != Some(last_sms) {
        send_mqtt_command(
            mqtt_command_tx,
            MqttCommand::PublishModemManagerLastSms(last_sms),
        )
        .await?;
        state.last_manager_last_sms = Some(last_sms);
    }

    Ok(())
}

async fn sync_modem_sms_state(
    state: &mut TresherState,
    mqtt_command_tx: &mpsc::Sender<MqttCommand>,
    modem_id: &ModemId,
) -> Result<()> {
    let Some(modem_state) = state.modems.get_mut(modem_id) else {
        return Ok(());
    };

    let sms_count = modem_state.sms_order.len();
    if modem_state.last_sms_count != Some(sms_count) {
        send_mqtt_command(
            mqtt_command_tx,
            MqttCommand::PublishModemSmsCount {
                modem_id: modem_id.clone(),
                sms_count,
            },
        )
        .await?;
        modem_state.last_sms_count = Some(sms_count);
    }

    if modem_state.selected_sms_id.is_none() {
        modem_state.selected_sms_id = modem_state.sms_order.first().cloned();
    }

    let selected_index = modem_state
        .selected_sms_id
        .as_ref()
        .and_then(|selected_sms_id| {
            modem_state
                .sms_order
                .iter()
                .position(|sms_id| sms_id == selected_sms_id)
        })
        .map(|index| (index + 1) as u32);
    let max_index = modem_state.sms_order.len().max(1) as u32;
    let writable = !modem_state.sms_order.is_empty();

    if modem_state.last_selected_index != Some(selected_index)
        || modem_state.last_selected_max_index != Some(max_index)
        || modem_state.last_selected_writable != Some(writable)
    {
        send_mqtt_command(
            mqtt_command_tx,
            MqttCommand::PublishModemSmsSelection {
                modem_id: modem_id.clone(),
                selected_index,
                max_index,
                writable,
            },
        )
        .await?;
        modem_state.last_selected_index = Some(selected_index);
        modem_state.last_selected_max_index = Some(max_index);
        modem_state.last_selected_writable = Some(writable);
    }

    Ok(())
}

async fn ensure_selected_sms(
    state: &mut TresherState,
    mqtt_command_tx: &mpsc::Sender<MqttCommand>,
    modem_id: &ModemId,
    dbus_command_tx: Option<&mpsc::Sender<DbusCommand>>,
) -> Result<()> {
    sync_modem_sms_state(state, mqtt_command_tx, modem_id).await?;
    request_selected_sms_refresh(state, mqtt_command_tx, modem_id, dbus_command_tx).await
}

async fn request_selected_sms_refresh(
    state: &mut TresherState,
    mqtt_command_tx: &mpsc::Sender<MqttCommand>,
    modem_id: &ModemId,
    dbus_command_tx: Option<&mpsc::Sender<DbusCommand>>,
) -> Result<()> {
    let Some(modem_state) = state.modems.get(modem_id) else {
        return Ok(());
    };

    let Some(selected_sms_id) = modem_state.selected_sms_id.clone() else {
        publish_selected_sms(state, mqtt_command_tx, modem_id).await?;
        return Ok(());
    };

    // Selection changes should exercise the real reverse path
    // MQTT -> tresher -> DBus -> tresher -> MQTT.
    // If DBus is temporarily unavailable, we still fall back to the cached
    // snapshot so the MQTT side does not stay blank.
    if let Some(dbus_command_tx) = dbus_command_tx {
        send_dbus_command(
            dbus_command_tx,
            DbusCommand::RefreshSelectedSms {
                modem_id: modem_id.clone(),
                sms_id: selected_sms_id,
            },
        )
        .await?;
    } else {
        publish_selected_sms(state, mqtt_command_tx, modem_id).await?;
    }

    Ok(())
}

async fn publish_selected_sms(
    state: &mut TresherState,
    mqtt_command_tx: &mpsc::Sender<MqttCommand>,
    modem_id: &ModemId,
) -> Result<()> {
    let Some(modem_state) = state.modems.get_mut(modem_id) else {
        return Ok(());
    };

    let selected_snapshot = modem_state
        .selected_sms_id
        .as_ref()
        .and_then(|selected_sms_id| modem_state.sms.get(selected_sms_id))
        .cloned();

    if modem_state.last_selected_snapshot != Some(selected_snapshot.clone()) {
        send_mqtt_command(
            mqtt_command_tx,
            MqttCommand::PublishSelectedSms {
                modem_id: modem_id.clone(),
                snapshot: selected_snapshot.clone(),
            },
        )
        .await?;
        modem_state.last_selected_snapshot = Some(selected_snapshot);
    }

    Ok(())
}

fn apply_sms_update(snapshot: &mut SmsSnapshot, update: &SmsUpdate) {
    match update {
        SmsUpdate::IsReceived(value) => snapshot.is_received = *value,
        SmsUpdate::Timestamp(value) => snapshot.timestamp = *value,
        SmsUpdate::Number(value) => snapshot.number = value.clone(),
        SmsUpdate::Text(value) => snapshot.text = value.clone(),
    }
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

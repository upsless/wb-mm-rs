use anyhow::Result;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::{debug, info};

use crate::dbus::{ModemManagerStatus, ModemSnapshot, ModemUpdate};
use crate::exchange::MqttCommand;
use crate::mqtt::logics;

/// Stage-0 MQTT stub.
///
/// For now it only models lifecycle: "connected", then "wait until asked to
/// stop", then "stopped". This keeps the orchestration shape in place before
/// we add a real MQTT client.
pub async fn run(
    mut shutdown_rx: watch::Receiver<bool>,
    mut command_rx: mpsc::Receiver<MqttCommand>,
) -> Result<()> {
    debug!("{}", logics::mqtt_connected_message());

    loop {
        tokio::select! {
            result = wait_for_shutdown(&mut shutdown_rx) => {
                result?;
                break;
            }
            maybe_command = command_rx.recv() => {
                let Some(command) = maybe_command else {
                    break;
                };
                handle_command(command);
            }
        }
    }

    debug!("{}", logics::mqtt_stopped_message());

    Ok(())
}

fn handle_command(command: MqttCommand) {
    match command {
        MqttCommand::EnsureModemManagerDevice => {
            info!("{}", logics::mqtt_ensure_mm_device_message());
        }
        MqttCommand::PublishModemManagerStatus(status) => {
            info!(
                "{}",
                logics::mqtt_publish_mm_status_message(modemmanager_status_name(status))
            );
        }
        MqttCommand::PublishModemManagerVersion(version) => {
            info!("{}", logics::mqtt_publish_mm_version_message(&version));
        }
        MqttCommand::PublishModemManagerModemCount(modem_count) => {
            info!(
                "{}",
                logics::mqtt_publish_mm_modem_count_message(modem_count)
            );
        }
        MqttCommand::EnsureModemDevice { modem_id } => {
            info!("{}", logics::mqtt_ensure_modem_device_message(&modem_id.0));
        }
        MqttCommand::PublishModemSnapshot { modem_id, snapshot } => {
            info!(
                "{}",
                logics::mqtt_publish_modem_snapshot_message(
                    &modem_id.0,
                    &format_modem_snapshot(&snapshot),
                )
            );
        }
        MqttCommand::PublishModemUpdate { modem_id, update } => {
            info!(
                "{}",
                logics::mqtt_publish_modem_update_message(
                    &modem_id.0,
                    &format_modem_update(&update),
                )
            );
        }
        MqttCommand::DeleteModemDevice { modem_id } => {
            info!("{}", logics::mqtt_delete_modem_device_message(&modem_id.0));
        }
    }
}

fn modemmanager_status_name(status: ModemManagerStatus) -> &'static str {
    match status {
        ModemManagerStatus::Active => "active",
        ModemManagerStatus::Inactive => "inactive",
        ModemManagerStatus::NotFound => "not_found",
    }
}

fn format_modem_snapshot(snapshot: &ModemSnapshot) -> String {
    snapshot.summary()
}

fn format_modem_update(update: &ModemUpdate) -> String {
    update.summary()
}

/// Mirrors the DBus loop helper so both subsystems react to shutdown in the
/// same way.
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

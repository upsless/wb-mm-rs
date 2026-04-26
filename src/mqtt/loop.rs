use anyhow::Result;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::{debug, info};

use crate::dbus::ModemManagerStatus;
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
    }
}

fn modemmanager_status_name(status: ModemManagerStatus) -> &'static str {
    match status {
        ModemManagerStatus::Active => "active",
        ModemManagerStatus::Inactive => "inactive",
        ModemManagerStatus::NotFound => "not_found",
    }
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

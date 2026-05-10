mod connection;
mod manager;
mod modem;
mod runtime;
mod schema;

use anyhow::Result;
use tokio::sync::{mpsc, watch};
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info};

use crate::exchange::{DbusCommand, DbusEvent};
use crate::shutdown::wait_for_shutdown;

pub use schema::{
    LOG_TARGET,
    ManagerUpdate, ModemId, ModemInfo, ManagerStatus, ModemUpdate, SmsId, SmsPropertyChange,
    SmsSnapshot, SmsUpdate, 
};

const RECONNECT_FAST_INTERVAL: Duration = Duration::from_secs(5);
const RECONNECT_SLOW_INTERVAL: Duration = Duration::from_secs(60);
const RECONNECT_FAST_ATTEMPTS: u32 = 24;
const COMMAND_CHANNEL_CAPACITY: usize = 32;

pub enum LifecycleExit {
    Shutdown,
    MqttEnded,
}

/// Runs and reconnects DBus for as long as the current MQTT session is alive.
///
/// Returns `LifecycleExit::Shutdown` on a graceful shutdown signal, or
/// `LifecycleExit::MqttEnded` when the MQTT task terminates and the DBus
/// session should be torn down.
pub async fn run_lifecycle(
    dbus_address: Option<String>,
    shutdown_rx: &mut watch::Receiver<bool>,
    mqtt_stop_tx: &watch::Sender<bool>,
    mqtt_task: &mut tokio::task::JoinHandle<Result<()>>,
    event_tx: mpsc::Sender<DbusEvent>,
    command_tx: &watch::Sender<Option<mpsc::Sender<DbusCommand>>>,
) -> Result<LifecycleExit> {
    let mut retry_attempt = 1;

    loop {
        let (dbus_command_tx, dbus_command_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        let _ = command_tx.send(Some(dbus_command_tx));
        let (dbus_stop_tx, dbus_stop_rx) = watch::channel(false);
        let mut dbus_task = tokio::spawn(connection::run(
            dbus_address.clone(),
            dbus_stop_rx,
            dbus_command_rx,
            event_tx.clone(),
        ));

        let retry_delay = tokio::select! {
            result = wait_for_shutdown(shutdown_rx) => {
                result?;
                let _ = command_tx.send(None);
                stop_task("DBus", dbus_stop_tx, dbus_task).await?;
                let _ = mqtt_stop_tx.send(true);
                return Ok(LifecycleExit::Shutdown);
            }
            mqtt_result = &mut *mqtt_task => {
                log_unexpected_exit("MQTT", mqtt_result)?;
                info!(target: LOG_TARGET, "Stopping DBus because MQTT connection is unavailable.");
                let _ = command_tx.send(None);
                let _ = dbus_stop_tx.send(true);
                stop_finished_task("DBus", dbus_task).await?;
                return Ok(LifecycleExit::MqttEnded);
            }
            dbus_result = &mut dbus_task => {
                log_unexpected_exit("DBus", dbus_result)?;
                let _ = command_tx.send(None);
                debug!(target: LOG_TARGET, "{}", schema::manager_deleted_message());
                if event_tx.send(DbusEvent::ManagerDeleted).await.is_err() {
                    debug!(
                        target: LOG_TARGET,
                        "DBus event channel closed while reporting ManagerDeleted"
                    );
                }

                let delay = reconnect_delay(retry_attempt);
                info!(
                    target: LOG_TARGET,
                    "DBus connection lost, retrying in {} second(s) (attempt {}).",
                    delay.as_secs(),
                    retry_attempt
                );
                retry_attempt += 1;
                delay
            }
        };

        tokio::select! {
            result = wait_for_shutdown(shutdown_rx) => {
                result?;
                let _ = command_tx.send(None);
                let _ = mqtt_stop_tx.send(true);
                return Ok(LifecycleExit::Shutdown);
            }
            mqtt_result = &mut *mqtt_task => {
                log_unexpected_exit("MQTT", mqtt_result)?;
                let _ = command_tx.send(None);
                return Ok(LifecycleExit::MqttEnded);
            }
            _ = sleep(retry_delay) => {}
        }
    }
}

fn reconnect_delay(attempt: u32) -> Duration {
    if attempt <= RECONNECT_FAST_ATTEMPTS {
        RECONNECT_FAST_INTERVAL
    } else {
        RECONNECT_SLOW_INTERVAL
    }
}

fn log_unexpected_exit(
    name: &str,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match result {
        Ok(Ok(())) => error!(target: LOG_TARGET, "{name} loop exited before shutdown."),
        Ok(Err(err)) => error!(target: LOG_TARGET, "{name} loop failed: {err:#}"),
        Err(join_error) => return Err(anyhow::anyhow!("{name} task join failed: {join_error}")),
    }
    Ok(())
}

async fn stop_task(
    name: &str,
    stop_tx: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<()>>,
) -> Result<()> {
    let _ = stop_tx.send(true);
    stop_finished_task(name, task).await
}

async fn stop_finished_task(
    name: &str,
    task: tokio::task::JoinHandle<Result<()>>,
) -> Result<()> {
    match task.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => {
            error!(target: LOG_TARGET, "{name} loop ended while stopping: {err:#}");
            Ok(())
        }
        Err(join_error) => Err(anyhow::anyhow!("{name} task join failed: {join_error}")),
    }
}
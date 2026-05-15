mod connection;
mod logstrings;
mod manager;
mod modem;
mod runtime;
mod schema;
mod sms;

use anyhow::Result;
use tokio::sync::{mpsc, watch};
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info};

use crate::common::{
    DBUS_COMMAND_CHANNEL_CAPACITY, DBUS_RECONNECT_FAST_ATTEMPTS, DBUS_RECONNECT_FAST_INTERVAL,
    DBUS_RECONNECT_SLOW_INTERVAL, wait_for_shutdown,
};
use crate::domain::{DbusCommand, DbusEvent};

pub use crate::domain::{
    ManagerStatus, ManagerUpdate, ModemId, ModemInfo, ModemUpdate, OutgoingSmsInfo,
    OutgoingSmsStatus, SmsId, SmsPropertyChange, SmsSnapshot, SmsUpdate,
};
pub use logstrings::LOG_TARGET;

/// Runs and reconnects DBus until daemon shutdown.
///
/// DBus now owns its own long-lived lifecycle and can be restarted on explicit
/// resync requests from other subsystems, for example when MQTT reconnects and
/// needs a fresh projection of live modem state.
pub async fn run_lifecycle(
    dbus_address: Option<String>,
    mut shutdown_rx: watch::Receiver<bool>,
    event_tx: mpsc::Sender<DbusEvent>,
    command_tx: watch::Sender<Option<mpsc::Sender<DbusCommand>>>,
    mut resync_rx: watch::Receiver<u64>,
) -> Result<()> {
    let mut retry_attempt = 1;
    let mut resync_generation = *resync_rx.borrow();

    loop {
        let (dbus_command_tx, dbus_command_rx) = mpsc::channel(DBUS_COMMAND_CHANNEL_CAPACITY);
        let _ = command_tx.send(Some(dbus_command_tx));
        let (dbus_stop_tx, dbus_stop_rx) = watch::channel(false);
        let mut dbus_task = tokio::spawn(connection::run(
            dbus_address.clone(),
            dbus_stop_rx,
            dbus_command_rx,
            event_tx.clone(),
        ));

        let retry_delay = tokio::select! {
            result = wait_for_shutdown(&mut shutdown_rx) => {
                result?;
                let _ = command_tx.send(None);
                stop_task("DBus", dbus_stop_tx, dbus_task).await?;
                return Ok(());
            }
            changed = resync_rx.changed() => {
                if changed.is_ok() {
                    let next_generation = *resync_rx.borrow();
                    if next_generation != resync_generation {
                        resync_generation = next_generation;
                        info!(
                            target: LOG_TARGET,
                            "Restarting DBus session to resync dependent frontend state."
                        );
                        let _ = command_tx.send(None);
                        stop_task("DBus", dbus_stop_tx, dbus_task).await?;
                        retry_attempt = 1;
                        continue;
                    }
                }
                continue;
            }
            dbus_result = &mut dbus_task => {
                log_unexpected_exit("DBus", dbus_result)?;
                let _ = command_tx.send(None);
                debug!(target: LOG_TARGET, "{}", logstrings::manager_deleted_message());
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
            result = wait_for_shutdown(&mut shutdown_rx) => {
                result?;
                let _ = command_tx.send(None);
                return Ok(());
            }
            _ = sleep(retry_delay) => {}
        }
    }
}

fn reconnect_delay(attempt: u32) -> Duration {
    if attempt <= DBUS_RECONNECT_FAST_ATTEMPTS {
        DBUS_RECONNECT_FAST_INTERVAL
    } else {
        DBUS_RECONNECT_SLOW_INTERVAL
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

async fn stop_finished_task(name: &str, task: tokio::task::JoinHandle<Result<()>>) -> Result<()> {
    match task.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => {
            error!(target: LOG_TARGET, "{name} loop ended while stopping: {err:#}");
            Ok(())
        }
        Err(join_error) => Err(anyhow::anyhow!("{name} task join failed: {join_error}")),
    }
}

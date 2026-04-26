mod cli;
mod dbus;
mod dispatcher;
mod exchange;
mod mqtt;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;

use crate::cli::Cli;

const DBUS_RECONNECT_FAST_INTERVAL: Duration = Duration::from_secs(5);
const DBUS_RECONNECT_SLOW_INTERVAL: Duration = Duration::from_secs(60);
const DBUS_RECONNECT_FAST_ATTEMPTS: u32 = 24;

const MQTT_RECONNECT_FAST_INTERVAL: Duration = Duration::from_secs(5);
const MQTT_RECONNECT_SLOW_INTERVAL: Duration = Duration::from_secs(60);
const MQTT_RECONNECT_FAST_ATTEMPTS: u32 = 24;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging()?;

    let cli = Cli::parse();

    info!("Starting wb-mm-mqtt");

    // `watch` is our simplest "global shutdown flag": one sender in `main`,
    // many receivers in background tasks. Each task can both read the current
    // flag value and asynchronously wait until it changes.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Under a normal terminal `Ctrl+C` turns into SIGINT; under VS Code
    // `Shift+F5`/Stop reaches us as SIGTERM because of `gracefulShutdown`.
    let mut sigint =
        signal(SignalKind::interrupt()).context("failed to register SIGINT handler")?;
    let mut sigterm =
        signal(SignalKind::terminate()).context("failed to register SIGTERM handler")?;
    let mut supervisor_task = tokio::spawn(run_supervisor(cli.dbus_address.clone(), shutdown_rx));

    // `tokio::select!` waits for whichever async branch completes first.
    // In our case that is either:
    // - a shutdown signal from the OS/debugger, or
    // - an unexpected supervisor exit.
    tokio::select! {
        _ = sigint.recv() => {
            info!("SIGINT received");
            info!("Termination requested");
            let _ = shutdown_tx.send(true);
        }
        _ = sigterm.recv() => {
            info!("SIGTERM received");
            info!("Termination requested");
            let _ = shutdown_tx.send(true);
        }
        supervisor_result = &mut supervisor_task => {
            let supervisor_result = task_result("Supervisor", supervisor_result);
            let _ = shutdown_tx.send(true);
            supervisor_result?;
        }
    }

    task_result("Supervisor", supervisor_task.await)?;

    info!("wb-mm-mqtt stopped");

    Ok(())
}

/// Supervises subsystem sessions according to the daemon lifecycle rules:
/// - MQTT is the top-level gate;
/// - DBus only runs while MQTT is alive;
/// - each subsystem reconnects with its own retry cadence.
async fn run_supervisor(
    dbus_address: Option<String>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let mut mqtt_retry_attempt = 1;

    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        let (dbus_event_tx, dbus_event_rx) = mpsc::channel(32);
        let (mqtt_command_tx, mqtt_command_rx) = mpsc::channel(32);
        let (mqtt_stop_tx, mqtt_stop_rx) = watch::channel(false);
        let mut mqtt_task = tokio::spawn(mqtt::run(mqtt_stop_rx.clone(), mqtt_command_rx));
        let dispatcher_task = tokio::spawn(dispatcher::run(
            mqtt_stop_rx,
            dbus_event_rx,
            mqtt_command_tx,
        ));

        match run_dbus_lifecycle(
            dbus_address.clone(),
            &mut shutdown_rx,
            &mqtt_stop_tx,
            &mut mqtt_task,
            dbus_event_tx,
        )
        .await?
        {
            SupervisorExit::Shutdown => {
                stop_child_task("Dispatcher", mqtt_stop_tx.clone(), dispatcher_task).await?;
                stop_child_task("MQTT", mqtt_stop_tx, mqtt_task).await?;
                return Ok(());
            }
            SupervisorExit::MqttEnded => {
                stop_child_task("Dispatcher", mqtt_stop_tx.clone(), dispatcher_task).await?;
                let delay = reconnect_delay(
                    mqtt_retry_attempt,
                    MQTT_RECONNECT_FAST_INTERVAL,
                    MQTT_RECONNECT_SLOW_INTERVAL,
                    MQTT_RECONNECT_FAST_ATTEMPTS,
                );
                info!(
                    "MQTT connection lost, retrying in {} second(s) (attempt {}).",
                    delay.as_secs(),
                    mqtt_retry_attempt
                );
                mqtt_retry_attempt += 1;

                if sleep_until_retry_or_shutdown(delay, &mut shutdown_rx).await? {
                    return Ok(());
                }
            }
        }
    }
}

/// Runs DBus sessions only while the current MQTT session is alive.
///
/// When DBus drops, we mark ModemManager as not found and retry only DBus.
/// When MQTT drops, we stop DBus and hand control back to the outer loop so
/// MQTT can reconnect first and DBus can then restart from a clean slate.
async fn run_dbus_lifecycle(
    dbus_address: Option<String>,
    shutdown_rx: &mut watch::Receiver<bool>,
    mqtt_stop_tx: &watch::Sender<bool>,
    mqtt_task: &mut tokio::task::JoinHandle<Result<()>>,
    dbus_event_tx: mpsc::Sender<exchange::DbusEvent>,
) -> Result<SupervisorExit> {
    let mut dbus_retry_attempt = 1;

    loop {
        let (dbus_stop_tx, dbus_stop_rx) = watch::channel(false);
        let mut dbus_task = tokio::spawn(dbus::run(
            dbus_address.clone(),
            dbus_stop_rx,
            dbus_event_tx.clone(),
        ));

        let retry_delay = tokio::select! {
            result = wait_for_shutdown(shutdown_rx) => {
                result?;
                stop_child_task("DBus", dbus_stop_tx, dbus_task).await?;
                let _ = mqtt_stop_tx.send(true);
                return Ok(SupervisorExit::Shutdown);
            }
            mqtt_result = &mut *mqtt_task => {
                log_unexpected_task_exit("MQTT", mqtt_result)?;
                info!("Stopping DBus because MQTT connection is unavailable.");
                let _ = dbus_stop_tx.send(true);
                stop_finished_or_stopping_task("DBus", dbus_task).await?;
                return Ok(SupervisorExit::MqttEnded);
            }
            dbus_result = &mut dbus_task => {
                log_unexpected_task_exit("DBus", dbus_result)?;
                debug!("{}", dbus::modemmanager_not_found_message());
                if dbus_event_tx
                    .send(exchange::DbusEvent::StatusChanged(
                        dbus::ModemManagerStatus::NotFound,
                    ))
                    .await
                    .is_err()
                {
                    debug!("DBus event channel closed while reporting NotFound");
                }

                let delay = reconnect_delay(
                    dbus_retry_attempt,
                    DBUS_RECONNECT_FAST_INTERVAL,
                    DBUS_RECONNECT_SLOW_INTERVAL,
                    DBUS_RECONNECT_FAST_ATTEMPTS,
                );
                info!(
                    "DBus connection lost, retrying in {} second(s) (attempt {}).",
                    delay.as_secs(),
                    dbus_retry_attempt
                );
                dbus_retry_attempt += 1;

                delay
            }
        };

        tokio::select! {
            result = wait_for_shutdown(shutdown_rx) => {
                result?;
                let _ = mqtt_stop_tx.send(true);
                return Ok(SupervisorExit::Shutdown);
            }
            mqtt_result = &mut *mqtt_task => {
                log_unexpected_task_exit("MQTT", mqtt_result)?;
                return Ok(SupervisorExit::MqttEnded);
            }
            _ = sleep(retry_delay) => {}
        }
    }
}

/// Turn a `JoinHandle<Result<...>>` into a plain `Result`.
///
/// Tokio separates:
/// - task execution failure (`JoinError`): task panicked or was aborted;
/// - business failure (`Result<()>`): task finished but returned an error.
fn task_result(
    name: &str,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    result
        .with_context(|| format!("{name} task join failed"))?
        .with_context(|| format!("{name} task failed"))
}

/// For reconnectable child sessions we treat inner `Result` errors as normal
/// lifecycle failures and retry them. A Tokio join failure still means
/// something is wrong in the runtime/task itself, so we keep that fatal.
fn log_unexpected_task_exit(
    name: &str,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match result {
        Ok(Ok(())) => error!("{name} loop exited before shutdown."),
        Ok(Err(err)) => error!("{name} loop failed: {err:#}"),
        Err(join_error) => {
            return Err(anyhow::anyhow!("{name} task join failed: {join_error}"));
        }
    }

    Ok(())
}

async fn stop_child_task(
    name: &str,
    stop_tx: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<()>>,
) -> Result<()> {
    let _ = stop_tx.send(true);
    stop_finished_or_stopping_task(name, task).await
}

async fn stop_finished_or_stopping_task(
    name: &str,
    task: tokio::task::JoinHandle<Result<()>>,
) -> Result<()> {
    match task.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => {
            error!("{name} loop ended while stopping: {err:#}");
            Ok(())
        }
        Err(join_error) => Err(anyhow::anyhow!("{name} task join failed: {join_error}")),
    }
}

fn reconnect_delay(
    attempt: u32,
    fast_interval: Duration,
    slow_interval: Duration,
    fast_attempts: u32,
) -> Duration {
    if attempt <= fast_attempts {
        fast_interval
    } else {
        slow_interval
    }
}

async fn sleep_until_retry_or_shutdown(
    delay: Duration,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<bool> {
    tokio::select! {
        result = wait_for_shutdown(shutdown_rx) => {
            result?;
            Ok(true)
        }
        _ = sleep(delay) => Ok(false),
    }
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

enum SupervisorExit {
    Shutdown,
    MqttEnded,
}

/// Keep logging setup in one place so the rest of the daemon can just use
/// `debug!/info!/error!` without worrying about subscribers and filters.
fn init_logging() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init()
        .map_err(|error| anyhow::anyhow!("failed to initialize logging: {error}"))?;

    Ok(())
}

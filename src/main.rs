mod cli;
mod common;
mod dbus;
mod domain;
mod mqtt;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::{Duration, sleep};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::cli::Cli;
use crate::common::wait_for_shutdown;
use crate::domain::DbusCommand;

const LOG_TARGET: &str = "MAIN";

const MQTT_RECONNECT_FAST_INTERVAL: Duration = Duration::from_secs(5);
const MQTT_RECONNECT_SLOW_INTERVAL: Duration = Duration::from_secs(60);
const MQTT_RECONNECT_FAST_ATTEMPTS: u32 = 24;
const DBUS_EVENT_CHANNEL_CAPACITY: usize = 32;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging()?;

    let cli = Cli::parse();

    info!(target: LOG_TARGET, "Starting wb-mm-mqtt");

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
    let mut supervisor_task = tokio::spawn(run_supervisor(
        cli.dbus_address.clone(),
        cli.mqtt_address.clone(),
        shutdown_rx,
    ));

    // `tokio::select!` waits for whichever async branch completes first.
    // In our case that is either:
    // - a shutdown signal from the OS/debugger, or
    // - an unexpected supervisor exit.
    tokio::select! {
        _ = sigint.recv() => {
            info!(target: LOG_TARGET, "SIGINT received");
            info!(target: LOG_TARGET, "Termination requested");
            let _ = shutdown_tx.send(true);
        }
        _ = sigterm.recv() => {
            info!(target: LOG_TARGET, "SIGTERM received");
            info!(target: LOG_TARGET, "Termination requested");
            let _ = shutdown_tx.send(true);
        }
        supervisor_result = &mut supervisor_task => {
            let supervisor_result = task_result("Supervisor", supervisor_result);
            let _ = shutdown_tx.send(true);
            supervisor_result?;
        }
    }

    task_result("Supervisor", supervisor_task.await)?;

    info!(target: LOG_TARGET, "wb-mm-mqtt stopped");

    Ok(())
}

/// Starts MQTT and DBus sessions and restarts them according to the configured
/// reconnect intervals.
async fn run_supervisor(
    dbus_address: Option<String>,
    mqtt_address: Option<String>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let mut mqtt_retry_attempt = 1;

    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        let (dbus_event_tx, dbus_event_rx) = mpsc::channel(DBUS_EVENT_CHANNEL_CAPACITY);
        let (actual_dbus_command_tx, actual_dbus_command_rx) =
            watch::channel(None::<mpsc::Sender<DbusCommand>>);
        let (mqtt_stop_tx, mqtt_stop_rx) = watch::channel(false);
        let mut mqtt_task = tokio::spawn(mqtt::run_lifecycle(
            mqtt_address.clone(),
            mqtt_stop_rx.clone(),
            dbus_event_rx,
            actual_dbus_command_rx,
        ));

        match dbus::run_lifecycle(
            dbus_address.clone(),
            &mut shutdown_rx,
            &mqtt_stop_tx,
            &mut mqtt_task,
            dbus_event_tx,
            &actual_dbus_command_tx,
        )
        .await?
        {
            dbus::LifecycleExit::Shutdown => {
                stop_child_task("MQTT", mqtt_stop_tx, mqtt_task).await?;
                return Ok(());
            }
            dbus::LifecycleExit::MqttEnded => {
                let delay = reconnect_delay(
                    mqtt_retry_attempt,
                    MQTT_RECONNECT_FAST_INTERVAL,
                    MQTT_RECONNECT_SLOW_INTERVAL,
                    MQTT_RECONNECT_FAST_ATTEMPTS,
                );
                info!(
                    target: LOG_TARGET,
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

/// Converts a task result into an `anyhow::Result` with the task name in the
/// error context.
fn task_result(
    name: &str,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    result
        .with_context(|| format!("{name} task join failed"))?
        .with_context(|| format!("{name} task failed"))
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
            error!(target: LOG_TARGET, "{name} loop ended while stopping: {err:#}");
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

/// Initializes tracing from `RUST_LOG` and suppresses noisy rumqttc state logs
/// unless that target is explicitly requested.
fn init_logging() -> Result<()> {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    let filter = if filter.contains("rumqttc::state") {
        filter
    } else {
        format!("{filter},rumqttc::state=warn")
    };
    let filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init()
        .map_err(|error| anyhow::anyhow!("failed to initialize logging: {error}"))?;

    Ok(())
}

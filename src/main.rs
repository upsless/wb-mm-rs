mod cli;
mod common;
mod core;
mod dbus;
mod domain;
mod mqtt;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::cli::Cli;
use crate::common::{
    CORE_COMMAND_CHANNEL_CAPACITY, CORE_EVENT_CHANNEL_CAPACITY, DBUS_EVENT_CHANNEL_CAPACITY,
    wait_for_shutdown,
};
use crate::domain::DbusCommand;

const LOG_TARGET: &str = "MAIN";

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
        cli.command_numbers.clone(),
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
    command_numbers: Vec<String>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let (dbus_event_tx, dbus_event_rx) = mpsc::channel(DBUS_EVENT_CHANNEL_CAPACITY);
    let (core_event_tx, core_event_rx) = mpsc::channel(CORE_EVENT_CHANNEL_CAPACITY);
    let (core_command_tx, core_command_rx) = mpsc::channel(CORE_COMMAND_CHANNEL_CAPACITY);
    let (actual_dbus_command_tx, actual_dbus_command_rx) =
        watch::channel(None::<mpsc::Sender<DbusCommand>>);
    let (dbus_resync_tx, dbus_resync_rx) = watch::channel(0u64);

    let mut dbus_task = tokio::spawn(dbus::run_lifecycle(
        dbus_address.clone(),
        shutdown_rx.clone(),
        dbus_event_tx,
        actual_dbus_command_tx,
        dbus_resync_rx,
    ));
    let mut core_task = tokio::spawn(core::run_lifecycle(
        core::CoreConfig::new(command_numbers),
        shutdown_rx.clone(),
        dbus_event_rx,
        core_event_tx,
        core_command_rx,
        actual_dbus_command_rx,
    ));
    let mut mqtt_task = tokio::spawn(mqtt::run_lifecycle(
        mqtt_address.clone(),
        shutdown_rx.clone(),
        core_event_rx,
        core_command_tx,
        dbus_resync_tx,
    ));

    tokio::select! {
        result = wait_for_shutdown(&mut shutdown_rx) => {
            result?;
        }
        dbus_result = &mut dbus_task => {
            task_result("DBus", dbus_result)?;
            return Err(anyhow::anyhow!("DBus lifecycle exited before shutdown"));
        }
        core_result = &mut core_task => {
            task_result("Core", core_result)?;
            return Err(anyhow::anyhow!("Core lifecycle exited before shutdown"));
        }
        mqtt_result = &mut mqtt_task => {
            task_result("MQTT", mqtt_result)?;
            return Err(anyhow::anyhow!("MQTT lifecycle exited before shutdown"));
        }
    }

    task_result("DBus", dbus_task.await)?;
    task_result("Core", core_task.await)?;
    task_result("MQTT", mqtt_task.await)?;

    Ok(())
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

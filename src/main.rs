mod cli;
mod dbus;
mod mqtt;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::cli::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging()?;

    let cli = Cli::parse();

    info!("Starting wb-mm-mqtt");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mut mqtt_task = tokio::spawn(mqtt::run(shutdown_rx.clone()));
    let mut dbus_task = tokio::spawn(dbus::run(cli.dbus_address.clone(), shutdown_rx));

    let mut sigint =
        signal(SignalKind::interrupt()).context("failed to register SIGINT handler")?;
    let mut sigterm =
        signal(SignalKind::terminate()).context("failed to register SIGTERM handler")?;

    tokio::select! {
        _ = sigint.recv() => {
            info!("SIGINT received");
            info!("Termination requested");
            let _ = shutdown_tx.send(true);

            task_result("MQTT", mqtt_task.await)?;
            task_result("DBus", dbus_task.await)?;
        }
        _ = sigterm.recv() => {
            info!("SIGTERM received");
            info!("Termination requested");
            let _ = shutdown_tx.send(true);

            task_result("MQTT", mqtt_task.await)?;
            task_result("DBus", dbus_task.await)?;
        }
        mqtt_result = &mut mqtt_task => {
            let mqtt_result = task_result("MQTT", mqtt_result);
            error!("MQTT loop exited before shutdown");
            let _ = shutdown_tx.send(true);
            task_result("DBus", dbus_task.await)?;
            mqtt_result?;
        }
        dbus_result = &mut dbus_task => {
            let dbus_result = task_result("DBus", dbus_result);
            error!("DBus loop exited before shutdown");
            let _ = shutdown_tx.send(true);
            task_result("MQTT", mqtt_task.await)?;
            dbus_result?;
        }
    }

    info!("wb-mm-mqtt stopped");

    Ok(())
}

fn task_result(
    name: &str,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    result
        .with_context(|| format!("{name} task join failed"))?
        .with_context(|| format!("{name} task failed"))
}

fn init_logging() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init()
        .map_err(|error| anyhow::anyhow!("failed to initialize logging: {error}"))?;

    Ok(())
}

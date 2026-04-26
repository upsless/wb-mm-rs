mod cli;
mod dbus;
mod mqtt;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::sync::watch;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::cli::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging()?;

    let cli = Cli::parse();

    info!("Starting wb-mm-mqtt");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mqtt_task = tokio::spawn(mqtt::run(shutdown_rx.clone()));
    let dbus_task = tokio::spawn(dbus::run(cli.dbus_address.clone(), shutdown_rx));

    tokio::signal::ctrl_c()
        .await
        .context("failed to wait for Ctrl+C signal")?;

    info!("Termination requested");

    let _ = shutdown_tx.send(true);

    let mqtt_result = mqtt_task.await.context("mqtt task join failed")?;
    let dbus_result = dbus_task.await.context("dbus task join failed")?;

    mqtt_result?;
    dbus_result?;

    info!("wb-mm-mqtt stopped");

    Ok(())
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

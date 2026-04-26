use anyhow::{Context, Result};
use tokio::sync::watch;
use tracing::info;
use zbus::{Connection, connection::Builder};

use crate::dbus::logics;

pub async fn run(
    dbus_address: Option<String>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let connection = connect(dbus_address.as_deref()).await?;

    info!("{}", logics::dbus_connected_message());

    wait_for_shutdown(&mut shutdown_rx).await?;

    drop(connection);

    info!("{}", logics::dbus_stopped_message());

    Ok(())
}

async fn connect(dbus_address: Option<&str>) -> Result<Connection> {
    match dbus_address {
        Some(address) => Builder::address(address)
            .context("failed to parse DBus address")?
            .build()
            .await
            .with_context(|| format!("failed to connect to DBus address {address}")),
        None => Connection::system()
            .await
            .context("failed to connect to system DBus"),
    }
}

async fn wait_for_shutdown(shutdown_rx: &mut watch::Receiver<bool>) -> Result<()> {
    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        shutdown_rx.changed().await?;
    }
}

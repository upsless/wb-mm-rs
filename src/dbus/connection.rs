use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::debug;
use zbus::{Connection, connection::Builder};

use crate::common::wait_for_shutdown;
use crate::domain::{DbusCommand, DbusEvent};

use super::logstrings;
use super::manager::{LoopFlow, ManagerLoopEvent};
use super::runtime::DbusRuntime;

/// Opens the system bus or the custom DBus address passed by the CLI.
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

pub(super) async fn emit_event(event_tx: &mpsc::Sender<DbusEvent>, event: DbusEvent) {
    debug!(target: logstrings::LOG_TARGET, "Sending DBus event to DISP: {event:?}");
    if event_tx.send(event).await.is_err() {
        debug!(target: logstrings::LOG_TARGET, "Event channel closed while sending");
    }
}

/// Connects to DBus, publishes the initial ModemManager state, and forwards
/// ModemManager changes until shutdown or connection loss.
pub async fn run(
    dbus_address: Option<String>,
    mut shutdown_rx: watch::Receiver<bool>,
    mut command_rx: mpsc::Receiver<DbusCommand>,
    event_tx: mpsc::Sender<DbusEvent>,
) -> Result<()> {
    // Connecting to a remote DBus bridge may block for a while, so we race the
    // connection attempt against shutdown. This lets the daemon exit cleanly
    // even if the bridge is slow or disappears mid-connect.
    let connection = tokio::select! {
        result = connect(dbus_address.as_deref()) => result?,
        result = wait_for_shutdown(&mut shutdown_rx) => {
            result?;
            debug!(target: logstrings::LOG_TARGET, "{}", logstrings::DBUS_STOPPED_BEFORE_CONNECT_MESSAGE);
            return Ok(());
        }
    };

    debug!(target: logstrings::LOG_TARGET, "{}", logstrings::DBUS_CONNECTED_MESSAGE);

    let mut runtime = DbusRuntime::new(connection, event_tx).await?;

    loop {
        let event = tokio::select! {
            // Shared shutdown path from `main`.
            result = wait_for_shutdown(&mut shutdown_rx) => {
                result?;
                Ok(ManagerLoopEvent::Shutdown)
            }
            event = runtime.read_manager_event() => event,
            maybe_command = command_rx.recv() => {
                Ok(match maybe_command {
                    Some(command) => ManagerLoopEvent::Command(command),
                    None => ManagerLoopEvent::Shutdown,
                })
            }
        }?;

        if runtime.handle_event(event).await? == LoopFlow::Stop {
            break;
        }
    }

    runtime.modem_manager.reset();

    debug!(target: logstrings::LOG_TARGET, "{}", logstrings::DBUS_STOPPED_MESSAGE);

    Ok(())
}

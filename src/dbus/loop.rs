use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::sync::watch;
use tracing::debug;
use zbus::{Connection, connection::Builder, fdo::DBusProxy, names::BusName};

use crate::dbus::logics;

/// Owns the DBus-side lifecycle:
/// - connect to the bus;
/// - inspect the current ModemManager state;
/// - subscribe to owner changes of the ModemManager DBus name;
/// - stay alive until shutdown is requested.
pub async fn run(
    dbus_address: Option<String>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    // Connecting to a remote DBus bridge may block for a while, so we race the
    // connection attempt against shutdown. This lets the daemon exit cleanly
    // even if the bridge is slow or disappears mid-connect.
    let connection = tokio::select! {
        result = connect(dbus_address.as_deref()) => result?,
        result = wait_for_shutdown(&mut shutdown_rx) => {
            result?;
            debug!("{}", logics::dbus_stopped_before_connect_message());
            return Ok(());
        }
    };

    debug!("{}", logics::dbus_connected_message());

    // The standard org.freedesktop.DBus proxy is enough for stage 0:
    // we use it both for an initial status snapshot and for the
    // NameOwnerChanged subscription of the ModemManager bus name.
    let dbus_proxy = DBusProxy::new(&connection)
        .await
        .context("failed to create org.freedesktop.DBus proxy")?;
    let mut mm_status = query_modemmanager_status(&dbus_proxy).await?;
    debug!("{}", logics::modemmanager_status_message(mm_status));
    let mut mm_status_changes = dbus_proxy
        .receive_name_owner_changed_with_args(&[(0, logics::MM_BUS_NAME)])
        .await
        .context("failed to subscribe to ModemManager DBus owner changes")?;

    loop {
        tokio::select! {
            // Shared shutdown path from `main`.
            result = wait_for_shutdown(&mut shutdown_rx) => {
                result?;
                break;
            }
            // DBus notifies us whenever ownership of the watched name changes.
            // We then re-query the derived state and log only real transitions.
            change = mm_status_changes.next() => {
                let Some(change) = change else {
                    debug!("{}", logics::dbus_name_owner_stream_closed_message());
                    break;
                };

                // Decoding the typed signal args here also validates that the
                // incoming DBus message matches what we subscribed to.
                change
                    .args()
                    .context("failed to parse ModemManager NameOwnerChanged signal")?;

                let new_status = query_modemmanager_status(&dbus_proxy).await?;
                if new_status != mm_status {
                    mm_status = new_status;
                    debug!("{}", logics::modemmanager_status_message(mm_status));
                }
            }
        }
    }

    debug!("{}", logics::dbus_stopped_message());

    Ok(())
}

/// Build a DBus connection either to the system bus or to a custom address
/// such as the remote `unixexec:` bridge we use during development.
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

/// Collapse raw DBus facts into the three states we care about at stage 0.
///
/// `Active`:
///   the well-known ModemManager bus name currently has an owner.
/// `Inactive`:
///   no owner yet, but the bus knows how to activate the service.
/// `NotFound`:
///   the name is neither owned nor activatable on this bus.
async fn query_modemmanager_status(
    dbus_proxy: &DBusProxy<'_>,
) -> Result<logics::ModemManagerStatus> {
    let mm_bus_name =
        BusName::try_from(logics::MM_BUS_NAME).context("failed to parse ModemManager bus name")?;

    if dbus_proxy
        .name_has_owner(mm_bus_name)
        .await
        .context("failed to query ModemManager DBus owner")?
    {
        Ok(logics::ModemManagerStatus::Active)
    } else {
        let activatable_names = dbus_proxy
            .list_activatable_names()
            .await
            .context("failed to query activatable DBus names")?;

        if activatable_names
            .iter()
            .any(|name| name.as_str() == logics::MM_BUS_NAME)
        {
            Ok(logics::ModemManagerStatus::Inactive)
        } else {
            Ok(logics::ModemManagerStatus::NotFound)
        }
    }
}

/// Small helper used by both MQTT and DBus loops.
///
/// The loop does two things:
/// - checks the current shutdown flag immediately;
/// - otherwise awaits the next flag change without busy-spinning.
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

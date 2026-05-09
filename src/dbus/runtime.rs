use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tracing::debug;
use zbus::{
    Connection,
    fdo::{DBusProxy, InterfacesAddedStream, InterfacesRemovedStream, NameOwnerChangedStream},
    names::BusName,
    proxy::PropertyStream,
};

use super::manager::{ManagerPresence, ManagerWatcher};
use crate::dbus::schema;
use crate::dbus::schema::LOG_TARGET;
use crate::exchange::DbusEvent;

pub(super) use super::manager::{LoopFlow, ManagerLoopEvent};

pub(super) struct DbusRuntime {
    dbus_proxy: DBusProxy<'static>,
    manager_owner_changes: NameOwnerChangedStream,
    event_tx: mpsc::Sender<DbusEvent>,
    modem_manager: ManagerWatcher,
}

impl DbusRuntime {
    pub(super) async fn new(
        connection: Connection,
        event_tx: mpsc::Sender<DbusEvent>,
    ) -> Result<Self> {
        let dbus_proxy = DBusProxy::new(&connection)
            .await
            .context("failed to create org.freedesktop.DBus proxy")?;
        let manager_owner_changes = dbus_proxy
            .receive_name_owner_changed_with_args(&[(0, schema::MM_BUS_NAME)])
            .await
            .context("failed to subscribe to ModemManager DBus owner changes")?;
        let manager_presence = query_manager_presence(&dbus_proxy).await?;
        emit_manager_presence(&event_tx, manager_presence).await;

        let mut modem_manager = ManagerWatcher::new(manager_presence);
        modem_manager.activate(&connection, &event_tx).await?;

        Ok(Self {
            dbus_proxy,
            manager_owner_changes,
            event_tx,
            modem_manager,
        })
    }

    pub(super) async fn read_manager_event(&mut self) -> Result<ManagerLoopEvent> {
        tokio::select! {
            // DBus notifies us whenever ownership of the watched name changes.
            // We then re-query the derived state and rebuild the active-only
            // state if needed.
            change = self.manager_owner_changes.next() => {
                match change {
                    Some(change) => {
                        change
                            .args()
                            .context("failed to parse ModemManager NameOwnerChanged signal")?;
                        Ok(ManagerLoopEvent::OwnerChanged)
                    }
                    None => Ok(ManagerLoopEvent::OwnerStreamClosed),
                }
            }
            // `Version` is a real ModemManager property, so we keep a dedicated
            // property stream for it while the service is active.
            version = async {
                let Some(version_changes) = self.modem_manager.streams.version_changes.as_mut() else {
                    return Ok::<Option<String>, anyhow::Error>(None);
                };
                read_version_change(version_changes).await
            }, if self.modem_manager.streams.version_changes.is_some() => {
                match version? {
                    Some(version) => Ok(ManagerLoopEvent::VersionChanged(version)),
                    None => Ok(ManagerLoopEvent::ActiveStreamClosed(schema::MM_VERSION_CHANGED_SIGNAL)),
                }
            }
            // ModemManager exports modems as ObjectManager child objects rather
            // than as a root "modem count" property. We therefore filter
            // add/remove signals by the modem interface and then re-read the
            // ObjectManager tree for the exact current count.
            added_modem = async {
                let Some(interfaces_added) = self.modem_manager.streams.interfaces_added.as_mut() else {
                    return Ok::<Option<Option<schema::ModemId>>, anyhow::Error>(None);
                };
                read_added_modem(interfaces_added).await
            }, if self.modem_manager.streams.interfaces_added.is_some() => {
                match added_modem? {
                    Some(added_modem) => Ok(ManagerLoopEvent::ModemAdded(added_modem)),
                    None => Ok(ManagerLoopEvent::ActiveStreamClosed(schema::MM_INTERFACES_ADDED_SIGNAL)),
                }
            }
            removed_modem = async {
                let Some(interfaces_removed) = self.modem_manager.streams.interfaces_removed.as_mut() else {
                    return Ok::<Option<Option<schema::ModemId>>, anyhow::Error>(None);
                };
                read_removed_modem(interfaces_removed).await
            }, if self.modem_manager.streams.interfaces_removed.is_some() => {
                match removed_modem? {
                    Some(removed_modem) => Ok(ManagerLoopEvent::ModemRemoved(removed_modem)),
                    None => Ok(ManagerLoopEvent::ActiveStreamClosed(schema::MM_INTERFACES_REMOVED_SIGNAL)),
                }
            }
        }
    }

    pub(super) async fn handle_event(&mut self, event: ManagerLoopEvent) -> Result<LoopFlow> {
        match event {
            ManagerLoopEvent::Shutdown => Ok(LoopFlow::Stop),
            ManagerLoopEvent::OwnerStreamClosed => {
                debug!(
                    target: LOG_TARGET,
                    "{}",
                    schema::dbus_signal_stream_closed_message(schema::MM_NAME_OWNER_CHANGED_SIGNAL)
                );
                Ok(LoopFlow::Stop)
            }
            ManagerLoopEvent::ActiveStreamClosed(signal_id) => {
                debug!(
                    target: LOG_TARGET,
                    "{}",
                    schema::dbus_signal_stream_closed_message(signal_id)
                );
                self.modem_manager.reset();
                Ok(LoopFlow::Continue)
            }
            ManagerLoopEvent::OwnerChanged => {
                self.handle_owner_changed().await?;
                Ok(LoopFlow::Continue)
            }
            ManagerLoopEvent::VersionChanged(version) => {
                self.modem_manager
                    .handle_version_changed(&self.event_tx, version)
                    .await;
                Ok(LoopFlow::Continue)
            }
            ManagerLoopEvent::ModemAdded(added_modem) => {
                let connection = self.connection().clone();
                let event_tx = self.event_tx.clone();
                self.modem_manager
                    .handle_modem_added(&connection, &event_tx, added_modem)
                    .await?;
                Ok(LoopFlow::Continue)
            }
            ManagerLoopEvent::ModemRemoved(removed_modem) => {
                let event_tx = self.event_tx.clone();
                self.modem_manager
                    .handle_modem_removed(&event_tx, removed_modem)
                    .await?;
                Ok(LoopFlow::Continue)
            }
            ManagerLoopEvent::Command(command) => {
                let connection = self.connection().clone();
                let event_tx = self.event_tx.clone();
                self.modem_manager
                    .handle_dbus_command(&connection, &event_tx, command)
                    .await?;
                Ok(LoopFlow::Continue)
            }
        }
    }

    pub(super) fn reset_manager(&mut self) {
        self.modem_manager.reset();
    }

    async fn handle_owner_changed(&mut self) -> Result<()> {
        let new_presence = query_manager_presence(&self.dbus_proxy).await?;
        if new_presence != self.modem_manager.presence {
            self.modem_manager.presence = new_presence;
            emit_manager_presence(&self.event_tx, self.modem_manager.presence).await;
        }

        let connection = self.connection().clone();
        let event_tx = self.event_tx.clone();
        self.modem_manager.reset();
        self.modem_manager.activate(&connection, &event_tx).await
    }

    fn connection(&self) -> &Connection {
        self.dbus_proxy.inner().connection()
    }
}

async fn read_version_change(
    version_changes: &mut PropertyStream<'static, String>,
) -> Result<Option<String>> {
    let Some(change) = version_changes.next().await else {
        return Ok(None);
    };
    let version = change
        .get()
        .await
        .context("failed to read ModemManager Version property change")?;
    Ok(Some(version))
}

async fn read_added_modem(
    interfaces_added: &mut InterfacesAddedStream,
) -> Result<Option<Option<schema::ModemId>>> {
    let Some(signal) = interfaces_added.next().await else {
        return Ok(None);
    };
    let args = signal
        .args()
        .context("failed to parse ModemManager InterfacesAdded signal")?;
    let touches_modem = args
        .interfaces_and_properties()
        .keys()
        .any(|name| name.as_str() == schema::MM_MODEM_INTERFACE);
    if !touches_modem {
        return Ok(Some(None));
    }
    Ok(Some(schema::modem_id_from_path(
        args.object_path().as_str(),
    )))
}

async fn read_removed_modem(
    interfaces_removed: &mut InterfacesRemovedStream,
) -> Result<Option<Option<schema::ModemId>>> {
    let Some(signal) = interfaces_removed.next().await else {
        return Ok(None);
    };
    let args = signal
        .args()
        .context("failed to parse ModemManager InterfacesRemoved signal")?;
    let touches_modem = args
        .interfaces()
        .iter()
        .any(|name| name.as_str() == schema::MM_MODEM_INTERFACE);
    if !touches_modem {
        return Ok(Some(None));
    }
    Ok(Some(schema::modem_id_from_path(
        args.object_path().as_str(),
    )))
}

/// Reads the ModemManager well-known name and activation metadata and returns
/// whether the manager is active, merely activatable, or gone entirely.
async fn query_manager_presence(dbus_proxy: &DBusProxy<'_>) -> Result<ManagerPresence> {
    let mm_bus_name =
        BusName::try_from(schema::MM_BUS_NAME).context("failed to parse ModemManager bus name")?;

    if dbus_proxy
        .name_has_owner(mm_bus_name)
        .await
        .context("failed to query ModemManager DBus owner")?
    {
        Ok(ManagerPresence::Present(schema::ModemManagerStatus::Active))
    } else {
        let activatable_names = dbus_proxy
            .list_activatable_names()
            .await
            .context("failed to query activatable DBus names")?;

        if activatable_names
            .iter()
            .any(|name| name.as_str() == schema::MM_BUS_NAME)
        {
            Ok(ManagerPresence::Present(
                schema::ModemManagerStatus::Inactive,
            ))
        } else {
            Ok(ManagerPresence::Absent)
        }
    }
}

async fn emit_manager_presence(event_tx: &mpsc::Sender<DbusEvent>, presence: ManagerPresence) {
    match presence {
        ManagerPresence::Present(status) => {
            tracing::info!(target: LOG_TARGET, "{}", schema::modemmanager_status_message(status));
            let update = schema::ManagerUpdate::Status(status);
            super::connection::emit_event(event_tx, DbusEvent::ManagerUpdated(update)).await;
        }
        ManagerPresence::Absent => {
            tracing::info!(target: LOG_TARGET, "{}", schema::manager_deleted_message());
            super::connection::emit_event(event_tx, DbusEvent::ManagerDeleted).await;
        }
    }
}

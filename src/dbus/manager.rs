use std::collections::HashMap;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tracing::{debug, info};
use zbus::{
    Connection, Proxy,
    fdo::{InterfacesAddedStream, InterfacesRemovedStream, ObjectManagerProxy},
    proxy::{Builder as ProxyBuilder, CacheProperties, PropertyStream},
};

use super::connection::emit_event;
use super::logstrings;
use super::modem::ModemWatcher;
use super::sms::{delete_sms, query_sms_snapshot, send_sms};
use crate::dbus::schema;
use crate::domain::{DbusCommand, DbusEvent, OutgoingSmsInfo, OutgoingSmsStatus};
use time::OffsetDateTime;

#[derive(Default)]
struct ManagerState {
    version: Option<String>,
    modem_count: Option<usize>,
    object_manager: Option<ObjectManagerProxy<'static>>,
    modems: Option<HashMap<schema::ModemId, ModemWatcher>>,
}

#[derive(Default)]
pub(super) struct ManagerStreams {
    pub(super) version_changes: Option<PropertyStream<'static, String>>,
    pub(super) interfaces_added: Option<InterfacesAddedStream>,
    pub(super) interfaces_removed: Option<InterfacesRemovedStream>,
}

impl ManagerState {
    fn activate(
        &mut self,
        version: String,
        object_manager: ObjectManagerProxy<'static>,
        modems: HashMap<schema::ModemId, ModemWatcher>,
    ) {
        self.version = Some(version);
        self.modem_count = None;
        self.object_manager = Some(object_manager);
        self.modems = Some(modems);
    }

    fn reset(&mut self) {
        self.version = None;
        self.modem_count = None;
        self.object_manager = None;
        self.clear_modems();
    }

    fn remove_modem(&mut self, modem_id: &schema::ModemId) {
        if let Some(modems) = self.modems.as_mut()
            && let Some(mut modem) = modems.remove(modem_id)
        {
            modem.abort();
        }
    }

    fn clear_modems(&mut self) {
        if let Some(modems) = self.modems.take() {
            for (_, mut modem) in modems {
                modem.abort();
            }
        }
    }
}

impl ManagerStreams {
    fn clear(&mut self) {
        *self = Self::default();
    }
}

pub(super) struct ManagerWatcher {
    pub(super) presence: ManagerPresence,
    state: ManagerState,
    pub(super) streams: ManagerStreams,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ManagerPresence {
    Present(schema::ManagerStatus),
    Absent,
}

pub(super) enum ManagerLoopEvent {
    Shutdown,
    OwnerChanged,
    OwnerStreamClosed,
    ActiveStreamClosed(schema::DbusSignalSpec),
    VersionChanged(String),
    ModemAdded(schema::ModemId),
    ModemRemoved(schema::ModemId),
    Command(DbusCommand),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LoopFlow {
    Continue,
    Stop,
}

impl ManagerWatcher {
    pub(super) fn new(presence: ManagerPresence) -> Self {
        Self {
            presence,
            state: ManagerState::default(),
            streams: ManagerStreams::default(),
        }
    }

    pub(super) fn reset(&mut self) {
        self.streams.clear();
        self.state.reset();
    }

    pub(super) async fn activate(
        &mut self,
        connection: &Connection,
        event_tx: &mpsc::Sender<DbusEvent>,
    ) -> Result<()> {
        if self.presence != ManagerPresence::Present(schema::ManagerStatus::Active) {
            return Ok(());
        }

        let mm_proxy: Proxy<'static> = ProxyBuilder::new(connection)
            .destination(schema::MM_BUS_NAME)
            .context("failed to set ModemManager proxy destination")?
            .path(schema::MM_OBJ_PATH)
            .context("failed to set ModemManager proxy path")?
            .interface(schema::MM_INTERFACE)
            .context("failed to set ModemManager proxy interface")?
            .cache_properties(CacheProperties::Yes)
            .build()
            .await
            .context("failed to create ModemManager proxy")?;

        let mut version_changes: PropertyStream<'static, String> =
            mm_proxy.receive_property_changed::<String>("Version").await;
        let version = version_changes
            .next()
            .await
            .context("ModemManager Version stream closed before the initial value was received")?
            .get()
            .await
            .context("failed to read initial ModemManager Version property")?;

        let object_manager =
            ObjectManagerProxy::new(connection, schema::MM_BUS_NAME, schema::MM_OBJ_PATH)
                .await
                .context("failed to create ModemManager ObjectManager proxy")?;
        let interfaces_added = object_manager
            .receive_interfaces_added()
            .await
            .context("failed to subscribe to ModemManager InterfacesAdded signal")?;
        let interfaces_removed = object_manager
            .receive_interfaces_removed()
            .await
            .context("failed to subscribe to ModemManager InterfacesRemoved signal")?;

        let mut modems = HashMap::new();
        for modem_id in query_modem_ids(&object_manager).await? {
            modems.insert(
                modem_id.clone(),
                ModemWatcher::new(modem_id, connection.clone(), event_tx.clone()),
            );
        }

        self.streams.version_changes = Some(version_changes);
        self.streams.interfaces_added = Some(interfaces_added);
        self.streams.interfaces_removed = Some(interfaces_removed);
        self.state.activate(version, object_manager, modems);
        self.sync_modem_count(event_tx).await
    }

    pub(super) async fn handle_version_changed(
        &mut self,
        event_tx: &mpsc::Sender<DbusEvent>,
        version: String,
    ) {
        if let Some(current_version) = self.state.version.as_mut()
            && *current_version != version
            && self.state.modem_count.is_some()
        {
            *current_version = version.clone();
            let update = schema::ManagerUpdate::Version(version);
            info!(target: logstrings::LOG_TARGET, "{}", logstrings::manager_update_message(&update));
            emit_event(event_tx, DbusEvent::ManagerUpdated(update)).await;
        }
    }

    pub(super) async fn handle_modem_added(
        &mut self,
        connection: &Connection,
        event_tx: &mpsc::Sender<DbusEvent>,
        modem_id: schema::ModemId,
    ) -> Result<()> {
        if let Some(modems) = self.state.modems.as_mut()
            && !modems.contains_key(&modem_id)
        {
            modems.insert(
                modem_id.clone(),
                ModemWatcher::new(modem_id, connection.clone(), event_tx.clone()),
            );
        }

        self.sync_modem_count(event_tx).await?;

        Ok(())
    }

    pub(super) async fn handle_modem_removed(
        &mut self,
        event_tx: &mpsc::Sender<DbusEvent>,
        modem_id: schema::ModemId,
    ) -> Result<()> {
        self.state.remove_modem(&modem_id);
        info!(target: logstrings::LOG_TARGET, "{}", logstrings::modem_deleted_message(&modem_id));
        emit_event(event_tx, DbusEvent::ModemDeleted { modem_id }).await;
        self.sync_modem_count(event_tx).await?;

        Ok(())
    }

    pub(super) async fn handle_dbus_command(
        &mut self,
        connection: &Connection,
        event_tx: &mpsc::Sender<DbusEvent>,
        command: DbusCommand,
    ) -> Result<()> {
        match command {
            DbusCommand::RefreshSms { modem_id, sms_id } => {
                if let Some(snapshot) = query_sms_snapshot(connection, &sms_id).await? {
                    emit_event(
                        event_tx,
                        DbusEvent::SmsSnapshot {
                            modem_id: modem_id.clone(),
                            snapshot,
                        },
                    )
                    .await;
                    if let Some(modem) = self
                        .state
                        .modems
                        .as_ref()
                        .and_then(|modems| modems.get(&modem_id))
                    {
                        modem.track_sms(sms_id.clone()).await;
                    }
                }
            }
            DbusCommand::DeleteSms { modem_id, sms_id } => {
                delete_sms(connection, &modem_id, &sms_id).await?;
            }
            DbusCommand::SendSms {
                modem_id,
                recipient,
                text,
            } => {
                let sending_info = OutgoingSmsInfo {
                    recipient: recipient.clone(),
                    text: text.clone(),
                    timestamp: None,
                    status: OutgoingSmsStatus::Sending,
                    error: None,
                };
                info!(
                    target: logstrings::LOG_TARGET,
                    "{}",
                    logstrings::outgoing_sms_update_message(&modem_id, &sending_info)
                );
                emit_event(
                    event_tx,
                    DbusEvent::OutgoingSmsUpdated {
                        modem_id: modem_id.clone(),
                        info: sending_info,
                    },
                )
                .await;

                match send_sms(connection, &modem_id, &recipient, &text).await {
                    Ok(()) => {
                        let sent_info = OutgoingSmsInfo {
                            recipient,
                            text,
                            timestamp: Some(OffsetDateTime::now_utc()),
                            status: OutgoingSmsStatus::Sent,
                            error: None,
                        };
                        info!(
                            target: logstrings::LOG_TARGET,
                            "{}",
                            logstrings::outgoing_sms_update_message(&modem_id, &sent_info)
                        );
                        emit_event(
                            event_tx,
                            DbusEvent::OutgoingSmsUpdated {
                                modem_id,
                                info: sent_info,
                            },
                        )
                        .await;
                    }
                    Err(err) => {
                        let failed_info = OutgoingSmsInfo {
                            recipient,
                            text,
                            timestamp: None,
                            status: OutgoingSmsStatus::Failed,
                            error: Some(outgoing_sms_error_text(&err)),
                        };
                        info!(
                            target: logstrings::LOG_TARGET,
                            "{}",
                            logstrings::outgoing_sms_update_message(&modem_id, &failed_info)
                        );
                        emit_event(
                            event_tx,
                            DbusEvent::OutgoingSmsUpdated {
                                modem_id: modem_id.clone(),
                                info: failed_info,
                            },
                        )
                        .await;
                        debug!(
                            target: logstrings::LOG_TARGET,
                            "Failed to send SMS through modem {}: {err:#}",
                            modem_id.0
                        );
                    }
                }
            }
        }

        Ok(())
    }

    async fn sync_modem_count(&mut self, event_tx: &mpsc::Sender<DbusEvent>) -> Result<()> {
        let Some(version) = self.state.version.clone() else {
            return Ok(());
        };
        let Some(object_manager) = self.state.object_manager.as_ref() else {
            return Ok(());
        };

        let modem_count = query_modem_count(object_manager).await?;
        match self.state.modem_count {
            None => {
                self.state.modem_count = Some(modem_count);
                info!(target: logstrings::LOG_TARGET, "{}", logstrings::manager_found_message(&version, modem_count));
                emit_event(
                    event_tx,
                    DbusEvent::ManagerFound {
                        version,
                        modem_count,
                    },
                )
                .await;
            }
            Some(current_count) if current_count != modem_count => {
                self.state.modem_count = Some(modem_count);
                let update = schema::ManagerUpdate::ModemCount(modem_count);
                info!(target: logstrings::LOG_TARGET, "{}", logstrings::manager_update_message(&update));
                emit_event(event_tx, DbusEvent::ManagerUpdated(update)).await;
            }
            Some(_) => {}
        }

        Ok(())
    }
}

fn outgoing_sms_error_text(err: &anyhow::Error) -> String {
    let message = err
        .chain()
        .last()
        .map(ToString::to_string)
        .unwrap_or_else(|| err.to_string());

    message
        .rsplit_once(": ")
        .map(|(_, reason)| reason.to_string())
        .unwrap_or(message)
}

/// Counts objects that implement the ModemManager Modem interface.
async fn query_modem_count(object_manager: &ObjectManagerProxy<'_>) -> Result<usize> {
    let managed_objects = object_manager
        .get_managed_objects()
        .await
        .context("failed to read ModemManager managed objects")?;

    Ok(managed_objects
        .values()
        .filter(|interfaces| {
            interfaces
                .keys()
                .any(|name| name.as_str() == schema::MM_MODEM_INTERFACE)
        })
        .count())
}

async fn query_modem_ids(object_manager: &ObjectManagerProxy<'_>) -> Result<Vec<schema::ModemId>> {
    let managed_objects = object_manager
        .get_managed_objects()
        .await
        .context("failed to read ModemManager managed objects")?;

    let mut modem_ids: Vec<_> = managed_objects
        .iter()
        .filter(|(_, interfaces)| {
            interfaces
                .keys()
                .any(|name| name.as_str() == schema::MM_MODEM_INTERFACE)
        })
        .filter_map(|(path, _)| schema::modem_id_from_path(path.as_str()))
        .collect();

    // DBus managed objects arrive as a map, so the raw iteration order is not
    // guaranteed. Sorting the discovered modem ids keeps the user-facing MQTT
    // numbering more stable after reconnects and cold starts.
    modem_ids.sort_by(
        |left, right| match (left.0.parse::<u32>(), right.0.parse::<u32>()) {
            (Ok(left), Ok(right)) => left.cmp(&right),
            _ => left.0.cmp(&right.0),
        },
    );

    Ok(modem_ids)
}

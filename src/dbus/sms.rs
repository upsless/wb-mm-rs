use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use time::{OffsetDateTime, format_description::well_known::Iso8601};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info};
use zbus::{
    Connection, Proxy,
    proxy::{Builder as ProxyBuilder, CacheProperties},
    zvariant::{ObjectPath, OwnedObjectPath},
};

use super::connection::emit_event;
use super::logstrings;
use crate::dbus::schema;
use crate::domain::{DbusEvent, SmsInventoryEntry};

const SMS_STATE_CHANGED_SIGNAL_ID: &str = "mm_sms_state_changed";
const SMS_TEXT_CHANGED_SIGNAL_ID: &str = "mm_sms_text_changed";
const SMS_TIMESTAMP_CHANGED_SIGNAL_ID: &str = "mm_sms_timestamp_changed";
const SMS_NUMBER_CHANGED_SIGNAL_ID: &str = "mm_sms_number_changed";
const SMS_STORAGE_CHANGED_SIGNAL_ID: &str = "mm_sms_storage_changed";

#[derive(Debug, Clone, PartialEq, Eq)]
enum SmsInventoryCommand {
    TrackSms { sms_id: schema::SmsId },
}

pub(super) struct SmsInventoryWatcher {
    command_tx: mpsc::Sender<SmsInventoryCommand>,
    task: Option<JoinHandle<()>>,
}

impl SmsInventoryWatcher {
    pub(super) fn new(
        connection: &Connection,
        modem_id: schema::ModemId,
        event_tx: mpsc::Sender<DbusEvent>,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::channel(16);
        let task = Some(spawn_modem_sms_task(
            connection, modem_id, event_tx, command_rx,
        ));

        Self { command_tx, task }
    }

    pub(super) async fn track_sms(&self, sms_id: schema::SmsId) {
        let _ = self
            .command_tx
            .send(SmsInventoryCommand::TrackSms { sms_id })
            .await;
    }

    pub(super) fn abort(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl Drop for SmsInventoryWatcher {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn spawn_modem_sms_task(
    connection: &Connection,
    modem_id: schema::ModemId,
    event_tx: mpsc::Sender<DbusEvent>,
    command_rx: mpsc::Receiver<SmsInventoryCommand>,
) -> JoinHandle<()> {
    let connection = connection.clone();
    tokio::spawn(async move {
        let worker = SmsInventoryWorker {
            connection,
            modem_id: modem_id.clone(),
            event_tx,
            command_rx,
        };
        if let Err(err) = worker.run().await {
            debug!(
                target: logstrings::LOG_TARGET,
                "Modem {} SMS inventory watcher failed: {err:#}",
                modem_id.0
            );
        }
    })
}

struct SmsInventoryWorker {
    connection: Connection,
    modem_id: schema::ModemId,
    event_tx: mpsc::Sender<DbusEvent>,
    command_rx: mpsc::Receiver<SmsInventoryCommand>,
}

impl SmsInventoryWorker {
    async fn run(mut self) -> Result<()> {
        let modem_path = schema::modem_path_from_id(&self.modem_id);
        let messaging_proxy: Proxy<'_> = ProxyBuilder::new(&self.connection)
            .destination(schema::MM_BUS_NAME)
            .context("failed to set modem messaging proxy destination")?
            .path(modem_path.as_str())
            .context("failed to set modem messaging proxy path")?
            .interface(schema::MM_MODEM_MESSAGING_INTERFACE)
            .context("failed to set modem messaging proxy interface")?
            .cache_properties(CacheProperties::Yes)
            .build()
            .await
            .with_context(|| {
                format!(
                    "failed to create modem messaging proxy for {}",
                    self.modem_id.0
                )
            })?;

        // Subscribe before the initial read so SMS added immediately after
        // ENABLED is not lost between snapshot and live mode.
        let mut messages_changes = messaging_proxy
            .receive_property_changed::<Vec<OwnedObjectPath>>("Messages")
            .await;

        let (inventory_cache, entries) = self.query_inventory_cache(&messaging_proxy).await?;
        info!(
            target: logstrings::LOG_TARGET,
            "{}",
            logstrings::sms_inventory_snapshot_message(
                &self.modem_id,
                entries.len(),
                None,
            )
        );
        emit_event(
            &self.event_tx,
            DbusEvent::SmsInventorySnapshot {
                modem_id: self.modem_id.clone(),
                entries,
            },
        )
        .await;

        let mut inventory_cache = inventory_cache;
        let mut sms_watcher = SmsWatcher::default();

        loop {
            tokio::select! {
                maybe_command = self.command_rx.recv() => {
                    let Some(command) = maybe_command else {
                        break;
                    };
                    match command {
                        SmsInventoryCommand::TrackSms { sms_id } => {
                            let sms_id = inventory_cache.contains_key(&sms_id).then_some(&sms_id);
                            sms_watcher
                                .retarget(
                                    &self.connection,
                                    &self.modem_id,
                                    &self.event_tx,
                                    sms_id,
                                )
                                .await?;
                        }
                    }
                }
                change = messages_changes.next() => {
                    let Some(change) = change else {
                        break;
                    };
                    let message_paths = change
                        .get()
                        .await
                        .context("failed to read modem Messages property change")?;
                    let entries = self
                        .update_inventory_cache_from_paths(&message_paths, &mut inventory_cache)
                        .await?;
                    sms_watcher
                        .sync_inventory(&self.event_tx, &self.modem_id, entries)
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn query_inventory_cache(
        &self,
        messaging_proxy: &Proxy<'_>,
    ) -> Result<(
        HashMap<schema::SmsId, SmsInventoryEntry>,
        Vec<SmsInventoryEntry>,
    )> {
        let message_paths: Vec<OwnedObjectPath> = messaging_proxy
            .get_property("Messages")
            .await
            .context("failed to read modem Messages property")?;

        let mut inventory_cache = HashMap::new();
        let entries = self
            .update_inventory_cache_from_paths(&message_paths, &mut inventory_cache)
            .await?;
        Ok((inventory_cache, entries))
    }

    async fn update_inventory_cache_from_paths(
        &self,
        message_paths: &[OwnedObjectPath],
        inventory_cache: &mut HashMap<schema::SmsId, SmsInventoryEntry>,
    ) -> Result<Vec<SmsInventoryEntry>> {
        let sms_ids: Vec<_> = message_paths
            .iter()
            .filter_map(|path| schema::sms_id_from_path(path.as_str()))
            .collect();

        let current_sms_ids: HashSet<_> = sms_ids.iter().cloned().collect();
        inventory_cache.retain(|sms_id, _| current_sms_ids.contains(sms_id));

        for sms_id in &sms_ids {
            if inventory_cache.contains_key(sms_id) {
                continue;
            }

            inventory_cache.insert(
                sms_id.clone(),
                SmsInventoryEntry {
                    sms_id: sms_id.clone(),
                    timestamp: query_sms_timestamp(&self.connection, sms_id).await?,
                },
            );
        }

        Ok(inventory_entries(inventory_cache, Some(&sms_ids)))
    }
}

fn inventory_entries(
    inventory_cache: &HashMap<schema::SmsId, SmsInventoryEntry>,
    sms_ids: Option<&[schema::SmsId]>,
) -> Vec<SmsInventoryEntry> {
    sms_ids
        .into_iter()
        .flatten()
        .filter_map(|sms_id| inventory_cache.get(sms_id).cloned())
        .collect()
}

#[derive(Default)]
struct SmsWatcher {
    sms_id: Option<schema::SmsId>,
    task: Option<JoinHandle<()>>,
}

impl Drop for SmsWatcher {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl SmsWatcher {
    async fn retarget(
        &mut self,
        connection: &Connection,
        modem_id: &schema::ModemId,
        event_tx: &mpsc::Sender<DbusEvent>,
        sms_id: Option<&schema::SmsId>,
    ) -> Result<()> {
        if self.sms_id.as_ref() == sms_id {
            return Ok(());
        }

        self.clear();

        let Some(sms_id) = sms_id else {
            return Ok(());
        };

        if let Some(task) = spawn_sms_watcher(
            connection,
            modem_id.clone(),
            sms_id.clone(),
            event_tx.clone(),
        )
        .await?
        {
            self.sms_id = Some(sms_id.clone());
            self.task = Some(task);
        }

        Ok(())
    }

    async fn sync_inventory(
        &mut self,
        event_tx: &mpsc::Sender<DbusEvent>,
        modem_id: &schema::ModemId,
        current_entries: Vec<SmsInventoryEntry>,
    ) -> Result<()> {
        emit_event(
            event_tx,
            DbusEvent::SmsListChanged {
                modem_id: modem_id.clone(),
                entries: current_entries.clone(),
            },
        )
        .await;

        let current_sms_ids: HashSet<_> = current_entries
            .into_iter()
            .map(|entry| entry.sms_id)
            .collect();
        let Some(sms_id) = self.sms_id.clone() else {
            return Ok(());
        };
        if current_sms_ids.contains(&sms_id) {
            return Ok(());
        }

        self.clear();
        info!(
            target: logstrings::LOG_TARGET,
            "{}",
            logstrings::sms_deleted_message(modem_id, &sms_id)
        );
        emit_event(
            event_tx,
            DbusEvent::SmsDeleted {
                modem_id: modem_id.clone(),
                sms_id,
            },
        )
        .await;

        Ok(())
    }

    fn clear(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        self.sms_id = None;
    }
}

async fn spawn_sms_watcher(
    connection: &Connection,
    modem_id: schema::ModemId,
    sms_id: schema::SmsId,
    event_tx: mpsc::Sender<DbusEvent>,
) -> Result<Option<JoinHandle<()>>> {
    let connection = connection.clone();
    let Some(snapshot) = query_sms_snapshot(&connection, &sms_id).await? else {
        return Ok(None);
    };

    Ok(Some(tokio::spawn(async move {
        let worker = SmsWorker {
            connection,
            modem_id: modem_id.clone(),
            sms_id: sms_id.clone(),
            snapshot,
            event_tx,
        };
        if let Err(err) = worker.run().await {
            debug!(
                target: logstrings::LOG_TARGET,
                "Modem {} SMS {} watcher failed: {err:#}",
                modem_id.0,
                sms_id.0
            );
        }
    })))
}

struct SmsWorker {
    connection: Connection,
    modem_id: schema::ModemId,
    sms_id: schema::SmsId,
    event_tx: mpsc::Sender<DbusEvent>,
    snapshot: schema::SmsSnapshot,
}

impl SmsWorker {
    async fn run(self) -> Result<()> {
        let Self {
            connection,
            modem_id,
            sms_id,
            event_tx,
            mut snapshot,
        } = self;

        let sms_path = schema::sms_path_from_id(&sms_id);
        let sms_proxy: Proxy<'_> = ProxyBuilder::new(&connection)
            .destination(schema::MM_BUS_NAME)
            .context("failed to set SMS proxy destination")?
            .path(sms_path.as_str())
            .context("failed to set SMS proxy path")?
            .interface(schema::MM_SMS_INTERFACE)
            .context("failed to set SMS proxy interface")?
            .cache_properties(CacheProperties::Yes)
            .build()
            .await
            .with_context(|| format!("failed to create SMS proxy for {}", sms_id.0))?;

        let mut state_changes = sms_proxy.receive_property_changed::<u32>("State").await;
        let mut storage_changes = sms_proxy.receive_property_changed::<u32>("Storage").await;
        let mut timestamp_changes = sms_proxy
            .receive_property_changed::<String>("Timestamp")
            .await;
        let mut number_changes = sms_proxy.receive_property_changed::<String>("Number").await;
        let mut text_changes = sms_proxy.receive_property_changed::<String>("Text").await;

        loop {
            tokio::select! {
                change = state_changes.next() => {
                    let Some(change) = change else {
                        debug!(target: logstrings::LOG_TARGET, "{}", logstrings::sms_signal_stream_closed_message(SMS_STATE_CHANGED_SIGNAL_ID, &sms_path));
                        break;
                    };
                    let is_received = sms_is_received(
                        change
                            .get()
                            .await
                            .context("failed to read SMS State property change")?,
                    );
                    if snapshot.is_received != is_received {
                        snapshot.is_received = is_received;
                        emit_sms_property_change(
                            &event_tx,
                            &modem_id,
                            &sms_id,
                            schema::SmsPropertyChange::IsReceived(is_received),
                        )
                        .await;
                    }
                }
                change = storage_changes.next() => {
                    let Some(change) = change else {
                        debug!(target: logstrings::LOG_TARGET, "{}", logstrings::sms_signal_stream_closed_message(SMS_STORAGE_CHANGED_SIGNAL_ID, &sms_path));
                        break;
                    };
                    let storage = sms_storage_name(
                        change
                            .get()
                            .await
                            .context("failed to read SMS Storage property change")?,
                    )
                    .to_string();
                    if snapshot.storage != storage {
                        snapshot.storage = storage.clone();
                        emit_sms_property_change(
                            &event_tx,
                            &modem_id,
                            &sms_id,
                            schema::SmsPropertyChange::Storage(storage),
                        )
                        .await;
                    }
                }
                change = timestamp_changes.next() => {
                    let Some(change) = change else {
                        debug!(target: logstrings::LOG_TARGET, "{}", logstrings::sms_signal_stream_closed_message(SMS_TIMESTAMP_CHANGED_SIGNAL_ID, &sms_path));
                        break;
                    };
                    let timestamp = parse_sms_timestamp(
                        &change
                            .get()
                            .await
                            .context("failed to read SMS Timestamp property change")?,
                    );
                    if snapshot.timestamp != timestamp {
                        snapshot.timestamp = timestamp;
                        emit_sms_property_change(
                            &event_tx,
                            &modem_id,
                            &sms_id,
                            schema::SmsPropertyChange::Timestamp(timestamp),
                        )
                        .await;
                    }
                }
                change = number_changes.next() => {
                    let Some(change) = change else {
                        debug!(target: logstrings::LOG_TARGET, "{}", logstrings::sms_signal_stream_closed_message(SMS_NUMBER_CHANGED_SIGNAL_ID, &sms_path));
                        break;
                    };
                    let number = normalize_string(
                        change
                            .get()
                            .await
                            .context("failed to read SMS Number property change")?,
                    );
                    if snapshot.number != number {
                        snapshot.number = number.clone();
                        emit_sms_property_change(
                            &event_tx,
                            &modem_id,
                            &sms_id,
                            schema::SmsPropertyChange::Number(number),
                        )
                        .await;
                    }
                }
                change = text_changes.next() => {
                    let Some(change) = change else {
                        debug!(target: logstrings::LOG_TARGET, "{}", logstrings::sms_signal_stream_closed_message(SMS_TEXT_CHANGED_SIGNAL_ID, &sms_path));
                        break;
                    };
                    let text = normalize_string(
                        change
                            .get()
                            .await
                            .context("failed to read SMS Text property change")?,
                    );
                    if snapshot.text != text {
                        snapshot.text = text.clone();
                        emit_sms_property_change(
                            &event_tx,
                            &modem_id,
                            &sms_id,
                            schema::SmsPropertyChange::Text(text),
                        )
                        .await;
                    }
                }
            }
        }

        Ok(())
    }
}

async fn emit_sms_property_change(
    event_tx: &mpsc::Sender<DbusEvent>,
    modem_id: &schema::ModemId,
    sms_id: &schema::SmsId,
    property: schema::SmsPropertyChange,
) {
    let update = schema::SmsUpdate {
        sms_id: sms_id.clone(),
        property,
    };
    info!(
        target: logstrings::LOG_TARGET,
        "{}",
        logstrings::sms_property_changed_message(modem_id, &update)
    );
    emit_event(
        event_tx,
        DbusEvent::SmsPropertyChanged {
            modem_id: modem_id.clone(),
            update,
        },
    )
    .await;
}

pub(super) async fn query_sms_snapshot(
    connection: &Connection,
    sms_id: &schema::SmsId,
) -> Result<Option<schema::SmsSnapshot>> {
    let sms_path = schema::sms_path_from_id(sms_id);
    let sms_proxy: Proxy<'_> = ProxyBuilder::new(connection)
        .destination(schema::MM_BUS_NAME)
        .context("failed to set SMS proxy destination")?
        .path(sms_path.as_str())
        .context("failed to set SMS proxy path")?
        .interface(schema::MM_SMS_INTERFACE)
        .context("failed to set SMS proxy interface")?
        .cache_properties(CacheProperties::Yes)
        .build()
        .await
        .with_context(|| format!("failed to create SMS proxy for {}", sms_id.0))?;

    let pdu_type: u32 = sms_proxy
        .get_property("PduType")
        .await
        .context("failed to read SMS PduType property")?;
    if !is_incoming_sms_pdu(pdu_type) {
        return Ok(None);
    }

    let state: u32 = sms_proxy
        .get_property("State")
        .await
        .context("failed to read SMS State property")?;
    let storage: u32 = sms_proxy
        .get_property("Storage")
        .await
        .context("failed to read SMS Storage property")?;
    let timestamp: String = sms_proxy
        .get_property("Timestamp")
        .await
        .context("failed to read SMS Timestamp property")?;
    let number: String = sms_proxy
        .get_property("Number")
        .await
        .context("failed to read SMS Number property")?;
    let text: String = sms_proxy
        .get_property("Text")
        .await
        .context("failed to read SMS Text property")?;

    Ok(Some(schema::SmsSnapshot {
        sms_id: sms_id.clone(),
        is_received: sms_is_received(state),
        storage: sms_storage_name(storage).to_string(),
        timestamp: parse_sms_timestamp(&timestamp),
        number: normalize_string(number),
        text: normalize_string(text),
    }))
}

async fn query_sms_timestamp(
    connection: &Connection,
    sms_id: &schema::SmsId,
) -> Result<Option<OffsetDateTime>> {
    let sms_path = schema::sms_path_from_id(sms_id);
    let sms_proxy: Proxy<'_> = ProxyBuilder::new(connection)
        .destination(schema::MM_BUS_NAME)
        .context("failed to set SMS proxy destination")?
        .path(sms_path.as_str())
        .context("failed to set SMS proxy path")?
        .interface(schema::MM_SMS_INTERFACE)
        .context("failed to set SMS proxy interface")?
        .cache_properties(CacheProperties::No)
        .build()
        .await
        .with_context(|| format!("failed to create SMS proxy for {}", sms_id.0))?;

    let timestamp: String = sms_proxy
        .get_property("Timestamp")
        .await
        .with_context(|| format!("failed to read SMS {} Timestamp property", sms_id.0))?;

    Ok(parse_sms_timestamp(&timestamp))
}

pub(super) async fn delete_sms(
    connection: &Connection,
    modem_id: &schema::ModemId,
    sms_id: &schema::SmsId,
) -> Result<()> {
    let modem_path = schema::modem_path_from_id(modem_id);
    let messaging_proxy: Proxy<'_> = ProxyBuilder::new(connection)
        .destination(schema::MM_BUS_NAME)
        .context("failed to set modem messaging proxy destination")?
        .path(modem_path.as_str())
        .context("failed to set modem messaging proxy path")?
        .interface(schema::MM_MODEM_MESSAGING_INTERFACE)
        .context("failed to set modem messaging proxy interface")?
        .cache_properties(CacheProperties::No)
        .build()
        .await
        .with_context(|| format!("failed to create modem messaging proxy for {}", modem_id.0))?;

    let sms_path = schema::sms_path_from_id(sms_id);
    let sms_path = ObjectPath::try_from(sms_path.as_str())
        .with_context(|| format!("failed to build SMS object path for {}", sms_id.0))?;
    messaging_proxy
        .call_method("Delete", &(sms_path,))
        .await
        .with_context(|| {
            format!(
                "failed to delete SMS {} from modem {}",
                sms_id.0, modem_id.0
            )
        })?;

    Ok(())
}

fn normalize_string(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn is_incoming_sms_pdu(pdu_type: u32) -> bool {
    matches!(pdu_type, 1 | 32)
}

fn sms_is_received(state: u32) -> bool {
    state == 3
}

fn sms_storage_name(storage: u32) -> &'static str {
    match storage {
        0 => "unknown",
        1 => "SIM",
        2 => "Mobile",
        3 => "SIM + Mobile",
        4 => "Status",
        5 => "Broadcast",
        6 => "Terminal",
        _ => "unknown",
    }
}

fn parse_sms_timestamp(timestamp: &str) -> Option<OffsetDateTime> {
    let trimmed = timestamp.trim();
    if trimmed.is_empty() {
        return None;
    }

    OffsetDateTime::parse(trimmed, &Iso8601::DEFAULT).ok()
}

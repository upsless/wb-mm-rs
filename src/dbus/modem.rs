use std::collections::HashSet;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info};
use zbus::{
    Connection, Proxy,
    proxy::{Builder as ProxyBuilder, CacheProperties, PropertyStream},
    zvariant::{ObjectPath, OwnedObjectPath},
};

use super::connection::emit_event;
use crate::dbus::schema;
use crate::dbus::schema::LOG_TARGET;
use crate::exchange::DbusEvent;

pub(super) struct ModemWatcher {
    command_tx: mpsc::Sender<ModemWatcherCommand>,
    task: Option<JoinHandle<()>>,
}

impl ModemWatcher {
    pub(super) fn new(
        id: schema::ModemId,
        connection: Connection,
        event_tx: mpsc::Sender<DbusEvent>,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::channel(16);
        let worker = ModemWatcherWorker {
            id: id.clone(),
            connection: connection.clone(),
            event_tx: event_tx.clone(),
            command_rx,
        };
        let modem_id = id.clone();
        let task = Some(tokio::spawn(async move {
            if let Err(err) = worker.run().await {
                debug!(target: LOG_TARGET, "Modem {} watcher failed: {err:#}", modem_id.0);
            }
        }));

        Self { command_tx, task }
    }

    pub(super) async fn track_sms(&self, sms_id: schema::SmsId) {
        let _ = self
            .command_tx
            .send(ModemWatcherCommand::TrackSms { sms_id })
            .await;
    }

    pub(super) fn abort(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl Drop for ModemWatcher {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct ModemWatcherWorker {
    id: schema::ModemId,
    connection: Connection,
    event_tx: mpsc::Sender<DbusEvent>,
    command_rx: mpsc::Receiver<ModemWatcherCommand>,
}

impl ModemWatcherWorker {
    async fn run(mut self) -> Result<()> {
        let modem_path = schema::modem_path_from_id(&self.id);
        let modem_proxy: Proxy<'_> = ProxyBuilder::new(&self.connection)
            .destination(schema::MM_BUS_NAME)
            .context("failed to set modem proxy destination")?
            .path(modem_path.as_str())
            .context("failed to set modem proxy path")?
            .interface(schema::MM_MODEM_INTERFACE)
            .context("failed to set modem proxy interface")?
            .cache_properties(CacheProperties::Yes)
            .build()
            .await
            .with_context(|| format!("failed to create modem proxy for {}", self.id.0))?;

        let mut streams = ModemStreams::new(&modem_proxy).await;
        let mut state =
            ModemState::new(query_modem_snapshot(&self.connection, &modem_proxy).await?);

        info!(
            target: LOG_TARGET,
            "Modem {} data: {}",
            self.id.0,
            state.info.summary(),
        );
        emit_event(
            &self.event_tx,
            DbusEvent::ModemFound {
                modem_id: self.id.clone(),
                info: state.info.clone(),
            },
        )
        .await;

        if schema::modem_state_is_active(state.raw_state) {
            state
                .start_sms_inventory(&self.connection, &self.id, &self.event_tx)
                .await;
        }

        loop {
            tokio::select! {
                maybe_command = self.command_rx.recv() => {
                    let Some(command) = maybe_command else {
                        break;
                    };
                    state.handle_command(command).await;
                }
                event = streams.read_event() => {
                    let Some(event) = event? else {
                        break;
                    };
                    state
                        .handle_event(
                            &self.connection,
                            &self.id,
                            &self.event_tx,
                            event,
                        )
                        .await?;
                }
            }
        }

        if let Some(task) = state.sms_inventory_task.take() {
            task.abort();
        }
        state.sms_inventory_command_tx = None;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ModemWatcherCommand {
    TrackSms { sms_id: schema::SmsId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ModemSmsCommand {
    TrackSms { sms_id: schema::SmsId },
}

struct ModemState {
    raw_state: i32,
    info: schema::ModemInfo,
    sms_inventory_task: Option<JoinHandle<()>>,
    sms_inventory_command_tx: Option<mpsc::Sender<ModemSmsCommand>>,
}

impl ModemState {
    fn new(queried: QueriedModemState) -> Self {
        Self {
            raw_state: queried.raw_state,
            info: queried.info,
            sms_inventory_task: None,
            sms_inventory_command_tx: None,
        }
    }

    async fn start_sms_inventory(
        &mut self,
        connection: &Connection,
        modem_id: &schema::ModemId,
        event_tx: &mpsc::Sender<DbusEvent>,
    ) {
        if self.sms_inventory_task.is_some() {
            return;
        }

        let (command_tx, command_rx) = mpsc::channel(16);
        self.sms_inventory_command_tx = Some(command_tx.clone());
        self.sms_inventory_task = Some(spawn_modem_sms_task(
            connection,
            modem_id.clone(),
            event_tx.clone(),
            command_rx,
        ));
    }

    async fn stop_sms_inventory(
        &mut self,
        modem_id: &schema::ModemId,
        event_tx: &mpsc::Sender<DbusEvent>,
    ) {
        if let Some(task) = self.sms_inventory_task.take() {
            self.sms_inventory_command_tx = None;
            task.abort();
            emit_event(
                event_tx,
                DbusEvent::SmsInventorySnapshot {
                    modem_id: modem_id.clone(),
                    sms_ids: Vec::new(),
                    initial_sms_snapshot: None,
                },
            )
            .await;
        }
    }

    async fn handle_command(&self, command: ModemWatcherCommand) {
        match command {
            ModemWatcherCommand::TrackSms { sms_id } => {
                if let Some(command_tx) = self.sms_inventory_command_tx.as_ref() {
                    let _ = command_tx.send(ModemSmsCommand::TrackSms { sms_id }).await;
                }
            }
        }
    }

    async fn handle_event(
        &mut self,
        connection: &Connection,
        modem_id: &schema::ModemId,
        event_tx: &mpsc::Sender<DbusEvent>,
        event: ModemEvent,
    ) -> Result<()> {
        match event {
            ModemEvent::Model(model) => {
                if self.info.model.as_deref() != Some(model.as_str()) {
                    self.info.model = Some(model.clone());
                    emit_modem_update(event_tx, modem_id, schema::ModemUpdate::Model(model)).await;
                }
            }
            ModemEvent::Revision(revision) => {
                if self.info.revision.as_deref() != Some(revision.as_str()) {
                    self.info.revision = Some(revision.clone());
                    emit_modem_update(event_tx, modem_id, schema::ModemUpdate::Revision(revision))
                        .await;
                }
            }
            ModemEvent::PrimarySimSlot(primary_sim_slot) => {
                if self.info.primary_sim_slot != Some(primary_sim_slot) {
                    self.info.primary_sim_slot = Some(primary_sim_slot);
                    emit_modem_update(
                        event_tx,
                        modem_id,
                        schema::ModemUpdate::PrimarySimSlot(primary_sim_slot),
                    )
                    .await;
                }
            }
            ModemEvent::State(raw_state) => {
                self.raw_state = raw_state;
                let state = Some(schema::modem_state_name(self.raw_state).to_string());
                if self.info.state != state {
                    self.info.state = state.clone();
                    emit_modem_update(event_tx, modem_id, schema::ModemUpdate::State(state)).await;
                }

                let is_active = schema::modem_state_is_active(self.raw_state);
                if self.info.is_active != is_active {
                    self.info.is_active = is_active;
                    emit_modem_update(event_tx, modem_id, schema::ModemUpdate::IsActive(is_active))
                        .await;
                }

                if is_active {
                    self.start_sms_inventory(connection, modem_id, event_tx)
                        .await;
                } else {
                    self.stop_sms_inventory(modem_id, event_tx).await;
                }
            }
            ModemEvent::SignalQuality(signal_quality) => {
                let signal_quality = Some(signal_quality);
                if self.info.signal_quality != signal_quality {
                    self.info.signal_quality = signal_quality;
                    emit_modem_update(
                        event_tx,
                        modem_id,
                        schema::ModemUpdate::SignalQuality(signal_quality),
                    )
                    .await;
                }
            }
            ModemEvent::OwnNumbers(own_numbers) => {
                if self.info.own_numbers != own_numbers {
                    self.info.own_numbers = own_numbers.clone();
                    emit_modem_update(
                        event_tx,
                        modem_id,
                        schema::ModemUpdate::OwnNumbers(own_numbers),
                    )
                    .await;
                }
            }
            ModemEvent::Sim(sim_path) => {
                let operator_name = query_operator_name(connection, sim_path).await?;
                if self.info.operator_name != operator_name {
                    self.info.operator_name = operator_name.clone();
                    emit_modem_update(
                        event_tx,
                        modem_id,
                        schema::ModemUpdate::OperatorName(operator_name),
                    )
                    .await;
                }
            }
        }

        Ok(())
    }
}

struct ModemStreams<'a> {
    model_changes: PropertyStream<'a, String>,
    revision_changes: PropertyStream<'a, String>,
    primary_sim_slot_changes: PropertyStream<'a, u32>,
    state_changes: PropertyStream<'a, i32>,
    signal_quality_changes: PropertyStream<'a, (u32, bool)>,
    own_numbers_changes: PropertyStream<'a, Vec<String>>,
    sim_changes: PropertyStream<'a, OwnedObjectPath>,
}

impl<'a> ModemStreams<'a> {
    async fn new(modem_proxy: &Proxy<'a>) -> Self {
        Self {
            model_changes: modem_proxy
                .receive_property_changed::<String>("Model")
                .await,
            revision_changes: modem_proxy
                .receive_property_changed::<String>("Revision")
                .await,
            primary_sim_slot_changes: modem_proxy
                .receive_property_changed::<u32>("PrimarySimSlot")
                .await,
            state_changes: modem_proxy.receive_property_changed::<i32>("State").await,
            signal_quality_changes: modem_proxy
                .receive_property_changed::<(u32, bool)>("SignalQuality")
                .await,
            own_numbers_changes: modem_proxy
                .receive_property_changed::<Vec<String>>("OwnNumbers")
                .await,
            sim_changes: modem_proxy
                .receive_property_changed::<OwnedObjectPath>("Sim")
                .await,
        }
    }

    async fn read_event(&mut self) -> Result<Option<ModemEvent>> {
        tokio::select! {
            change = self.model_changes.next() => {
                let Some(change) = change else { return Ok(None); };
                let model = change
                    .get()
                    .await
                    .context("failed to read modem Model property change")?;
                Ok(Some(ModemEvent::Model(model)))
            }
            change = self.revision_changes.next() => {
                let Some(change) = change else { return Ok(None); };
                let revision = change
                    .get()
                    .await
                    .context("failed to read modem Revision property change")?;
                Ok(Some(ModemEvent::Revision(revision)))
            }
            change = self.primary_sim_slot_changes.next() => {
                let Some(change) = change else { return Ok(None); };
                let primary_sim_slot = change
                    .get()
                    .await
                    .context("failed to read modem PrimarySimSlot property change")?;
                Ok(Some(ModemEvent::PrimarySimSlot(primary_sim_slot)))
            }
            change = self.state_changes.next() => {
                let Some(change) = change else { return Ok(None); };
                let raw_state = change
                    .get()
                    .await
                    .context("failed to read modem State property change")?;
                Ok(Some(ModemEvent::State(raw_state)))
            }
            change = self.signal_quality_changes.next() => {
                let Some(change) = change else { return Ok(None); };
                let signal_quality = change
                    .get()
                    .await
                    .context("failed to read modem SignalQuality property change")?
                    .0;
                Ok(Some(ModemEvent::SignalQuality(signal_quality)))
            }
            change = self.own_numbers_changes.next() => {
                let Some(change) = change else { return Ok(None); };
                let own_numbers = change
                    .get()
                    .await
                    .context("failed to read modem OwnNumbers property change")?;
                Ok(Some(ModemEvent::OwnNumbers(own_numbers)))
            }
            change = self.sim_changes.next() => {
                let Some(change) = change else { return Ok(None); };
                let sim_path = change
                    .get()
                    .await
                    .context("failed to read modem Sim property change")?;
                Ok(Some(ModemEvent::Sim(sim_path)))
            }
        }
    }
}

/// Raw DBus property change as received from ModemManager streams.
/// Intentionally distinct from [`schema::ModemUpdate`]: some events carry
/// raw DBus types (e.g. `Sim` holds an `OwnedObjectPath` that requires async
/// resolution) and `State` produces two semantic updates (`State` + `IsActive`).
enum ModemEvent {
    Model(String),
    Revision(String),
    PrimarySimSlot(u32),
    State(i32),
    SignalQuality(u32),
    OwnNumbers(Vec<String>),
    Sim(OwnedObjectPath),
}

fn spawn_modem_sms_task(
    connection: &Connection,
    modem_id: schema::ModemId,
    event_tx: mpsc::Sender<DbusEvent>,
    command_rx: mpsc::Receiver<ModemSmsCommand>,
) -> JoinHandle<()> {
    let connection = connection.clone();
    tokio::spawn(async move {
        if let Err(err) =
            run_modem_sms_task(connection, modem_id.clone(), event_tx, command_rx).await
        {
            debug!(
                target: LOG_TARGET,
                "Modem {} SMS inventory watcher failed: {err:#}",
                modem_id.0
            );
        }
    })
}

async fn run_modem_sms_task(
    connection: Connection,
    modem_id: schema::ModemId,
    event_tx: mpsc::Sender<DbusEvent>,
    mut command_rx: mpsc::Receiver<ModemSmsCommand>,
) -> Result<()> {
    let modem_path = schema::modem_path_from_id(&modem_id);
    let messaging_proxy: Proxy<'_> = ProxyBuilder::new(&connection)
        .destination(schema::MM_BUS_NAME)
        .context("failed to set modem messaging proxy destination")?
        .path(modem_path.as_str())
        .context("failed to set modem messaging proxy path")?
        .interface(schema::MM_MODEM_MESSAGING_INTERFACE)
        .context("failed to set modem messaging proxy interface")?
        .cache_properties(CacheProperties::Yes)
        .build()
        .await
        .with_context(|| format!("failed to create modem messaging proxy for {}", modem_id.0))?;

    // Subscribe before the initial read so SMS added immediately after
    // ENABLED is not lost between snapshot and live mode.
    let mut messages_changes = messaging_proxy
        .receive_property_changed::<Vec<OwnedObjectPath>>("Messages")
        .await;

    let initial_sms_ids = query_modem_sms_ids(&messaging_proxy).await?;
    let initial_sms_snapshot = query_initial_sms_snapshot(&connection, &initial_sms_ids).await?;
    info!(
        target: LOG_TARGET,
        "{}",
        schema::sms_inventory_snapshot_message(
            &modem_id,
            initial_sms_ids.len(),
            initial_sms_snapshot.as_ref().map(|snapshot| &snapshot.sms_id),
        )
    );
    emit_event(
        &event_tx,
        DbusEvent::SmsInventorySnapshot {
            modem_id: modem_id.clone(),
            sms_ids: initial_sms_ids.clone(),
            initial_sms_snapshot: initial_sms_snapshot.clone(),
        },
    )
    .await;

    let mut known_sms_ids: HashSet<_> = initial_sms_ids.into_iter().collect();
    let mut tracked_sms = TrackedSmsTask::default();
    if let Some(initial_sms_snapshot) = initial_sms_snapshot {
        retarget_sms_task(
            &connection,
            &modem_id,
            &event_tx,
            &mut tracked_sms,
            &known_sms_ids,
            Some(&initial_sms_snapshot.sms_id),
        )
        .await?;
    }

    loop {
        tokio::select! {
            maybe_command = command_rx.recv() => {
                let Some(command) = maybe_command else {
                    break;
                };
                match command {
                    ModemSmsCommand::TrackSms { sms_id } => {
                        retarget_sms_task(
                            &connection,
                            &modem_id,
                            &event_tx,
                            &mut tracked_sms,
                            &known_sms_ids,
                            Some(&sms_id),
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
                let sms_ids = sms_ids_from_paths(&message_paths);
                known_sms_ids = sms_ids.iter().cloned().collect();
                sync_tracked_sms_task(
                    &event_tx,
                    &modem_id,
                    &mut tracked_sms,
                    sms_ids,
                )
                .await?;
            }
        }
    }

    Ok(())
}

#[derive(Default)]
struct TrackedSmsTask {
    sms_id: Option<schema::SmsId>,
    task: Option<JoinHandle<()>>,
}

impl Drop for TrackedSmsTask {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn retarget_sms_task(
    connection: &Connection,
    modem_id: &schema::ModemId,
    event_tx: &mpsc::Sender<DbusEvent>,
    tracked_sms: &mut TrackedSmsTask,
    known_sms_ids: &HashSet<schema::SmsId>,
    sms_id: Option<&schema::SmsId>,
) -> Result<()> {
    if tracked_sms.sms_id.as_ref() == sms_id {
        return Ok(());
    }

    if let Some(task) = tracked_sms.task.take() {
        task.abort();
    }
    tracked_sms.sms_id = None;

    let Some(sms_id) = sms_id else {
        return Ok(());
    };
    if !known_sms_ids.contains(sms_id) {
        return Ok(());
    }

    if let Some(task) = spawn_sms_task(
        connection,
        modem_id.clone(),
        sms_id.clone(),
        event_tx.clone(),
    )
    .await?
    {
        tracked_sms.sms_id = Some(sms_id.clone());
        tracked_sms.task = Some(task);
    }

    Ok(())
}

async fn sync_tracked_sms_task(
    event_tx: &mpsc::Sender<DbusEvent>,
    modem_id: &schema::ModemId,
    tracked_sms: &mut TrackedSmsTask,
    current_sms_ids: Vec<schema::SmsId>,
) -> Result<()> {
    emit_event(
        event_tx,
        DbusEvent::SmsListChanged {
            modem_id: modem_id.clone(),
            sms_ids: current_sms_ids.clone(),
        },
    )
    .await;

    let current_sms_ids: std::collections::HashSet<_> = current_sms_ids.into_iter().collect();
    let Some(sms_id) = tracked_sms.sms_id.clone() else {
        return Ok(());
    };
    if current_sms_ids.contains(&sms_id) {
        return Ok(());
    }

    if let Some(task) = tracked_sms.task.take() {
        task.abort();
    }
    tracked_sms.sms_id = None;
    info!(
        target: LOG_TARGET,
        "{}",
        schema::sms_deleted_message(modem_id, &sms_id)
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

async fn spawn_sms_task(
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
        if let Err(err) = run_sms_task(
            connection,
            modem_id.clone(),
            sms_id.clone(),
            snapshot,
            event_tx,
        )
        .await
        {
            debug!(
                target: LOG_TARGET,
                "Modem {} SMS {} watcher failed: {err:#}",
                modem_id.0,
                sms_id.0
            );
        }
    })))
}

async fn run_sms_task(
    connection: Connection,
    modem_id: schema::ModemId,
    sms_id: schema::SmsId,
    mut snapshot: schema::SmsSnapshot,
    event_tx: mpsc::Sender<DbusEvent>,
) -> Result<()> {
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
                    debug!(target: LOG_TARGET, "{}", schema::sms_signal_stream_closed_message(schema::SMS_STATE_CHANGED_SIGNAL_ID, &sms_path));
                    break;
                };
                let is_received = schema::sms_is_received(
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
                    debug!(target: LOG_TARGET, "{}", schema::sms_signal_stream_closed_message(schema::SMS_STORAGE_CHANGED_SIGNAL_ID, &sms_path));
                    break;
                };
                let storage = schema::sms_storage_name(
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
                    debug!(target: LOG_TARGET, "{}", schema::sms_signal_stream_closed_message(schema::SMS_TIMESTAMP_CHANGED_SIGNAL_ID, &sms_path));
                    break;
                };
                let timestamp = schema::parse_sms_timestamp(
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
                    debug!(target: LOG_TARGET, "{}", schema::sms_signal_stream_closed_message(schema::SMS_NUMBER_CHANGED_SIGNAL_ID, &sms_path));
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
                    debug!(target: LOG_TARGET, "{}", schema::sms_signal_stream_closed_message(schema::SMS_TEXT_CHANGED_SIGNAL_ID, &sms_path));
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
        target: LOG_TARGET,
        "{}",
        schema::sms_property_changed_message(modem_id, &update)
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

async fn query_modem_sms_ids(messaging_proxy: &Proxy<'_>) -> Result<Vec<schema::SmsId>> {
    let message_paths: Vec<OwnedObjectPath> = messaging_proxy
        .get_property("Messages")
        .await
        .context("failed to read modem Messages property")?;

    Ok(sms_ids_from_paths(&message_paths))
}

fn sms_ids_from_paths(message_paths: &[OwnedObjectPath]) -> Vec<schema::SmsId> {
    let mut sms_ids: Vec<_> = message_paths
        .iter()
        .filter_map(|path| schema::sms_id_from_path(path.as_str()))
        .collect();
    sms_ids.sort_by(
        |left, right| match (left.0.parse::<u32>(), right.0.parse::<u32>()) {
            (Ok(left), Ok(right)) => left.cmp(&right),
            _ => left.0.cmp(&right.0),
        },
    );
    sms_ids
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueriedModemState {
    info: schema::ModemInfo,
    raw_state: i32,
}

async fn query_modem_snapshot(
    connection: &Connection,
    modem_proxy: &Proxy<'_>,
) -> Result<QueriedModemState> {
    let model: String = modem_proxy
        .get_property("Model")
        .await
        .context("failed to read modem Model property")?;
    let revision: String = modem_proxy
        .get_property("Revision")
        .await
        .context("failed to read modem Revision property")?;
    let primary_sim_slot: u32 = modem_proxy
        .get_property("PrimarySimSlot")
        .await
        .context("failed to read modem PrimarySimSlot property")?;
    let state: i32 = modem_proxy
        .get_property("State")
        .await
        .context("failed to read modem State property")?;
    let signal_quality: (u32, bool) = modem_proxy
        .get_property("SignalQuality")
        .await
        .context("failed to read modem SignalQuality property")?;
    let own_numbers: Vec<String> = modem_proxy
        .get_property("OwnNumbers")
        .await
        .context("failed to read modem OwnNumbers property")?;
    let sim_path: OwnedObjectPath = modem_proxy
        .get_property("Sim")
        .await
        .context("failed to read modem Sim property")?;

    Ok(QueriedModemState {
        info: schema::ModemInfo {
            is_active: schema::modem_state_is_active(state),
            model: Some(model),
            revision: Some(revision),
            state: Some(schema::modem_state_name(state).to_string()),
            primary_sim_slot: Some(primary_sim_slot),
            operator_name: query_operator_name(connection, sim_path).await?,
            own_numbers,
            signal_quality: Some(signal_quality.0),
        },
        raw_state: state,
    })
}

async fn query_initial_sms_snapshot(
    connection: &Connection,
    sms_ids: &[schema::SmsId],
) -> Result<Option<schema::SmsSnapshot>> {
    let Some(initial_sms_id) = sms_ids.first() else {
        return Ok(None);
    };

    query_sms_snapshot(connection, initial_sms_id).await
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
    if !schema::is_incoming_sms_pdu(pdu_type) {
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
        is_received: schema::sms_is_received(state),
        storage: schema::sms_storage_name(storage).to_string(),
        timestamp: schema::parse_sms_timestamp(&timestamp),
        number: normalize_string(number),
        text: normalize_string(text),
    }))
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

async fn query_operator_name(
    connection: &Connection,
    sim_path: OwnedObjectPath,
) -> Result<Option<String>> {
    if sim_path.as_str() == "/" {
        return Ok(None);
    }

    let sim_proxy: Proxy<'_> = ProxyBuilder::new(connection)
        .destination(schema::MM_BUS_NAME)
        .context("failed to set SIM proxy destination")?
        .path(sim_path.as_str())
        .context("failed to set SIM proxy path")?
        .interface(schema::MM_SIM_INTERFACE)
        .context("failed to set SIM proxy interface")?
        .build()
        .await
        .context("failed to create SIM proxy")?;

    let operator_name: String = sim_proxy
        .get_property("OperatorName")
        .await
        .context("failed to read SIM OperatorName property")?;

    if operator_name.is_empty() {
        Ok(None)
    } else {
        Ok(Some(operator_name))
    }
}

fn normalize_string(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

async fn emit_modem_update(
    event_tx: &mpsc::Sender<DbusEvent>,
    modem_id: &schema::ModemId,
    update: schema::ModemUpdate,
) {
    info!(target: LOG_TARGET, "{}", schema::modem_update_message(modem_id, &update));
    emit_event(
        event_tx,
        DbusEvent::ModemUpdated {
            modem_id: modem_id.clone(),
            update,
        },
    )
    .await;
}

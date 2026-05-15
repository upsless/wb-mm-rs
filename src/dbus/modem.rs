use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info};
use zbus::{
    Connection, Proxy,
    proxy::{Builder as ProxyBuilder, CacheProperties, PropertyStream},
    zvariant::OwnedObjectPath,
};

use super::connection::emit_event;
use super::logstrings;
use super::sms::SmsInventoryWatcher;
use crate::common::DBUS_MODEM_COMMAND_CHANNEL_CAPACITY;
use crate::dbus::schema;
use crate::domain::DbusEvent;

pub(super) struct ModemWatcher {
    command_tx: mpsc::Sender<ModemWatcherCommand>,
    task: Option<JoinHandle<()>>,
}

impl ModemWatcher {
    pub(super) fn new(
        modem_id: schema::ModemId,
        connection: Connection,
        event_tx: mpsc::Sender<DbusEvent>,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::channel(DBUS_MODEM_COMMAND_CHANNEL_CAPACITY);
        let worker = ModemWatcherWorker {
            id: modem_id.clone(),
            connection: connection.clone(),
            event_tx: event_tx.clone(),
            command_rx,
        };
        let log_modem_id = modem_id.clone();
        let task = Some(tokio::spawn(async move {
            if let Err(err) = worker.run().await {
                debug!(target: logstrings::LOG_TARGET, "Modem {} watcher failed: {err:#}", log_modem_id.0);
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
            target: logstrings::LOG_TARGET,
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

        if let Some(mut sms_inventory) = state.sms_inventory.take() {
            sms_inventory.abort();
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ModemWatcherCommand {
    TrackSms { sms_id: schema::SmsId },
}

struct ModemState {
    raw_state: i32,
    info: schema::ModemInfo,
    sms_inventory: Option<SmsInventoryWatcher>,
}

impl ModemState {
    fn new(queried: QueriedModemState) -> Self {
        Self {
            raw_state: queried.raw_state,
            info: queried.info,
            sms_inventory: None,
        }
    }

    async fn start_sms_inventory(
        &mut self,
        connection: &Connection,
        modem_id: &schema::ModemId,
        event_tx: &mpsc::Sender<DbusEvent>,
    ) {
        if self.sms_inventory.is_some() {
            return;
        }

        self.sms_inventory = Some(SmsInventoryWatcher::new(
            connection,
            modem_id.clone(),
            event_tx.clone(),
        ));
    }

    async fn stop_sms_inventory(
        &mut self,
        modem_id: &schema::ModemId,
        event_tx: &mpsc::Sender<DbusEvent>,
    ) {
        if let Some(mut sms_inventory) = self.sms_inventory.take() {
            sms_inventory.abort();
            emit_event(
                event_tx,
                DbusEvent::SmsInventorySnapshot {
                    modem_id: modem_id.clone(),
                    entries: Vec::new(),
                },
            )
            .await;
        }
    }

    async fn handle_command(&self, command: ModemWatcherCommand) {
        match command {
            ModemWatcherCommand::TrackSms { sms_id } => {
                if let Some(sms_inventory) = self.sms_inventory.as_ref() {
                    sms_inventory.track_sms(sms_id).await;
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

async fn emit_modem_update(
    event_tx: &mpsc::Sender<DbusEvent>,
    modem_id: &schema::ModemId,
    update: schema::ModemUpdate,
) {
    info!(target: logstrings::LOG_TARGET, "{}", logstrings::modem_update_message(modem_id, &update));
    emit_event(
        event_tx,
        DbusEvent::ModemUpdated {
            modem_id: modem_id.clone(),
            update,
        },
    )
    .await;
}

use std::collections::HashMap;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info};
use zbus::{
    Connection, Proxy,
    connection::Builder,
    fdo::{DBusProxy, InterfacesAddedStream, InterfacesRemovedStream, ObjectManagerProxy},
    names::BusName,
    proxy::{Builder as ProxyBuilder, CacheProperties, PropertyStream},
    zvariant::OwnedObjectPath,
};

use crate::dbus::logics;
use crate::exchange::DbusEvent;

/// Active-only ModemManager watchers.
///
/// We rebuild this bundle every time the ModemManager DBus name becomes active.
/// That keeps the streams tied to the current service owner instead of trying
/// to carry subscriptions across service restarts.
struct ActiveModemManagerState {
    snapshot: logics::ModemManagerSnapshot,
    version_changes: PropertyStream<'static, String>,
    object_manager: ObjectManagerProxy<'static>,
    interfaces_added: InterfacesAddedStream,
    interfaces_removed: InterfacesRemovedStream,
    modem_tasks: HashMap<logics::ModemId, JoinHandle<()>>,
}

/// Owns the DBus-side lifecycle:
/// - connect to the bus;
/// - inspect the current ModemManager state;
/// - subscribe to owner, property, and object changes;
/// - stay alive until shutdown is requested.
pub async fn run(
    dbus_address: Option<String>,
    mut shutdown_rx: watch::Receiver<bool>,
    event_tx: mpsc::Sender<DbusEvent>,
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

    // The standard org.freedesktop.DBus proxy tells us whether the
    // ModemManager well-known name currently has an owner and when that owner
    // changes.
    let dbus_proxy = DBusProxy::new(&connection)
        .await
        .context("failed to create org.freedesktop.DBus proxy")?;

    let mut mm_status = query_modemmanager_status(&dbus_proxy).await?;
    info!("{}", logics::modemmanager_status_message(mm_status));
    emit_event(&event_tx, DbusEvent::StatusChanged(mm_status)).await;

    let mut mm_snapshot = None;
    let mut mm_object_manager = None;
    let mut mm_version_changes = None;
    let mut mm_interfaces_added = None;
    let mut mm_interfaces_removed = None;
    let mut mm_modem_tasks = None;
    let mut mm_modem_count_known = false;

    if mm_status == logics::ModemManagerStatus::Active {
        let state = activate_modemmanager_state(&connection, &event_tx).await?;
        install_active_state(
            state,
            &mut mm_snapshot,
            &mut mm_object_manager,
            &mut mm_version_changes,
            &mut mm_interfaces_added,
            &mut mm_interfaces_removed,
            &mut mm_modem_tasks,
        );
        mm_modem_count_known = false;
        sync_modem_count(
            mm_snapshot.as_mut(),
            mm_object_manager.as_ref(),
            &mut mm_modem_count_known,
            &event_tx,
        )
        .await?;
    }

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
            // We then re-query the derived state and rebuild the active-only
            // state if needed.
            change = mm_status_changes.next() => {
                let Some(change) = change else {
                    debug!(
                        "{}",
                        logics::dbus_signal_stream_closed_message(logics::MM_NAME_OWNER_CHANGED_SIGNAL)
                    );
                    break;
                };

                change
                    .args()
                    .context("failed to parse ModemManager NameOwnerChanged signal")?;

                let new_status = query_modemmanager_status(&dbus_proxy).await?;
                if new_status != mm_status {
                    mm_status = new_status;
                    info!("{}", logics::modemmanager_status_message(mm_status));
                    emit_event(
                        &event_tx,
                        DbusEvent::StatusChanged(mm_status),
                    )
                    .await;
                }

                clear_active_state(
                    &mut mm_snapshot,
                    &mut mm_object_manager,
                    &mut mm_version_changes,
                    &mut mm_interfaces_added,
                    &mut mm_interfaces_removed,
                    &mut mm_modem_tasks,
                );
                mm_modem_count_known = false;

                if mm_status == logics::ModemManagerStatus::Active {
                    let state = activate_modemmanager_state(&connection, &event_tx).await?;
                    install_active_state(
                        state,
                        &mut mm_snapshot,
                        &mut mm_object_manager,
                        &mut mm_version_changes,
                        &mut mm_interfaces_added,
                        &mut mm_interfaces_removed,
                        &mut mm_modem_tasks,
                    );
                    sync_modem_count(
                        mm_snapshot.as_mut(),
                        mm_object_manager.as_ref(),
                        &mut mm_modem_count_known,
                        &event_tx,
                    )
                    .await?;
                }
            }
            // `Version` is a real ModemManager property, so we keep a dedicated
            // property stream for it while the service is active.
            version = async {
                let Some(version_changes) = mm_version_changes.as_mut() else {
                    return Ok::<Option<String>, anyhow::Error>(None);
                };
                let Some(change) = version_changes.next().await else {
                    return Ok::<Option<String>, anyhow::Error>(None);
                };
                let version = change
                    .get()
                    .await
                    .context("failed to read ModemManager Version property change")?;
                Ok(Some(version))
            }, if mm_version_changes.is_some() => {
                let Some(version) = version? else {
                    debug!(
                        "{}",
                        logics::dbus_signal_stream_closed_message(logics::MM_VERSION_CHANGED_SIGNAL)
                    );
                    clear_active_state(
                        &mut mm_snapshot,
                        &mut mm_object_manager,
                        &mut mm_version_changes,
                        &mut mm_interfaces_added,
                        &mut mm_interfaces_removed,
                        &mut mm_modem_tasks,
                    );
                    mm_modem_count_known = false;
                    continue;
                };

                if let Some(snapshot) = mm_snapshot.as_mut()
                    && snapshot.version != version
                    && mm_modem_count_known
                {
                    snapshot.version = version;
                    info!("{}", logics::modemmanager_snapshot_message(snapshot));
                    emit_event(
                        &event_tx,
                        DbusEvent::Snapshot {
                            version: snapshot.version.clone(),
                            modem_count: snapshot.modem_count,
                        },
                    )
                    .await;
                }
            }
            // ModemManager exports modems as ObjectManager child objects rather
            // than as a root "modem count" property. We therefore filter
            // add/remove signals by the modem interface and then re-read the
            // ObjectManager tree for the exact current count.
            added_modem = async {
                let Some(interfaces_added) = mm_interfaces_added.as_mut() else {
                    return Ok::<Option<Option<logics::ModemId>>, anyhow::Error>(None);
                };
                let Some(signal) = interfaces_added.next().await else {
                    return Ok::<Option<Option<logics::ModemId>>, anyhow::Error>(None);
                };
                let args = signal
                    .args()
                    .context("failed to parse ModemManager InterfacesAdded signal")?;
                let touches_modem = args
                    .interfaces_and_properties()
                    .keys()
                    .any(|name| name.as_str() == logics::MM_MODEM_INTERFACE);
                if !touches_modem {
                    return Ok(Some(None));
                }
                Ok(Some(logics::modem_id_from_path(args.object_path().as_str())))
            }, if mm_interfaces_added.is_some() => {
                let Some(added_modem) = added_modem? else {
                    debug!(
                        "{}",
                        logics::dbus_signal_stream_closed_message(logics::MM_INTERFACES_ADDED_SIGNAL)
                    );
                    clear_active_state(
                        &mut mm_snapshot,
                        &mut mm_object_manager,
                        &mut mm_version_changes,
                        &mut mm_interfaces_added,
                        &mut mm_interfaces_removed,
                        &mut mm_modem_tasks,
                    );
                    mm_modem_count_known = false;
                    continue;
                };

                if let Some(modem_id) = added_modem {
                    if let Some(modem_tasks) = mm_modem_tasks.as_mut()
                        && !modem_tasks.contains_key(&modem_id)
                    {
                        modem_tasks.insert(
                            modem_id.clone(),
                            spawn_modem_task(&connection, modem_id, event_tx.clone()),
                        );
                    }
                    sync_modem_count(
                        mm_snapshot.as_mut(),
                        mm_object_manager.as_ref(),
                        &mut mm_modem_count_known,
                        &event_tx,
                    )
                    .await?;
                }
            }
            removed_modem = async {
                let Some(interfaces_removed) = mm_interfaces_removed.as_mut() else {
                    return Ok::<Option<Option<logics::ModemId>>, anyhow::Error>(None);
                };
                let Some(signal) = interfaces_removed.next().await else {
                    return Ok::<Option<Option<logics::ModemId>>, anyhow::Error>(None);
                };
                let args = signal
                    .args()
                    .context("failed to parse ModemManager InterfacesRemoved signal")?;
                let touches_modem = args
                    .interfaces()
                    .iter()
                    .any(|name| name.as_str() == logics::MM_MODEM_INTERFACE);
                if !touches_modem {
                    return Ok(Some(None));
                }
                Ok(Some(logics::modem_id_from_path(args.object_path().as_str())))
            }, if mm_interfaces_removed.is_some() => {
                let Some(removed_modem) = removed_modem? else {
                    debug!(
                        "{}",
                        logics::dbus_signal_stream_closed_message(logics::MM_INTERFACES_REMOVED_SIGNAL)
                    );
                    clear_active_state(
                        &mut mm_snapshot,
                        &mut mm_object_manager,
                        &mut mm_version_changes,
                        &mut mm_interfaces_added,
                        &mut mm_interfaces_removed,
                        &mut mm_modem_tasks,
                    );
                    mm_modem_count_known = false;
                    continue;
                };

                if let Some(modem_id) = removed_modem {
                    if let Some(modem_tasks) = mm_modem_tasks.as_mut()
                        && let Some(task) = modem_tasks.remove(&modem_id)
                    {
                        task.abort();
                    }
                    info!("{}", logics::modem_deleted_message(&modem_id));
                    emit_event(&event_tx, DbusEvent::ModemDeleted { modem_id }).await;
                    sync_modem_count(
                        mm_snapshot.as_mut(),
                        mm_object_manager.as_ref(),
                        &mut mm_modem_count_known,
                        &event_tx,
                    )
                    .await?;
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

/// Build the active-only state when ModemManager has an owner.
///
/// We subscribe to `ObjectManager` signals first and only then ask for the
/// current object set. That mirrors the intended python-style flow: first arm
/// change detection, then take the initial count snapshot.
async fn activate_modemmanager_state(
    connection: &Connection,
    event_tx: &mpsc::Sender<DbusEvent>,
) -> Result<ActiveModemManagerState> {
    let mm_proxy: Proxy<'static> = ProxyBuilder::new(connection)
        .destination(logics::MM_BUS_NAME)
        .context("failed to set ModemManager proxy destination")?
        .path(logics::MM_OBJ_PATH)
        .context("failed to set ModemManager proxy path")?
        .interface(logics::MM_INTERFACE)
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
        ObjectManagerProxy::new(connection, logics::MM_BUS_NAME, logics::MM_OBJ_PATH)
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
    let modem_tasks = spawn_initial_modem_tasks(connection, &object_manager, event_tx).await?;

    Ok(ActiveModemManagerState {
        snapshot: logics::ModemManagerSnapshot {
            version,
            modem_count: 0,
        },
        version_changes,
        object_manager,
        interfaces_added,
        interfaces_removed,
        modem_tasks,
    })
}

/// Read only modem objects from the ObjectManager tree.
///
/// This is the Rust equivalent of the old python project keeping an explicit
/// modem list off the ModemManager service rather than treating every managed
/// object as countable state.
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
                .any(|name| name.as_str() == logics::MM_MODEM_INTERFACE)
        })
        .count())
}

async fn sync_modem_count(
    snapshot: Option<&mut logics::ModemManagerSnapshot>,
    object_manager: Option<&ObjectManagerProxy<'_>>,
    modem_count_known: &mut bool,
    event_tx: &mpsc::Sender<DbusEvent>,
) -> Result<()> {
    let Some(snapshot) = snapshot else {
        return Ok(());
    };
    let Some(object_manager) = object_manager else {
        return Ok(());
    };

    let modem_count = query_modem_count(object_manager).await?;
    if !*modem_count_known {
        snapshot.modem_count = modem_count;
        *modem_count_known = true;
        info!("{}", logics::modemmanager_snapshot_message(snapshot));
        emit_event(
            event_tx,
            DbusEvent::Snapshot {
                version: snapshot.version.clone(),
                modem_count,
            },
        )
        .await;
    } else if snapshot.modem_count != modem_count {
        snapshot.modem_count = modem_count;
        info!(
            "{}",
            logics::modemmanager_modem_count_changed_message(modem_count)
        );
        emit_event(event_tx, DbusEvent::ModemCountChanged { modem_count }).await;
    }

    Ok(())
}

async fn emit_event(event_tx: &mpsc::Sender<DbusEvent>, event: DbusEvent) {
    if event_tx.send(event).await.is_err() {
        debug!("DBus event channel closed while sending");
    }
}

fn install_active_state(
    state: ActiveModemManagerState,
    mm_snapshot: &mut Option<logics::ModemManagerSnapshot>,
    mm_object_manager: &mut Option<ObjectManagerProxy<'static>>,
    mm_version_changes: &mut Option<PropertyStream<'static, String>>,
    mm_interfaces_added: &mut Option<InterfacesAddedStream>,
    mm_interfaces_removed: &mut Option<InterfacesRemovedStream>,
    mm_modem_tasks: &mut Option<HashMap<logics::ModemId, JoinHandle<()>>>,
) {
    *mm_snapshot = Some(state.snapshot);
    *mm_object_manager = Some(state.object_manager);
    *mm_version_changes = Some(state.version_changes);
    *mm_interfaces_added = Some(state.interfaces_added);
    *mm_interfaces_removed = Some(state.interfaces_removed);
    *mm_modem_tasks = Some(state.modem_tasks);
}

fn clear_active_state(
    mm_snapshot: &mut Option<logics::ModemManagerSnapshot>,
    mm_object_manager: &mut Option<ObjectManagerProxy<'static>>,
    mm_version_changes: &mut Option<PropertyStream<'static, String>>,
    mm_interfaces_added: &mut Option<InterfacesAddedStream>,
    mm_interfaces_removed: &mut Option<InterfacesRemovedStream>,
    mm_modem_tasks: &mut Option<HashMap<logics::ModemId, JoinHandle<()>>>,
) {
    *mm_snapshot = None;
    *mm_object_manager = None;
    *mm_version_changes = None;
    *mm_interfaces_added = None;
    *mm_interfaces_removed = None;
    if let Some(tasks) = mm_modem_tasks.take() {
        for (_, task) in tasks {
            task.abort();
        }
    }
}

async fn query_modem_ids(object_manager: &ObjectManagerProxy<'_>) -> Result<Vec<logics::ModemId>> {
    let managed_objects = object_manager
        .get_managed_objects()
        .await
        .context("failed to read ModemManager managed objects")?;

    Ok(managed_objects
        .iter()
        .filter(|(_, interfaces)| {
            interfaces
                .keys()
                .any(|name| name.as_str() == logics::MM_MODEM_INTERFACE)
        })
        .filter_map(|(path, _)| logics::modem_id_from_path(path.as_str()))
        .collect())
}

async fn spawn_initial_modem_tasks(
    connection: &Connection,
    object_manager: &ObjectManagerProxy<'_>,
    event_tx: &mpsc::Sender<DbusEvent>,
) -> Result<HashMap<logics::ModemId, JoinHandle<()>>> {
    let mut modem_tasks = HashMap::new();
    for modem_id in query_modem_ids(object_manager).await? {
        modem_tasks.insert(
            modem_id.clone(),
            spawn_modem_task(connection, modem_id, event_tx.clone()),
        );
    }
    Ok(modem_tasks)
}

fn spawn_modem_task(
    connection: &Connection,
    modem_id: logics::ModemId,
    event_tx: mpsc::Sender<DbusEvent>,
) -> JoinHandle<()> {
    let connection = connection.clone();
    tokio::spawn(async move {
        if let Err(err) = run_modem_task(connection, modem_id.clone(), event_tx).await {
            debug!("Modem {} watcher failed: {err:#}", modem_id.0);
        }
    })
}

async fn run_modem_task(
    connection: Connection,
    modem_id: logics::ModemId,
    event_tx: mpsc::Sender<DbusEvent>,
) -> Result<()> {
    let modem_path = logics::modem_path_from_id(&modem_id);
    let modem_proxy: Proxy<'_> = ProxyBuilder::new(&connection)
        .destination(logics::MM_BUS_NAME)
        .context("failed to set modem proxy destination")?
        .path(modem_path.as_str())
        .context("failed to set modem proxy path")?
        .interface(logics::MM_MODEM_INTERFACE)
        .context("failed to set modem proxy interface")?
        .cache_properties(CacheProperties::Yes)
        .build()
        .await
        .with_context(|| format!("failed to create modem proxy for {}", modem_id.0))?;

    let mut model_changes = modem_proxy
        .receive_property_changed::<String>("Model")
        .await;
    let mut revision_changes = modem_proxy
        .receive_property_changed::<String>("Revision")
        .await;
    let mut primary_sim_slot_changes = modem_proxy
        .receive_property_changed::<u32>("PrimarySimSlot")
        .await;
    let mut state_changes = modem_proxy.receive_property_changed::<i32>("State").await;
    let mut signal_quality_changes = modem_proxy
        .receive_property_changed::<(u32, bool)>("SignalQuality")
        .await;
    let mut sim_changes = modem_proxy
        .receive_property_changed::<OwnedObjectPath>("Sim")
        .await;

    let mut snapshot = query_modem_snapshot(&connection, &modem_proxy).await?;

    info!("{}", logics::modem_found_message(&modem_id));
    emit_event(
        &event_tx,
        DbusEvent::ModemFound {
            modem_id: modem_id.clone(),
        },
    )
    .await;

    info!("{}", logics::modem_snapshot_message(&modem_id, &snapshot));
    emit_event(
        &event_tx,
        DbusEvent::ModemSnapshot {
            modem_id: modem_id.clone(),
            snapshot: snapshot.clone(),
        },
    )
    .await;

    loop {
        tokio::select! {
            change = model_changes.next() => {
                let Some(change) = change else { break; };
                let model = change
                    .get()
                    .await
                    .context("failed to read modem Model property change")?;
                if snapshot.model.as_deref() != Some(model.as_str()) {
                    snapshot.model = Some(model.clone());
                    emit_modem_update(&event_tx, &modem_id, logics::ModemUpdate::Model(model))
                        .await;
                }
            }
            change = revision_changes.next() => {
                let Some(change) = change else { break; };
                let revision = change
                    .get()
                    .await
                    .context("failed to read modem Revision property change")?;
                if snapshot.revision.as_deref() != Some(revision.as_str()) {
                    snapshot.revision = Some(revision.clone());
                    emit_modem_update(
                        &event_tx,
                        &modem_id,
                        logics::ModemUpdate::Revision(revision),
                    )
                    .await;
                }
            }
            change = primary_sim_slot_changes.next() => {
                let Some(change) = change else { break; };
                let primary_sim_slot = change
                    .get()
                    .await
                    .context("failed to read modem PrimarySimSlot property change")?;
                if snapshot.primary_sim_slot != Some(primary_sim_slot) {
                    snapshot.primary_sim_slot = Some(primary_sim_slot);
                    emit_modem_update(
                        &event_tx,
                        &modem_id,
                        logics::ModemUpdate::PrimarySimSlot(primary_sim_slot),
                    )
                    .await;
                }
            }
            change = state_changes.next() => {
                let Some(change) = change else { break; };
                let state = Some(
                    logics::modem_state_name(
                        change
                            .get()
                            .await
                            .context("failed to read modem State property change")?,
                    )
                    .to_string(),
                );
                if snapshot.state != state {
                    snapshot.state = state.clone();
                    emit_modem_update(&event_tx, &modem_id, logics::ModemUpdate::State(state))
                        .await;
                }
            }
            change = signal_quality_changes.next() => {
                let Some(change) = change else { break; };
                let signal_quality = Some(
                    change
                        .get()
                        .await
                        .context("failed to read modem SignalQuality property change")?
                        .0,
                );
                if snapshot.signal_quality != signal_quality {
                    snapshot.signal_quality = signal_quality;
                    emit_modem_update(
                        &event_tx,
                        &modem_id,
                        logics::ModemUpdate::SignalQuality(signal_quality),
                    )
                    .await;
                }
            }
            change = sim_changes.next() => {
                let Some(change) = change else { break; };
                let sim_path = change
                    .get()
                    .await
                    .context("failed to read modem Sim property change")?;
                let operator_name = query_operator_name(&connection, sim_path).await?;
                if snapshot.operator_name != operator_name {
                    snapshot.operator_name = operator_name.clone();
                    emit_modem_update(
                        &event_tx,
                        &modem_id,
                        logics::ModemUpdate::OperatorName(operator_name),
                    )
                    .await;
                }
            }
        }
    }

    Ok(())
}

async fn query_modem_snapshot(
    connection: &Connection,
    modem_proxy: &Proxy<'_>,
) -> Result<logics::ModemSnapshot> {
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
    let sim_path: OwnedObjectPath = modem_proxy
        .get_property("Sim")
        .await
        .context("failed to read modem Sim property")?;

    Ok(logics::ModemSnapshot {
        is_active: true,
        model: Some(model),
        revision: Some(revision),
        state: Some(logics::modem_state_name(state).to_string()),
        primary_sim_slot: Some(primary_sim_slot),
        operator_name: query_operator_name(connection, sim_path).await?,
        signal_quality: Some(signal_quality.0),
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
        .destination(logics::MM_BUS_NAME)
        .context("failed to set SIM proxy destination")?
        .path(sim_path.as_str())
        .context("failed to set SIM proxy path")?
        .interface(logics::MM_SIM_INTERFACE)
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
    modem_id: &logics::ModemId,
    update: logics::ModemUpdate,
) {
    info!("{}", logics::modem_update_message(modem_id, &update));
    emit_event(
        event_tx,
        DbusEvent::ModemUpdated {
            modem_id: modem_id.clone(),
            update,
        },
    )
    .await;
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

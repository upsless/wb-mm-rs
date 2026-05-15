use std::collections::{HashMap, HashSet};

use anyhow::Result;
use tokio::sync::{mpsc, watch};
use tracing::{debug, info};

use crate::dbus::{ModemId, SmsId, SmsSnapshot};
use crate::domain::{
    DbusCommand, DbusEvent, OutgoingSmsInfo, OutgoingSmsStatus, SmsInventoryEntry,
    canonicalize_phone_number, validate_outgoing_sms_request,
};

const LOG_TARGET: &str = "CORE";

#[derive(Debug, Clone, Default)]
pub(crate) struct CoreConfig {
    command_numbers: HashSet<String>,
}

impl CoreConfig {
    pub(crate) fn new(command_numbers: Vec<String>) -> Self {
        Self {
            command_numbers: command_numbers
                .into_iter()
                .map(|value| canonicalize_phone_number(&value))
                .filter(|value| !value.is_empty())
                .collect(),
        }
    }

    fn sender_is_allowed(&self, sender: &str) -> bool {
        self.command_numbers
            .contains(&canonicalize_phone_number(sender))
    }
}

#[derive(Debug, Default)]
struct CoreState {
    modems: HashMap<ModemId, CoreModemState>,
}

#[derive(Debug, Default)]
struct CoreModemState {
    known_sms_ids: HashSet<SmsId>,
    known_sms_snapshots: HashMap<SmsId, SmsSnapshot>,
    handled_command_sms_ids: HashSet<SmsId>,
}

impl CoreState {
    fn update_sms_inventory(
        &mut self,
        modem_id: &ModemId,
        entries: &[SmsInventoryEntry],
    ) -> Vec<SmsId> {
        let modem = self.modems.entry(modem_id.clone()).or_default();
        let previous_sms_ids = modem.known_sms_ids.clone();
        let current_sms_ids: HashSet<_> =
            entries.iter().map(|entry| entry.sms_id.clone()).collect();
        let added_sms_ids = entries
            .iter()
            .map(|entry| entry.sms_id.clone())
            .filter(|sms_id| !previous_sms_ids.contains(sms_id))
            .collect::<Vec<_>>();

        modem.known_sms_ids = current_sms_ids;
        modem
            .known_sms_snapshots
            .retain(|sms_id, _| modem.known_sms_ids.contains(sms_id));
        modem
            .handled_command_sms_ids
            .retain(|sms_id| modem.known_sms_ids.contains(sms_id));

        added_sms_ids
    }

    fn remember_sms_snapshot(&mut self, modem_id: &ModemId, snapshot: &SmsSnapshot) {
        let modem = self.modems.entry(modem_id.clone()).or_default();
        modem.known_sms_ids.insert(snapshot.sms_id.clone());
        modem
            .known_sms_snapshots
            .insert(snapshot.sms_id.clone(), snapshot.clone());
    }

    fn forget_sms(&mut self, modem_id: &ModemId, sms_id: &SmsId) {
        let Some(modem) = self.modems.get_mut(modem_id) else {
            return;
        };

        modem.known_sms_ids.remove(sms_id);
        modem.known_sms_snapshots.remove(sms_id);
        modem.handled_command_sms_ids.remove(sms_id);
    }

    fn forget_modem(&mut self, modem_id: &ModemId) {
        self.modems.remove(modem_id);
    }

    fn mark_command_sms_handled(&mut self, modem_id: &ModemId, sms_id: &SmsId) -> bool {
        self.modems
            .entry(modem_id.clone())
            .or_default()
            .handled_command_sms_ids
            .insert(sms_id.clone())
    }

    fn unmark_command_sms_handled(&mut self, modem_id: &ModemId, sms_id: &SmsId) {
        let Some(modem) = self.modems.get_mut(modem_id) else {
            return;
        };

        modem.handled_command_sms_ids.remove(sms_id);
    }
}

/// Runs the first Core loop between DBus and MQTT.
///
/// Core now owns:
/// - routing between DBus and MQTT;
/// - repeated validation of outbound SMS before DBus;
/// - a first internal model of incoming SMS inventory so future command
///   filtering can be added here without pushing that logic back into MQTT.
///
/// For now all incoming SMS-related traffic is still forwarded to MQTT
/// unchanged. Core only requests snapshots for newly observed SMS ids so it
/// can build state for future policy decisions.
pub async fn run_lifecycle(
    config: CoreConfig,
    mut shutdown_rx: watch::Receiver<bool>,
    mut dbus_event_rx: mpsc::Receiver<DbusEvent>,
    mqtt_event_tx: mpsc::Sender<DbusEvent>,
    mut mqtt_command_rx: mpsc::Receiver<DbusCommand>,
    mut dbus_command_tx_rx: watch::Receiver<Option<mpsc::Sender<DbusCommand>>>,
) -> Result<()> {
    let mut current_dbus_command_tx = dbus_command_tx_rx.borrow().clone();
    let mut state = CoreState::default();

    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return Ok(());
                }
            }
            maybe_event = dbus_event_rx.recv() => {
                let Some(event) = maybe_event else {
                    return Ok(());
                };

                let should_forward =
                    handle_dbus_event(&mut state, &config, &event, current_dbus_command_tx.as_ref()).await;

                if should_forward && mqtt_event_tx.send(event).await.is_err() {
                    debug!(target: LOG_TARGET, "MQTT event channel closed while forwarding DBus event");
                    return Ok(());
                }
            }
            maybe_command = mqtt_command_rx.recv() => {
                let Some(command) = maybe_command else {
                    return Ok(());
                };

                if let Some(rejected_event) = validate_or_reject_command(&command) {
                    info!(target: LOG_TARGET, "Rejected outbound SMS request in Core before DBus");
                    if mqtt_event_tx.send(rejected_event).await.is_err() {
                        debug!(target: LOG_TARGET, "MQTT event channel closed while reporting Core-side command rejection");
                        return Ok(());
                    }
                    continue;
                }

                let Some(dbus_command_tx) = current_dbus_command_tx.as_ref() else {
                    debug!(target: LOG_TARGET, "Dropping MQTT command while no active DBus command sender is available");
                    if report_failed_outgoing_sms(
                        &mqtt_event_tx,
                        &command,
                        "DBus command sender is unavailable",
                    )
                    .await
                    {
                        return Ok(());
                    }
                    continue;
                };

                if dbus_command_tx.send(command.clone()).await.is_err() {
                    debug!(target: LOG_TARGET, "Active DBus command sender closed while forwarding MQTT command");
                    current_dbus_command_tx = None;
                    if report_failed_outgoing_sms(
                        &mqtt_event_tx,
                        &command,
                        "DBus command sender closed while forwarding command",
                    )
                    .await
                    {
                        return Ok(());
                    }
                }
            }
            changed = dbus_command_tx_rx.changed() => {
                if changed.is_err() {
                    return Ok(());
                }

                current_dbus_command_tx = dbus_command_tx_rx.borrow().clone();
            }
        }
    }
}

async fn handle_dbus_event(
    state: &mut CoreState,
    config: &CoreConfig,
    event: &DbusEvent,
    dbus_command_tx: Option<&mpsc::Sender<DbusCommand>>,
) -> bool {
    match event {
        DbusEvent::SmsInventorySnapshot { modem_id, entries }
        | DbusEvent::SmsListChanged { modem_id, entries } => {
            let added_sms_ids = state.update_sms_inventory(modem_id, entries);
            if added_sms_ids.is_empty() {
                return true;
            }

            debug!(
                target: LOG_TARGET,
                "Core observed {} new SMS id(s) for modem {}; requesting snapshots",
                added_sms_ids.len(),
                modem_id.0
            );
            for sms_id in added_sms_ids {
                request_sms_snapshot(dbus_command_tx, modem_id, &sms_id).await;
            }
            true
        }
        DbusEvent::SmsSnapshot { modem_id, snapshot } => {
            state.remember_sms_snapshot(modem_id, snapshot);
            !maybe_handle_command_sms(state, config, modem_id, snapshot, dbus_command_tx).await
        }
        DbusEvent::SmsDeleted { modem_id, sms_id } => {
            state.forget_sms(modem_id, sms_id);
            true
        }
        DbusEvent::ModemDeleted { modem_id } => {
            state.forget_modem(modem_id);
            true
        }
        _ => true,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CoreSmsCommand {
    Help { command: Option<String> },
}

fn parse_core_sms_command(snapshot: &SmsSnapshot) -> Option<CoreSmsCommand> {
    let text = snapshot.text.as_deref()?.trim();
    let command_text = text.strip_prefix('#')?.trim_start();
    if command_text.is_empty() {
        return None;
    }

    let mut parts = command_text.splitn(2, char::is_whitespace);
    let command_name = parts.next()?.trim().to_ascii_lowercase();
    let argument = parts
        .next()
        .map(str::trim)
        .filter(|arg| !arg.is_empty())
        .map(str::to_string);

    match command_name.as_str() {
        "help" => Some(CoreSmsCommand::Help { command: argument }),
        _ => None,
    }
}

async fn maybe_handle_command_sms(
    state: &mut CoreState,
    config: &CoreConfig,
    modem_id: &ModemId,
    snapshot: &SmsSnapshot,
    dbus_command_tx: Option<&mpsc::Sender<DbusCommand>>,
) -> bool {
    let Some(command) = parse_core_sms_command(snapshot) else {
        return false;
    };

    let Some(recipient) = snapshot.number.as_deref().filter(|value| !value.is_empty()) else {
        debug!(
            target: LOG_TARGET,
            "Ignoring Core SMS command without sender number for modem {} SMS {}",
            modem_id.0,
            snapshot.sms_id.0
        );
        return false;
    };

    let Some(dbus_command_tx) = dbus_command_tx else {
        debug!(
            target: LOG_TARGET,
            "Ignoring Core SMS command while no active DBus command sender is available"
        );
        return false;
    };

    if !state.mark_command_sms_handled(modem_id, &snapshot.sms_id) {
        return true;
    }

    if !config.sender_is_allowed(recipient) {
        info!(
            target: LOG_TARGET,
            "Rejected Core SMS command `{}` from unauthorized sender {} on modem {} SMS {}",
            core_sms_command_name(&command),
            recipient,
            modem_id.0,
            snapshot.sms_id.0
        );
        delete_handled_command_sms(dbus_command_tx, modem_id, &snapshot.sms_id).await;
        return true;
    }

    let reply_command = DbusCommand::SendSms {
        modem_id: modem_id.clone(),
        recipient: recipient.to_string(),
        text: render_core_sms_command_reply(&command),
        check_phone_format: false,
    };
    if dbus_command_tx.send(reply_command).await.is_err() {
        state.unmark_command_sms_handled(modem_id, &snapshot.sms_id);
        debug!(
            target: LOG_TARGET,
            "DBus command channel closed while sending Core SMS command reply"
        );
        return false;
    }

    delete_handled_command_sms(dbus_command_tx, modem_id, &snapshot.sms_id).await;
    true
}

fn core_sms_command_name(command: &CoreSmsCommand) -> &'static str {
    match command {
        CoreSmsCommand::Help { .. } => "help",
    }
}

async fn delete_handled_command_sms(
    dbus_command_tx: &mpsc::Sender<DbusCommand>,
    modem_id: &ModemId,
    sms_id: &SmsId,
) {
    if dbus_command_tx
        .send(DbusCommand::DeleteSms {
            modem_id: modem_id.clone(),
            sms_id: sms_id.clone(),
        })
        .await
        .is_err()
    {
        debug!(
            target: LOG_TARGET,
            "DBus command channel closed while deleting handled Core SMS command"
        );
    }
}

fn render_core_sms_command_reply(command: &CoreSmsCommand) -> String {
    match command {
        CoreSmsCommand::Help {
            command: Some(command),
        } if command.eq_ignore_ascii_case("help") => "Commands:\n#help [command]".to_string(),
        CoreSmsCommand::Help { .. } => "Commands:\n#help [command]".to_string(),
    }
}

async fn request_sms_snapshot(
    dbus_command_tx: Option<&mpsc::Sender<DbusCommand>>,
    modem_id: &ModemId,
    sms_id: &SmsId,
) {
    let Some(dbus_command_tx) = dbus_command_tx else {
        debug!(target: LOG_TARGET, "Skipping automatic SMS snapshot request while no active DBus command sender is available");
        return;
    };

    if dbus_command_tx
        .send(DbusCommand::RefreshSms {
            modem_id: modem_id.clone(),
            sms_id: sms_id.clone(),
        })
        .await
        .is_err()
    {
        debug!(target: LOG_TARGET, "DBus command channel closed while requesting automatic SMS snapshot");
    }
}

fn validate_or_reject_command(command: &DbusCommand) -> Option<DbusEvent> {
    let DbusCommand::SendSms {
        modem_id,
        recipient,
        text,
        check_phone_format,
    } = command
    else {
        return None;
    };

    let Err(err) = validate_outgoing_sms_request(recipient, text, *check_phone_format) else {
        return None;
    };

    Some(DbusEvent::OutgoingSmsUpdated {
        modem_id: modem_id.clone(),
        info: OutgoingSmsInfo {
            recipient: recipient.clone(),
            text: text.clone(),
            timestamp: None,
            status: OutgoingSmsStatus::Failed,
            error: Some(err.as_str().to_string()),
        },
    })
}

fn failed_outgoing_sms_event(command: &DbusCommand, error: &str) -> Option<DbusEvent> {
    let DbusCommand::SendSms {
        modem_id,
        recipient,
        text,
        ..
    } = command
    else {
        return None;
    };

    Some(DbusEvent::OutgoingSmsUpdated {
        modem_id: modem_id.clone(),
        info: OutgoingSmsInfo {
            recipient: recipient.clone(),
            text: text.clone(),
            timestamp: None,
            status: OutgoingSmsStatus::Failed,
            error: Some(error.to_string()),
        },
    })
}

async fn report_failed_outgoing_sms(
    mqtt_event_tx: &mpsc::Sender<DbusEvent>,
    command: &DbusCommand,
    error: &str,
) -> bool {
    let Some(failed_event) = failed_outgoing_sms_event(command, error) else {
        return false;
    };

    if mqtt_event_tx.send(failed_event).await.is_ok() {
        return false;
    }

    debug!(target: LOG_TARGET, "MQTT event channel closed while reporting failed outgoing SMS");
    true
}

#[cfg(test)]
mod tests {
    use super::{
        CoreConfig, CoreSmsCommand, CoreState, handle_dbus_event, parse_core_sms_command,
        render_core_sms_command_reply, validate_or_reject_command,
    };
    use crate::dbus::{ModemId, SmsId, SmsSnapshot};
    use crate::domain::{DbusCommand, DbusEvent, OutgoingSmsStatus, SmsInventoryEntry};
    use time::OffsetDateTime;
    use tokio::sync::mpsc;

    #[test]
    fn rejects_invalid_outgoing_sms_in_core() {
        let command = DbusCommand::SendSms {
            modem_id: ModemId("0".to_string()),
            recipient: "12345".to_string(),
            text: "hello".to_string(),
            check_phone_format: true,
        };

        let Some(DbusEvent::OutgoingSmsUpdated { modem_id, info }) =
            validate_or_reject_command(&command)
        else {
            panic!("expected rejected outgoing SMS event");
        };

        assert_eq!(modem_id.0, "0");
        assert_eq!(info.recipient, "12345");
        assert_eq!(info.text, "hello");
        assert_eq!(info.status, OutgoingSmsStatus::Failed);
        assert_eq!(
            info.error.as_deref(),
            Some("Recipient number does not match the allowed format")
        );
    }

    #[test]
    fn initial_inventory_marks_all_sms_as_new() {
        let mut state = CoreState::default();
        let modem_id = ModemId("0".to_string());
        let entries = vec![
            SmsInventoryEntry {
                sms_id: SmsId("10".to_string()),
                timestamp: None,
            },
            SmsInventoryEntry {
                sms_id: SmsId("11".to_string()),
                timestamp: None,
            },
        ];

        let added = state.update_sms_inventory(&modem_id, &entries);

        assert_eq!(
            added,
            vec![SmsId("10".to_string()), SmsId("11".to_string())]
        );
    }

    #[test]
    fn inventory_change_marks_only_new_sms_as_added() {
        let mut state = CoreState::default();
        let modem_id = ModemId("0".to_string());
        let initial_entries = vec![
            SmsInventoryEntry {
                sms_id: SmsId("10".to_string()),
                timestamp: None,
            },
            SmsInventoryEntry {
                sms_id: SmsId("11".to_string()),
                timestamp: None,
            },
        ];
        let changed_entries = vec![
            SmsInventoryEntry {
                sms_id: SmsId("11".to_string()),
                timestamp: None,
            },
            SmsInventoryEntry {
                sms_id: SmsId("12".to_string()),
                timestamp: None,
            },
        ];

        let _ = state.update_sms_inventory(&modem_id, &initial_entries);
        let added = state.update_sms_inventory(&modem_id, &changed_entries);

        assert_eq!(added, vec![SmsId("12".to_string())]);
    }

    #[test]
    fn forgetting_removed_sms_also_drops_cached_snapshot() {
        let mut state = CoreState::default();
        let modem_id = ModemId("0".to_string());
        let sms_id = SmsId("11".to_string());

        let _ = state.update_sms_inventory(
            &modem_id,
            &[SmsInventoryEntry {
                sms_id: sms_id.clone(),
                timestamp: None,
            }],
        );
        state.remember_sms_snapshot(
            &modem_id,
            &SmsSnapshot {
                sms_id: sms_id.clone(),
                is_received: true,
                storage: "Mobile".to_string(),
                timestamp: Some(OffsetDateTime::UNIX_EPOCH),
                number: Some("+79850000000".to_string()),
                text: Some("hello".to_string()),
            },
        );

        state.forget_sms(&modem_id, &sms_id);

        let modem = state.modems.get(&modem_id).expect("modem state must exist");
        assert!(!modem.known_sms_ids.contains(&sms_id));
        assert!(!modem.known_sms_snapshots.contains_key(&sms_id));
    }

    #[test]
    fn parses_help_sms_command() {
        let snapshot = SmsSnapshot {
            sms_id: SmsId("11".to_string()),
            is_received: true,
            storage: "Mobile".to_string(),
            timestamp: None,
            number: Some("+79850000000".to_string()),
            text: Some("#help sms".to_string()),
        };

        assert_eq!(
            parse_core_sms_command(&snapshot),
            Some(CoreSmsCommand::Help {
                command: Some("sms".to_string())
            })
        );
    }

    #[test]
    fn non_command_sms_is_not_parsed_as_core_command() {
        let snapshot = SmsSnapshot {
            sms_id: SmsId("11".to_string()),
            is_received: true,
            storage: "Mobile".to_string(),
            timestamp: None,
            number: Some("+79850000000".to_string()),
            text: Some("help".to_string()),
        };

        assert_eq!(parse_core_sms_command(&snapshot), None);
    }

    #[test]
    fn help_command_reply_lists_known_commands() {
        let reply = render_core_sms_command_reply(&CoreSmsCommand::Help { command: None });
        assert_eq!(reply, "Commands:\n#help [command]");
    }

    #[test]
    fn command_number_normalization_matches_8_and_plus7() {
        let config = CoreConfig::new(vec!["89858619773".to_string()]);

        assert!(config.sender_is_allowed("+79858619773"));
        assert!(config.sender_is_allowed("89858619773"));
        assert!(!config.sender_is_allowed("+79990000000"));
    }

    #[tokio::test]
    async fn authorized_help_sms_is_filtered_and_replied_in_core() {
        let mut state = CoreState::default();
        let config = CoreConfig::new(vec!["89858619773".to_string()]);
        let modem_id = ModemId("0".to_string());
        let (dbus_command_tx, mut dbus_command_rx) = mpsc::channel(4);
        let event = DbusEvent::SmsSnapshot {
            modem_id: modem_id.clone(),
            snapshot: SmsSnapshot {
                sms_id: SmsId("11".to_string()),
                is_received: true,
                storage: "Mobile".to_string(),
                timestamp: None,
                number: Some("+79858619773".to_string()),
                text: Some("#help".to_string()),
            },
        };

        let should_forward =
            handle_dbus_event(&mut state, &config, &event, Some(&dbus_command_tx)).await;

        assert!(!should_forward);

        let Some(DbusCommand::SendSms {
            modem_id: reply_modem_id,
            recipient,
            text,
            check_phone_format,
        }) = dbus_command_rx.recv().await
        else {
            panic!("expected reply SMS command");
        };
        assert_eq!(reply_modem_id.0, "0");
        assert_eq!(recipient, "+79858619773");
        assert_eq!(text, "Commands:\n#help [command]");
        assert!(!check_phone_format);

        let Some(DbusCommand::DeleteSms {
            modem_id: delete_modem_id,
            sms_id,
        }) = dbus_command_rx.recv().await
        else {
            panic!("expected delete SMS command");
        };
        assert_eq!(delete_modem_id.0, "0");
        assert_eq!(sms_id.0, "11");
    }

    #[tokio::test]
    async fn unauthorized_help_sms_is_filtered_without_reply() {
        let mut state = CoreState::default();
        let config = CoreConfig::new(vec!["89858619773".to_string()]);
        let modem_id = ModemId("0".to_string());
        let (dbus_command_tx, mut dbus_command_rx) = mpsc::channel(4);
        let event = DbusEvent::SmsSnapshot {
            modem_id: modem_id.clone(),
            snapshot: SmsSnapshot {
                sms_id: SmsId("12".to_string()),
                is_received: true,
                storage: "Mobile".to_string(),
                timestamp: None,
                number: Some("+79990000000".to_string()),
                text: Some("#help".to_string()),
            },
        };

        let should_forward =
            handle_dbus_event(&mut state, &config, &event, Some(&dbus_command_tx)).await;

        assert!(!should_forward);

        let Some(DbusCommand::DeleteSms {
            modem_id: delete_modem_id,
            sms_id,
        }) = dbus_command_rx.recv().await
        else {
            panic!("expected delete SMS command");
        };
        assert_eq!(delete_modem_id.0, "0");
        assert_eq!(sms_id.0, "12");
        assert!(dbus_command_rx.try_recv().is_err());
    }
}

use super::{MqttModemSmsState, MqttSessionState};
use crate::dbus::{ModemId, SmsId, SmsSnapshot};
use time::OffsetDateTime;

#[test]
fn modem_indices_start_from_one_and_reuse_gaps() {
    let mut state = MqttSessionState::default();

    let (first, first_created) = state.ensure_modem_index(&ModemId("0".to_string()));
    let (second, second_created) = state.ensure_modem_index(&ModemId("1".to_string()));
    let _ = state.remove_modem_index(&ModemId("0".to_string()));
    let (reused, reused_created) = state.ensure_modem_index(&ModemId("2".to_string()));

    assert_eq!((first, first_created), (1, true));
    assert_eq!((second, second_created), (2, true));
    assert_eq!((reused, reused_created), (1, true));
}

#[test]
fn picked_sms_id_uses_picked_sms_index() {
    let state = MqttModemSmsState {
        sms_order: sms_ids(&["144", "145", "146"]),
        picked_sms_index: 2,
        ..Default::default()
    };

    assert_eq!(state.picked_sms_id(), Some(&SmsId("145".to_string())));
}

#[test]
fn sms_order_keeps_picked_index_not_dbus_id() {
    let mut state = MqttModemSmsState {
        sms_order: sms_ids(&["144", "145", "146"]),
        picked_sms_index: 2,
        ..Default::default()
    };

    let request_sms_id = state.apply_sms_order(sms_ids(&["145", "146"]));

    assert_eq!(state.picked_sms_index, 2);
    assert_eq!(state.picked_sms_id(), Some(&SmsId("146".to_string())));
    assert_eq!(request_sms_id, Some(SmsId("146".to_string())));
}

#[test]
fn sms_order_keeps_picked_position_when_displayed_sms_moves() {
    let mut state = MqttModemSmsState {
        sms_order: sms_ids(&["144", "145", "146"]),
        picked_sms_index: 2,
        displayed_sms_id: Some(SmsId("145".to_string())),
    };

    let picked_sms_id = state.apply_sms_order(sms_ids(&["146", "144", "145"]));

    assert_eq!(picked_sms_id, Some(SmsId("144".to_string())));
    assert_eq!(state.picked_sms_index, 2);
    assert_eq!(state.picked_sms_id(), Some(&SmsId("144".to_string())));
    assert_eq!(state.displayed_sms_index(), Some(3));
}

#[test]
fn sms_order_does_not_request_when_picked_sms_id_survives() {
    let mut state = MqttModemSmsState {
        sms_order: sms_ids(&["144", "145", "146"]),
        picked_sms_index: 3,
        displayed_sms_id: Some(SmsId("145".to_string())),
    };

    let picked_sms_id = state.apply_sms_order(sms_ids(&["144", "146"]));

    assert_eq!(picked_sms_id, None);
    assert_eq!(state.picked_sms_index, 2);
    assert_eq!(state.picked_sms_id(), Some(&SmsId("146".to_string())));
    assert_eq!(state.displayed_sms_index(), None);
}

#[test]
fn sms_order_requests_snapshot_when_picked_sms_id_changes() {
    let mut state = MqttModemSmsState {
        sms_order: sms_ids(&["144", "145", "146"]),
        picked_sms_index: 2,
        displayed_sms_id: Some(SmsId("145".to_string())),
    };

    let picked_sms_id = state.apply_sms_order(sms_ids(&["144", "146"]));

    assert_eq!(picked_sms_id, Some(SmsId("146".to_string())));
    assert_eq!(state.picked_sms_index, 2);
    assert_eq!(state.picked_sms_id(), Some(&SmsId("146".to_string())));
    assert_eq!(state.displayed_sms_index(), None);
}

#[test]
fn sms_order_clamps_picked_index_when_list_shrinks() {
    let mut state = MqttModemSmsState {
        sms_order: sms_ids(&["144", "145", "146"]),
        picked_sms_index: 3,
        ..Default::default()
    };

    let request_sms_id = state.apply_sms_order(sms_ids(&["144"]));

    assert_eq!(state.picked_sms_index, 1);
    assert_eq!(state.picked_sms_id(), Some(&SmsId("144".to_string())));
    assert_eq!(request_sms_id, Some(SmsId("144".to_string())));
}

#[test]
fn sms_order_clears_empty_selection() {
    let mut state = MqttModemSmsState {
        sms_order: sms_ids(&["144"]),
        ..Default::default()
    };

    let request_sms_id = state.apply_sms_order(Vec::new());

    assert_eq!(state.picked_sms_index, 1);
    assert_eq!(state.picked_sms_id(), None);
    assert_eq!(request_sms_id, None);
}

#[test]
fn sms_order_does_not_clear_displayed_sms() {
    let mut state = MqttModemSmsState {
        sms_order: sms_ids(&["144"]),
        displayed_sms_id: Some(SmsId("144".to_string())),
        ..Default::default()
    };

    let _ = state.apply_sms_order(Vec::new());

    assert_eq!(state.delete_message(), Some(SmsId("144".to_string())));
    assert_eq!(state.displayed_sms_index(), None);
}

#[test]
fn remove_sms_after_picked_index_only_reduces_count() {
    let mut state = MqttModemSmsState {
        sms_order: sms_ids(&["144", "145", "146"]),
        picked_sms_index: 1,
        displayed_sms_id: Some(SmsId("144".to_string())),
    };

    let request_sms_id = state.remove_sms(&SmsId("146".to_string()));

    assert_eq!(request_sms_id, None);
    assert_eq!(state.picked_sms_index, 1);
    assert_eq!(state.picked_sms_id(), Some(&SmsId("144".to_string())));
    assert_eq!(state.delete_message(), Some(SmsId("144".to_string())));
}

#[test]
fn remove_sms_before_picked_index_shifts_pick_without_snapshot() {
    let mut state = MqttModemSmsState {
        sms_order: sms_ids(&["144", "145", "146"]),
        picked_sms_index: 2,
        displayed_sms_id: Some(SmsId("145".to_string())),
    };

    let request_sms_id = state.remove_sms(&SmsId("144".to_string()));

    assert_eq!(request_sms_id, None);
    assert_eq!(state.picked_sms_index, 1);
    assert_eq!(state.picked_sms_id(), Some(&SmsId("145".to_string())));
    assert_eq!(state.delete_message(), Some(SmsId("145".to_string())));
}

#[test]
fn remove_sms_at_picked_index_requests_replacement_snapshot() {
    let mut state = MqttModemSmsState {
        sms_order: sms_ids(&["144", "145"]),
        picked_sms_index: 1,
        displayed_sms_id: Some(SmsId("144".to_string())),
    };

    let request_sms_id = state.remove_sms(&SmsId("144".to_string()));

    assert_eq!(state.picked_sms_index, 1);
    assert_eq!(state.picked_sms_id(), Some(&SmsId("145".to_string())));
    assert_eq!(request_sms_id, Some(SmsId("145".to_string())));
    assert_eq!(state.delete_message(), Some(SmsId("144".to_string())));
}

#[test]
fn remove_last_sms_at_picked_index_keeps_displayed_id_but_has_no_valid_display() {
    let mut state = MqttModemSmsState {
        sms_order: sms_ids(&["144"]),
        picked_sms_index: 1,
        displayed_sms_id: Some(SmsId("144".to_string())),
    };

    let request_sms_id = state.remove_sms(&SmsId("144".to_string()));

    assert_eq!(request_sms_id, None);
    assert_eq!(state.picked_sms_index, 1);
    assert_eq!(state.picked_sms_id(), None);
    assert_eq!(state.delete_message(), Some(SmsId("144".to_string())));
    assert_eq!(state.displayed_sms_index(), None);
}

#[test]
fn update_picked_sms_index_returns_snapshot_request_only_when_changed() {
    let mut state = MqttModemSmsState {
        sms_order: sms_ids(&["144", "145", "146"]),
        picked_sms_index: 2,
        ..Default::default()
    };

    assert_eq!(state.update_picked_sms_index(2), None);
    assert_eq!(
        state.update_picked_sms_index(3),
        Some(SmsId("146".to_string()))
    );
    assert_eq!(state.picked_sms_index, 3);
}

#[test]
fn update_picked_sms_index_clamps_to_message_select_range() {
    let mut state = MqttModemSmsState {
        sms_order: sms_ids(&["144", "145", "146"]),
        picked_sms_index: 2,
        ..Default::default()
    };

    assert_eq!(
        state.update_picked_sms_index(99),
        Some(SmsId("146".to_string()))
    );
    assert_eq!(state.picked_sms_index, 3);
    assert_eq!(
        state.update_picked_sms_index(0),
        Some(SmsId("144".to_string()))
    );
    assert_eq!(state.picked_sms_index, 1);
}

#[test]
fn update_picked_sms_index_does_not_request_snapshot_for_empty_list() {
    let mut state = MqttModemSmsState::default();

    assert_eq!(state.update_picked_sms_index(99), None);
    assert_eq!(state.picked_sms_index, 1);
}

#[test]
fn accepts_snapshot_only_for_current_picked_index() {
    let mut state = MqttModemSmsState {
        sms_order: sms_ids(&["144", "145"]),
        picked_sms_index: 2,
        ..Default::default()
    };

    assert_eq!(state.apply_snapshot(&sms_snapshot("144")), None);
    assert_eq!(state.apply_snapshot(&sms_snapshot("145")), Some(2));
    assert_eq!(state.displayed_sms_id(), Some(&SmsId("145".to_string())));
    assert_eq!(state.delete_message(), Some(SmsId("145".to_string())));
}

fn sms_ids(values: &[&str]) -> Vec<SmsId> {
    values
        .iter()
        .map(|value| SmsId((*value).to_string()))
        .collect()
}

fn sms_snapshot(sms_id: &str) -> SmsSnapshot {
    SmsSnapshot {
        sms_id: SmsId(sms_id.to_string()),
        is_received: true,
        storage: "SIM".to_string(),
        timestamp: Some(OffsetDateTime::UNIX_EPOCH),
        number: Some("+70000000000".to_string()),
        text: Some("message".to_string()),
    }
}

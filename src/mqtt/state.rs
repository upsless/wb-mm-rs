use crate::dbus::SmsId;
use std::cmp::Ordering;

#[derive(Debug)]
pub(super) struct MqttModemSmsState {
    sms_order: Vec<SmsId>,
    picked_sms_index: u32,
    displayed_sms_id: Option<SmsId>,
}

impl Default for MqttModemSmsState {
    fn default() -> Self {
        Self {
            sms_order: Vec::new(),
            picked_sms_index: 1,
            displayed_sms_id: None,
        }
    }
}

impl MqttModemSmsState {
    pub(super) fn sms_count(&self) -> usize {
        self.sms_order.len()
    }

    pub(super) fn last_sms_id(&self) -> Option<&SmsId> {
        self.sms_order.last()
    }

    pub(super) fn picked_sms_index(&self) -> u32 {
        self.picked_sms_index
    }

    pub(super) fn apply_sms_order(&mut self, sms_order: Vec<SmsId>) -> Option<SmsId> {
        let old_picked_sms_id = self.picked_sms_id().cloned();

        self.sms_order = sms_order;
        self.picked_sms_index =
            clamp_message_select_index(self.picked_sms_index, self.sms_order.len());

        let picked_sms_id = self.picked_sms_id().cloned();
        (old_picked_sms_id != picked_sms_id)
            .then_some(picked_sms_id)
            .flatten()
    }

    pub(super) fn remove_sms(&mut self, sms_id: &SmsId) -> Option<SmsId> {
        let removed_index = self
            .sms_order
            .iter()
            .position(|current_sms_id| current_sms_id == sms_id)?;
        self.sms_order.remove(removed_index);

        let removed_index = u32::try_from(removed_index + 1).unwrap_or(u32::MAX);
        match removed_index.cmp(&self.picked_sms_index) {
            Ordering::Greater => None,
            Ordering::Less => {
                self.picked_sms_index = clamp_message_select_index(
                    self.picked_sms_index.saturating_sub(1),
                    self.sms_order.len(),
                );
                None
            }
            Ordering::Equal => {
                self.picked_sms_index =
                    clamp_message_select_index(self.picked_sms_index, self.sms_order.len());
                self.picked_sms_id().cloned()
            }
        }
    }

    fn picked_sms_id(&self) -> Option<&SmsId> {
        let picked_index = self.picked_sms_index.checked_sub(1)?;
        self.sms_order.get(picked_index as usize)
    }

    pub(super) fn update_picked_sms_index(&mut self, picked_sms_index: u32) -> Option<SmsId> {
        let picked_sms_index = clamp_message_select_index(picked_sms_index, self.sms_order.len());
        if self.picked_sms_index == picked_sms_index {
            return None;
        }

        self.picked_sms_index = picked_sms_index;
        self.picked_sms_id().cloned()
    }

    pub(super) fn accepts_sms_snapshot(&self, sms_id: &SmsId) -> bool {
        self.picked_sms_id() == Some(sms_id)
    }

    pub(super) fn accept_sms_snapshot(&mut self, sms_id: SmsId) -> u32 {
        self.displayed_sms_id = Some(sms_id);
        self.picked_sms_index
    }

    pub(super) fn accepts_sms_update(&self, sms_id: &SmsId) -> bool {
        self.displayed_sms_id.as_ref() == Some(sms_id)
    }

    pub(super) fn delete_message(&self) -> Option<SmsId> {
        self.displayed_sms_id.clone()
    }

    pub(super) fn has_displayed_sms(&self) -> bool {
        self.displayed_sms_id.is_some() && !self.sms_order.is_empty()
    }
}

pub(super) fn max_message_select_index(sms_count: usize) -> u32 {
    u32::try_from(sms_count).unwrap_or(u32::MAX).max(1)
}

fn clamp_message_select_index(index: u32, sms_count: usize) -> u32 {
    index.clamp(1, max_message_select_index(sms_count))
}

#[cfg(test)]
mod tests {
    use super::MqttModemSmsState;
    use crate::dbus::SmsId;

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
        assert!(!state.has_displayed_sms());
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
        assert!(!state.has_displayed_sms());
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

        assert!(!state.accepts_sms_snapshot(&SmsId("144".to_string())));
        assert!(state.accepts_sms_snapshot(&SmsId("145".to_string())));
        assert_eq!(state.accept_sms_snapshot(SmsId("145".to_string())), 2);
        assert!(state.accepts_sms_update(&SmsId("145".to_string())));
        assert_eq!(state.delete_message(), Some(SmsId("145".to_string())));
    }

    fn sms_ids(values: &[&str]) -> Vec<SmsId> {
        values
            .iter()
            .map(|value| SmsId((*value).to_string()))
            .collect()
    }
}

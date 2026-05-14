use crate::dbus::{SmsId, SmsSnapshot};
use std::cmp::Ordering;
use std::collections::HashMap;

use crate::dbus::ModemId;

#[derive(Debug, Default)]
pub(super) struct MqttSessionState {
    pub(super) manager_available: bool,
    pub(super) modems: HashMap<ModemId, MqttModemState>,
    pub(super) reverse_modem_indices: HashMap<u32, ModemId>,
}

#[derive(Debug, Default)]
pub(super) struct MqttModemState {
    pub(super) index: u32,
    is_active: bool,
    pub(super) sms_state: Option<MqttModemSmsState>,
}

#[derive(Debug)]
pub(super) struct MqttModemSmsState {
    sms_order: Vec<SmsId>,
    picked_sms_index: u32,
    last_published_sms_id: Option<SmsId>,
}

impl Default for MqttModemSmsState {
    fn default() -> Self {
        Self {
            sms_order: Vec::new(),
            picked_sms_index: 1,
            last_published_sms_id: None,
        }
    }
}

impl MqttSessionState {
    pub(super) fn ensure_modem_index(&mut self, modem_id: &ModemId) -> (u32, bool) {
        if let Some(modem) = self.modems.get(modem_id) {
            return (modem.index, false);
        }

        let mut candidate = 1;
        while self.modems.values().any(|modem| modem.index == candidate) {
            candidate += 1;
        }

        self.modems.insert(
            modem_id.clone(),
            MqttModemState {
                index: candidate,
                is_active: false,
                sms_state: None,
            },
        );
        self.reverse_modem_indices
            .insert(candidate, modem_id.clone());
        (candidate, true)
    }

    pub(super) fn remove_modem_index(&mut self, modem_id: &ModemId) -> Option<u32> {
        let modem_index = self.modems.remove(modem_id)?.index;
        self.reverse_modem_indices.remove(&modem_index);
        Some(modem_index)
    }

    pub(super) fn modem_index(&self, modem_id: &ModemId) -> Option<u32> {
        self.modems.get(modem_id).map(|modem| modem.index)
    }

    pub(super) fn modem_is_active(&self, modem_id: &ModemId) -> bool {
        self.modems
            .get(modem_id)
            .is_some_and(|modem| modem.is_active)
    }

    pub(super) fn set_modem_active(&mut self, modem_id: &ModemId, is_active: bool) {
        if let Some(modem) = self.modems.get_mut(modem_id) {
            modem.is_active = is_active;
        }
    }

    pub(super) fn modem_id_for_index(&self, modem_index: u32) -> Option<&ModemId> {
        self.reverse_modem_indices.get(&modem_index)
    }
}

impl MqttModemSmsState {
    pub(super) fn sms_count(&self) -> usize {
        self.sms_order.len()
    }

    pub(super) fn last_received_sms_id(&self) -> Option<&SmsId> {
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

    pub(super) fn apply_snapshot(&mut self, snapshot: &SmsSnapshot) -> Option<u32> {
        if self.picked_sms_id() != Some(&snapshot.sms_id) {
            return None;
        }

        self.last_published_sms_id = Some(snapshot.sms_id.clone());
        Some(self.picked_sms_index)
    }

    pub(super) fn last_published_sms_id(&self) -> Option<&SmsId> {
        self.last_published_sms_id.as_ref()
    }

    pub(super) fn displayed_sms_index(&self) -> Option<u32> {
        let displayed_sms_id = self.last_published_sms_id.as_ref()?;
        self.sms_order
            .iter()
            .position(|sms_id| sms_id == displayed_sms_id)
            .map(|index| u32::try_from(index + 1).unwrap_or(u32::MAX))
    }

    pub(super) fn delete_message(&self) -> Option<SmsId> {
        self.last_published_sms_id.clone()
    }
}

pub(super) fn max_message_select_index(sms_count: usize) -> u32 {
    u32::try_from(sms_count).unwrap_or(u32::MAX).max(1)
}

fn clamp_message_select_index(index: u32, sms_count: usize) -> u32 {
    index.clamp(1, max_message_select_index(sms_count))
}

#[cfg(test)]
mod tests;

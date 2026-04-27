mod logics;
mod r#loop;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::exchange::{DbusCommand, DbusEvent};

pub use logics::{
    ModemId, ModemManagerStatus, ModemSnapshot, ModemUpdate, SmsId, SmsSnapshot, SmsUpdate,
    format_timestamp_for_wb,
};

pub async fn run(
    dbus_address: Option<String>,
    shutdown_rx: watch::Receiver<bool>,
    command_rx: mpsc::Receiver<DbusCommand>,
    event_tx: mpsc::Sender<DbusEvent>,
) -> Result<()> {
    r#loop::run(dbus_address, shutdown_rx, command_rx, event_tx).await
}

pub fn modemmanager_not_found_message() -> &'static str {
    logics::modemmanager_status_message(logics::ModemManagerStatus::NotFound)
}

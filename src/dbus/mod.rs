mod logics;
mod r#loop;

use anyhow::Result;
pub use logics::{ModemId, ModemManagerStatus, ModemSnapshot, ModemUpdate};
use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::exchange::DbusEvent;

pub async fn run(
    dbus_address: Option<String>,
    shutdown_rx: watch::Receiver<bool>,
    event_tx: mpsc::Sender<DbusEvent>,
) -> Result<()> {
    r#loop::run(dbus_address, shutdown_rx, event_tx).await
}

pub fn modemmanager_not_found_message() -> &'static str {
    logics::modemmanager_status_message(logics::ModemManagerStatus::NotFound)
}

mod connection;
mod manager;
mod modem;
mod runtime;
mod schema;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::exchange::{DbusCommand, DbusEvent};

pub use schema::{
    ManagerUpdate, ModemId, ModemInfo, ModemManagerStatus, ModemUpdate, SmsId, SmsPropertyChange,
    SmsSnapshot, SmsUpdate, format_timestamp_for_wb,
};

pub async fn run(
    dbus_address: Option<String>,
    shutdown_rx: watch::Receiver<bool>,
    command_rx: mpsc::Receiver<DbusCommand>,
    event_tx: mpsc::Sender<DbusEvent>,
) -> Result<()> {
    connection::run(dbus_address, shutdown_rx, command_rx, event_tx).await
}

pub fn modemmanager_not_found_message() -> &'static str {
    schema::manager_deleted_message()
}

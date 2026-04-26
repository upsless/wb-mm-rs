mod logics;
mod r#loop;

use anyhow::Result;
use tokio::sync::watch;

pub async fn run(dbus_address: Option<String>, shutdown_rx: watch::Receiver<bool>) -> Result<()> {
    r#loop::run(dbus_address, shutdown_rx).await
}

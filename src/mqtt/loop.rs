use anyhow::Result;
use tokio::sync::watch;
use tracing::info;

use crate::mqtt::logics;

pub async fn run(mut shutdown_rx: watch::Receiver<bool>) -> Result<()> {
    info!("{}", logics::mqtt_connected_message());

    wait_for_shutdown(&mut shutdown_rx).await?;

    info!("{}", logics::mqtt_stopped_message());

    Ok(())
}

async fn wait_for_shutdown(shutdown_rx: &mut watch::Receiver<bool>) -> Result<()> {
    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        shutdown_rx.changed().await?;
    }
}

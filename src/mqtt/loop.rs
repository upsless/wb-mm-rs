use anyhow::Result;
use tokio::sync::watch;
use tracing::debug;

use crate::mqtt::logics;

/// Stage-0 MQTT stub.
///
/// For now it only models lifecycle: "connected", then "wait until asked to
/// stop", then "stopped". This keeps the orchestration shape in place before
/// we add a real MQTT client.
pub async fn run(mut shutdown_rx: watch::Receiver<bool>) -> Result<()> {
    debug!("{}", logics::mqtt_connected_message());

    wait_for_shutdown(&mut shutdown_rx).await?;

    debug!("{}", logics::mqtt_stopped_message());

    Ok(())
}

/// Mirrors the DBus loop helper so both subsystems react to shutdown in the
/// same way.
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

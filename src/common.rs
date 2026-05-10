use anyhow::Result;
use tokio::sync::watch;

/// Waits until the shared shutdown flag becomes true or all senders disappear.
pub async fn wait_for_shutdown(shutdown_rx: &mut watch::Receiver<bool>) -> Result<()> {
    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        if shutdown_rx.changed().await.is_err() {
            return Ok(());
        }
    }
}

mod logics;
mod r#loop;

use anyhow::Result;
use tokio::sync::watch;

pub async fn run(shutdown_rx: watch::Receiver<bool>) -> Result<()> {
    r#loop::run(shutdown_rx).await
}

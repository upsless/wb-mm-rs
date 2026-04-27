mod logics;
mod r#loop;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::exchange::MqttCommand;

pub async fn run(
    mqtt_address: Option<String>,
    shutdown_rx: watch::Receiver<bool>,
    command_rx: mpsc::Receiver<MqttCommand>,
) -> Result<()> {
    r#loop::run(mqtt_address, shutdown_rx, command_rx).await
}

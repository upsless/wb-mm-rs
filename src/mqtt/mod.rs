mod logics;
mod r#loop;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::exchange::{MqttCommand, MqttEvent};

pub async fn run(
    mqtt_address: Option<String>,
    shutdown_rx: watch::Receiver<bool>,
    command_rx: mpsc::Receiver<MqttCommand>,
    event_tx: mpsc::Sender<MqttEvent>,
) -> Result<()> {
    r#loop::run(mqtt_address, shutdown_rx, command_rx, event_tx).await
}

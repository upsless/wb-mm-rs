use std::time::Duration;

use anyhow::{Context, Result};
use rumqttc::{Event, Packet, Publish};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::debug;

use crate::mqtt::schema;

pub(super) const LOG_TARGET: &str = "MQTT";
pub(super) const MQTT_GRACEFUL_CLEANUP_FLUSH_DELAY: Duration = Duration::from_millis(500);

pub(super) async fn run_eventloop(
    stop_rx: watch::Receiver<bool>,
    mut eventloop: rumqttc::EventLoop,
    incoming_publish_tx: mpsc::Sender<Publish>,
) -> Result<()> {
    let mut connected = false;

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                if !connected {
                    connected = true;
                    debug!(target: LOG_TARGET, "{}", schema::mqtt_connected_message());
                }
            }
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                if incoming_publish_tx.send(publish).await.is_err() {
                    return Ok(());
                }
            }
            Ok(Event::Outgoing(rumqttc::Outgoing::Disconnect)) if *stop_rx.borrow() => {
                return Ok(());
            }
            Ok(_) => {}
            Err(_) if *stop_rx.borrow() => {
                return Ok(());
            }
            Err(err) => {
                return Err(err).context("failed to poll MQTT event loop");
            }
        }
    }
}

pub(super) fn eventloop_result(
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    result.map_err(|error| anyhow::anyhow!("MQTT event loop task join failed: {error}"))?
}

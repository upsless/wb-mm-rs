use std::time::Duration;

use anyhow::{Context, Result, bail};
use rumqttc::{AsyncClient, Event, LastWill, MqttOptions, Packet, Publish, QoS, Transport};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::debug;

use crate::exchange::{MqttCommand, MqttEvent};
use crate::mqtt::frontend::MqttFrontend;
use crate::mqtt::publish::switch_payload;
use crate::mqtt::schema;
use crate::shutdown::wait_for_shutdown;

pub(super) const LOG_TARGET: &str = "MQTT";
const DEFAULT_MQTT_ADDRESS: &str = "unix:///var/run/mosquitto/mosquitto.sock";
const DEFAULT_MQTT_PORT: u16 = 1883;
const MQTT_CLIENT_ID_PREFIX: &str = "wb-mm-mqtt";
const MQTT_KEEP_ALIVE: Duration = Duration::from_secs(60);
const MQTT_REQUEST_QUEUE_CAPACITY: usize = 16;
const MQTT_INCOMING_CHANNEL_CAPACITY: usize = 32;
pub(super) const MQTT_GRACEFUL_CLEANUP_FLUSH_DELAY: Duration = Duration::from_millis(500);

/// MQTT lifecycle loop with a real broker connection, retained publishes and
/// incoming `/on` command handling.
pub async fn run(
    mqtt_address: Option<String>,
    mut shutdown_rx: watch::Receiver<bool>,
    mut command_rx: mpsc::Receiver<MqttCommand>,
    mqtt_event_tx: mpsc::Sender<MqttEvent>,
) -> Result<()> {
    let mqtt_options = build_mqtt_options(mqtt_address.as_deref())?;
    let (client, eventloop) = AsyncClient::new(mqtt_options, MQTT_REQUEST_QUEUE_CAPACITY);
    let mut frontend = MqttFrontend::new(client.clone());
    let (eventloop_stop_tx, eventloop_stop_rx) = watch::channel(false);
    let (incoming_publish_tx, mut incoming_publish_rx) =
        mpsc::channel(MQTT_INCOMING_CHANNEL_CAPACITY);
    let mut eventloop_task = tokio::spawn(run_eventloop(
        eventloop_stop_rx,
        eventloop,
        incoming_publish_tx,
    ));
    frontend.ensure_main_device().await?;

    loop {
        tokio::select! {
            result = wait_for_shutdown(&mut shutdown_rx) => {
                result?;
                frontend.stop(&eventloop_stop_tx, &mut eventloop_task).await?;
                break;
            }
            maybe_command = command_rx.recv() => {
                let Some(command) = maybe_command else {
                    frontend.stop(&eventloop_stop_tx, &mut eventloop_task).await?;
                    break;
                };
                frontend.handle_command(command, &mqtt_event_tx).await?;
            }
            maybe_publish = incoming_publish_rx.recv() => {
                let Some(publish) = maybe_publish else {
                    return Ok(());
                };
                frontend
                    .handle_incoming_publish(publish, &mqtt_event_tx)
                    .await?;
            }
            result = &mut eventloop_task => {
                return eventloop_result(result);
            }
        }
    }

    debug!(target: LOG_TARGET, "{}", schema::mqtt_stopped_message());

    Ok(())
}

fn build_mqtt_options(mqtt_address: Option<&str>) -> Result<MqttOptions> {
    let mqtt_address = mqtt_address.unwrap_or(DEFAULT_MQTT_ADDRESS);
    let client_id = format!("{MQTT_CLIENT_ID_PREFIX}-{}", std::process::id());

    let mut mqtt_options = match parse_mqtt_endpoint(mqtt_address)? {
        MqttEndpoint::Unix { path } => {
            let mut options = MqttOptions::new(client_id, path, DEFAULT_MQTT_PORT);
            options.set_transport(Transport::unix());
            options
        }
        MqttEndpoint::Tcp { host, port } => MqttOptions::new(client_id, host, port),
    };

    mqtt_options.set_keep_alive(MQTT_KEEP_ALIVE);
    // If the daemon dies unexpectedly, the only user-facing trust marker must
    // flip to unavailable without waiting for any explicit cleanup path.
    mqtt_options.set_last_will(LastWill::new(
        schema::mm_availability_topic(),
        switch_payload(false).as_str(),
        QoS::AtMostOnce,
        true,
    ));

    Ok(mqtt_options)
}

async fn run_eventloop(
    stop_rx: watch::Receiver<bool>,
    mut eventloop: rumqttc::EventLoop,
    incoming_publish_tx: mpsc::Sender<Publish>,
) -> Result<()> {
    let mut connected = false;
    let stop_rx = stop_rx;

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

#[derive(Debug, Clone, PartialEq, Eq)]
enum MqttEndpoint {
    Unix { path: String },
    Tcp { host: String, port: u16 },
}

fn parse_mqtt_endpoint(mqtt_address: &str) -> Result<MqttEndpoint> {
    let (scheme, remainder) = mqtt_address
        .split_once("://")
        .with_context(|| format!("invalid MQTT address `{mqtt_address}`: missing scheme"))?;

    match scheme {
        "unix" => {
            if remainder.is_empty() {
                bail!("invalid MQTT address `{mqtt_address}`: empty unix socket path");
            }

            Ok(MqttEndpoint::Unix {
                path: remainder.to_string(),
            })
        }
        "tcp" | "mqtt" | "mqtt-tcp" => {
            let broker = remainder
                .split('/')
                .next()
                .filter(|broker| !broker.is_empty())
                .with_context(|| format!("invalid MQTT address `{mqtt_address}`: empty broker"))?;

            let (host, port) = match broker.rsplit_once(':') {
                Some((host, port)) if !host.is_empty() => (
                    host.to_string(),
                    port.parse::<u16>().with_context(|| {
                        format!("invalid MQTT address `{mqtt_address}`: bad port `{port}`")
                    })?,
                ),
                _ => (broker.to_string(), DEFAULT_MQTT_PORT),
            };

            Ok(MqttEndpoint::Tcp { host, port })
        }
        _ => bail!(
            "unsupported MQTT address scheme `{scheme}` in `{mqtt_address}`; supported schemes are unix://, tcp://, mqtt:// and mqtt-tcp://"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{MqttEndpoint, parse_mqtt_endpoint};

    #[test]
    fn parses_unix_endpoint() {
        let endpoint = parse_mqtt_endpoint("unix:///var/run/mosquitto/mosquitto.sock").unwrap();
        assert_eq!(
            endpoint,
            MqttEndpoint::Unix {
                path: "/var/run/mosquitto/mosquitto.sock".to_string(),
            }
        );
    }

    #[test]
    fn parses_tcp_endpoint_with_default_port() {
        let endpoint = parse_mqtt_endpoint("tcp://wb.loc").unwrap();
        assert_eq!(
            endpoint,
            MqttEndpoint::Tcp {
                host: "wb.loc".to_string(),
                port: 1883,
            }
        );
    }
}

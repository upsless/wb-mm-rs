mod frontend;
mod logstrings;
mod r#loop;
mod publish;
mod schema;
mod state;

use anyhow::{Context, Result, bail};
use rumqttc::{AsyncClient, LastWill, MqttOptions, QoS, Transport};
use tokio::sync::{mpsc, watch};
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info};

use crate::common::{
    MQTT_INCOMING_CHANNEL_CAPACITY, MQTT_RECONNECT_FAST_ATTEMPTS, MQTT_RECONNECT_FAST_INTERVAL,
    MQTT_RECONNECT_SLOW_INTERVAL, MQTT_REQUEST_QUEUE_CAPACITY, wait_for_shutdown,
};
use crate::domain::{DbusCommand, DbusEvent};
use crate::mqtt::frontend::MqttFrontend;
use crate::mqtt::publish::switch_payload;

const DEFAULT_MQTT_ADDRESS: &str = "unix:///var/run/mosquitto/mosquitto.sock";
const DEFAULT_MQTT_PORT: u16 = 1883;
const MQTT_CLIENT_ID_PREFIX: &str = "wb-mm-mqtt";
const MQTT_KEEP_ALIVE: std::time::Duration = std::time::Duration::from_secs(60);

pub async fn run_lifecycle(
    mqtt_address: Option<String>,
    mut shutdown_rx: watch::Receiver<bool>,
    mut core_event_rx: mpsc::Receiver<DbusEvent>,
    core_command_tx: mpsc::Sender<DbusCommand>,
    dbus_resync_tx: watch::Sender<u64>,
) -> Result<()> {
    let mut retry_attempt = 1;

    loop {
        let mqtt_options = build_mqtt_options(mqtt_address.as_deref())?;
        let (client, eventloop) = AsyncClient::new(mqtt_options, MQTT_REQUEST_QUEUE_CAPACITY);
        let mut frontend = MqttFrontend::new(client.clone());
        let (eventloop_stop_tx, eventloop_stop_rx) = watch::channel(false);
        let (incoming_publish_tx, incoming_publish_rx) =
            mpsc::channel(MQTT_INCOMING_CHANNEL_CAPACITY);
        let mut eventloop_task = tokio::spawn(r#loop::run_eventloop(
            eventloop_stop_rx,
            eventloop,
            incoming_publish_tx,
        ));

        if let Err(err) = frontend.ensure_main_device().await {
            let transport_closed = is_transport_closed_error(&err);
            teardown_failed_session(&frontend, &eventloop_stop_tx, &mut eventloop_task).await;

            if transport_closed {
                let delay = reconnect_delay(retry_attempt);
                info!(
                    target: logstrings::LOG_TARGET,
                    "MQTT transport disappeared during session bootstrap, retrying in {} second(s) (attempt {}).",
                    delay.as_secs(),
                    retry_attempt
                );
                retry_attempt += 1;
                if wait_until_retry_or_shutdown(delay, &mut shutdown_rx, &mut core_event_rx).await?
                {
                    break;
                }
                continue;
            }

            return Err(err.context("failed to bootstrap MQTT session"));
        }

        drop_stale_core_events(&mut core_event_rx);
        request_dbus_resync(&dbus_resync_tx);

        match run_session(
            &mut frontend,
            &eventloop_stop_tx,
            &mut eventloop_task,
            &mut shutdown_rx,
            &mut core_event_rx,
            &core_command_tx,
            incoming_publish_rx,
        )
        .await
        {
            Ok(SessionExit::Shutdown) => break,
            Ok(SessionExit::CoreEventChannelClosed) => {
                return Ok(());
            }
            Ok(SessionExit::TransportEnded) => {
                let delay = reconnect_delay(retry_attempt);
                info!(
                    target: logstrings::LOG_TARGET,
                    "MQTT connection lost, retrying in {} second(s) (attempt {}).",
                    delay.as_secs(),
                    retry_attempt
                );
                retry_attempt += 1;
                if wait_until_retry_or_shutdown(delay, &mut shutdown_rx, &mut core_event_rx).await?
                {
                    break;
                }
            }
            Err(err) => {
                let delay = reconnect_delay(retry_attempt);
                error!(
                    target: logstrings::LOG_TARGET,
                    "MQTT session failed: {err:#}. Retrying in {} second(s) (attempt {}).",
                    delay.as_secs(),
                    retry_attempt
                );
                retry_attempt += 1;
                if wait_until_retry_or_shutdown(delay, &mut shutdown_rx, &mut core_event_rx).await?
                {
                    break;
                }
            }
        }
    }

    debug!(target: logstrings::LOG_TARGET, "{}", logstrings::mqtt_stopped_message());

    Ok(())
}

async fn teardown_failed_session(
    frontend: &MqttFrontend,
    eventloop_stop_tx: &watch::Sender<bool>,
    eventloop_task: &mut tokio::task::JoinHandle<Result<()>>,
) {
    let _ = eventloop_stop_tx.send(true);
    let _ = frontend.disconnect_transport().await;
    eventloop_task.abort();
    let _ = eventloop_task.await;
}

fn is_transport_closed_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        let text = cause.to_string();
        text.contains("Failed to send mqtt requests to eventloop")
            || text.contains("failed to poll MQTT event loop")
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionExit {
    Shutdown,
    TransportEnded,
    CoreEventChannelClosed,
}

async fn run_session(
    frontend: &mut MqttFrontend,
    eventloop_stop_tx: &watch::Sender<bool>,
    eventloop_task: &mut tokio::task::JoinHandle<Result<()>>,
    shutdown_rx: &mut watch::Receiver<bool>,
    core_event_rx: &mut mpsc::Receiver<DbusEvent>,
    core_command_tx: &mpsc::Sender<DbusCommand>,
    mut incoming_publish_rx: mpsc::Receiver<rumqttc::Publish>,
) -> Result<SessionExit> {
    loop {
        tokio::select! {
            result = wait_for_shutdown(shutdown_rx) => {
                result?;
                frontend.stop(eventloop_stop_tx, eventloop_task).await?;
                return Ok(SessionExit::Shutdown);
            }
            maybe_event = core_event_rx.recv() => {
                let Some(event) = maybe_event else {
                    frontend.stop(eventloop_stop_tx, eventloop_task).await?;
                    return Ok(SessionExit::CoreEventChannelClosed);
                };
                frontend
                    .handle_dbus_event(event, Some(core_command_tx))
                    .await?;
            }
            maybe_publish = incoming_publish_rx.recv() => {
                let Some(publish) = maybe_publish else {
                    return Ok(SessionExit::TransportEnded);
                };
                frontend
                    .handle_incoming_publish(publish, Some(core_command_tx))
                    .await?;
            }
            result = &mut *eventloop_task => {
                match r#loop::eventloop_result(result) {
                    Ok(()) => return Ok(SessionExit::TransportEnded),
                    Err(err) => return Err(err),
                }
            }
        }
    }
}

fn reconnect_delay(attempt: u32) -> Duration {
    if attempt <= MQTT_RECONNECT_FAST_ATTEMPTS {
        MQTT_RECONNECT_FAST_INTERVAL
    } else {
        MQTT_RECONNECT_SLOW_INTERVAL
    }
}

async fn wait_until_retry_or_shutdown(
    delay: Duration,
    shutdown_rx: &mut watch::Receiver<bool>,
    core_event_rx: &mut mpsc::Receiver<DbusEvent>,
) -> Result<bool> {
    let sleep = sleep(delay);
    tokio::pin!(sleep);

    loop {
        tokio::select! {
            result = wait_for_shutdown(shutdown_rx) => {
                result?;
                return Ok(true);
            }
            maybe_event = core_event_rx.recv() => {
                if maybe_event.is_none() {
                    return Ok(false);
                }
            }
            _ = &mut sleep => return Ok(false),
        }
    }
}

fn drop_stale_core_events(core_event_rx: &mut mpsc::Receiver<DbusEvent>) {
    let mut dropped = 0usize;

    while core_event_rx.try_recv().is_ok() {
        dropped += 1;
    }

    if dropped > 0 {
        debug!(
            target: logstrings::LOG_TARGET,
            "Dropped {dropped} stale Core event(s) before rebuilding MQTT session state"
        );
    }
}

fn request_dbus_resync(dbus_resync_tx: &watch::Sender<u64>) {
    let next_generation = dbus_resync_tx.borrow().saturating_add(1);
    let _ = dbus_resync_tx.send(next_generation);
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

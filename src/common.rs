use anyhow::Result;
use tokio::sync::watch;
use tokio::time::Duration;

/// Static application settings collected from CLI flags and shared across
/// long-lived runtime components.
#[derive(Debug, Clone, Default)]
pub struct AppConfig {
    pub dbus_address: Option<String>,
    pub mqtt_address: Option<String>,
    pub allow_outgoing_sms: bool,
    pub log_level: Option<String>,
}

impl AppConfig {
    pub fn dbus_address(&self) -> Option<&str> {
        self.dbus_address.as_deref()
    }

    pub fn mqtt_address(&self) -> Option<&str> {
        self.mqtt_address.as_deref()
    }

    pub fn log_level(&self) -> Option<&str> {
        self.log_level.as_deref()
    }
}

/// Fast reconnect delay for the top-level MQTT supervisor loop.
pub const MQTT_RECONNECT_FAST_INTERVAL: Duration = Duration::from_secs(5);
/// Slow reconnect delay after repeated MQTT failures.
pub const MQTT_RECONNECT_SLOW_INTERVAL: Duration = Duration::from_secs(60);
/// Number of fast MQTT reconnect attempts before switching to the slow delay.
pub const MQTT_RECONNECT_FAST_ATTEMPTS: u32 = 24;

/// Capacity of the DBus -> MQTT event channel owned by the supervisor.
pub const DBUS_EVENT_CHANNEL_CAPACITY: usize = 32;
/// Capacity of the per-session DBus command channel exposed to MQTT.
pub const DBUS_COMMAND_CHANNEL_CAPACITY: usize = 32;
/// Capacity of the rumqttc request queue used by the MQTT client.
pub const MQTT_REQUEST_QUEUE_CAPACITY: usize = 16;
/// Capacity of the channel that forwards incoming MQTT publishes from the event loop.
pub const MQTT_INCOMING_CHANNEL_CAPACITY: usize = 32;
/// Grace period that lets retained cleanup publishes flush before disconnect.
pub const MQTT_GRACEFUL_CLEANUP_FLUSH_DELAY: Duration = Duration::from_millis(500);

/// Fast reconnect delay for DBus while the current MQTT session is still alive.
pub const DBUS_RECONNECT_FAST_INTERVAL: Duration = Duration::from_secs(5);
/// Slow reconnect delay after repeated DBus failures within one MQTT session.
pub const DBUS_RECONNECT_SLOW_INTERVAL: Duration = Duration::from_secs(60);
/// Number of fast DBus reconnect attempts before switching to the slow delay.
pub const DBUS_RECONNECT_FAST_ATTEMPTS: u32 = 24;

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

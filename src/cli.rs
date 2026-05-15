use clap::ArgAction;
use clap::Parser;

use crate::common::AppConfig;

#[derive(Debug, Parser, Clone)]
#[command(name = "wb-mm-mqtt")]
#[command(about = "WirenBoard ModemManager MQTT bridge daemon")]
#[command(version)]
#[command(disable_help_flag = true)]
#[command(disable_version_flag = true)]
pub struct Cli {
    #[arg(
        long,
        help = "Remote DBus address to connect to (default: local system bus)"
    )]
    pub dbus_address: Option<String>,

    #[arg(
        long,
        help = "MQTT broker address to connect to (default: local MQTT broker)"
    )]
    pub mqtt_address: Option<String>,

    #[arg(long, help = "TRACE | DEBUG | INFO (default) | WARN | ERROR")]
    pub log_level: Option<String>,

    #[arg(
        long,
        help = "Enable outgoing SMS functionality. Use it with care! Any SMS can be sent to any phone number from your modem by WB users and scripts!"
    )]
    pub allow_outgoing_sms: bool,

    #[arg(short = 'h', long = "help", action = ArgAction::Help, help = "Print this help")]
    pub help: Option<bool>,

    #[arg(short = 'V', long = "version", action = ArgAction::Version, help = "Print version")]
    pub version: Option<bool>,
}

impl From<Cli> for AppConfig {
    fn from(value: Cli) -> Self {
        AppConfig::new(
            value.dbus_address,
            value.mqtt_address,
            value.allow_outgoing_sms,
            value.log_level,
        )
    }
}

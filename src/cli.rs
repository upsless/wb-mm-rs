use clap::Parser;

#[derive(Debug, Parser, Clone)]
#[command(name = "wb-mm-mqtt")]
#[command(about = "Wiren Board ModemManager MQTT bridge daemon")]
pub struct Cli {
    #[arg(long)]
    pub dbus_address: Option<String>,

    #[arg(long)]
    pub mqtt_address: Option<String>,

    #[arg(long = "command-number")]
    pub command_numbers: Vec<String>,
}

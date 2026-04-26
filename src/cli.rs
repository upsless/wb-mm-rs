use clap::Parser;

#[derive(Debug, Parser, Clone)]
#[command(name = "wb-mm-mqtt")]
#[command(about = "Wiren Board ModemManager MQTT bridge daemon")]
pub struct Cli {
    #[arg(long)]
    pub dbus_address: Option<String>,
}

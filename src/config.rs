use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::cli::Cli;

pub const DEFAULT_CONFIG_PATH: &str = "/etc/wb-mm-mqtt.conf";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeSettings {
    pub dbus_address: Option<String>,
    pub mqtt_address: Option<String>,
    pub log_level: Option<String>,
    pub allow_outgoing_sms: bool,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    #[serde(rename = "logLevel", alias = "log_level")]
    log_level: Option<String>,
    #[serde(rename = "allowOutgoingSms", alias = "allow_outgoing_sms")]
    allow_outgoing_sms: Option<bool>,
}

impl RuntimeSettings {
    pub fn load(cli: Cli) -> Result<Self> {
        let file_config = load_file_config(cli.config.as_deref().unwrap_or(DEFAULT_CONFIG_PATH))?;
        Ok(Self::merge(cli, file_config))
    }

    fn merge(cli: Cli, file_config: Option<FileConfig>) -> Self {
        Self {
            dbus_address: cli.dbus_address,
            mqtt_address: cli.mqtt_address,
            log_level: cli.log_level.or_else(|| {
                file_config
                    .as_ref()
                    .and_then(|config| config.log_level.clone())
            }),
            allow_outgoing_sms: cli.allow_outgoing_sms
                || file_config
                    .as_ref()
                    .and_then(|config| config.allow_outgoing_sms)
                    .unwrap_or(false),
        }
    }
}

fn load_file_config(path: &str) -> Result<Option<FileConfig>> {
    let path = Path::new(path);
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "failed to read runtime configuration file {}",
                    path.display()
                )
            });
        }
    };

    let config = serde_json::from_str::<FileConfig>(&contents).with_context(|| {
        format!(
            "failed to parse runtime configuration file {} as JSON",
            path.display()
        )
    })?;
    Ok(Some(config))
}

#[cfg(test)]
mod tests {
    use super::{FileConfig, RuntimeSettings};
    use crate::cli::Cli;

    #[test]
    fn supports_wb_style_camel_case_config_fields() {
        let config: FileConfig = serde_json::from_str(
            r#"{
                "logLevel": "debug",
                "allowOutgoingSms": true
            }"#,
        )
        .unwrap();

        assert_eq!(config.log_level.as_deref(), Some("debug"));
        assert_eq!(config.allow_outgoing_sms, Some(true));
    }

    #[test]
    fn cli_values_override_file_values() {
        let cli = Cli {
            config: None,
            dbus_address: None,
            mqtt_address: None,
            log_level: Some("warn".to_string()),
            allow_outgoing_sms: true,
            help: None,
            version: None,
        };
        let file_config = FileConfig {
            log_level: Some("debug".to_string()),
            allow_outgoing_sms: Some(false),
        };

        let settings = RuntimeSettings::merge(cli, Some(file_config));

        assert_eq!(settings.log_level.as_deref(), Some("warn"));
        assert!(settings.allow_outgoing_sms);
    }
}

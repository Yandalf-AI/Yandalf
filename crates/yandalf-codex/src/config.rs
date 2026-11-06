use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CodexConfig {
    pub endpoint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_token_env: Option<String>,
    pub request_timeout_secs: u64,
}

impl Default for CodexConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            bearer_token_env: None,
            request_timeout_secs: 30,
        }
    }
}

impl CodexConfig {
    pub fn validate(&self) -> Result<()> {
        if self.request_timeout_secs == 0 {
            bail!("Codex request timeout must be positive");
        }
        if let Some(path) = self.endpoint.strip_prefix("unix://") {
            if !Path::new(path).is_absolute() {
                bail!("Codex Unix socket path must be absolute");
            }
        } else {
            if !self.endpoint.starts_with("ws://") && !self.endpoint.starts_with("wss://") {
                bail!("Codex endpoint must be an explicit unix://, ws:// or wss:// address");
            }
            let request = self
                .endpoint
                .as_str()
                .into_client_request()
                .context("invalid Codex endpoint")?;
            if request
                .uri()
                .authority()
                .is_some_and(|authority| authority.as_str().contains('@'))
            {
                bail!("Codex endpoint must not contain credentials; use bearer_token_env");
            }
        }
        if self
            .bearer_token_env
            .as_ref()
            .is_some_and(|name| name.is_empty() || name.contains('=') || name.contains('\0'))
        {
            bail!("invalid Codex bearer token environment variable name");
        }
        Ok(())
    }
}

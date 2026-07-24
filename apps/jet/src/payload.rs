//! Transaction payload handling for the Jet ecosystem.
//!
//! This module implements a dual-format transaction system:
//! 1. Legacy format: Simple binary serialized transactions for backward compatibility
//! 2. New format: Structured format with additional metadata and configuration
//!
//! The system ensures backward compatibility in the following ways:
//! - New -> Old: New clients can detect old servers and fall back to legacy format
//! - Old -> New: Old clients send legacy format which new servers can still process
//! - New -> New: Full feature support with structured format
//!
//! Version detection happens at the protocol level, allowing graceful degradation
//! of features when communicating with older versions.

use {
    anyhow::Result,
    serde::{Deserialize, Serialize},
    solana_client::rpc_config::RpcSendTransactionConfig,
    solana_pubkey::Pubkey,
    std::str::FromStr,
    tracing::debug,
};

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JetRpcSendTransactionConfig {
    #[serde(flatten)]
    pub config: RpcSendTransactionConfig,
    #[serde(default, deserialize_with = "deserialize_forwarding_policies")]
    pub forwarding_policies: Vec<Pubkey>,
}

fn deserialize_forwarding_policies<'de, D>(deserializer: D) -> Result<Vec<Pubkey>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let vec: Vec<String> = Vec::deserialize(deserializer)?;
    let result = vec
        .into_iter()
        .filter_map(|s| Pubkey::from_str(&s).ok())
        .collect();

    Ok(result)
}

impl JetRpcSendTransactionConfig {
    pub fn new(
        config: Option<RpcSendTransactionConfig>,
        forwarding_policies: Option<Vec<String>>,
    ) -> Self {
        let config = config.unwrap_or_default();
        let forwarding_policies = forwarding_policies
            .unwrap_or_default()
            .iter()
            .filter_map(|key| Pubkey::from_str(key).ok())
            .collect::<Vec<Pubkey>>();
        debug!("Forwarding policies: {:?}", forwarding_policies);

        Self {
            config,
            forwarding_policies,
        }
    }
}

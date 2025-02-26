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
    crate::{
        proto::jet::{
            publish_transaction::Payload, PublishTransaction, TransactionConfig, TransactionWrapper,
        },
        util::ms_since_epoch,
    },
    anyhow::{Context, Result},
    solana_client::rpc_config::RpcSendTransactionConfig,
    solana_sdk::transaction::VersionedTransaction,
    solana_transaction_status::UiTransactionEncoding,
};

#[derive(Debug)]
pub enum TransactionPayload {
    Legacy(Vec<u8>),
    New(TransactionWrapper),
}

impl TransactionPayload {
    pub fn from_proto(tx: PublishTransaction) -> Result<Self> {
        match tx.payload {
            Some(Payload::LegacyPayload(bytes)) => Ok(Self::Legacy(bytes)),
            Some(Payload::NewPayload(wrapper)) => Ok(Self::New(wrapper)),
            None => Err(anyhow::anyhow!("Empty transaction payload")),
        }
    }

    pub fn to_proto(&self) -> PublishTransaction {
        match self {
            Self::Legacy(bytes) => PublishTransaction {
                payload: Some(Payload::LegacyPayload(bytes.clone())),
            },
            Self::New(wrapper) => PublishTransaction {
                payload: Some(Payload::NewPayload(wrapper.clone())),
            },
        }
    }

    pub fn decode(&self) -> Result<(VersionedTransaction, Option<RpcSendTransactionConfig>)> {
        match self {
            Self::Legacy(bytes) => {
                let tx = bincode::deserialize(bytes)
                    .context("Failed to deserialize legacy transaction")?;
                Ok((tx, None))
            }
            Self::New(wrapper) => {
                let tx = bincode::deserialize(&wrapper.transaction)
                    .context("Failed to deserialize new transaction")?;
                let config = wrapper.config.as_ref().map(convert_transaction_config);
                Ok((tx, config))
            }
        }
    }
    pub fn from_transaction(
        transaction: &VersionedTransaction,
        config: RpcSendTransactionConfig,
    ) -> Result<Self> {
        let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Base64);

        match encoding {
            UiTransactionEncoding::Base64 | UiTransactionEncoding::Base58 => (),
            _ => {
                return Err(anyhow::anyhow!(
                    "Only Base58 and Base64 encodings are supported"
                ))
            }
        }

        let serialized_transaction = bincode::serialize(transaction)?;

        let wrapper = TransactionWrapper {
            transaction: serialized_transaction,
            config: Some(TransactionConfig {
                max_retries: config.max_retries.map(|r| r as u32),
                blocklist_pdas: vec![],
                skip_preflight: config.skip_preflight,
                skip_sanitize: config.skip_sanitize,
            }),
            timestamp: Some(ms_since_epoch()),
        };

        Ok(Self::New(wrapper))
    }
}

fn convert_transaction_config(proto_config: &TransactionConfig) -> RpcSendTransactionConfig {
    RpcSendTransactionConfig {
        max_retries: proto_config.max_retries.map(|r| r as usize),
        skip_preflight: proto_config.skip_preflight,
        skip_sanitize: proto_config.skip_sanitize,
        preflight_commitment: None,
        encoding: None,
        min_context_slot: None,
    }
}

#[cfg(test)]
mod tests {
    use crate::util::ms_since_epoch;

    use super::*;

    fn create_test_wrapper(tx: &VersionedTransaction) -> TransactionWrapper {
        TransactionWrapper {
            transaction: bincode::serialize(tx).unwrap(),
            config: Some(TransactionConfig {
                max_retries: Some(5),
                blocklist_pdas: vec!["test1".to_string(), "test2".to_string()],
                skip_preflight: true,
                skip_sanitize: true,
            }),
            timestamp: Some(1234567890),
        }
    }

    #[test]
    fn test_legacy_compatibility() {
        let tx = VersionedTransaction::default();
        let legacy_bytes = bincode::serialize(&tx).unwrap();

        let payload = TransactionPayload::Legacy(legacy_bytes);
        let proto_tx = payload.to_proto();
        let decoded = TransactionPayload::from_proto(proto_tx).unwrap();

        let (decoded_tx, config) = decoded.decode().unwrap();
        assert!(config.is_none());
        assert_eq!(decoded_tx.signatures, tx.signatures);
    }
    #[test]
    fn test_new_format_features() {
        let tx = VersionedTransaction::default();
        let wrapper = TransactionWrapper {
            transaction: bincode::serialize(&tx).unwrap(),
            config: Some(TransactionConfig {
                max_retries: Some(5),
                blocklist_pdas: vec!["test1".to_string()],
                skip_preflight: true,
                skip_sanitize: true,
            }),
            timestamp: Some(ms_since_epoch()),
        };

        let payload = TransactionPayload::New(wrapper);
        let (_decoded_tx, config) = payload.decode().unwrap();

        // Verify all new features are preserved
        assert!(config.is_some());
        let config = config.unwrap();
        assert_eq!(config.max_retries, Some(5));
    }

    #[test]
    fn test_new_format() {
        let tx = VersionedTransaction::default();
        let wrapper = create_test_wrapper(&tx);

        let payload = TransactionPayload::New(wrapper);
        let proto_tx = payload.to_proto();
        let decoded = TransactionPayload::from_proto(proto_tx).unwrap();

        let (decoded_tx, config) = decoded.decode().unwrap();
        assert!(config.is_some());
        let config = config.unwrap();
        assert_eq!(config.max_retries, Some(5));
        assert!(config.skip_preflight); // Verify default value
        assert_eq!(decoded_tx.signatures, tx.signatures);
    }

    #[test]
    fn test_config_conversion() {
        let proto_config = TransactionConfig {
            max_retries: Some(3),
            blocklist_pdas: vec!["test".to_string()],
            skip_preflight: true,
            skip_sanitize: true,
        };

        let rpc_config = convert_transaction_config(&proto_config);
        assert_eq!(rpc_config.max_retries, Some(3));
        assert!(rpc_config.skip_preflight);
        assert_eq!(rpc_config.preflight_commitment, None);
        assert_eq!(rpc_config.encoding, None);
        assert_eq!(rpc_config.min_context_slot, None);
    }
    #[test]
    fn test_transaction_config_sanitize_flags() {
        let tx = VersionedTransaction::default();
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            skip_sanitize: true,
            ..Default::default()
        };

        let payload = TransactionPayload::from_transaction(&tx, config).unwrap();
        let (_, decoded_config) = payload.decode().unwrap();

        assert!(decoded_config.is_some());
        let decoded_config = decoded_config.unwrap();
        assert!(decoded_config.skip_preflight);
        assert!(decoded_config.skip_sanitize);
    }

    #[test]
    fn test_config_preservation_through_conversion() {
        let tx = VersionedTransaction::default();
        let original_config = RpcSendTransactionConfig {
            skip_preflight: true,
            skip_sanitize: false,
            max_retries: Some(3),
            preflight_commitment: None,
            encoding: Some(UiTransactionEncoding::Base64),
            min_context_slot: None,
        };

        // Convert to payload
        let payload = TransactionPayload::from_transaction(&tx, original_config.clone()).unwrap();

        // Convert back
        let (_, decoded_config) = payload.decode().unwrap();

        // Check all relevant fields are preserved
        let decoded_config = decoded_config.unwrap();
        assert_eq!(decoded_config.max_retries, original_config.max_retries);
        assert_eq!(
            decoded_config.skip_preflight,
            original_config.skip_preflight
        );
        assert_eq!(decoded_config.skip_sanitize, original_config.skip_sanitize);
    }

    #[test]
    fn test_invalid_encoding() {
        let tx = VersionedTransaction::default();
        let config = RpcSendTransactionConfig {
            encoding: Some(UiTransactionEncoding::JsonParsed),
            ..Default::default()
        };

        assert!(TransactionPayload::from_transaction(&tx, config).is_err());
    }

    #[test]
    fn test_empty_config_conversion() {
        let proto_config = TransactionConfig::default();
        let rpc_config = convert_transaction_config(&proto_config);

        assert_eq!(rpc_config.max_retries, None);
        assert!(!rpc_config.skip_preflight);
        assert!(!rpc_config.skip_sanitize);
    }
}

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
            publish_transaction,
            subscribe_transaction::{self, Payload},
            PublishTransaction, SubscribeTransaction, TransactionConfig, TransactionWrapper,
        },
        util::ms_since_epoch,
    },
    anyhow::Result,
    base64::prelude::{Engine, BASE64_STANDARD},
    serde::{Deserialize, Serialize},
    solana_client::rpc_config::RpcSendTransactionConfig,
    solana_sdk::{pubkey::Pubkey, transaction::VersionedTransaction},
    solana_transaction_status::UiTransactionEncoding,
    std::str::FromStr,
    thiserror::Error,
};

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcSendTransactionConfigWithBlockList {
    #[serde(flatten)]
    pub config: Option<RpcSendTransactionConfig>,
    pub blocklist_pdas: Vec<Pubkey>,
}

impl From<RpcSendTransactionConfigWithBlockList> for RpcSendTransactionConfig {
    fn from(extended: RpcSendTransactionConfigWithBlockList) -> Self {
        extended.config.unwrap_or_default()
    }
}

#[derive(Debug, Error)]
pub enum PayloadError {
    #[error("empty transaction payload")]
    EmptyPayload,
    #[error("failed to deserialize legacy JSON payload: {0}")]
    LegacyDeserialize(#[from] serde_json::Error),
    #[error("failed to decode base58 transaction: {0}")]
    Base58Decode(#[from] bs58::decode::Error),
    #[error("failed to decode base64 transaction: {0}")]
    Base64Decode(#[from] base64::DecodeError),
    #[error("unsupported encoding in legacy payload")]
    UnsupportedEncoding,
    #[error("failed to deserialize transaction: {0}")]
    BincodeError(#[from] bincode::Error),
    #[error("failed to parse pubkey: {0}")]
    InvalidPubkey(#[from] solana_sdk::pubkey::ParsePubkeyError),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LegacyPayload {
    pub transaction: String, // base58/base64 encoded transaction
    pub config: RpcSendTransactionConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
}

#[derive(Debug)]
pub enum TransactionPayload {
    Legacy(LegacyPayload),
    New(TransactionWrapper),
}

impl TransactionPayload {
    /// Creates a payload in either legacy or new format based on use_legacy flag
    pub fn create(
        transaction: &VersionedTransaction,
        config: RpcSendTransactionConfigWithBlockList,
        use_legacy: bool,
    ) -> Result<Self, PayloadError> {
        if use_legacy {
            Self::to_legacy(transaction, &config)
        } else {
            Self::try_from((transaction, config))
        }
    }

    /// Encodes a transaction using the specified encoding
    fn encode_transaction(
        tx: &VersionedTransaction,
        encoding: UiTransactionEncoding,
    ) -> Result<String, PayloadError> {
        let tx_bytes = bincode::serialize(tx)?;
        Ok(match encoding {
            UiTransactionEncoding::Base58 => bs58::encode(tx_bytes).into_string(),
            UiTransactionEncoding::Base64 => BASE64_STANDARD.encode(tx_bytes),
            _ => return Err(PayloadError::UnsupportedEncoding),
        })
    }

    /// Decodes a transaction string using the specified encoding
    fn decode_transaction(
        tx_string: &str,
        encoding: UiTransactionEncoding,
    ) -> Result<Vec<u8>, PayloadError> {
        Ok(match encoding {
            UiTransactionEncoding::Base58 => bs58::decode(tx_string).into_vec()?,
            UiTransactionEncoding::Base64 => BASE64_STANDARD.decode(tx_string)?,
            _ => return Err(PayloadError::UnsupportedEncoding),
        })
    }

    /// Creates a legacy payload from a transaction and configuration
    pub fn to_legacy(
        transaction: &VersionedTransaction,
        config_with_blocklist: &RpcSendTransactionConfigWithBlockList,
    ) -> Result<Self, PayloadError> {
        let encoding = config_with_blocklist
            .config
            .as_ref()
            .and_then(|c| c.encoding)
            .unwrap_or(UiTransactionEncoding::Base58);

        match encoding {
            UiTransactionEncoding::Base64 | UiTransactionEncoding::Base58 => (),
            _ => return Err(PayloadError::UnsupportedEncoding),
        }

        let tx_str = Self::encode_transaction(transaction, encoding)?;

        Ok(Self::Legacy(LegacyPayload {
            transaction: tx_str,
            config: config_with_blocklist.config.unwrap_or_default(),
            timestamp: Some(ms_since_epoch()),
        }))
    }

    /// Converts the payload to an appropriate protobuf message type
    pub fn to_proto<T>(&self) -> T
    where
        T: From<TransactionWrapper>, // For the New Payload
        T: From<Vec<u8>>,            // For the Legacy Payload
    {
        match self {
            Self::Legacy(legacy) => {
                let bytes = serde_json::to_vec(legacy).expect("Failed to serialize legacy payload");
                T::from(bytes)
            }
            Self::New(wrapper) => T::from(wrapper.clone()),
        }
    }
}

impl From<TransactionWrapper> for PublishTransaction {
    fn from(wrapper: TransactionWrapper) -> Self {
        Self {
            payload: Some(publish_transaction::Payload::NewPayload(wrapper)),
        }
    }
}

impl From<TransactionWrapper> for SubscribeTransaction {
    fn from(wrapper: TransactionWrapper) -> Self {
        Self {
            payload: Some(subscribe_transaction::Payload::NewPayload(wrapper)),
        }
    }
}

impl TryFrom<SubscribeTransaction> for TransactionPayload {
    type Error = PayloadError;

    fn try_from(tx: SubscribeTransaction) -> Result<Self, Self::Error> {
        match tx.payload {
            Some(Payload::LegacyPayload(bytes)) => {
                let legacy = serde_json::from_slice(&bytes)?;
                Ok(Self::Legacy(legacy))
            }
            Some(Payload::NewPayload(wrapper)) => Ok(Self::New(wrapper)),
            None => Err(PayloadError::EmptyPayload),
        }
    }
}

impl From<Vec<u8>> for PublishTransaction {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            payload: Some(publish_transaction::Payload::LegacyPayload(bytes)),
        }
    }
}

impl From<Vec<u8>> for SubscribeTransaction {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            payload: Some(subscribe_transaction::Payload::LegacyPayload(bytes)),
        }
    }
}

impl TryFrom<(&VersionedTransaction, RpcSendTransactionConfigWithBlockList)>
    for TransactionPayload
{
    type Error = PayloadError;

    fn try_from(
        (transaction, config_with_blocklist): (
            &VersionedTransaction,
            RpcSendTransactionConfigWithBlockList,
        ),
    ) -> Result<Self, Self::Error> {
        let tx_bytes = bincode::serialize(transaction)?;

        // Create new payload format with blocklist_pdas supported
        Ok(Self::New(TransactionWrapper {
            transaction: tx_bytes,
            config: Some(TransactionConfig {
                max_retries: config_with_blocklist
                    .config
                    .as_ref()
                    .and_then(|c| c.max_retries.map(|r| r as u32)),
                blocklist_pdas: config_with_blocklist
                    .blocklist_pdas
                    .iter()
                    .map(|p| p.to_string())
                    .collect(),
                skip_preflight: config_with_blocklist
                    .config
                    .as_ref()
                    .map(|c| c.skip_preflight)
                    .unwrap_or_default(),
                skip_sanitize: config_with_blocklist
                    .config
                    .as_ref()
                    .map(|c| c.skip_sanitize)
                    .unwrap_or_default(),
            }),
            timestamp: Some(ms_since_epoch()),
        }))
    }
}

pub struct TransactionDecoder;

impl TransactionDecoder {
    pub fn decode(
        payload: &TransactionPayload,
    ) -> Result<
        (
            VersionedTransaction,
            Option<RpcSendTransactionConfigWithBlockList>,
        ),
        PayloadError,
    > {
        match payload {
            TransactionPayload::Legacy(legacy) => {
                let encoding = legacy
                    .config
                    .encoding
                    .unwrap_or(UiTransactionEncoding::Base58);
                let tx_bytes =
                    TransactionPayload::decode_transaction(&legacy.transaction, encoding)?;

                let tx = bincode::deserialize(&tx_bytes)?;
                Ok((
                    tx,
                    Some(RpcSendTransactionConfigWithBlockList {
                        config: Some(legacy.config),
                        blocklist_pdas: vec![], // Legacy format doesn't have blocklist
                    }),
                ))
            }
            TransactionPayload::New(wrapper) => {
                let tx = bincode::deserialize(&wrapper.transaction)?;
                let config = if let Some(proto_config) = &wrapper.config {
                    Some(proto_config.try_into()?)
                } else {
                    None
                };
                Ok((tx, config))
            }
        }
    }
}

impl TryFrom<&TransactionConfig> for RpcSendTransactionConfigWithBlockList {
    type Error = PayloadError;

    fn try_from(proto_config: &TransactionConfig) -> Result<Self, Self::Error> {
        // Convert string pubkeys to actual Pubkey objects, ignoring invalid ones
        let blocklist_pdas = proto_config
            .blocklist_pdas
            .iter()
            .filter_map(|pubkey_str| {
                // Only keep valid pubkeys
                Pubkey::from_str(pubkey_str).ok()
            })
            .collect();

        Ok(Self {
            config: Some(RpcSendTransactionConfig {
                max_retries: proto_config.max_retries.map(|r| r as usize),
                skip_preflight: proto_config.skip_preflight,
                skip_sanitize: proto_config.skip_sanitize,
                preflight_commitment: None,
                encoding: None,
                min_context_slot: None,
            }),
            blocklist_pdas,
        })
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::util::ms_since_epoch};

    fn create_test_wrapper(tx: &VersionedTransaction) -> TransactionWrapper {
        TransactionWrapper {
            transaction: bincode::serialize(tx).unwrap(),
            config: Some(TransactionConfig {
                max_retries: Some(5),
                blocklist_pdas: vec![],
                skip_preflight: true,
                skip_sanitize: true,
            }),
            timestamp: Some(1234567890),
        }
    }

    #[test]
    fn test_legacy_format() {
        let tx = VersionedTransaction::default();
        let config = RpcSendTransactionConfigWithBlockList {
            config: Some(RpcSendTransactionConfig {
                encoding: Some(UiTransactionEncoding::Base58),
                skip_preflight: true,
                skip_sanitize: true,
                ..Default::default()
            }),
            blocklist_pdas: vec![],
        };

        let payload = TransactionPayload::try_from((&tx, config.clone())).unwrap();

        let proto_tx = payload.to_proto::<SubscribeTransaction>();
        let decoded = TransactionPayload::try_from(proto_tx).unwrap();

        let (decoded_tx, decoded_config) = TransactionDecoder::decode(&decoded).unwrap();
        assert!(decoded_config.is_some());
        let config_with_blocklist = decoded_config.unwrap();

        assert_eq!(decoded_tx.signatures, tx.signatures);
        assert!(config_with_blocklist.config.is_some());
        let decoded_config = config_with_blocklist.config.unwrap();
        assert_eq!(
            decoded_config.skip_preflight,
            config.config.unwrap().skip_preflight
        );
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
        let (_, config) = TransactionDecoder::decode(&payload).unwrap();

        assert!(config.is_some());
        let config_with_blocklist = config.unwrap();
        assert!(config_with_blocklist.config.is_some());
        let config = config_with_blocklist.config.unwrap();
        assert_eq!(config.max_retries, Some(5));
    }

    #[test]
    fn test_new_format() {
        let tx = VersionedTransaction::default();
        let wrapper = create_test_wrapper(&tx);

        let payload = TransactionPayload::New(wrapper);
        let proto_tx = payload.to_proto::<SubscribeTransaction>();
        let decoded = TransactionPayload::try_from(proto_tx).unwrap();

        let (decoded_tx, config) = TransactionDecoder::decode(&decoded).unwrap();
        assert!(config.is_some());
        let config_with_blocklist = config.unwrap();
        assert!(config_with_blocklist.config.is_some());
        let config = config_with_blocklist.config.unwrap();

        assert_eq!(config.max_retries, Some(5));
        assert!(config.skip_preflight);
        assert_eq!(decoded_tx.signatures, tx.signatures);
    }

    #[test]
    fn test_config_conversion() {
        let proto_config = TransactionConfig {
            max_retries: Some(3),
            blocklist_pdas: vec!["11111111111111111111111111111111".to_string()],
            skip_preflight: true,
            skip_sanitize: true,
        };

        let rpc_config_result: Result<RpcSendTransactionConfigWithBlockList, _> =
            (&proto_config).try_into();
        let rpc_config = rpc_config_result.unwrap();

        assert!(rpc_config.config.is_some());
        let config = rpc_config.config.unwrap();
        assert_eq!(config.max_retries, Some(3));
        assert!(config.skip_preflight);
        assert_eq!(config.preflight_commitment, None);
        assert_eq!(config.encoding, None);
        assert_eq!(config.min_context_slot, None);
    }

    #[test]
    fn test_config_invalid_pubkey_conversion() {
        let proto_config = TransactionConfig {
            max_retries: Some(3),
            blocklist_pdas: vec![
                "11111111111111111111111111111111".to_string(), // Valid pubkey
                "invalid_pubkey".to_string(),                   // Invalid pubkey
            ],
            skip_preflight: true,
            skip_sanitize: true,
        };

        let rpc_config_result: Result<RpcSendTransactionConfigWithBlockList, _> =
            (&proto_config).try_into();
        // Now we expect success with the invalid pubkey filtered out
        assert!(rpc_config_result.is_ok());

        let rpc_config = rpc_config_result.unwrap();
        assert_eq!(rpc_config.blocklist_pdas.len(), 1); // Only the valid pubkey remains
    }

    #[test]
    fn test_transaction_config_sanitize_flags() {
        let tx = VersionedTransaction::default();
        let config = RpcSendTransactionConfigWithBlockList {
            config: Some(RpcSendTransactionConfig {
                skip_preflight: true,
                skip_sanitize: true,
                encoding: Some(UiTransactionEncoding::Base64),
                ..Default::default()
            }),
            blocklist_pdas: vec![],
        };

        let payload = TransactionPayload::try_from((&tx, config)).unwrap();
        let proto_tx = payload.to_proto::<SubscribeTransaction>();

        let decoded = TransactionPayload::try_from(proto_tx).unwrap();
        let (_, decoded_config) = TransactionDecoder::decode(&decoded).unwrap();

        assert!(decoded_config.is_some());
        let config_with_blocklist = decoded_config.unwrap();
        assert!(config_with_blocklist.config.is_some());
        let decoded_config = config_with_blocklist.config.unwrap();
        assert!(decoded_config.skip_preflight);
        assert!(decoded_config.skip_sanitize);
    }

    #[test]
    fn test_config_preservation_through_conversion() {
        let tx = VersionedTransaction::default();
        let original_config = RpcSendTransactionConfigWithBlockList {
            config: Some(RpcSendTransactionConfig {
                skip_preflight: true,
                skip_sanitize: false,
                max_retries: Some(3),
                preflight_commitment: None,
                encoding: Some(UiTransactionEncoding::Base64),
                min_context_slot: None,
            }),
            blocklist_pdas: vec![],
        };

        let payload = TransactionPayload::try_from((&tx, original_config.clone())).unwrap();
        let (_, decoded_config) = TransactionDecoder::decode(&payload).unwrap();

        assert!(decoded_config.is_some());
        let config_with_blocklist = decoded_config.unwrap();
        assert!(config_with_blocklist.config.is_some());
        let decoded_config = config_with_blocklist.config.unwrap();
        assert_eq!(
            decoded_config.max_retries,
            original_config.config.unwrap().max_retries
        );
        assert_eq!(
            decoded_config.skip_preflight,
            original_config.config.unwrap().skip_preflight
        );
        assert_eq!(
            decoded_config.skip_sanitize,
            original_config.config.unwrap().skip_sanitize
        );
    }

    #[test]
    fn test_invalid_encoding() {
        let tx = VersionedTransaction::default();
        let config = RpcSendTransactionConfigWithBlockList {
            config: Some(RpcSendTransactionConfig {
                encoding: Some(UiTransactionEncoding::JsonParsed),
                ..Default::default()
            }),
            blocklist_pdas: vec![],
        };

        assert!(TransactionPayload::try_from((&tx, config)).is_err());
    }

    #[test]
    fn test_empty_config_conversion() {
        let proto_config = TransactionConfig {
            max_retries: None,
            blocklist_pdas: vec![],
            skip_preflight: false,
            skip_sanitize: false,
        };

        let rpc_config_result: Result<RpcSendTransactionConfigWithBlockList, _> =
            (&proto_config).try_into();
        let rpc_config = rpc_config_result.unwrap();

        assert!(rpc_config.config.is_some());
        let config = rpc_config.config.unwrap();
        assert_eq!(config.max_retries, None);
        assert!(!config.skip_preflight);
        assert!(!config.skip_sanitize);
    }
}

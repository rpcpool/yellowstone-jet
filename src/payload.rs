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
            PublishTransaction, SubscribeTransaction, TransactionConfig, TransactionWrapper,
            publish_transaction,
            subscribe_transaction::{self, Payload},
        },
        util::ms_since_epoch,
    },
    anyhow::Result,
    base64::prelude::{BASE64_STANDARD, Engine},
    serde::{Deserialize, Serialize},
    solana_client::rpc_config::RpcSendTransactionConfig,
    solana_pubkey::{ParsePubkeyError, Pubkey},
    solana_transaction::versioned::VersionedTransaction,
    solana_transaction_status_client_types::UiTransactionEncoding,
    std::str::FromStr,
    thiserror::Error,
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
    InvalidPubkey(#[from] ParsePubkeyError),
    #[error("failed to convert proto message: {0}")]
    ProtoConversionError(String),
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
        config: JetRpcSendTransactionConfig,
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
        config_with_forwarding: &JetRpcSendTransactionConfig,
    ) -> Result<Self, PayloadError> {
        let encoding = config_with_forwarding
            .config
            .encoding
            .unwrap_or(UiTransactionEncoding::Base58);

        match encoding {
            UiTransactionEncoding::Base64 | UiTransactionEncoding::Base58 => (),
            _ => return Err(PayloadError::UnsupportedEncoding),
        }

        let tx_str = Self::encode_transaction(transaction, encoding)?;

        // Legacy format doesn't support forwarding policies, but we can preserve the rest of the config
        Ok(Self::Legacy(LegacyPayload {
            transaction: tx_str,
            config: config_with_forwarding.config,
            timestamp: Some(ms_since_epoch()),
        }))
    }

    /// Converts the payload to an appropriate protobuf message type
    pub fn to_proto<T>(&self) -> Result<T, PayloadError>
    where
        T: From<TransactionWrapper>, // For the New Payload
        T: From<Vec<u8>>,            // For the Legacy Payload
    {
        match self {
            Self::Legacy(legacy) => match serde_json::to_vec(legacy) {
                Ok(bytes) => Ok(T::from(bytes)),
                Err(err) => Err(PayloadError::LegacyDeserialize(err)),
            },
            Self::New(wrapper) => Ok(T::from(wrapper.clone())),
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

impl TryFrom<Result<SubscribeTransaction, PayloadError>> for TransactionPayload {
    type Error = PayloadError;

    fn try_from(result: Result<SubscribeTransaction, PayloadError>) -> Result<Self, Self::Error> {
        match result {
            Ok(tx) => tx.try_into(),
            Err(err) => Err(err),
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

impl TryFrom<(&VersionedTransaction, JetRpcSendTransactionConfig)> for TransactionPayload {
    type Error = PayloadError;

    fn try_from(
        (transaction, config_with_forwarding_policies): (
            &VersionedTransaction,
            JetRpcSendTransactionConfig,
        ),
    ) -> Result<Self, Self::Error> {
        // Check encoding first to fail early if it's invalid
        if let Some(encoding) = config_with_forwarding_policies.config.encoding {
            match encoding {
                UiTransactionEncoding::Base58 | UiTransactionEncoding::Base64 => {}
                _ => return Err(PayloadError::UnsupportedEncoding),
            }
        }

        let tx_bytes = bincode::serialize(transaction)?;

        // Create new payload format with forwarding policies supported
        Ok(Self::New(TransactionWrapper {
            transaction: tx_bytes,
            config: Some(TransactionConfig {
                max_retries: config_with_forwarding_policies
                    .config
                    .max_retries
                    .map(|r| r as u32),
                forwarding_policies: config_with_forwarding_policies
                    .forwarding_policies
                    .iter()
                    .map(|p| p.to_string())
                    .collect(),
                skip_preflight: config_with_forwarding_policies.config.skip_preflight,
                skip_sanitize: config_with_forwarding_policies.config.skip_sanitize,
            }),
            timestamp: Some(ms_since_epoch()),
        }))
    }
}

pub struct TransactionDecoder;

impl TransactionDecoder {
    pub fn decode(
        payload: &TransactionPayload,
    ) -> Result<(VersionedTransaction, Option<JetRpcSendTransactionConfig>), PayloadError> {
        match payload {
            TransactionPayload::Legacy(legacy) => {
                let encoding = legacy
                    .config
                    .encoding
                    .unwrap_or(UiTransactionEncoding::Base58);

                let tx_bytes =
                    TransactionPayload::decode_transaction(&legacy.transaction, encoding)?;
                let tx = bincode::deserialize(&tx_bytes)?;

                // Legacy format doesn't have forwarding policies, so we pass None
                Ok((
                    tx,
                    Some(JetRpcSendTransactionConfig::new(Some(legacy.config), None)),
                ))
            }
            TransactionPayload::New(wrapper) => {
                let tx = bincode::deserialize(&wrapper.transaction)?;
                let config = if let Some(proto_config) = &wrapper.config {
                    match proto_config.try_into() {
                        Ok(config) => Some(config),
                        Err(err) => return Err(err),
                    }
                } else {
                    None
                };
                Ok((tx, config))
            }
        }
    }
}

impl TryFrom<&TransactionConfig> for JetRpcSendTransactionConfig {
    type Error = PayloadError;

    fn try_from(proto_config: &TransactionConfig) -> Result<Self, Self::Error> {
        Ok(JetRpcSendTransactionConfig::new(
            Some(RpcSendTransactionConfig {
                max_retries: proto_config.max_retries.map(|r| r as usize),
                skip_preflight: proto_config.skip_preflight,
                skip_sanitize: proto_config.skip_sanitize,
                preflight_commitment: None,
                encoding: None,
                min_context_slot: None,
            }),
            Some(proto_config.forwarding_policies.clone()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::util::ms_since_epoch};

    fn create_test_wrapper(
        tx: &VersionedTransaction,
    ) -> Result<TransactionWrapper, bincode::Error> {
        let tx_bytes = bincode::serialize(tx)?;
        Ok(TransactionWrapper {
            transaction: tx_bytes,
            config: Some(TransactionConfig {
                max_retries: Some(5),
                forwarding_policies: vec![],
                skip_preflight: true,
                skip_sanitize: true,
            }),
            timestamp: Some(1234567890),
        })
    }

    #[test]
    fn test_legacy_format() -> Result<(), Box<dyn std::error::Error>> {
        let tx = VersionedTransaction::default();
        let config = JetRpcSendTransactionConfig::new(
            Some(RpcSendTransactionConfig {
                encoding: Some(UiTransactionEncoding::Base58),
                skip_preflight: true,
                skip_sanitize: true,
                ..Default::default()
            }),
            None,
        );

        let payload = TransactionPayload::try_from((&tx, config.clone()))?;

        let proto_tx = payload.to_proto::<SubscribeTransaction>()?;
        let decoded = TransactionPayload::try_from(proto_tx)?;

        let (decoded_tx, decoded_config) = TransactionDecoder::decode(&decoded)?;
        assert!(decoded_config.is_some());
        let config_with_forwarding_policies = decoded_config.unwrap();

        assert_eq!(decoded_tx.signatures, tx.signatures);
        assert_eq!(
            config_with_forwarding_policies.config.skip_preflight,
            config.config.skip_preflight
        );
        Ok(())
    }

    #[test]
    fn test_new_format_features() -> Result<(), Box<dyn std::error::Error>> {
        let tx = VersionedTransaction::default();
        let tx_bytes = bincode::serialize(&tx)?;

        let wrapper = TransactionWrapper {
            transaction: tx_bytes,
            config: Some(TransactionConfig {
                max_retries: Some(5),
                forwarding_policies: vec!["test1".to_string()],
                skip_preflight: true,
                skip_sanitize: true,
            }),
            timestamp: Some(ms_since_epoch()),
        };

        let payload = TransactionPayload::New(wrapper);
        let (_, config) = TransactionDecoder::decode(&payload)?;

        assert!(config.is_some());
        let config_with_forwarding_policies = config.unwrap();
        assert_eq!(config_with_forwarding_policies.config.max_retries, Some(5));
        Ok(())
    }

    #[test]
    fn test_new_format() -> Result<(), Box<dyn std::error::Error>> {
        let tx = VersionedTransaction::default();
        let wrapper = create_test_wrapper(&tx)?;

        let payload = TransactionPayload::New(wrapper);
        let proto_tx = payload.to_proto::<SubscribeTransaction>()?;
        let decoded = TransactionPayload::try_from(proto_tx)?;

        let (decoded_tx, config) = TransactionDecoder::decode(&decoded)?;
        assert!(config.is_some());
        let config_with_forwarding_policies = config.unwrap();

        assert_eq!(config_with_forwarding_policies.config.max_retries, Some(5));
        assert!(config_with_forwarding_policies.config.skip_preflight);
        assert_eq!(decoded_tx.signatures, tx.signatures);
        Ok(())
    }

    #[test]
    fn test_config_conversion() -> Result<(), Box<dyn std::error::Error>> {
        let proto_config = TransactionConfig {
            max_retries: Some(3),
            forwarding_policies: vec!["11111111111111111111111111111111".to_string()],
            skip_preflight: true,
            skip_sanitize: true,
        };

        let rpc_config_result: Result<JetRpcSendTransactionConfig, _> = (&proto_config).try_into();
        let rpc_config = rpc_config_result?;

        assert_eq!(rpc_config.config.max_retries, Some(3));
        assert!(rpc_config.config.skip_preflight);
        assert_eq!(rpc_config.config.preflight_commitment, None);
        assert_eq!(rpc_config.config.encoding, None);
        assert_eq!(rpc_config.config.min_context_slot, None);
        assert_eq!(rpc_config.forwarding_policies.len(), 1);
        Ok(())
    }

    #[test]
    fn test_config_invalid_pubkey_conversion() -> Result<(), Box<dyn std::error::Error>> {
        let proto_config = TransactionConfig {
            max_retries: Some(3),
            forwarding_policies: vec![
                "11111111111111111111111111111111".to_string(), // Valid pubkey
                "invalid_pubkey".to_string(),                   // Invalid pubkey
            ],
            skip_preflight: true,
            skip_sanitize: true,
        };

        let rpc_config_result: Result<JetRpcSendTransactionConfig, _> = (&proto_config).try_into();
        assert!(rpc_config_result.is_ok());

        let rpc_config = rpc_config_result?;
        assert_eq!(rpc_config.forwarding_policies.len(), 1); // Only valid pubkey is included
        Ok(())
    }

    #[test]
    fn test_transaction_config_sanitize_flags() -> Result<(), Box<dyn std::error::Error>> {
        let tx = VersionedTransaction::default();
        let config = JetRpcSendTransactionConfig::new(
            Some(RpcSendTransactionConfig {
                skip_preflight: true,
                skip_sanitize: true,
                encoding: Some(UiTransactionEncoding::Base64),
                ..Default::default()
            }),
            None,
        );

        let payload = TransactionPayload::try_from((&tx, config))?;
        let proto_tx = payload.to_proto::<SubscribeTransaction>()?;

        let decoded = TransactionPayload::try_from(proto_tx)?;
        let (_, decoded_config) = TransactionDecoder::decode(&decoded)?;

        assert!(decoded_config.is_some());
        let config_with_forwarding_policies = decoded_config.unwrap();
        assert!(config_with_forwarding_policies.config.skip_preflight);
        assert!(config_with_forwarding_policies.config.skip_sanitize);
        Ok(())
    }

    #[test]
    fn test_config_preservation_through_conversion() -> Result<(), Box<dyn std::error::Error>> {
        let tx = VersionedTransaction::default();
        let original_config = JetRpcSendTransactionConfig::new(
            Some(RpcSendTransactionConfig {
                skip_preflight: true,
                skip_sanitize: false,
                max_retries: Some(3),
                preflight_commitment: None,
                encoding: Some(UiTransactionEncoding::Base64),
                min_context_slot: None,
            }),
            None,
        );

        let payload = TransactionPayload::try_from((&tx, original_config.clone()))?;
        let (_, decoded_config) = TransactionDecoder::decode(&payload)?;

        assert!(decoded_config.is_some());
        let config_with_forwarding_policies = decoded_config.unwrap();
        assert_eq!(
            config_with_forwarding_policies.config.max_retries,
            original_config.config.max_retries
        );
        assert_eq!(
            config_with_forwarding_policies.config.skip_preflight,
            original_config.config.skip_preflight
        );
        assert_eq!(
            config_with_forwarding_policies.config.skip_sanitize,
            original_config.config.skip_sanitize
        );
        Ok(())
    }

    #[test]
    fn test_invalid_encoding() {
        let tx = VersionedTransaction::default();
        let config = JetRpcSendTransactionConfig::new(
            Some(RpcSendTransactionConfig {
                encoding: Some(UiTransactionEncoding::JsonParsed),
                ..Default::default()
            }),
            None,
        );

        assert!(TransactionPayload::try_from((&tx, config)).is_err());
    }

    #[test]
    fn test_empty_config_conversion() -> Result<(), Box<dyn std::error::Error>> {
        let proto_config = TransactionConfig {
            max_retries: None,
            forwarding_policies: vec![],
            skip_preflight: false,
            skip_sanitize: false,
        };

        let rpc_config_result: Result<JetRpcSendTransactionConfig, _> = (&proto_config).try_into();
        let rpc_config = rpc_config_result?;

        assert_eq!(rpc_config.config.max_retries, None);
        assert!(!rpc_config.config.skip_preflight);
        assert!(!rpc_config.config.skip_sanitize);
        assert_eq!(rpc_config.forwarding_policies.len(), 0);
        Ok(())
    }

    #[test]
    fn test_forwarding_policies_conversion() -> Result<(), Box<dyn std::error::Error>> {
        let tx = VersionedTransaction::default();
        let config = JetRpcSendTransactionConfig::new(
            Some(RpcSendTransactionConfig::default()),
            Some(vec![
                "11111111111111111111111111111111".to_string(),
                "22222222222222222222222222222222".to_string(),
            ]),
        );

        let payload = TransactionPayload::try_from((&tx, config.clone()))?;
        let (_, decoded_config) = TransactionDecoder::decode(&payload)?;

        assert!(decoded_config.is_some());
        let decoded_forwarding_policies = decoded_config.unwrap().forwarding_policies;
        assert!(decoded_forwarding_policies.len() == config.forwarding_policies.len());
        Ok(())
    }

    #[test]
    fn test_forwarding_policies_pubkeys_conversion() -> Result<(), Box<dyn std::error::Error>> {
        let config = JetRpcSendTransactionConfig::new(
            None,
            Some(vec![
                "11111111111111111111111111111111".to_string(), // Valid pubkey
                "invalid_pubkey".to_string(),                   // Invalid pubkey
            ]),
        );

        assert_eq!(config.forwarding_policies.len(), 1); // Only valid pubkey is included

        Ok(())
    }

    #[test]
    fn test_legacy_to_new_conversion() -> Result<(), Box<dyn std::error::Error>> {
        let tx = VersionedTransaction::default();
        let config = JetRpcSendTransactionConfig::new(
            Some(RpcSendTransactionConfig {
                encoding: Some(UiTransactionEncoding::Base58),
                skip_preflight: true,
                skip_sanitize: true,
                ..Default::default()
            }),
            Some(vec!["11111111111111111111111111111111".to_string()]),
        );

        // Create legacy payload
        let legacy_payload = TransactionPayload::to_legacy(&tx, &config)?;

        // Convert to proto and back to test round-trip conversion
        let proto_tx = legacy_payload.to_proto::<SubscribeTransaction>()?;
        let decoded = TransactionPayload::try_from(proto_tx)?;

        // Decode and check transaction and config
        let (decoded_tx, decoded_config) = TransactionDecoder::decode(&decoded)?;

        // Verify transaction is preserved
        assert_eq!(decoded_tx.signatures, tx.signatures);

        // Legacy format doesn't preserve forwarding policies
        if let Some(config) = decoded_config {
            assert_eq!(config.forwarding_policies.len(), 0);
            assert!(config.config.skip_preflight);
            assert!(config.config.skip_sanitize);
        } else {
            panic!("Decoded config is None");
        }

        Ok(())
    }
}

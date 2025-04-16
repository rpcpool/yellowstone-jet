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
    solana_sdk::transaction::VersionedTransaction,
    solana_transaction_status::UiTransactionEncoding,
    std::str::FromStr,
    thiserror::Error,
};

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcSendTransactionConfigWithBlockList {
    #[serde(flatten)]
    pub config: Option<RpcSendTransactionConfig>,
    /// base58-encoded pubkeys
    pub blocklist_pdas: Option<Vec<String>>,
}

impl RpcSendTransactionConfigWithBlockList {
    /// Converts the optional string-based blocklist_pdas into a Vec<Pubkey>
    /// Invalid pubkeys are filtered out
    pub fn blocklist_pubkeys(&self) -> Vec<solana_sdk::pubkey::Pubkey> {
        match &self.blocklist_pdas {
            Some(keys) => keys
                .iter()
                .filter_map(
                    |key_str| match solana_sdk::pubkey::Pubkey::from_str(key_str) {
                        Ok(pubkey) => Some(pubkey),
                        Err(_) => None,
                    },
                )
                .collect(),
            None => Vec::new(),
        }
    }

    /// Get a safe reference to config or a default config if None
    pub fn get_config(&self) -> RpcSendTransactionConfig {
        self.config.unwrap_or(RpcSendTransactionConfig::default())
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
        let config = config_with_blocklist
            .config
            .unwrap_or(RpcSendTransactionConfig::default());

        Ok(Self::Legacy(LegacyPayload {
            transaction: tx_str,
            config,
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
        // Check encoding first to fail early if it's invalid
        if let Some(encoding) = config_with_blocklist
            .config
            .as_ref()
            .and_then(|c| c.encoding)
        {
            match encoding {
                UiTransactionEncoding::Base58 | UiTransactionEncoding::Base64 => {}
                _ => return Err(PayloadError::UnsupportedEncoding),
            }
        }

        let tx_bytes = bincode::serialize(transaction)?;

        // Get max_retries safely
        let max_retries = config_with_blocklist
            .config
            .as_ref()
            .and_then(|c| c.max_retries.map(|r| r as u32));

        // Get skip_preflight safely
        let skip_preflight = config_with_blocklist
            .config
            .as_ref()
            .map(|c| c.skip_preflight)
            .unwrap_or(false);

        // Get skip_sanitize safely
        let skip_sanitize = config_with_blocklist
            .config
            .as_ref()
            .map(|c| c.skip_sanitize)
            .unwrap_or(false);

        // Get blocklist_pdas safely
        let blocklist_pdas = config_with_blocklist.blocklist_pdas.unwrap_or_default();

        // Create new payload format with blocklist_pdas supported
        Ok(Self::New(TransactionWrapper {
            transaction: tx_bytes,
            config: Some(TransactionConfig {
                max_retries,
                blocklist_pdas,
                skip_preflight,
                skip_sanitize,
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
                        blocklist_pdas: None, // Legacy format doesn't have blocklist
                    }),
                ))
            }
            TransactionPayload::New(wrapper) => {
                let tx = bincode::deserialize(&wrapper.transaction)?;
                let config = wrapper.config.as_ref().map(TryInto::try_into).transpose()?;
                Ok((tx, config))
            }
        }
    }
}

impl TryFrom<&TransactionConfig> for RpcSendTransactionConfigWithBlockList {
    type Error = PayloadError;

    fn try_from(proto_config: &TransactionConfig) -> Result<Self, Self::Error> {
        let blocklist_pdas = (!proto_config.blocklist_pdas.is_empty())
            .then_some(proto_config.blocklist_pdas.clone());

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

    fn create_test_wrapper(
        tx: &VersionedTransaction,
    ) -> Result<TransactionWrapper, bincode::Error> {
        let tx_bytes = bincode::serialize(tx)?;
        Ok(TransactionWrapper {
            transaction: tx_bytes,
            config: Some(TransactionConfig {
                max_retries: Some(5),
                blocklist_pdas: vec![],
                skip_preflight: true,
                skip_sanitize: true,
            }),
            timestamp: Some(1234567890),
        })
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
            blocklist_pdas: None,
        };

        let payload =
            TransactionPayload::try_from((&tx, config.clone())).expect("Failed to create payload");

        let proto_tx = payload
            .to_proto::<SubscribeTransaction>()
            .expect("Failed to convert to proto");
        let decoded = TransactionPayload::try_from(proto_tx).expect("Failed to convert from proto");

        let (decoded_tx, decoded_config) =
            TransactionDecoder::decode(&decoded).expect("Failed to decode");
        assert!(decoded_config.is_some());
        let config_with_blocklist = decoded_config.unwrap();

        assert_eq!(decoded_tx.signatures, tx.signatures);
        assert!(config_with_blocklist.config.is_some());
        let decoded_config = config_with_blocklist.config.unwrap();
        assert_eq!(
            decoded_config.skip_preflight,
            config
                .config
                .as_ref()
                .map(|c| c.skip_preflight)
                .unwrap_or_default()
        );
    }

    #[test]
    fn test_new_format_features() {
        let tx = VersionedTransaction::default();
        let tx_bytes = bincode::serialize(&tx).expect("Failed to serialize transaction");

        let wrapper = TransactionWrapper {
            transaction: tx_bytes,
            config: Some(TransactionConfig {
                max_retries: Some(5),
                blocklist_pdas: vec!["test1".to_string()],
                skip_preflight: true,
                skip_sanitize: true,
            }),
            timestamp: Some(ms_since_epoch()),
        };

        let payload = TransactionPayload::New(wrapper);
        let (_, config) = TransactionDecoder::decode(&payload).expect("Failed to decode");

        assert!(config.is_some());
        let config_with_blocklist = config.unwrap();
        assert!(config_with_blocklist.config.is_some());
        let config = config_with_blocklist.config.unwrap();
        assert_eq!(config.max_retries, Some(5));
    }

    #[test]
    fn test_new_format() {
        let tx = VersionedTransaction::default();
        let wrapper = create_test_wrapper(&tx).expect("Failed to create test wrapper");

        let payload = TransactionPayload::New(wrapper);
        let proto_tx = payload
            .to_proto::<SubscribeTransaction>()
            .expect("Failed to convert to proto");
        let decoded = TransactionPayload::try_from(proto_tx).expect("Failed to convert from proto");

        let (decoded_tx, config) = TransactionDecoder::decode(&decoded).expect("Failed to decode");
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

        let rpc_config: RpcSendTransactionConfigWithBlockList = (&proto_config)
            .try_into()
            .expect("Failed to convert config");

        assert!(rpc_config.config.is_some());
        let config = rpc_config.config.unwrap();
        assert_eq!(config.max_retries, Some(3));
        assert!(config.skip_preflight);
        assert_eq!(config.preflight_commitment, None);
        assert_eq!(config.encoding, None);
        assert_eq!(config.min_context_slot, None);
        assert!(rpc_config.blocklist_pdas.is_some());
        assert_eq!(rpc_config.blocklist_pdas.as_ref().map(|v| v.len()), Some(1));
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

        let rpc_config: RpcSendTransactionConfigWithBlockList = (&proto_config)
            .try_into()
            .expect("Failed to convert config");

        assert!(rpc_config.blocklist_pdas.is_some());
        assert_eq!(rpc_config.blocklist_pdas.as_ref().map(|v| v.len()), Some(2));
        // Both pubkeys are kept
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
            blocklist_pdas: None,
        };

        let payload =
            TransactionPayload::try_from((&tx, config)).expect("Failed to create payload");
        let proto_tx = payload
            .to_proto::<SubscribeTransaction>()
            .expect("Failed to convert to proto");

        let decoded = TransactionPayload::try_from(proto_tx).expect("Failed to convert from proto");
        let (_, decoded_config) = TransactionDecoder::decode(&decoded).expect("Failed to decode");

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
            blocklist_pdas: None,
        };

        let payload = TransactionPayload::try_from((&tx, original_config.clone()))
            .expect("Failed to create payload");
        let (_, decoded_config) = TransactionDecoder::decode(&payload).expect("Failed to decode");

        assert!(decoded_config.is_some());
        let config_with_blocklist = decoded_config.unwrap();
        assert!(config_with_blocklist.config.is_some());
        let decoded_config = config_with_blocklist.config.unwrap();
        assert_eq!(
            decoded_config.max_retries,
            original_config.config.as_ref().and_then(|c| c.max_retries)
        );
        assert_eq!(
            decoded_config.skip_preflight,
            original_config
                .config
                .as_ref()
                .map(|c| c.skip_preflight)
                .unwrap_or_default()
        );
        assert_eq!(
            decoded_config.skip_sanitize,
            original_config
                .config
                .as_ref()
                .map(|c| c.skip_sanitize)
                .unwrap_or_default()
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
            blocklist_pdas: None,
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

        let rpc_config: RpcSendTransactionConfigWithBlockList = (&proto_config)
            .try_into()
            .expect("Failed to convert config");

        assert!(rpc_config.config.is_some());
        let config = rpc_config.config.unwrap();
        assert_eq!(config.max_retries, None);
        assert!(!config.skip_preflight);
        assert!(!config.skip_sanitize);
        assert!(rpc_config.blocklist_pdas.is_none());
    }

    #[test]
    fn test_blocklist_pdas_conversion() {
        let tx = VersionedTransaction::default();
        let config = RpcSendTransactionConfigWithBlockList {
            config: Some(RpcSendTransactionConfig::default()),
            blocklist_pdas: Some(vec![
                "11111111111111111111111111111111".to_string(),
                "22222222222222222222222222222222".to_string(),
            ]),
        };

        let payload =
            TransactionPayload::try_from((&tx, config.clone())).expect("Failed to create payload");
        let (_, decoded_config) = TransactionDecoder::decode(&payload).expect("Failed to decode");

        assert!(decoded_config.is_some());
        let decoded_blocklist = decoded_config.unwrap().blocklist_pdas;
        assert!(decoded_blocklist.is_some());
        let blocklist = decoded_blocklist.unwrap();
        assert_eq!(blocklist.len(), 2);
        assert_eq!(blocklist[0], "11111111111111111111111111111111");
        assert_eq!(blocklist[1], "22222222222222222222222222222222");
    }

    #[test]
    fn test_blocklist_pubkeys_conversion() {
        let config = RpcSendTransactionConfigWithBlockList {
            config: None,
            blocklist_pdas: Some(vec![
                "11111111111111111111111111111111".to_string(), // Valid pubkey
                "invalid_pubkey".to_string(),                   // Invalid pubkey
            ]),
        };

        let pubkeys = config.blocklist_pubkeys();
        assert_eq!(pubkeys.len(), 1); // Only the valid pubkey should be included
    }
}

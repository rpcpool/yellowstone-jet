use {
    crate::{
        payload::JetRpcSendTransactionConfig, solana::decode_and_deserialize,
        transactions::SendTransactionRequest,
    },
    anyhow::Result,
    jsonrpsee::types::error::{ErrorObject, ErrorObjectOwned, INTERNAL_ERROR_CODE},
    solana_client::rpc_response::RpcVersionInfo,
    solana_rpc_client_api::config::RpcSendTransactionConfig,
    solana_transaction::versioned::VersionedTransaction,
    solana_transaction_status_client_types::UiTransactionEncoding,
    solana_version::Version,
    std::sync::Arc,
    thiserror::Error,
    tokio::sync::mpsc,
    yellowstone_jet_tpu_client::core::PACKET_DATA_SIZE,
};

#[derive(Debug, Error)]
pub enum TransactionHandlerError {
    #[error("invalid transaction: {0}")]
    InvalidTransaction(String),

    #[error("failed to serialize transaction: {0}")]
    SerializationFailed(#[from] bincode::Error),

    #[error("preflight check is not supported")]
    PreflightNotSupported,

    #[error("invalid parameters: {0}")]
    InvalidParams(String),

    #[error("unsupported encoding")]
    UnsupportedEncoding,
}

impl From<ErrorObjectOwned> for TransactionHandlerError {
    fn from(err: ErrorObjectOwned) -> Self {
        TransactionHandlerError::InvalidParams(err.message().to_string())
    }
}

impl From<TransactionHandlerError> for ErrorObjectOwned {
    fn from(err: TransactionHandlerError) -> Self {
        ErrorObject::owned(INTERNAL_ERROR_CODE, err.to_string(), None::<()>)
    }
}

#[derive(Clone)]
pub struct TransactionHandler {
    pub transaction_sink: mpsc::UnboundedSender<Arc<SendTransactionRequest>>,
}

impl TransactionHandler {
    pub const fn new(transaction_sink: mpsc::UnboundedSender<Arc<SendTransactionRequest>>) -> Self {
        Self { transaction_sink }
    }

    pub fn get_version() -> RpcVersionInfo {
        let version = Version::default();
        RpcVersionInfo {
            solana_core: version.to_string(),
            feature_set: Some(version.feature_set),
        }
    }

    pub async fn handle_versioned_transaction(
        &self,
        transaction: VersionedTransaction,
        config_with_forwarding_policies: JetRpcSendTransactionConfig,
    ) -> Result<String /* Signature */, TransactionHandlerError> {
        let config = config_with_forwarding_policies.config;

        // Reject transactions requesting preflight, not supported
        if !config.skip_preflight {
            return Err(TransactionHandlerError::PreflightNotSupported);
        }

        // Basic sanitize check
        transaction
            .sanitize()
            .map_err(|e| TransactionHandlerError::InvalidTransaction(e.to_string()))?;

        let signature = transaction.signatures[0];
        let mut wire_transaction = bincode::serialize(&transaction)?;
        if wire_transaction.len() > PACKET_DATA_SIZE {
            wire_transaction.shrink_to_fit();
            if wire_transaction.len() > PACKET_DATA_SIZE {
                return Err(TransactionHandlerError::InvalidTransaction(format!(
                    "transaction size {} exceeds maximum allowed size of {} bytes",
                    wire_transaction.len(),
                    PACKET_DATA_SIZE
                )));
            }
        }

        self.transaction_sink
            .send(Arc::new(SendTransactionRequest {
                signature,
                transaction,
                wire_transaction,
                max_retries: config.max_retries,
                policies: config_with_forwarding_policies.forwarding_policies,
            }))
            .expect("transaction sink closed");

        Ok(signature.to_string())
    }

    pub async fn handle_transaction(
        &self,
        data: String,
        config_with_forwarding_policies: Option<JetRpcSendTransactionConfig>,
    ) -> Result<String /* Signature */, TransactionHandlerError> {
        let config_with_forwarding_policies = config_with_forwarding_policies.unwrap_or_default();
        let config = config_with_forwarding_policies.config;

        let (wire_transaction, transaction) = self.prepare_transaction(data, config).await?;
        let signature = transaction.signatures[0];

        self.transaction_sink
            .send(Arc::new(SendTransactionRequest {
                signature,
                transaction,
                wire_transaction,
                max_retries: config.max_retries,
                policies: config_with_forwarding_policies.forwarding_policies,
            }))
            .expect("transaction sink closed");

        Ok(signature.to_string())
    }

    async fn prepare_transaction(
        &self,
        data: String,
        config: RpcSendTransactionConfig,
    ) -> Result<(Vec<u8>, VersionedTransaction), TransactionHandlerError> {
        let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Base58);

        let (wire_transaction, transaction) = decode_and_deserialize::<VersionedTransaction>(
            data,
            encoding
                .into_binary_encoding()
                .ok_or(TransactionHandlerError::UnsupportedEncoding)?,
        )
        .map_err(|e| TransactionHandlerError::InvalidParams(e.to_string()))?;

        // Reject transactions requesting preflight, not supported
        if !config.skip_preflight {
            return Err(TransactionHandlerError::PreflightNotSupported);
        }

        transaction
            .sanitize()
            .map_err(|e| TransactionHandlerError::InvalidTransaction(e.to_string()))?;

        Ok((wire_transaction, transaction))
    }
}

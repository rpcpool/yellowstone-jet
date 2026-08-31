use {
    crate::{
        payload::JetRpcSendTransactionConfig,
        solana::{decode_and_deserialize, get_durable_nonce},
        transactions::SendTransactionRequest,
    },
    anyhow::Result,
    bytes::Bytes,
    jsonrpsee::types::error::{ErrorObject, ErrorObjectOwned, INTERNAL_ERROR_CODE},
    solana_client::rpc_response::RpcVersionInfo,
    solana_rpc_client_api::config::RpcSendTransactionConfig,
    solana_transaction::versioned::VersionedTransaction,
    solana_transaction_status_client_types::UiTransactionEncoding,
    solana_version::Version,
    thiserror::Error,
    tokio::sync::mpsc,
    uuid::Uuid,
    yellowstone_jet_tpu_client::core::PACKET_DATA_SIZE,
};

#[derive(Debug, Error)]
pub enum TransactionHandlerError {
    #[error("invalid transaction: {0}")]
    InvalidTransaction(String),

    #[error("failed to serialize transaction: {0}")]
    SerializationFailed(#[from] wincode::WriteError),

    #[error("preflight check is not supported")]
    PreflightNotSupported,

    #[error("invalid parameters: {0}")]
    InvalidParams(String),

    #[error("unsupported encoding")]
    UnsupportedEncoding,
}

impl TransactionHandlerError {
    pub const fn variant_name(&self) -> &'static str {
        match self {
            TransactionHandlerError::InvalidTransaction(_) => "InvalidTransaction",
            TransactionHandlerError::SerializationFailed(_) => "SerializationFailed",
            TransactionHandlerError::PreflightNotSupported => "PreflightNotSupported",
            TransactionHandlerError::InvalidParams(_) => "InvalidParams",
            TransactionHandlerError::UnsupportedEncoding => "UnsupportedEncoding",
        }
    }
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
    ///
    /// If true (default), the handler will reject transactions that request preflight checks, as preflight is not supported.
    fail_on_preflight: bool,
    transaction_sink: mpsc::Sender<SendTransactionRequest>,
}

impl TransactionHandler {
    pub const fn new(
        transaction_sink: mpsc::Sender<SendTransactionRequest>,
        fail_on_preflight: bool,
    ) -> Self {
        Self {
            fail_on_preflight,
            transaction_sink,
        }
    }

    pub fn get_version() -> RpcVersionInfo {
        let version = Version::default();
        RpcVersionInfo {
            solana_core: version.to_string(),
            feature_set: Some(version.feature_set()),
        }
    }

    pub async fn handle_versioned_transaction(
        &self,
        transaction: VersionedTransaction,
        config_with_forwarding_policies: JetRpcSendTransactionConfig,
        x_request_id: Option<Uuid>,
        x_subscription_id: Option<Uuid>,
    ) -> Result<String /* Signature */, TransactionHandlerError> {
        let config = config_with_forwarding_policies.config;

        // Reject transactions requesting preflight, not supported
        if !config.skip_preflight && self.fail_on_preflight {
            return Err(TransactionHandlerError::PreflightNotSupported);
        }

        // Basic sanitize check
        transaction
            .sanitize()
            .map_err(|e| TransactionHandlerError::InvalidTransaction(e.to_string()))?;

        let signature = transaction.signatures[0];
        let wire_transaction = wincode::serialize(&transaction)?;
        if wire_transaction.len() > PACKET_DATA_SIZE {
            return Err(TransactionHandlerError::InvalidTransaction(format!(
                "transaction size {} exceeds maximum allowed size of {} bytes",
                wire_transaction.len(),
                PACKET_DATA_SIZE
            )));
        }
        let signer = transaction.message.static_account_keys()[0];
        let req = SendTransactionRequest {
            signature,
            wire_transaction: wire_transaction.into(),
            policies: config_with_forwarding_policies.forwarding_policies,
            x_request_id,
            x_subscription_id,
            signer,
            durable_nonce: get_durable_nonce(&transaction),
            recent_blockhash: *transaction.message.recent_blockhash(),
        };
        self.transaction_sink
            .send(req)
            .await
            .expect("transaction sink closed");

        Ok(signature.to_string())
    }

    pub async fn handle_raw_transaction(
        &self,
        wire_transaction: Bytes,
        config_with_forwarding_policies: JetRpcSendTransactionConfig,
        x_request_id: Option<Uuid>,
        x_subscription_id: Option<Uuid>,
    ) -> Result<String /* Signature */, TransactionHandlerError> {
        if wire_transaction.len() > PACKET_DATA_SIZE {
            return Err(TransactionHandlerError::InvalidTransaction(format!(
                "transaction size {} exceeds maximum allowed size of {} bytes",
                wire_transaction.len(),
                PACKET_DATA_SIZE
            )));
        }

        let transaction: VersionedTransaction = wincode::deserialize(wire_transaction.as_ref())
            .map_err(|e| {
                TransactionHandlerError::InvalidParams(format!(
                    "failed to deserialize transaction: {e}"
                ))
            })?;

        transaction
            .sanitize()
            .map_err(|e| TransactionHandlerError::InvalidTransaction(e.to_string()))?;

        let signature = transaction.signatures[0];
        let signer = transaction.message.static_account_keys()[0];
        let req = SendTransactionRequest {
            signature,
            wire_transaction,
            policies: config_with_forwarding_policies.forwarding_policies,
            x_request_id,
            durable_nonce: get_durable_nonce(&transaction),
            recent_blockhash: *transaction.message.recent_blockhash(),
            x_subscription_id,
            signer,
        };
        self.transaction_sink
            .send(req)
            .await
            .expect("transaction sink closed");

        Ok(signature.to_string())
    }

    pub async fn handle_transaction(
        &self,
        data: String,
        config_with_forwarding_policies: Option<JetRpcSendTransactionConfig>,
        x_request_id: Option<Uuid>,
        x_subscription_id: Option<Uuid>,
    ) -> Result<String /* Signature */, TransactionHandlerError> {
        let config_with_forwarding_policies = config_with_forwarding_policies.unwrap_or_default();
        let config = config_with_forwarding_policies.config;

        let (wire_transaction, transaction) = self.prepare_transaction(data, config).await?;
        let signature = transaction.signatures[0];

        let signer = transaction.message.static_account_keys()[0];
        let req = SendTransactionRequest {
            signature,
            wire_transaction: wire_transaction.into(),
            policies: config_with_forwarding_policies.forwarding_policies,
            x_request_id,
            durable_nonce: get_durable_nonce(&transaction),
            recent_blockhash: *transaction.message.recent_blockhash(),
            x_subscription_id,
            signer,
        };
        self.transaction_sink
            .send(req)
            .await
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
        if !config.skip_preflight && self.fail_on_preflight {
            return Err(TransactionHandlerError::PreflightNotSupported);
        }

        transaction
            .sanitize()
            .map_err(|e| TransactionHandlerError::InvalidTransaction(e.to_string()))?;

        Ok((wire_transaction, transaction))
    }
}

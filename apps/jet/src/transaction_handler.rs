use {
    crate::{
        metrics::jet as jet_metrics, payload::JetRpcSendTransactionConfig,
        solana::decode_and_deserialize, transactions::SendTransactionRequest,
    },
    anyhow::Result,
    bytes::Bytes,
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

    #[error("transaction pipeline unavailable")]
    TransactionPipelineUnavailable,
}

impl TransactionHandlerError {
    pub const fn variant_name(&self) -> &'static str {
        match self {
            TransactionHandlerError::InvalidTransaction(_) => "InvalidTransaction",
            TransactionHandlerError::SerializationFailed(_) => "SerializationFailed",
            TransactionHandlerError::PreflightNotSupported => "PreflightNotSupported",
            TransactionHandlerError::InvalidParams(_) => "InvalidParams",
            TransactionHandlerError::UnsupportedEncoding => "UnsupportedEncoding",
            TransactionHandlerError::TransactionPipelineUnavailable => {
                "TransactionPipelineUnavailable"
            }
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

    fn enqueue_transaction(
        &self,
        request: SendTransactionRequest,
    ) -> Result<(), TransactionHandlerError> {
        self.transaction_sink.send(Arc::new(request)).map_err(|_| {
            jet_metrics::mark_transaction_pipeline_unhealthy("transaction sink closed");
            TransactionHandlerError::TransactionPipelineUnavailable
        })?;

        jet_metrics::mark_transaction_pipeline_healthy();
        Ok(())
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
        let wire_transaction = bincode::serialize(&transaction)?;
        if wire_transaction.len() > PACKET_DATA_SIZE {
            return Err(TransactionHandlerError::InvalidTransaction(format!(
                "transaction size {} exceeds maximum allowed size of {} bytes",
                wire_transaction.len(),
                PACKET_DATA_SIZE
            )));
        }

        self.enqueue_transaction(SendTransactionRequest {
            signature,
            transaction,
            wire_transaction: wire_transaction.into(),
            max_retries: config.max_retries,
            policies: config_with_forwarding_policies.forwarding_policies,
        })?;

        Ok(signature.to_string())
    }

    pub async fn handle_raw_transaction(
        &self,
        wire_transaction: Bytes,
        config_with_forwarding_policies: JetRpcSendTransactionConfig,
    ) -> Result<String /* Signature */, TransactionHandlerError> {
        if wire_transaction.len() > PACKET_DATA_SIZE {
            return Err(TransactionHandlerError::InvalidTransaction(format!(
                "transaction size {} exceeds maximum allowed size of {} bytes",
                wire_transaction.len(),
                PACKET_DATA_SIZE
            )));
        }

        let transaction: VersionedTransaction = bincode::deserialize(wire_transaction.as_ref())
            .map_err(|e| {
                TransactionHandlerError::InvalidParams(format!(
                    "failed to deserialize transaction: {e}"
                ))
            })?;

        transaction
            .sanitize()
            .map_err(|e| TransactionHandlerError::InvalidTransaction(e.to_string()))?;

        let signature = transaction.signatures[0];

        self.enqueue_transaction(SendTransactionRequest {
            signature,
            transaction,
            wire_transaction,
            max_retries: config_with_forwarding_policies.config.max_retries,
            policies: config_with_forwarding_policies.forwarding_policies,
        })?;

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

        self.enqueue_transaction(SendTransactionRequest {
            signature,
            transaction,
            wire_transaction: wire_transaction.into(),
            max_retries: config.max_retries,
            policies: config_with_forwarding_policies.forwarding_policies,
        })?;

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

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::metrics::jet as jet_metrics,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_message::{VersionedMessage, v0},
        solana_rpc_client_api::config::RpcSendTransactionConfig,
        solana_signer::Signer,
        solana_system_interface::instruction::transfer,
    };

    fn test_transaction() -> VersionedTransaction {
        let payer = Keypair::new();
        let receiver = Keypair::new();
        let instructions = [transfer(&payer.pubkey(), &receiver.pubkey(), 1)];

        VersionedTransaction::try_new(
            VersionedMessage::V0(
                v0::Message::try_compile(&payer.pubkey(), &instructions, &[], Hash::new_unique())
                    .expect("try compile"),
            ),
            &[&payer],
        )
        .expect("try new")
    }

    fn send_config() -> JetRpcSendTransactionConfig {
        JetRpcSendTransactionConfig {
            config: RpcSendTransactionConfig {
                skip_preflight: true,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn closed_transaction_sink_returns_pipeline_unavailable_and_marks_unhealthy() {
        jet_metrics::mark_transaction_pipeline_healthy();
        let (transaction_sink, transaction_source) = mpsc::unbounded_channel();
        drop(transaction_source);

        let handler = TransactionHandler::new(transaction_sink);
        let err = handler
            .handle_versioned_transaction(test_transaction(), send_config())
            .await
            .expect_err("closed sink should fail");

        assert!(matches!(
            err,
            TransactionHandlerError::TransactionPipelineUnavailable
        ));
        assert!(
            jet_metrics::get_transaction_pipeline_health_status()
                .expect_err("closed sink should mark health unhealthy")
                .to_string()
                .contains("transaction sink closed")
        );

        jet_metrics::mark_transaction_pipeline_healthy();
    }
}

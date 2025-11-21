use {
    crate::{
        payload::JetRpcSendTransactionConfig, solana::decode_and_deserialize,
        transactions::SendTransactionRequest,
    },
    anyhow::Result,
    jsonrpsee::types::error::{ErrorObject, ErrorObjectOwned, INTERNAL_ERROR_CODE},
    solana_client::{
        client_error::ClientErrorKind,
        nonblocking::rpc_client::RpcClient,
        rpc_config::{RcpSanitizeTransactionConfig, RpcSimulateTransactionConfig},
        rpc_request::{RpcError, RpcResponseErrorData},
        rpc_response::{Response as RpcResponse, RpcSimulateTransactionResult, RpcVersionInfo},
    },
    solana_commitment_config::CommitmentConfig,
    solana_rpc_client_api::config::RpcSendTransactionConfig,
    solana_transaction::versioned::VersionedTransaction,
    solana_transaction_status_client_types::UiTransactionEncoding,
    solana_version::Version,
    std::sync::Arc,
    thiserror::Error,
    tokio::sync::mpsc,
};

#[derive(Debug, Error)]
pub enum TransactionHandlerError {
    #[error("invalid transaction: {0}")]
    InvalidTransaction(String),

    #[error("transaction simulation failed: {0}")]
    SimulationFailed(String),

    #[error("failed to serialize transaction: {0}")]
    SerializationFailed(#[from] bincode::Error),

    #[error("transaction sanitize check failed: {0}")]
    SanitizeCheckFailed(String),

    #[error("node unhealthy: {num_slots_behind} slots behind")]
    NodeUnhealthy { num_slots_behind: u64 },

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
    pub rpc: Arc<RpcClient>,
    pub proxy_sanitize_check: bool,
    pub proxy_preflight_check: bool,
}

impl TransactionHandler {
    pub fn new(
        transaction_sink: mpsc::UnboundedSender<Arc<SendTransactionRequest>>,
        rpc: &Arc<RpcClient>,
        proxy_sanitize_check: bool,
        proxy_preflight_check: bool,
    ) -> Self {
        Self {
            transaction_sink,
            rpc: Arc::clone(rpc),
            proxy_sanitize_check,
            proxy_preflight_check,
        }
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

        // Basic sanitize check first
        transaction
            .sanitize()
            .map_err(|e| TransactionHandlerError::InvalidTransaction(e.to_string()))?;

        // Run preflight/sanitize checks if needed
        if !config.skip_preflight && self.proxy_preflight_check {
            self.handle_preflight(&transaction, &config).await?;
        } else if !config.skip_sanitize && self.proxy_sanitize_check {
            self.handle_sanitize(&transaction, &config).await?;
        }

        let signature = transaction.signatures[0];
        let wire_transaction = bincode::serialize(&transaction)?;

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

        let (wire_transaction, transaction) = decode_and_deserialize(
            data,
            encoding
                .into_binary_encoding()
                .ok_or(TransactionHandlerError::UnsupportedEncoding)?,
        )
        .map_err(|e| TransactionHandlerError::InvalidParams(e.to_string()))?;

        if !config.skip_preflight && self.proxy_preflight_check {
            self.handle_preflight(&transaction, &config).await?;
        } else if !config.skip_sanitize && self.proxy_sanitize_check {
            self.handle_sanitize(&transaction, &config).await?;
        } else {
            transaction
                .sanitize()
                .map_err(|e| TransactionHandlerError::InvalidTransaction(e.to_string()))?;
        }

        Ok((wire_transaction, transaction))
    }

    async fn handle_preflight(
        &self,
        transaction: &VersionedTransaction,
        config: &RpcSendTransactionConfig,
    ) -> Result<(), TransactionHandlerError> {
        match self
            .rpc
            .simulate_transaction_with_config(
                transaction,
                RpcSimulateTransactionConfig {
                    sig_verify: true,
                    commitment: config
                        .preflight_commitment
                        .map(|commitment| CommitmentConfig { commitment }),
                    min_context_slot: config.min_context_slot,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(RpcResponse {
                value:
                    RpcSimulateTransactionResult {
                        err: Some(error), ..
                    },
                ..
            }) => Err(TransactionHandlerError::SimulationFailed(error.to_string())),
            Ok(_) => Ok(()),
            Err(error) => match *error.kind {
                ClientErrorKind::RpcError(RpcError::RpcResponseError {
                    data: RpcResponseErrorData::NodeUnhealthy { num_slots_behind },
                    ..
                }) => Err(TransactionHandlerError::NodeUnhealthy {
                    num_slots_behind: num_slots_behind.unwrap_or(0),
                }),
                _ => Err(TransactionHandlerError::SimulationFailed(error.to_string())),
            },
        }
    }

    async fn handle_sanitize(
        &self,
        transaction: &VersionedTransaction,
        config: &RpcSendTransactionConfig,
    ) -> Result<(), TransactionHandlerError> {
        match self
            .rpc
            .sanitize_transaction(
                transaction,
                RcpSanitizeTransactionConfig {
                    sig_verify: true,
                    commitment: config
                        .preflight_commitment
                        .map(|commitment| CommitmentConfig { commitment }),
                    min_context_slot: config.min_context_slot,
                    ..Default::default()
                },
            )
            .await
        {
            Err(error) => Err(TransactionHandlerError::SanitizeCheckFailed(
                error.to_string(),
            )),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_client::{
            nonblocking::pubsub_client::PubsubClientResult, rpc_response::RpcResponseContext,
        },
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_message::Message,
        solana_pubkey::Pubkey,
        solana_signer::Signer,
        solana_transaction::{Transaction, TransactionError},
    };

    #[derive(Debug)]
    struct MockRpcClient;

    impl MockRpcClient {
        fn new() -> Arc<Self> {
            Arc::new(Self)
        }

        async fn simulate_transaction_with_config(
            &self,
            _transaction: &VersionedTransaction,
            _config: RpcSimulateTransactionConfig,
        ) -> PubsubClientResult<RpcResponse<RpcSimulateTransactionResult>> {
            Ok(RpcResponse {
                context: RpcResponseContext {
                    slot: 0,
                    api_version: None,
                },
                value: RpcSimulateTransactionResult {
                    err: Some(
                        solana_transaction_status_client_types::UiTransactionError::from(
                            TransactionError::AccountBorrowOutstanding,
                        ),
                    ),
                    logs: None,
                    accounts: None,
                    units_consumed: None,
                    return_data: None,
                    inner_instructions: None,
                    replacement_blockhash: None,
                    loaded_accounts_data_size: None,
                    pre_balances: Some(vec![]),
                    post_balances: Some(vec![]),
                    fee: Some(0),
                    loaded_addresses: None,
                    pre_token_balances: None,
                    post_token_balances: None,
                },
            })
        }

        async fn sanitize_transaction(
            &self,
            _transaction: &VersionedTransaction,
            _config: RcpSanitizeTransactionConfig,
        ) -> PubsubClientResult<()> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct MockTxHandler {
        rpc: Arc<MockRpcClient>,
    }

    impl MockTxHandler {
        fn new() -> Self {
            Self {
                rpc: MockRpcClient::new(),
            }
        }

        async fn handle_preflight(
            &self,
            transaction: &VersionedTransaction,
            config: &RpcSendTransactionConfig,
        ) -> Result<(), TransactionHandlerError> {
            match self
                .rpc
                .simulate_transaction_with_config(
                    transaction,
                    RpcSimulateTransactionConfig {
                        sig_verify: true,
                        commitment: config
                            .preflight_commitment
                            .map(|commitment| CommitmentConfig { commitment }),
                        min_context_slot: config.min_context_slot,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(RpcResponse {
                    value:
                        RpcSimulateTransactionResult {
                            err: Some(error), ..
                        },
                    ..
                }) => Err(TransactionHandlerError::SimulationFailed(error.to_string())),
                Ok(_) => Ok(()),
                Err(error) => Err(TransactionHandlerError::SimulationFailed(error.to_string())),
            }
        }

        async fn handle_sanitize(
            &self,
            transaction: &VersionedTransaction,
            config: &RpcSendTransactionConfig,
        ) -> Result<(), TransactionHandlerError> {
            match self
                .rpc
                .sanitize_transaction(
                    transaction,
                    RcpSanitizeTransactionConfig {
                        sig_verify: true,
                        commitment: config
                            .preflight_commitment
                            .map(|commitment| CommitmentConfig { commitment }),
                        min_context_slot: config.min_context_slot,
                        ..Default::default()
                    },
                )
                .await
            {
                Err(error) => Err(TransactionHandlerError::SanitizeCheckFailed(
                    error.to_string(),
                )),
                _ => Ok(()),
            }
        }
    }

    #[tokio::test]
    async fn test_handle_preflight_invalid_transaction() {
        let handler = MockTxHandler::new();

        let keypair = Keypair::new();
        let recipient = Pubkey::new_unique();
        let instruction = solana_system_interface::instruction::transfer(
            &keypair.pubkey(),
            &recipient,
            1_000_000_000_000,
        );
        let message = Message::new(&[instruction], Some(&keypair.pubkey()));
        let tx = Transaction::new(&[&keypair], message, Hash::default());
        let versioned_tx = VersionedTransaction::from(tx);

        let result = handler
            .handle_preflight(&versioned_tx, &RpcSendTransactionConfig::default())
            .await;

        assert!(result.is_err());
        match result {
            Err(TransactionHandlerError::SimulationFailed(_)) => {}
            _ => panic!("Expected SimulationFailed error"),
        }
    }

    #[tokio::test]
    async fn test_handle_sanitize_check() {
        let handler = MockTxHandler::new();

        let keypair = Keypair::new();
        let recipient = Pubkey::new_unique();
        let instruction =
            solana_system_interface::instruction::transfer(&keypair.pubkey(), &recipient, 1_000);
        let message = Message::new(&[instruction], Some(&keypair.pubkey()));
        let tx = Transaction::new(&[&keypair], message, Hash::default());
        let versioned_tx = VersionedTransaction::from(tx);

        let result = handler
            .handle_sanitize(&versioned_tx, &RpcSendTransactionConfig::default())
            .await;

        assert!(result.is_ok());
    }
}

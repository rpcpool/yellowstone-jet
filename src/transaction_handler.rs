use {
    crate::{
        rpc::invalid_params,
        solana::decode_and_deserialize,
        transactions::{SendTransactionRequest, SendTransactionsPool},
    },
    anyhow::Result,
    jsonrpsee::types::error::{
        ErrorObject, ErrorObjectOwned, INTERNAL_ERROR_CODE, INTERNAL_ERROR_MSG,
    },
    solana_client::{
        client_error::{ClientError, ClientErrorKind},
        nonblocking::rpc_client::RpcClient,
        rpc_config::{RcpSanitizeTransactionConfig, RpcSimulateTransactionConfig},
        rpc_request::{RpcError, RpcResponseErrorData},
        rpc_response::{Response as RpcResponse, RpcSimulateTransactionResult, RpcVersionInfo},
    },
    solana_rpc_client_api::{
        config::RpcSendTransactionConfig,
        custom_error::{
            NodeUnhealthyErrorData, JSON_RPC_SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE,
        },
    },
    solana_sdk::{commitment_config::CommitmentConfig, transaction::VersionedTransaction},
    solana_transaction_status::UiTransactionEncoding,
    solana_version::Version,
    std::sync::Arc,
    tracing::warn,
};

pub struct TransactionHandler<'a> {
    pub sts: &'a SendTransactionsPool,
    pub rpc: Arc<RpcClient>,
    pub proxy_sanitize_check: bool,
    pub proxy_preflight_check: bool,
}

impl<'a> TransactionHandler<'a> {
    pub fn new(
        sts: &'a SendTransactionsPool,
        rpc: &'a Arc<RpcClient>,
        proxy_sanitize_check: bool,
        proxy_preflight_check: bool,
    ) -> Self {
        Self {
            sts,
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
        config: Option<RpcSendTransactionConfig>,
    ) -> Result<String, ErrorObjectOwned> {
        let config = config.unwrap_or_default();

        // Basic sanitize check first
        transaction
            .sanitize()
            .map_err(|e| invalid_params(format!("invalid transaction: {}", e)))?;

        // Run preflight/sanitize checks if needed
        if !config.skip_preflight && self.proxy_preflight_check {
            self.handle_preflight(&transaction, &config).await?;
        } else if !config.skip_sanitize && self.proxy_sanitize_check {
            self.handle_sanitize(&transaction, &config).await?;
        }

        let signature = transaction.signatures[0];
        let wire_transaction = bincode::serialize(&transaction)
            .map_err(|e| invalid_params(format!("failed to serialize transaction: {e}")))?;

        if let Err(error) = self.sts.send_transaction(SendTransactionRequest {
            signature,
            transaction,
            wire_transaction,
            max_retries: config.max_retries,
        }) {
            return Err(ErrorObject::owned(
                INTERNAL_ERROR_CODE,
                INTERNAL_ERROR_MSG,
                Some(format!("{error}")),
            ));
        }

        Ok(signature.to_string())
    }

    pub async fn handle_transaction(
        &self,
        data: String,
        config: Option<RpcSendTransactionConfig>,
    ) -> Result<String, ErrorObjectOwned> {
        let (wire_transaction, transaction) = self.prepare_transaction(data, config).await?;
        let signature = transaction.signatures[0];

        if let Err(error) = self.sts.send_transaction(SendTransactionRequest {
            signature,
            transaction,
            wire_transaction,
            max_retries: config.and_then(|c| c.max_retries),
        }) {
            return Err(ErrorObject::owned(
                INTERNAL_ERROR_CODE,
                INTERNAL_ERROR_MSG,
                Some(format!("{error}")),
            ));
        }

        Ok(signature.to_string())
    }

    async fn prepare_transaction(
        &self,
        data: String,
        config: Option<RpcSendTransactionConfig>,
    ) -> Result<(Vec<u8>, VersionedTransaction), ErrorObjectOwned> {
        let config = config.unwrap_or_default();
        let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Base58);

        let (wire_transaction, transaction) = decode_and_deserialize(
            data,
            encoding
                .into_binary_encoding()
                .ok_or_else(|| invalid_params("unsupported encoding"))?,
        )?;

        if !config.skip_preflight && self.proxy_preflight_check {
            self.handle_preflight(&transaction, &config).await?;
        } else if !config.skip_sanitize && self.proxy_sanitize_check {
            self.handle_sanitize(&transaction, &config).await?;
        } else {
            transaction
                .sanitize()
                .map_err(|e| invalid_params(format!("invalid transaction: {}", e)))?;
        }

        Ok((wire_transaction, transaction))
    }

    async fn handle_preflight(
        &self,
        transaction: &VersionedTransaction,
        config: &RpcSendTransactionConfig,
    ) -> Result<(), ErrorObjectOwned> {
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
                context: _,
                value:
                    RpcSimulateTransactionResult {
                        err,
                        logs,
                        units_consumed,
                        return_data,
                        ..
                    },
            }) => {
                if let Some(error) = err {
                    return Err(ErrorObject::owned(
                        JSON_RPC_SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE as i32,
                        format!("Transaction simulation failed: {error}"),
                        Some(RpcSimulateTransactionResult {
                            err: Some(error),
                            logs,
                            accounts: None,
                            units_consumed,
                            return_data,
                            inner_instructions: None,
                            replacement_blockhash: None,
                        }),
                    ));
                }
                Ok(())
            }
            Err(error) => Err(self.unwrap_client_error("simulate_transaction", error)),
        }
    }

    async fn handle_sanitize(
        &self,
        transaction: &VersionedTransaction,
        config: &RpcSendTransactionConfig,
    ) -> Result<(), ErrorObjectOwned> {
        if let Err(error) = self
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
            Err(self.unwrap_client_error("sanitize_transaction", error))
        } else {
            Ok(())
        }
    }

    fn unwrap_client_error(&self, call: &str, error: ClientError) -> ErrorObjectOwned {
        if let ClientErrorKind::RpcError(RpcError::RpcResponseError {
            code,
            message,
            data,
        }) = error.kind
        {
            ErrorObject::owned(
                code as i32,
                message,
                match data {
                    RpcResponseErrorData::Empty => None,
                    RpcResponseErrorData::SendTransactionPreflightFailure(_) => {
                        unreachable!("impossible response")
                    }
                    RpcResponseErrorData::NodeUnhealthy { num_slots_behind } => {
                        Some(NodeUnhealthyErrorData { num_slots_behind })
                    }
                },
            )
        } else {
            warn!("{call} failed: {error}");
            ErrorObject::owned::<()>(INTERNAL_ERROR_CODE, INTERNAL_ERROR_MSG, None)
        }
    }
}

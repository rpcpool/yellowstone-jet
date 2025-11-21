use {
    crate::{metrics, rpc::invalid_params},
    base64::{Engine, prelude::BASE64_STANDARD},
    bincode::config::Options,
    jsonrpsee::core::RpcResult,
    solana_bincode::limited_deserialize,
    solana_client::{
        client_error::{ClientError, ClientErrorKind},
        nonblocking::rpc_client::RpcClient,
        rpc_config::RcpSanitizeTransactionConfig,
        rpc_request::RpcError,
    },
    solana_nonce::NONCED_TX_MARKER_IX_INDEX,
    solana_packet::PACKET_DATA_SIZE,
    solana_pubkey::Pubkey,
    solana_system_interface::instruction::SystemInstruction,
    solana_transaction::{Transaction, versioned::VersionedTransaction},
    solana_transaction_status_client_types::TransactionBinaryEncoding,
    std::any::type_name,
};

const MAX_BASE58_SIZE: usize = 1683; // Golden, bump if PACKET_DATA_SIZE changes
const MAX_BASE64_SIZE: usize = 1644; // Golden, bump if PACKET_DATA_SIZE changes
pub fn decode_and_deserialize<T>(
    encoded: String,
    encoding: TransactionBinaryEncoding,
) -> RpcResult<(Vec<u8>, T)>
where
    T: serde::de::DeserializeOwned,
{
    let wire_output = match encoding {
        TransactionBinaryEncoding::Base58 => {
            if encoded.len() > MAX_BASE58_SIZE {
                metrics::jet::increment_transaction_decode_error("base58_size_exceeded");
                return Err(invalid_params(format!(
                    "base58 encoded {} too large: {} bytes (max: encoded/raw {}/{})",
                    type_name::<T>(),
                    encoded.len(),
                    MAX_BASE58_SIZE,
                    PACKET_DATA_SIZE,
                )));
            }
            match bs58::decode(encoded).into_vec() {
                Ok(v) => v,
                Err(e) => {
                    metrics::jet::increment_transaction_decode_error("invalid_base58");
                    return Err(invalid_params(format!("invalid base58 encoding: {e:?}")));
                }
            }
        }
        TransactionBinaryEncoding::Base64 => {
            if encoded.len() > MAX_BASE64_SIZE {
                metrics::jet::increment_transaction_decode_error("base64_size_exceeded");
                return Err(invalid_params(format!(
                    "base64 encoded {} too large: {} bytes (max: encoded/raw {}/{})",
                    type_name::<T>(),
                    encoded.len(),
                    MAX_BASE64_SIZE,
                    PACKET_DATA_SIZE,
                )));
            }
            match BASE64_STANDARD.decode(encoded) {
                Ok(v) => v,
                Err(e) => {
                    metrics::jet::increment_transaction_decode_error("invalid_base64");
                    return Err(invalid_params(format!("invalid base64 encoding: {e:?}")));
                }
            }
        }
    };
    if wire_output.len() > PACKET_DATA_SIZE {
        metrics::jet::increment_transaction_decode_error("decoded_size_exceeded");
        return Err(invalid_params(format!(
            "decoded {} too large: {} bytes (max: {} bytes)",
            type_name::<T>(),
            wire_output.len(),
            PACKET_DATA_SIZE
        )));
    }
    match bincode::options()
        .with_limit(PACKET_DATA_SIZE as u64)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from(&wire_output[..])
    {
        Ok(output) => Ok((wire_output, output)),
        Err(err) => {
            metrics::jet::increment_transaction_deserialize_error("bincode");
            Err(invalid_params(format!(
                "failed to deserialize {}: {}",
                type_name::<T>(),
                &err.to_string()
            )))
        }
    }
}

pub fn get_durable_nonce(tx: &VersionedTransaction) -> Option<Pubkey> {
    tx.message
        .instructions()
        .get(NONCED_TX_MARKER_IX_INDEX as usize)
        .filter(|ix| {
            // limited by static account keys
            // let account_keys = self.account_keys();
            let account_keys = tx.message.static_account_keys();
            match account_keys.get(ix.program_id_index as usize) {
                Some(program_id) => solana_sdk_ids::system_program::check_id(program_id),
                _ => false,
            }
        })
        .filter(|ix| {
            matches!(
                limited_deserialize::<SystemInstruction>(
                    &ix.data, 4 /* serialized size of AdvanceNonceAccount */
                ),
                Ok(SystemInstruction::AdvanceNonceAccount)
            )
        })
        .and_then(|ix| {
            ix.accounts.first().and_then(|idx| {
                let idx = *idx as usize;
                // if !self.is_writable(idx) {
                //     None
                // } else {
                //     self.account_keys().get(idx)
                // }
                let account_keys = tx.message.static_account_keys();
                account_keys.get(idx)
            })
        })
        .cloned()
}

pub async fn sanitize_transaction_support_check(rpc: &RpcClient) -> anyhow::Result<bool> {
    if let Err(ClientError { kind, .. }) = rpc
        .sanitize_transaction(
            &Transaction::default(),
            RcpSanitizeTransactionConfig::default(),
        )
        .await
    {
        if let ClientErrorKind::RpcError(RpcError::RpcResponseError { code, .. }) = *kind {
            match code {
                -32601 => return Ok(false),
                -32602 => return Ok(true),
                _ => {}
            }
        }
    }

    anyhow::bail!("failed to check sanitizeTransaction in RPC")
}

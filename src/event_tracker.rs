use {
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{net::SocketAddr, time::SystemTime},
};

// TODO: This is just a simple even tracking implementation for lewis, we will want to
// add more things from the proto, I'm not adding it all because we need to define how we will track the
// ids for cascade, gateway and jet.
#[derive(Debug, Clone)]
pub struct SendAttempt {
    pub validator: Pubkey,
    pub tpu_addr: SocketAddr,
    pub result: SendResult,
    pub timestamp: SystemTime,
}

impl SendAttempt {
    pub fn success(validator: Pubkey, tpu_addr: SocketAddr) -> Self {
        Self {
            validator,
            tpu_addr,
            result: SendResult::Success,
            timestamp: SystemTime::now(),
        }
    }

    pub fn skipped(validator: Pubkey, tpu_addr: SocketAddr, reason: String) -> Self {
        Self {
            validator,
            tpu_addr,
            result: SendResult::Skipped { reason },
            timestamp: SystemTime::now(),
        }
    }

    pub fn failed(validator: Pubkey, tpu_addr: SocketAddr, error: String) -> Self {
        Self {
            validator,
            tpu_addr,
            result: SendResult::Failed { error },
            timestamp: SystemTime::now(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum SendResult {
    Success,
    Skipped { reason: String },
    Failed { error: String },
}

impl SendResult {
    pub const fn is_success(&self) -> bool {
        matches!(self, SendResult::Success)
    }
}

#[async_trait::async_trait]
pub trait TransactionEventTracker: Send + Sync {
    fn track_transaction_send(
        &self,
        signature: &Signature,
        slot: Slot,
        send_attempts: Vec<SendAttempt>,
    );
}

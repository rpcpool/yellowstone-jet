use {
    crate::metrics::jet::shield_policies_not_found_inc,
    solana_sdk::{pubkey::Pubkey, transaction::VersionedTransaction},
    std::{collections::HashSet, sync::Arc},
    yellowstone_shield_store::{CheckError, PolicyStore, PolicyStoreTrait},
};

#[derive(Clone)]
/// IdentitiesBlocker is a blocking service that checks if the transaction signer or the next leader is in the
/// list of identities that are not allowed.
/// It uses a policy store to check if the identity is allowed to sign transactions or be a leader.
pub struct IdentitiesBlocker {
    config_identities: HashSet<Pubkey>,
    policies_blocker: Arc<dyn IsAllowed + Send + Sync + 'static>,
}

impl IdentitiesBlocker {
    pub const fn new(
        config_identities: HashSet<Pubkey>,
        policies_blocker: Arc<dyn IsAllowed + Send + Sync + 'static>,
    ) -> Self {
        Self {
            config_identities,
            policies_blocker,
        }
    }
}

pub trait AllowTxSigner {
    fn allow_tx_signer(&self, policies: &[Pubkey], tx: &VersionedTransaction) -> bool;
}

impl AllowTxSigner for IdentitiesBlocker {
    fn allow_tx_signer(&self, policies: &[Pubkey], tx: &VersionedTransaction) -> bool {
        let tx_message = &tx.message;
        let accounts = tx_message.static_account_keys();
        for identity in accounts {
            match self.policies_blocker.is_allowed(policies, identity) {
                Ok(true) => {
                    if self.config_identities.contains(identity) {
                        return false;
                    }
                }
                Ok(false) => {
                    return false;
                }
                Err(CheckError::PolicyNotFound) => {
                    shield_policies_not_found_inc();
                    return false;
                }
            }
        }
        true
    }
}
pub trait AllowLeader {
    fn allow_leader(&self, policies: &[Pubkey], identity: &Pubkey) -> bool;
}

impl AllowLeader for IdentitiesBlocker {
    fn allow_leader(&self, policies: &[Pubkey], identity: &Pubkey) -> bool {
        match self.policies_blocker.is_allowed(policies, identity) {
            Ok(true) => !self.config_identities.contains(identity),
            Ok(false) => false,
            Err(CheckError::PolicyNotFound) => {
                shield_policies_not_found_inc();
                false
            }
        }
    }
}

/// Trait implementation to create easier tests for the IdentitiesBlocker
/// It abstracts `fn is_allowed` from yellowstone_shield_store
pub trait IsAllowed {
    fn is_allowed(&self, policies: &[Pubkey], identity: &Pubkey) -> Result<bool, CheckError>;
}

impl IsAllowed for PolicyStore {
    fn is_allowed(&self, policies: &[Pubkey], identity: &Pubkey) -> Result<bool, CheckError> {
        if policies.is_empty() {
            return Ok(true);
        }
        self.snapshot().is_allowed(policies, identity)
    }
}

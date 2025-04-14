use {
    solana_sdk::{
        hash::Hash,
        message::{v0, VersionedMessage},
        pubkey::Pubkey,
        signature::Keypair,
        signer::Signer,
        system_instruction,
        transaction::VersionedTransaction,
    },
    std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    },
    yellowstone_jet::blocking_services::{
        AllowLeader, AllowTxSigner, IdentitiesBlocker, IsAllowed,
    },
    yellowstone_shield_store::CheckError,
};

#[tokio::test]
async fn test_block_tpu() {
    let leader = Pubkey::new_unique();

    let policy_key = Pubkey::new_unique();
    let policy_value = HashSet::from([leader]);
    let mut identities = HashMap::new();

    identities.insert(policy_key, policy_value);
    let mut strategies = HashMap::new();
    strategies.insert(policy_key, PermissionStrategy::Deny);

    let policy_store = MockPolicyStore::new(identities, strategies);
    let identities_blocker = IdentitiesBlocker::new(HashSet::new(), Arc::new(policy_store));
    assert!(!identities_blocker.allow_leader(&[policy_key], &leader));
}

#[tokio::test]
async fn test_allow_tpu() {
    let leader = Pubkey::new_unique();

    let policy_key = Pubkey::new_unique();
    let policy_value = HashSet::from([leader]);
    let mut identities = HashMap::new();

    identities.insert(policy_key, policy_value);
    let mut strategies = HashMap::new();
    strategies.insert(policy_key, PermissionStrategy::Allow);

    let policy_store = MockPolicyStore::new(identities, strategies);
    let identities_blocker = IdentitiesBlocker::new(HashSet::new(), Arc::new(policy_store));
    assert!(identities_blocker.allow_leader(&[policy_key], &leader));
}

#[tokio::test]
async fn test_allow_denied_identities_takes_prescendence_tpu() {
    let leader = Pubkey::new_unique();

    let policy_key = Pubkey::new_unique();
    let policy_value = HashSet::from([leader]);
    let mut identities = HashMap::new();

    identities.insert(policy_key, policy_value);
    let mut strategies = HashMap::new();
    strategies.insert(policy_key, PermissionStrategy::Allow);

    let policy_store = MockPolicyStore::new(identities, strategies);
    let identities_blocker =
        IdentitiesBlocker::new(HashSet::from([leader]), Arc::new(policy_store));
    assert!(!identities_blocker.allow_leader(&[policy_key], &leader));
}

#[tokio::test]
async fn test_block_tpu_not_found_policy() {
    let leader = Pubkey::new_unique();

    let policy_key = Pubkey::new_unique();
    let policy_value = HashSet::from([leader]);
    let mut identities = HashMap::new();

    identities.insert(policy_key, policy_value);
    let strategies = HashMap::new();

    let policy_store = MockPolicyStore::new(identities, strategies);
    let identities_blocker = IdentitiesBlocker::new(HashSet::new(), Arc::new(policy_store));
    assert!(!identities_blocker.allow_leader(&[policy_key], &leader));
}

#[tokio::test]
async fn test_block_tx() {
    let signer = Keypair::new();
    let fake_wallet_keypair = Keypair::new();
    let tx_hash = Hash::new_unique();
    let policy_key = Pubkey::new_unique();
    let policy_value = HashSet::from([signer.pubkey()]);
    let mut identities = HashMap::new();

    identities.insert(policy_key, policy_value);
    let mut strategies = HashMap::new();
    strategies.insert(policy_key, PermissionStrategy::Deny);

    let policy_store = MockPolicyStore::new(identities, strategies);

    let identities_blocker = IdentitiesBlocker::new(HashSet::new(), Arc::new(policy_store));

    let instructions = vec![system_instruction::transfer(
        &signer.pubkey(),
        &fake_wallet_keypair.pubkey(),
        10,
    )];

    let tx = VersionedTransaction::try_new(
        VersionedMessage::V0(
            v0::Message::try_compile(&signer.pubkey(), &instructions, &[], tx_hash)
                .expect("try compile"),
        ),
        &[&signer],
    )
    .expect("try new");

    assert!(!identities_blocker.allow_tx_signer(&[policy_key], &tx));
}

#[tokio::test]
async fn test_allow_tx() {
    let signer = Keypair::new();
    let fake_wallet_keypair = Keypair::new();
    let tx_hash = Hash::new_unique();
    let policy_key = Pubkey::new_unique();
    let policy_value = HashSet::from([signer.pubkey()]);
    let mut identities = HashMap::new();

    identities.insert(policy_key, policy_value);
    let mut strategies = HashMap::new();
    strategies.insert(policy_key, PermissionStrategy::Allow);

    let policy_store = MockPolicyStore::new(identities, strategies);

    let identities_blocker = IdentitiesBlocker::new(HashSet::new(), Arc::new(policy_store));

    let instructions = vec![system_instruction::transfer(
        &signer.pubkey(),
        &fake_wallet_keypair.pubkey(),
        10,
    )];

    let tx = VersionedTransaction::try_new(
        VersionedMessage::V0(
            v0::Message::try_compile(&signer.pubkey(), &instructions, &[], tx_hash)
                .expect("try compile"),
        ),
        &[&signer],
    )
    .expect("try new");

    assert!(identities_blocker.allow_tx_signer(&[policy_key], &tx));
}

#[tokio::test]
async fn test_block_tx_not_found_policy() {
    let signer = Keypair::new();
    let fake_wallet_keypair = Keypair::new();
    let tx_hash = Hash::new_unique();
    let policy_key = Pubkey::new_unique();
    let policy_value = HashSet::from([signer.pubkey()]);
    let mut identities = HashMap::new();

    identities.insert(policy_key, policy_value);
    let strategies = HashMap::new();

    let policy_store = MockPolicyStore::new(identities, strategies);

    let identities_blocker = IdentitiesBlocker::new(HashSet::new(), Arc::new(policy_store));

    let instructions = vec![system_instruction::transfer(
        &signer.pubkey(),
        &fake_wallet_keypair.pubkey(),
        10,
    )];

    let tx = VersionedTransaction::try_new(
        VersionedMessage::V0(
            v0::Message::try_compile(&signer.pubkey(), &instructions, &[], tx_hash)
                .expect("try compile"),
        ),
        &[&signer],
    )
    .expect("try new");

    assert!(!identities_blocker.allow_tx_signer(&[policy_key], &tx));
}

#[tokio::test]
async fn test_allow_denied_identities_takes_prescendence_tx() {
    let signer = Keypair::new();
    let fake_wallet_keypair = Keypair::new();
    let tx_hash = Hash::new_unique();
    let policy_key = Pubkey::new_unique();
    let policy_value = HashSet::from([signer.pubkey()]);
    let mut identities = HashMap::new();

    identities.insert(policy_key, policy_value);
    let mut strategies = HashMap::new();
    strategies.insert(policy_key, PermissionStrategy::Allow);

    let policy_store = MockPolicyStore::new(identities, strategies);

    let identities_blocker =
        IdentitiesBlocker::new(HashSet::from([signer.pubkey()]), Arc::new(policy_store));

    let instructions = vec![system_instruction::transfer(
        &signer.pubkey(),
        &fake_wallet_keypair.pubkey(),
        10,
    )];

    let tx = VersionedTransaction::try_new(
        VersionedMessage::V0(
            v0::Message::try_compile(&signer.pubkey(), &instructions, &[], tx_hash)
                .expect("try compile"),
        ),
        &[&signer],
    )
    .expect("try new");

    assert!(!identities_blocker.allow_tx_signer(&[policy_key], &tx));
}

pub enum PermissionStrategy {
    Allow,
    Deny,
}

pub struct MockPolicyStore {
    identities: HashMap<Pubkey, HashSet<Pubkey>>,
    strategies: HashMap<Pubkey, PermissionStrategy>,
}

impl MockPolicyStore {
    pub const fn new(
        identities: HashMap<Pubkey, HashSet<Pubkey>>,
        strategies: HashMap<Pubkey, PermissionStrategy>,
    ) -> Self {
        Self {
            identities,
            strategies,
        }
    }

    pub fn add_identity(&mut self, identity: Pubkey, policies: HashSet<Pubkey>) {
        self.identities.insert(identity, policies);
    }
}

impl IsAllowed for MockPolicyStore {
    fn is_allowed(&self, policies: &[Pubkey], identity: &Pubkey) -> Result<bool, CheckError> {
        for policy in policies {
            let permission = self
                .strategies
                .get(policy)
                .ok_or(CheckError::PolicyNotFound)?;

            if let Some(identities) = self.identities.get(policy) {
                if identities.contains(identity) {
                    match permission {
                        PermissionStrategy::Deny => {
                            return Ok(false);
                        }
                        PermissionStrategy::Allow => {
                            return Ok(true);
                        }
                    }
                }
            }
        }
        Ok(true)
    }
}

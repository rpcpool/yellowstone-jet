use {
    maplit::hashset,
    solana_sdk::{
        hash::Hash,
        message::{v0, VersionedMessage},
        signature::Keypair,
        signer::Signer,
        system_instruction,
        transaction::VersionedTransaction,
    },
    yellowstone_jet::blocking_services::BannedAccounts,
};

#[tokio::test]
async fn test_block_transaction() {
    let signer = Keypair::new();
    let fake_wallet_keypair = Keypair::new();

    let banned_accounts = BannedAccounts::new(hashset! {signer.pubkey()});
    let tx_hash = Hash::new_unique();

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

    let contains_signer = banned_accounts.contains_banned_accounts(&tx);
    assert!(contains_signer);
}

#[tokio::test]
async fn test_signer_missing_transaction_pass() {
    let signer = Keypair::new();
    let fake_wallet_keypair = Keypair::new();

    let banned_accounts = BannedAccounts::new(hashset! {});
    let tx_hash = Hash::new_unique();

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

    let contains_signer = banned_accounts.contains_banned_accounts(&tx);
    assert!(!contains_signer);
}

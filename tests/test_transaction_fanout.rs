mod testkit;

use {
    bytes::Bytes,
    solana_sdk::{
        hash::Hash,
        message::{VersionedMessage, v0},
        pubkey::Pubkey,
        signature::Keypair,
        signer::Signer,
        system_instruction,
        transaction::VersionedTransaction,
    },
    std::{
        sync::{Arc, RwLock as StdRwLock},
        vec,
    },
    tokio::sync::mpsc,
    yellowstone_jet::transactions::{
        AlwaysAllowTransactionPolicyStore, QuicGatewayBidi, SendTransactionRequest,
        TransactionFanout, TransactionPolicyStore, UpcomingLeaderSchedule,
    },
    yellowstone_shield_store::CheckError,
};

pub fn create_send_transaction_request(hash: Hash, max_resent: usize) -> SendTransactionRequest {
    let fake_wallet_keypair1 = Keypair::new();
    let fake_wallet_keypair2 = Keypair::new();
    let instructions = vec![system_instruction::transfer(
        &fake_wallet_keypair1.pubkey(),
        &fake_wallet_keypair2.pubkey(),
        10,
    )];

    let tx = VersionedTransaction::try_new(
        VersionedMessage::V0(
            v0::Message::try_compile(&fake_wallet_keypair1.pubkey(), &instructions, &[], hash)
                .expect("try compile"),
        ),
        &[&fake_wallet_keypair1],
    )
    .expect("try new");

    let wire_transaction = bincode::serialize(&tx).expect("Error getting wire_transaction");

    SendTransactionRequest {
        max_retries: Some(max_resent),
        signature: tx.signatures[0],
        wire_transaction: Bytes::from(wire_transaction),
        transaction: tx,
        policies: vec![],
    }
}

#[derive(Default, Clone)]
pub struct FakeLeaderSchedule {
    share: Arc<StdRwLock<Vec<Pubkey>>>,
}

impl FakeLeaderSchedule {
    pub fn set_schedule(&self, schedule: Vec<Pubkey>) {
        let mut curr = self.share.write().unwrap();
        *curr = schedule;
    }
}

impl UpcomingLeaderSchedule for FakeLeaderSchedule {
    fn leader_lookahead(&self, leader_forward_lookahead: usize) -> Vec<Pubkey> {
        let schedule = self.share.read().unwrap();
        schedule[..leader_forward_lookahead].to_vec()
    }
}

#[tokio::test]
async fn it_should_fanout_three_times() {
    const FANOUT_FACTOR: usize = 3;
    let (sink, source) = mpsc::unbounded_channel();
    let (gateway_tx, mut gateway_rx) = mpsc::channel(100);
    let (_gateway_response_tx, gateway_response_rx) = mpsc::unbounded_channel();
    let gateway_bidi = QuicGatewayBidi {
        sink: gateway_tx,
        source: gateway_response_rx,
    };
    let fake_schedule = FakeLeaderSchedule::default();

    let my_schedule = vec![
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
    ];
    fake_schedule.set_schedule(my_schedule.clone());

    let mut fanout = TransactionFanout::new(
        Arc::new(fake_schedule),
        Arc::new(AlwaysAllowTransactionPolicyStore),
        source,
        gateway_bidi,
        FANOUT_FACTOR,
    );
    let _fanout_jh = tokio::spawn(async move {
        fanout.run().await;
    });

    let tx = create_send_transaction_request(Hash::new_unique(), 0);
    let tx = Arc::new(tx);
    sink.send(Arc::clone(&tx)).unwrap();

    let mut actual_tx_sent = vec![];
    for pubkey in my_schedule.iter().take(FANOUT_FACTOR) {
        let actual_tx = gateway_rx.recv().await.unwrap();
        assert_eq!(actual_tx.tx_sig, actual_tx.tx_sig);
        assert_eq!(actual_tx.remote_peer, *pubkey);
        actual_tx_sent.push(actual_tx);
    }
    assert_eq!(actual_tx_sent.len(), FANOUT_FACTOR);
}

#[tokio::test]
async fn it_should_apply_shield_policies() {
    const FANOUT_FACTOR: usize = 3;
    let (sink, source) = mpsc::unbounded_channel();
    let (gateway_tx, mut gateway_rx) = mpsc::channel(100);
    let (_gateway_response_tx, gateway_response_rx) = mpsc::unbounded_channel();
    let gateway_bidi = QuicGatewayBidi {
        sink: gateway_tx,
        source: gateway_response_rx,
    };
    let fake_schedule = FakeLeaderSchedule::default();

    let my_schedule = vec![
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
    ];
    fake_schedule.set_schedule(my_schedule.clone());

    pub struct MyPolicy {
        blacklist: Vec<Pubkey>,
    }

    impl TransactionPolicyStore for MyPolicy {
        fn is_allowed(&self, _policies: &[Pubkey], leader: &Pubkey) -> Result<bool, CheckError> {
            Ok(!self.blacklist.contains(leader))
        }
    }
    let policy = MyPolicy {
        blacklist: vec![my_schedule[0], my_schedule[1]],
    };

    let mut fanout = TransactionFanout::new(
        Arc::new(fake_schedule),
        Arc::new(policy),
        source,
        gateway_bidi,
        FANOUT_FACTOR,
    );
    let _fanout_jh = tokio::spawn(async move {
        fanout.run().await;
    });

    let tx = create_send_transaction_request(Hash::new_unique(), 0);
    let tx = Arc::new(tx);
    sink.send(Arc::clone(&tx)).unwrap();
    let actual_tx = gateway_rx.recv().await.unwrap();
    assert!(gateway_rx.try_recv().is_err());
    assert_eq!(actual_tx.tx_sig, actual_tx.tx_sig);
    assert_eq!(actual_tx.remote_peer, my_schedule[2]);
}

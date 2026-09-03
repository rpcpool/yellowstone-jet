mod testkit;

use {
    futures::{StreamExt, channel::mpsc},
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_message::{VersionedMessage, v0},
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    solana_system_interface::instruction::transfer,
    solana_transaction::versioned::VersionedTransaction,
    std::{
        mem::MaybeUninit,
        sync::{Arc, RwLock as StdRwLock},
        vec,
    },
    yellowstone_jet::transactions::{
        AlwaysAllowTransactionPolicyStore, FanoutConfig, JetTxnInfo, SendTransactionRequest,
        TransactionFanout, TransactionPolicyStore, UpcomingLeaderSchedule,
    },
    yellowstone_shield_store::CheckError,
};

pub fn create_send_transaction_request(hash: Hash) -> SendTransactionRequest {
    let fake_wallet_keypair1 = Keypair::new();
    let fake_wallet_keypair2 = Keypair::new();
    let instructions = vec![transfer(
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

    let wire_transaction = wincode::serialize(&tx).expect("Error getting wire_transaction");
    let signer = tx.message.static_account_keys()[0];
    SendTransactionRequest {
        signature: tx.signatures[0],
        wire_transaction: wire_transaction.into(),
        policies: vec![],
        x_request_id: None,
        durable_nonce: None,
        recent_blockhash: hash,
        x_subscription_id: None,
        signer,
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
    fn leader_lookahead(
        &self,
        leader_forward_lookahead: usize,
        out: &mut [MaybeUninit<Pubkey>],
    ) -> usize {
        let schedule = self.share.read().unwrap();

        let it = schedule[..leader_forward_lookahead]
            .iter()
            .zip(out.iter_mut());
        let mut i = 0;
        for (src, dst) in it {
            dst.write(*src);
            i += 1;
        }
        i
    }
    fn get_current_slot(&self) -> solana_clock::Slot {
        // For testing purposes, we can return a dummy slot.
        // In a real implementation, this would return the current slot.
        0
    }
}

#[tokio::test]
async fn it_should_fanout_three_times() {
    const FANOUT_FACTOR: usize = 3;
    let (sink, source) = mpsc::unbounded();
    let (gateway_tx, mut gateway_rx) = mpsc::channel(100);
    let fake_schedule = FakeLeaderSchedule::default();

    let my_schedule = vec![
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
    ];
    fake_schedule.set_schedule(my_schedule.clone());

    #[allow(deprecated)]
    let mut fanout = TransactionFanout::new(
        fake_schedule,
        AlwaysAllowTransactionPolicyStore,
        source,
        gateway_tx,
        FanoutConfig::Custom(FANOUT_FACTOR),
        Vec::new(),
    );
    let _fanout_jh = tokio::spawn(async move {
        fanout.run().await;
    });

    let tx = create_send_transaction_request(Hash::new_unique());
    sink.unbounded_send(tx.clone()).unwrap();

    let mut actual_tx_sent = vec![];
    for pubkey in my_schedule.iter().take(FANOUT_FACTOR) {
        let actual_tx = gateway_rx.next().await.unwrap();
        assert_eq!(
            actual_tx
                .info
                .as_ref()
                .and_then(|info| info.downcast_ref::<JetTxnInfo>())
                .map(|info| info.signature),
            Some(tx.signature)
        );
        assert_eq!(actual_tx.remote_peer, *pubkey);
        actual_tx_sent.push(actual_tx);
    }
    assert_eq!(actual_tx_sent.len(), FANOUT_FACTOR);
}

#[tokio::test]
async fn it_should_apply_shield_policies() {
    const FANOUT_FACTOR: usize = 3;
    let (sink, source) = mpsc::unbounded();
    let (gateway_tx, mut gateway_rx) = mpsc::channel(100);
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

    #[allow(deprecated)]
    let mut fanout = TransactionFanout::new(
        fake_schedule,
        policy,
        source,
        gateway_tx,
        FanoutConfig::Custom(FANOUT_FACTOR),
        Vec::new(),
    );
    let _fanout_jh = tokio::spawn(async move {
        fanout.run().await;
    });

    let tx = create_send_transaction_request(Hash::new_unique());
    sink.unbounded_send(tx.clone()).unwrap();
    let actual_tx = gateway_rx.next().await.unwrap();
    assert!(gateway_rx.try_recv().is_err());
    assert_eq!(
        actual_tx
            .info
            .as_ref()
            .and_then(|info| info.downcast_ref::<JetTxnInfo>())
            .map(|info| info.signature),
        Some(tx.signature)
    );
    assert_eq!(actual_tx.remote_peer, my_schedule[2]);
}

#[tokio::test]
async fn it_should_continue_fanout_after_policy_check_error() {
    const FANOUT_FACTOR: usize = 3;
    let (sink, source) = mpsc::unbounded();
    let (gateway_tx, mut gateway_rx) = mpsc::channel(100);
    let fake_schedule = FakeLeaderSchedule::default();

    let extra_fanout_pubkeys = vec![Pubkey::new_unique()];

    let my_schedule = vec![
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
    ];
    fake_schedule.set_schedule(my_schedule.clone());

    // Simulates a policy store that fails to find the policy account (e.g. not yet
    // indexed) for the first leader, but works normally for the rest.
    pub struct FlakyPolicy {
        errors_on: Pubkey,
    }

    impl TransactionPolicyStore for FlakyPolicy {
        fn is_allowed(&self, _policies: &[Pubkey], leader: &Pubkey) -> Result<bool, CheckError> {
            if *leader == self.errors_on {
                Err(CheckError::PolicyNotFound)
            } else {
                Ok(true)
            }
        }
    }
    let policy = FlakyPolicy {
        errors_on: my_schedule[0],
    };

    #[allow(deprecated)]
    let mut fanout = TransactionFanout::new(
        fake_schedule,
        policy,
        source,
        gateway_tx,
        FanoutConfig::Custom(FANOUT_FACTOR),
        extra_fanout_pubkeys.clone(),
    );
    let _fanout_jh = tokio::spawn(async move {
        fanout.run().await;
    });

    let tx = create_send_transaction_request(Hash::new_unique());
    sink.unbounded_send(tx.clone()).unwrap();

    // Only my_schedule[0] errors out; the other two scheduled leaders plus the extra
    // fanout target must still receive the transaction.
    let expected_recipients = FANOUT_FACTOR - 1 + extra_fanout_pubkeys.len();
    let mut actual_tx_sent = vec![];
    for _i in 0..expected_recipients {
        let actual_tx = gateway_rx.next().await.unwrap();
        actual_tx_sent.push(actual_tx.remote_peer);
    }

    assert_eq!(actual_tx_sent.len(), expected_recipients);
    assert!(!actual_tx_sent.contains(&my_schedule[0]));
    assert!(actual_tx_sent.contains(&my_schedule[1]));
    assert!(actual_tx_sent.contains(&my_schedule[2]));
    assert!(actual_tx_sent.contains(&extra_fanout_pubkeys[0]));
}

#[tokio::test]
async fn it_should_support_extra_fanout() {
    const FANOUT_FACTOR: usize = 3;
    let (sink, source) = mpsc::unbounded();
    let (gateway_tx, mut gateway_rx) = mpsc::channel(100);
    let fake_schedule = FakeLeaderSchedule::default();

    let extra_fanout_pubkeys = vec![Pubkey::new_unique(), Pubkey::new_unique()];

    let my_schedule = vec![
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
    ];
    fake_schedule.set_schedule(my_schedule.clone());

    #[allow(deprecated)]
    let mut fanout = TransactionFanout::new(
        fake_schedule,
        AlwaysAllowTransactionPolicyStore,
        source,
        gateway_tx,
        FanoutConfig::Custom(FANOUT_FACTOR),
        extra_fanout_pubkeys.clone(),
    );
    let _fanout_jh = tokio::spawn(async move {
        fanout.run().await;
    });

    let tx = create_send_transaction_request(Hash::new_unique());
    sink.unbounded_send(tx.clone()).unwrap();

    let mut actual_tx_sent = vec![];
    for _i in 0..FANOUT_FACTOR + extra_fanout_pubkeys.len() {
        let actual_tx = gateway_rx.next().await.unwrap();
        actual_tx_sent.push(actual_tx);
    }

    assert_eq!(
        actual_tx_sent.len(),
        FANOUT_FACTOR + extra_fanout_pubkeys.len()
    );
    assert!(
        extra_fanout_pubkeys
            .iter()
            .chain(my_schedule.iter().take(FANOUT_FACTOR))
            .all(|extra_pk| {
                actual_tx_sent
                    .iter()
                    .any(|sent| sent.remote_peer == *extra_pk)
            })
    );
}

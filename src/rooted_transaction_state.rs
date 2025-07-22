use {
    crate::{
        grpc_geyser::{BlockMetaWithCommitment, TransactionReceived},
        util::{BlockHeight, CommitmentLevel},
    },
    solana_clock::{MAX_RECENT_BLOCKHASHES, Slot},
    solana_signature::Signature,
    std::collections::{HashMap, HashSet},
};

#[derive(Debug, Clone)]
pub enum RootedTxEvent {
    TransactionReceived(TransactionReceived),
    BlockMetaUpdate(BlockMetaWithCommitment),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootedTxEffect {
    NotifyWatcher {
        signature: Signature,
        commitment: CommitmentLevel,
    },
}

#[derive(Debug, Default, Clone)]
pub struct SlotInfo {
    pub transactions: HashSet<Signature>,
    pub blockmeta: Option<BlockMetaWithCommitment>,
}

#[derive(Debug, Clone, Default)]
pub struct RootedTransactionsState {
    pub slots: HashMap<Slot, SlotInfo>,
    pub transactions: HashMap<Signature, Slot>,
    pub watched_signatures: HashSet<Signature>,
}

/*
 * State machine for tracking transaction commitment levels.
 *
 * Tracks which transactions are in which slots and notifies watchers when
 * slot commitment levels change (Processed -> Confirmed -> Finalized).
 * Cleans up old finalized slots based on block height to prevent unbounded growth.
 *
 * Pure functions with no async operations for easy testing.
 */
#[derive(Debug, Default)]
pub struct RootedTxStateMachine {
    pub state: RootedTransactionsState,
}

impl RootedTxStateMachine {
    pub fn new() -> Self {
        Self {
            state: RootedTransactionsState::default(),
        }
    }

    pub fn process_event(&mut self, event: RootedTxEvent) -> Vec<RootedTxEffect> {
        match event {
            RootedTxEvent::TransactionReceived(tx) => {
                let mut effects = Vec::new();
                let entry = self.state.slots.entry(tx.slot).or_default();

                if entry.transactions.insert(tx.signature) {
                    if let Some(blockmeta) = &entry.blockmeta {
                        if self.state.watched_signatures.contains(&tx.signature) {
                            effects.push(RootedTxEffect::NotifyWatcher {
                                signature: tx.signature,
                                commitment: blockmeta.commitment,
                            });
                        }
                    }
                    self.state.transactions.insert(tx.signature, tx.slot);
                }
                effects
            }

            RootedTxEvent::BlockMetaUpdate(meta) => {
                let mut effects = Vec::new();
                let entry = self.state.slots.entry(meta.slot).or_default();
                entry.blockmeta = Some(meta);

                for signature in entry.transactions.iter() {
                    if self.state.watched_signatures.contains(signature) {
                        effects.push(RootedTxEffect::NotifyWatcher {
                            signature: *signature,
                            commitment: meta.commitment,
                        });
                    }
                }

                if meta.commitment == CommitmentLevel::Finalized {
                    self.cleanup_old_slots(meta.block_height, meta.slot);
                }

                effects
            }
        }
    }

    fn cleanup_old_slots(&mut self, finalized_block_height: BlockHeight, finalized_slot: Slot) {
        let mut removed_signatures = Vec::new();

        self.state.slots.retain(|_slot, info| {
            let should_retain = if let Some(blockmeta) = info.blockmeta {
                if blockmeta.commitment == CommitmentLevel::Finalized {
                    should_retain_finalized_slot(blockmeta.block_height, finalized_block_height)
                } else {
                    blockmeta.slot > finalized_slot
                }
            } else {
                true
            };

            if !should_retain {
                removed_signatures.extend(info.transactions.iter().copied());
            }
            should_retain
        });

        for signature in removed_signatures {
            self.state.transactions.remove(&signature);
        }
    }
}

pub const fn should_retain_finalized_slot(
    slot_block_height: BlockHeight,
    finalized_block_height: BlockHeight,
) -> bool {
    slot_block_height + MAX_RECENT_BLOCKHASHES as u64 > finalized_block_height
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_recent_blockhashes() {
        assert_eq!(MAX_RECENT_BLOCKHASHES, 300);
    }

    #[test]
    fn test_transaction_lifecycle_and_cleanup() {
        let mut machine = RootedTxStateMachine::new();
        let old_sig = Signature::new_unique();
        let recent_sig = Signature::new_unique();

        machine.state.watched_signatures.insert(old_sig);
        machine.state.watched_signatures.insert(recent_sig);

        // Old transaction at slot 100, height 1000
        machine.process_event(RootedTxEvent::TransactionReceived(TransactionReceived {
            slot: 100,
            signature: old_sig,
        }));
        let effects =
            machine.process_event(RootedTxEvent::BlockMetaUpdate(BlockMetaWithCommitment {
                slot: 100,
                block_height: 1000,
                block_hash: solana_hash::Hash::new_unique(),
                commitment: CommitmentLevel::Finalized,
            }));
        assert_eq!(effects.len(), 1);
        assert_eq!(
            effects[0],
            RootedTxEffect::NotifyWatcher {
                signature: old_sig,
                commitment: CommitmentLevel::Finalized,
            }
        );

        // Recent transaction at slot 200, height 1300
        machine.process_event(RootedTxEvent::TransactionReceived(TransactionReceived {
            slot: 200,
            signature: recent_sig,
        }));
        machine.process_event(RootedTxEvent::BlockMetaUpdate(BlockMetaWithCommitment {
            slot: 200,
            block_height: 1300,
            block_hash: solana_hash::Hash::new_unique(),
            commitment: CommitmentLevel::Finalized,
        }));

        // Trigger cleanup with slot 300 at height 1400
        // Old: 1000 + 300 = 1300 < 1400 (cleaned up)
        // Recent: 1300 + 300 = 1600 > 1400 (kept)
        machine.process_event(RootedTxEvent::BlockMetaUpdate(BlockMetaWithCommitment {
            slot: 300,
            block_height: 1400,
            block_hash: solana_hash::Hash::new_unique(),
            commitment: CommitmentLevel::Finalized,
        }));

        // Old slot cleaned up, recent slot remains
        assert!(!machine.state.slots.contains_key(&100));
        assert!(machine.state.slots.contains_key(&200));
        assert!(!machine.state.transactions.contains_key(&old_sig));
        assert!(machine.state.transactions.contains_key(&recent_sig));
    }
}

//! Shared table of the current and upcoming leaders' worker channels.
//!
//! The driver is the only writer. Every [`TpuSenderDriverInlet`](crate::core::TpuSenderDriverInlet)
//! reads it so that a transaction for a leader that already has a worker skips the driver.

use {
    crate::core::txn::TpuSenderTxn,
    arc_swap::ArcSwapOption,
    solana_pubkey::Pubkey,
    std::{
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, Instant},
    },
    tokio::sync::mpsc::{self, error::TrySendError},
};

///
/// Maximum number of leaders the fast path tracks, current leader included.
///
pub(crate) const MAX_FAST_PATH_LEADERS: usize = 8;

///
/// Minimum time between two activity updates of the same entry, so concurrent senders rarely
/// write to the shared cache line.
///
const ACTIVITY_UPDATE_INTERVAL: Duration = Duration::from_millis(10);

///
/// One leader with an installed worker.
///
struct FastPathEntry {
    remote_peer: Pubkey,
    worker_tx: mpsc::Sender<TpuSenderTxn>,
    ///
    /// When a fast-path send last succeeded, in microseconds since [`LeaderFastPath::epoch`],
    /// plus one. `0` means never.
    ///
    last_used_micros: AtomicU64,
}

impl FastPathEntry {
    ///
    /// Records a successful send. The first send is always recorded; later ones at most once
    /// per [`ACTIVITY_UPDATE_INTERVAL`].
    ///
    /// # Arguments
    ///
    /// * `epoch` - The [`Instant`] that `last_used_micros` counts from.
    ///
    fn touch(&self, epoch: Instant) {
        let now = epoch.elapsed().as_micros() as u64 + 1;
        let last = self.last_used_micros.load(Ordering::Relaxed);
        if last == 0 || now.saturating_sub(last) >= ACTIVITY_UPDATE_INTERVAL.as_micros() as u64 {
            self.last_used_micros.store(now, Ordering::Relaxed);
        }
    }
}

type FastPathLeaders = [Option<FastPathEntry>; MAX_FAST_PATH_LEADERS];

///
/// Handle to the shared leader table. Cloning is cheap; every clone sees the same table.
///
/// The table is `None` while the fast path is disabled, e.g. during an identity update. Readers
/// must only hold a loaded table for the duration of one send: a worker only stops once every
/// [`mpsc::Sender`] to it is dropped, so a reader that kept a table would keep old workers alive.
///
#[derive(Clone)]
pub(crate) struct LeaderFastPath {
    leaders: Arc<ArcSwapOption<FastPathLeaders>>,
    epoch: Instant,
}

impl LeaderFastPath {
    ///
    /// Creates a disabled fast path.
    ///
    /// # Returns
    ///
    /// A new [`LeaderFastPath`] whose table is `None` until [`LeaderFastPath::publish`] runs.
    ///
    pub(crate) fn new() -> Self {
        Self {
            leaders: Arc::new(ArcSwapOption::empty()),
            epoch: Instant::now(),
        }
    }

    ///
    /// Sends `txn` straight to its leader's worker if that leader is in the table.
    ///
    /// # Arguments
    ///
    /// * `txn` - The [`TpuSenderTxn`] to send; its `remote_peer` selects the worker.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the worker accepted `txn`. `Err` gives `txn` back when the fast path is
    /// disabled, the leader isn't in the table, or its worker's channel is full or closed; the
    /// caller then sends it through the driver.
    ///
    pub(crate) fn try_send(&self, txn: TpuSenderTxn) -> Result<(), TpuSenderTxn> {
        let guard = self.leaders.load();
        let Some(leaders) = guard.as_deref() else {
            return Err(txn);
        };
        let Some(entry) = leaders
            .iter()
            .flatten()
            .find(|entry| entry.remote_peer == txn.remote_peer)
        else {
            return Err(txn);
        };
        entry
            .worker_tx
            .try_send(txn)
            .map_err(TrySendError::into_inner)?;
        entry.touch(self.epoch);
        Ok(())
    }

    ///
    /// Replaces the table with the given leaders, keeping the activity already recorded for
    /// leaders that stay in it. Leaders beyond [`MAX_FAST_PATH_LEADERS`] are ignored.
    ///
    /// # Arguments
    ///
    /// * `workers` - Each leader with the [`mpsc::Sender`] of its installed worker.
    ///
    pub(crate) fn publish(
        &self,
        workers: impl IntoIterator<Item = (Pubkey, mpsc::Sender<TpuSenderTxn>)>,
    ) {
        let current = self.leaders.load();
        let previous_last_used = |remote_peer: &Pubkey| {
            current
                .as_deref()
                .and_then(|leaders| {
                    leaders
                        .iter()
                        .flatten()
                        .find(|entry| entry.remote_peer == *remote_peer)
                })
                .map_or(0, |entry| entry.last_used_micros.load(Ordering::Relaxed))
        };
        let mut leaders: FastPathLeaders = std::array::from_fn(|_| None);
        for (slot, (remote_peer, worker_tx)) in leaders.iter_mut().zip(workers) {
            *slot = Some(FastPathEntry {
                last_used_micros: AtomicU64::new(previous_last_used(&remote_peer)),
                remote_peer,
                worker_tx,
            });
        }
        drop(current);
        self.leaders.store(Some(Arc::new(leaders)));
    }

    ///
    /// Disables the fast path and drops the table's references to worker channels. Readers
    /// that already loaded the table finish their current send, then let go of it.
    ///
    pub(crate) fn disable(&self) {
        self.leaders.store(None);
    }

    ///
    /// Calls `f` with the time of the last successful fast-path send to each leader in the
    /// table. Leaders never used through the fast path are skipped.
    ///
    /// # Arguments
    ///
    /// * `f` - Called once per used leader with its [`Pubkey`] and last-use [`Instant`].
    ///
    pub(crate) fn for_each_last_used(&self, mut f: impl FnMut(Pubkey, Instant)) {
        let guard = self.leaders.load();
        let Some(leaders) = guard.as_deref() else {
            return;
        };
        for entry in leaders.iter().flatten() {
            let last_used = entry.last_used_micros.load(Ordering::Relaxed);
            if last_used != 0 {
                f(
                    entry.remote_peer,
                    self.epoch + Duration::from_micros(last_used - 1),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::LeaderFastPath,
        crate::core::txn::TpuSenderTxn,
        solana_pubkey::Pubkey,
        tokio::sync::mpsc::{self, error::TryRecvError},
    };

    fn txn(remote_peer: Pubkey) -> TpuSenderTxn {
        TpuSenderTxn::from_owned(remote_peer, b"txn".to_vec(), None)
    }

    #[test]
    fn disabled_fast_path_gives_txn_back() {
        let fast_path = LeaderFastPath::new();
        let peer = Pubkey::new_unique();

        let returned = fast_path
            .try_send(txn(peer))
            .expect_err("fast path is disabled");
        assert_eq!(returned.remote_peer, peer);
    }

    #[test]
    fn published_leader_receives_txn_directly() {
        let fast_path = LeaderFastPath::new();
        let peer = Pubkey::new_unique();
        let (worker_tx, mut worker_rx) = mpsc::channel(4);
        fast_path.publish([(peer, worker_tx)]);

        fast_path.try_send(txn(peer)).expect("fast path hit");
        assert_eq!(worker_rx.try_recv().expect("recv").remote_peer, peer);
    }

    #[test]
    fn unknown_leader_gives_txn_back() {
        let fast_path = LeaderFastPath::new();
        let (worker_tx, _worker_rx) = mpsc::channel(4);
        fast_path.publish([(Pubkey::new_unique(), worker_tx)]);

        let other_peer = Pubkey::new_unique();
        let returned = fast_path
            .try_send(txn(other_peer))
            .expect_err("leader not in table");
        assert_eq!(returned.remote_peer, other_peer);
    }

    #[test]
    fn closed_worker_channel_gives_txn_back() {
        let fast_path = LeaderFastPath::new();
        let peer = Pubkey::new_unique();
        let (worker_tx, mut worker_rx) = mpsc::channel(4);
        fast_path.publish([(peer, worker_tx)]);
        worker_rx.close();

        let returned = fast_path
            .try_send(txn(peer))
            .expect_err("worker channel is closed");
        assert_eq!(returned.remote_peer, peer);
    }

    #[test]
    fn successful_send_is_recorded_as_activity() {
        let fast_path = LeaderFastPath::new();
        let used_peer = Pubkey::new_unique();
        let unused_peer = Pubkey::new_unique();
        let (used_tx, _used_rx) = mpsc::channel(4);
        let (unused_tx, _unused_rx) = mpsc::channel(4);
        fast_path.publish([(used_peer, used_tx), (unused_peer, unused_tx)]);

        fast_path.try_send(txn(used_peer)).expect("fast path hit");

        let mut seen = Vec::new();
        fast_path.for_each_last_used(|peer, _| seen.push(peer));
        assert_eq!(seen, vec![used_peer]);
    }

    #[test]
    fn activity_survives_republish() {
        let fast_path = LeaderFastPath::new();
        let peer = Pubkey::new_unique();
        let (worker_tx, _worker_rx) = mpsc::channel(4);
        fast_path.publish([(peer, worker_tx.clone())]);
        fast_path.try_send(txn(peer)).expect("fast path hit");

        fast_path.publish([(peer, worker_tx)]);

        let mut seen = Vec::new();
        fast_path.for_each_last_used(|peer, _| seen.push(peer));
        assert_eq!(seen, vec![peer]);
    }

    #[test]
    fn disable_releases_worker_senders() {
        let fast_path = LeaderFastPath::new();
        let peer = Pubkey::new_unique();
        let (worker_tx, mut worker_rx) = mpsc::channel(4);
        fast_path.publish([(peer, worker_tx)]);
        assert!(matches!(worker_rx.try_recv(), Err(TryRecvError::Empty)));

        // The table held the only sender, so disabling it must let the worker see its channel
        // close, which is how a graceful drop stops workers.
        fast_path.disable();
        assert!(matches!(
            worker_rx.try_recv(),
            Err(TryRecvError::Disconnected)
        ));
    }
}

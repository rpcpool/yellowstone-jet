//! Background watcher that detects remote peer TPU address changes.

use {
    crate::{config::TpuPortKind, core::services::LeaderTpuInfoService},
    solana_pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet},
        net::SocketAddr,
        sync::{Arc, Mutex as StdMutex},
        time::Duration,
    },
    tokio::{
        sync::{
            Notify,
            mpsc::{self},
        },
        time::interval,
    },
};

pub(crate) struct RemotePeerAddrWatcher {
    changes: Arc<StdMutex<HashSet<Pubkey>>>,
    cnc_tx: mpsc::UnboundedSender<RemotePeerAddrWatcherCommand>,
    notify: Arc<Notify>,
}

impl RemotePeerAddrWatcher {
    pub(crate) fn new(
        refresh_interval: Duration,
        tpu_port_kind: TpuPortKind,
        leader_tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync + 'static>,
    ) -> Self {
        tracing::trace!(
            "Spawning remote peer address watcher with refresh interval: {refresh_interval:?}"
        );
        let shared = Default::default();
        let (cnc_tx, cnc_rx) = mpsc::unbounded_channel();
        let notify = Arc::new(Notify::new());

        // The event loop closes when the watcher is dropped.
        let ev_loop = RemotePeerAddrWatcherEvLoop {
            changes: Arc::clone(&shared),
            leader_tpu_info_service,
            cnc_rx,
            peers_to_watch_map: HashMap::new(),
            tpu_port_kind,
            interval: refresh_interval,
            notify: Arc::clone(&notify),
        };

        tokio::spawn(async move {
            ev_loop.run().await;
        });

        RemotePeerAddrWatcher {
            changes: shared,
            cnc_tx,
            notify,
        }
    }

    ///
    /// Registers a watch for a remote peer's address.
    ///
    /// If the remote peer's address changes, it will be notified through the `notified` method.
    /// Note that remote peer address watches are only good for one address change. That means if the remote peer address changes again,
    /// you need to register a new watch in order to be notified about the next change.
    ///
    pub(crate) fn register_watch(&self, remote_peer: Pubkey, addr: SocketAddr) {
        self.cnc_tx
            .send(RemotePeerAddrWatcherCommand::RegisterWatch(
                remote_peer,
                addr,
            ))
            .expect("Remote peer address watcher command channel is closed");
    }

    ///
    /// Deregisters a watch for a remote peer's address.
    ///
    pub(crate) fn forget(&self, remote_peer: Pubkey) {
        self.cnc_tx
            .send(RemotePeerAddrWatcherCommand::DeregisterWatch(remote_peer))
            .expect("Remote peer address watcher command channel is closed");
    }

    ///
    /// Waits for a remote peer address change notification.
    /// Returns a set of remote peers whose addresses have changed since the last notification.
    ///
    /// Cancel-Sefety:
    ///
    /// This method is cancel-safe and won't lose notifications.
    pub(crate) async fn notified(&mut self) -> HashSet<Pubkey> {
        self.notify.notified().await;
        let mut guard = self.changes.lock().expect("Mutex is poisoned");
        std::mem::take(&mut *guard)
    }
}

enum RemotePeerAddrWatcherCommand {
    RegisterWatch(Pubkey, SocketAddr),
    DeregisterWatch(Pubkey),
}

struct RemotePeerAddrWatcherEvLoop {
    changes: Arc<StdMutex<HashSet<Pubkey>>>,
    leader_tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync + 'static>,
    cnc_rx: mpsc::UnboundedReceiver<RemotePeerAddrWatcherCommand>,
    peers_to_watch_map: HashMap<Pubkey, SocketAddr>,
    tpu_port_kind: TpuPortKind,
    interval: Duration,
    notify: Arc<Notify>,
}

impl RemotePeerAddrWatcherEvLoop {
    fn handle_cnc(&mut self, command: RemotePeerAddrWatcherCommand) {
        match command {
            RemotePeerAddrWatcherCommand::RegisterWatch(pubkey, initial_addr) => {
                self.peers_to_watch_map
                    .entry(pubkey)
                    .or_insert(initial_addr);
            }
            RemotePeerAddrWatcherCommand::DeregisterWatch(pubkey) => {
                self.peers_to_watch_map.remove(&pubkey);
            }
        }
    }

    fn update_peers_to_watch(&mut self) {
        let diff = self
            .peers_to_watch_map
            .iter()
            .filter_map(|(pubkey, last_known_addr)| {
                let new_addr = self
                    .leader_tpu_info_service
                    .get_quic_dest_addr(pubkey, self.tpu_port_kind);
                match new_addr {
                    Some(new_addr) => {
                        if new_addr != *last_known_addr {
                            Some(*pubkey)
                        } else {
                            None
                        }
                    }
                    None => Some(*pubkey),
                }
            })
            .collect::<Vec<_>>();
        if diff.is_empty() {
            return;
        }
        {
            let mut guard = self.changes.lock().expect("Mutex is poisoned");
            for pubkey in diff {
                let old = self.peers_to_watch_map.remove(&pubkey).unwrap();
                tracing::trace!(
                    "Remote peer address changed: {}, used to be advertised on {:?}",
                    pubkey,
                    old,
                );
                guard.insert(pubkey);
            }
            self.notify.notify_one();
        }
    }

    async fn run(mut self) {
        let mut interval = interval(self.interval);
        loop {
            tokio::select! {
                _  = interval.tick() => {
                    self.update_peers_to_watch();
                },
                maybe = self.cnc_rx.recv() => {
                    match maybe {
                        Some(command) => {
                            self.handle_cnc(command);
                        }
                        None => {
                            tracing::debug!("Remote peer address watcher inlet closed");
                            break;
                        }
                    }
                }
            }
        }
    }
}

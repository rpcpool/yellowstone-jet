//! The driver's main `select!` loop.

#[cfg(feature = "prometheus")]
use crate::{core::constants::METRIC_UPDATE_INTERVAL, prom};
use {
    super::{DriverTaskMeta, TpuSenderDriver},
    crate::core::response::TpuSenderResponseCallback,
    std::{pin::Pin, time::Instant},
    tokio::{
        task::{Id, JoinError},
        time::Sleep,
    },
};

impl<CB> TpuSenderDriver<CB>
where
    CB: TpuSenderResponseCallback + Send + Sync + 'static,
{
    fn handle_tasklet_result(&mut self, result: Result<(Id, ()), JoinError>) {
        let id = match &result {
            Ok((id, _)) => *id,
            Err(join_err) => join_err.id(),
        };
        let meta = self.tasklet_meta.remove(&id).expect("tasklet meta");

        match meta {
            DriverTaskMeta::DropAllWorkers => {
                tracing::info!(
                    "finished graceful drop of all transaction workers with : {result:?}"
                );
            }
        }
    }

    #[cfg(feature = "prometheus")]
    fn update_prom_metrics(&self) {
        let num_active_workers = self.tx_worker_handle_map.len();
        let num_connecting_tasks = self.connecting_tasks.len();
        let num_queued_tx = self.tx_queues.values().map(|q| q.len()).sum::<usize>();
        prom::set_orphan_connections(self.orphan_connection_set.len());
        prom::set_active_quic_tx_senders(num_active_workers);
        prom::set_active_quic_connections(self.connection_map.len());
        prom::set_quic_gw_connecting_cnt(num_connecting_tasks);
        prom::set_num_conn_to_evict(self.pending_connection_eviction_set.len());
        prom::set_txn_blocked_by_connection(num_queued_tx);
    }

    #[allow(unused_variables)]
    pub async fn run(mut self) {
        #[cfg(feature = "prometheus")]
        {
            prom::quic_set_identity(self.identity.pubkey());
        }
        #[allow(unused_mut, dead_code)]
        let mut last_metric_update = Instant::now();
        let mut sleep_timer: Option<Pin<Box<Sleep>>> = None;
        loop {
            self.do_eviction_if_required();
            #[cfg(feature = "prometheus")]
            {
                if last_metric_update.elapsed() >= METRIC_UPDATE_INTERVAL {
                    self.update_prom_metrics();
                    last_metric_update = Instant::now();
                }
            }
            self.try_predict_upcoming_leaders_if_necessary();

            let next_connection_expiration = self.next_orphan_connection_expiration();
            match next_connection_expiration {
                Some(expiration_instant) => {
                    let now = Instant::now();
                    if sleep_timer.is_none() {
                        let sleep_dur = expiration_instant.saturating_duration_since(now);
                        // If I understand tokio correclty, the first time you poll a timer it must acquire a mutex lock.
                        // So we box it and pin it to avoid re-creating the timer on every loop iteration since next orphan connection expiration
                        // is unlikely to change until we evict some connections.
                        // Also, the next orphan connection deadline can only increase overtime.
                        sleep_timer = Some(Box::pin(tokio::time::sleep(sleep_dur)));
                    }
                }
                None => {
                    sleep_timer = None;
                }
            };
            tokio::select! {
                maybe = self.tx_inlet.recv() => {
                    match maybe {
                        Some(tx) => {
                            self.accept_tx(tx);
                        }
                        None => {
                            tracing::debug!("Transaction driver inlet closed");
                            break;
                        }
                    }
                }
                _ = async { sleep_timer.as_mut().unwrap().await }, if sleep_timer.is_some() => {
                    self.try_evict_orphan_connections();
                }
                // If cnc_rx returns None, we don't care as clients can safely drop cnc sender and the runtime should keep function.
                Some(command) = self.cnc_rx.recv() => {
                    self.handle_cnc(command).await;
                }

                Some(result) = self.tx_worker_set.join_next_with_id() => {
                    self.handle_worker_result(result);
                }

                Some(result) = self.connecting_tasks.join_next_with_id() => {
                    self.handle_connecting_result(result);
                }

                Some(result) = self.tasklet.join_next_with_id() => {
                    self.handle_tasklet_result(result);
                }
                changes = self.remote_peer_addr_watcher.notified() => {
                    self.handle_remote_peer_addr_change(changes);
                }
            }
        }

        self.schedule_graceful_drop_all_worker();
        while let Some(result) = self.tasklet.join_next_with_id().await {
            self.handle_tasklet_result(result);
        }
    }
}

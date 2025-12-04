//!
//! RPC-based TPU info service for fetching TPU QUIC contact info from Solana cluster nodes.
//!
//! This module provides an implementation of a TPU info service that periodically fetches
//! TPU QUIC contact information from the Solana RPC cluster nodes.
//!
//! It maintains an up-to date mapping of leader public keys to their TPU QUIC socket addresses,
//! allowing clients to retrieve the appropriate TPU QUIC address for a given leader.
//!
//! The service runs a background task to refresh the cluster nodes info at a configurable interval.
//!
use {
    crate::{core::LeaderTpuInfoService, rpc::solana_rpc_utils::SolanaRpcErrorKindExt},
    serde::Deserialize,
    solana_client::{nonblocking::rpc_client::RpcClient, rpc_response::RpcContactInfo},
    solana_pubkey::Pubkey,
    std::{
        collections::HashMap,
        net::SocketAddr,
        panic,
        str::FromStr,
        sync::{Arc, RwLock},
    },
    tokio::task::JoinHandle,
    tokio_util::sync::CancellationToken,
};

pub const DEFAULT_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
pub const DEFAULT_MAX_RETRY_ATTEMPTS: usize = 3;

///
/// TPU QUIC contact info for a Solana cluster node.
///
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RpcTpuQuicContactInfo {
    /// TPU QUIC socket address
    pub tpu_quic: Option<SocketAddr>,
    /// TPU Forwards QUIC socket address
    pub tpu_forwards_quic: Option<SocketAddr>,
}

impl From<&RpcContactInfo> for RpcTpuQuicContactInfo {
    fn from(rpc_contact_info: &RpcContactInfo) -> Self {
        Self {
            tpu_quic: rpc_contact_info.tpu_quic,
            tpu_forwards_quic: rpc_contact_info.tpu_forwards_quic,
        }
    }
}

///
/// A TPU info service that fetches TPU QUIC contact info from the Solana RPC cluster nodes.
///
/// Starts a background task to periodically refresh the cluster nodes info.
/// See [`rpc_cluster_tpu_info_service`] for creating an instance of this service.
///
/// # Clone
///
/// Cheap to clone, as it uses an `Arc<RwLock<...>>` internally to share state.
///
/// # Safety
///
/// This service is thread-safe.
///
#[derive(Clone)]
pub struct RpcClusterTpuQuicInfoService {
    shared: Arc<RwLock<HashMap<Pubkey, RpcTpuQuicContactInfo>>>,
    _on_drop: Arc<OnDrop>,
}

impl RpcClusterTpuQuicInfoService {
    ///
    /// Get the current topology mapping of leader pubkeys to their TPU QUIC contact info.
    ///
    pub fn get_current_topology_mapping(&self) -> HashMap<Pubkey, RpcTpuQuicContactInfo> {
        self.shared.read().expect("read lock").clone()
    }
}

struct OnDrop {
    handle: CancellationToken,
}

impl Drop for OnDrop {
    fn drop(&mut self) {
        self.handle.cancel();
    }
}

///
/// Configuration for the [`RpcClusterTpuQuicInfoService`].
///
#[derive(Debug, Clone, Deserialize)]
pub struct RpcClusterTpuQuicInfoServiceConfig {
    /// Interval between cluster info refreshes.
    #[serde(
        with = "humantime_serde",
        default = "RpcClusterTpuQuicInfoServiceConfig::default_refresh_interval"
    )]
    pub refresh_interval: std::time::Duration,
    /// Maximum number of retry attempts for fetching cluster info.
    #[serde(default = "RpcClusterTpuQuicInfoServiceConfig::default_max_retry_attempts")]
    pub max_retry_attempts: usize,
}

impl RpcClusterTpuQuicInfoServiceConfig {
    pub fn default_refresh_interval() -> std::time::Duration {
        DEFAULT_REFRESH_INTERVAL
    }

    pub fn default_max_retry_attempts() -> usize {
        DEFAULT_MAX_RETRY_ATTEMPTS
    }
}

impl Default for RpcClusterTpuQuicInfoServiceConfig {
    fn default() -> Self {
        Self {
            refresh_interval: Self::default_refresh_interval(),
            max_retry_attempts: Self::default_max_retry_attempts(),
        }
    }
}

enum Diff {
    Upsert((Pubkey, RpcTpuQuicContactInfo)),
    Remove(Pubkey),
}

fn detect_tpu_contact_info_diff(
    current: &HashMap<Pubkey, RpcTpuQuicContactInfo>,
    new: &[RpcContactInfo],
) -> Vec<Diff> {
    let mut to_remove = current
        .keys()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    let mut ret = new
        .iter()
        .filter_map(|node| {
            let pubkey = Pubkey::from_str(&node.pubkey).expect("Pubkey::from_str");
            let contact_info = RpcTpuQuicContactInfo::from(node);
            to_remove.remove(&pubkey);
            match current.get(&pubkey) {
                Some(existing_info) => {
                    if existing_info != &contact_info {
                        Some(Diff::Upsert((pubkey, contact_info)))
                    } else {
                        None
                    }
                }
                None => Some(Diff::Upsert((pubkey, contact_info))),
            }
        })
        .collect::<Vec<_>>();

    for removed_pubkey in to_remove {
        ret.push(Diff::Remove(removed_pubkey));
    }

    ret
}

fn apply_diff_patch(
    current: &mut HashMap<Pubkey, RpcTpuQuicContactInfo>,
    diffs: impl IntoIterator<Item = Diff>,
) {
    for diff in diffs {
        match diff {
            Diff::Upsert((pubkey, contact_info)) => {
                current.insert(pubkey, contact_info);
            }
            Diff::Remove(pubkey) => {
                current.remove(&pubkey);
            }
        }
    }
}

async fn cluster_info_refresh_loop(
    rpc_client: Arc<RpcClient>,
    shared: Arc<RwLock<HashMap<Pubkey, RpcTpuQuicContactInfo>>>,
    config: RpcClusterTpuQuicInfoServiceConfig,
    cancellation_token: CancellationToken,
) {
    let mut interval = tokio::time::interval(config.refresh_interval);
    let last_snapshot = shared.read().expect("read").clone();
    let mut consecutive_fetch_errors = 0;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Continue to refresh
            }
            _ = cancellation_token.cancelled() => {
                tracing::info!("Cancellation requested, exiting cluster info refresh loop.");
                break;
            }
        }

        // Fetch cluster nodes
        match rpc_client.get_cluster_nodes().await {
            Ok(nodes) => {
                // Update shared state
                consecutive_fetch_errors = 0;
                let diff = detect_tpu_contact_info_diff(&last_snapshot, &nodes);

                if !diff.is_empty() {
                    tracing::trace!("Updating cluster nodes with {} changes.", diff.len());
                    let mut writable = shared.write().expect("write");
                    apply_diff_patch(&mut writable, diff);
                } else {
                    tracing::trace!("No changes in cluster nodes detected.");
                }
            }
            Err(e) => {
                consecutive_fetch_errors += 1;

                if e.is_transient() && consecutive_fetch_errors < config.max_retry_attempts {
                    tracing::warn!(
                        "Transient error fetching cluster nodes, will retry: {:?}",
                        e
                    );
                    continue;
                } else {
                    tracing::error!("Failed to fetch cluster nodes: {:?}", e);
                    panic::panic_any(e)
                }
            }
        }
    }
}

///
/// Creates a new [`RpcClusterTpuQuicInfoService`] along with its background refresh task.
///
/// # Arguments
///
/// * `rpc_client` - The Solana RPC client to fetch cluster nodes from.
/// * `config` - Configuration for the TPU info service.
///
/// # Returns
///
/// A tuple containing the [`RpcClusterTpuQuicInfoService`] and a `JoinHandle` for the background task.
///
/// Dropping the `JoinHandle` will not stop the background task; to stop it, drop the all [`RpcClusterTpuQuicInfoService`] instance.
///
pub async fn rpc_cluster_tpu_info_service(
    rpc_client: Arc<RpcClient>,
    config: RpcClusterTpuQuicInfoServiceConfig,
) -> Result<(RpcClusterTpuQuicInfoService, JoinHandle<()>), solana_client::client_error::ClientError>
{
    let initial_topology_mapping = rpc_client
        .get_cluster_nodes()
        .await?
        .into_iter()
        .map(|node| {
            (
                Pubkey::from_str(node.pubkey.as_str()).expect("Pubkey::from_str"),
                RpcTpuQuicContactInfo::from(&node),
            )
        })
        .collect::<std::collections::HashMap<_, _>>();

    tracing::trace!(
        "Initial cluster nodes fetched: {} nodes.",
        initial_topology_mapping.len()
    );
    let shared = std::sync::Arc::new(std::sync::RwLock::new(initial_topology_mapping));

    let cancellation_token = CancellationToken::new();
    let on_drop = OnDrop {
        handle: cancellation_token.clone(),
    };
    let ret = RpcClusterTpuQuicInfoService {
        shared: Arc::clone(&shared),
        _on_drop: Arc::new(on_drop),
    };

    let handle = tokio::spawn(cluster_info_refresh_loop(
        rpc_client,
        shared,
        config,
        cancellation_token,
    ));

    Ok((ret, handle))
}

impl LeaderTpuInfoService for RpcClusterTpuQuicInfoService {
    fn get_quic_tpu_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr> {
        self.shared
            .read()
            .expect("read lock")
            .get(leader_pubkey)
            .and_then(|info| info.tpu_quic)
    }

    fn get_quic_tpu_fwd_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr> {
        self.shared
            .read()
            .expect("read lock")
            .get(leader_pubkey)
            .and_then(|info| info.tpu_forwards_quic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_diff() {
        let pk1 = Pubkey::new_from_array([1u8; 32]);
        let pk2 = Pubkey::new_from_array([2u8; 32]);
        let pk3 = Pubkey::new_from_array([3u8; 32]);
        let node1_tpu_quic: Option<SocketAddr> = Some("127.0.0.1:5000".parse().unwrap());
        let node1_tpu_fwd_quic: Option<SocketAddr> = Some("127.0.0.1:5001".parse().unwrap());
        let node3_tpu_quic: Option<SocketAddr> = Some("127.0.0.1:5002".parse().unwrap());
        let node3_tpu_fwd_quic: Option<SocketAddr> = Some("127.0.0.1:5003".parse().unwrap());

        let info1 = RpcContactInfo {
            pubkey: pk1.to_string(),
            gossip: None,
            tpu: None,
            rpc: None,
            tvu: None,
            tpu_quic: node1_tpu_quic,
            tpu_forwards: None,
            tpu_forwards_quic: node1_tpu_fwd_quic,
            tpu_vote: None,
            serve_repair: None,
            pubsub: None,
            version: None,
            feature_set: None,
            shred_version: None,
        };

        let info3 = RpcContactInfo {
            pubkey: pk3.to_string(),
            gossip: None,
            tpu: None,
            rpc: None,
            tvu: None,
            tpu_quic: node3_tpu_quic,
            tpu_forwards: None,
            tpu_forwards_quic: node3_tpu_fwd_quic,
            tpu_vote: None,
            serve_repair: None,
            pubsub: None,
            version: None,
            feature_set: None,
            shred_version: None,
        };

        let mut info1_prime = info1.clone();
        info1_prime.tpu_quic = Some("127.0.0.1:6000".parse().unwrap());

        let mut current_mapping: HashMap<Pubkey, RpcTpuQuicContactInfo> = HashMap::from([
            (
                pk1,
                RpcTpuQuicContactInfo {
                    tpu_quic: node1_tpu_quic,
                    tpu_forwards_quic: node1_tpu_fwd_quic,
                },
            ),
            (
                pk2,
                RpcTpuQuicContactInfo {
                    tpu_quic: node3_tpu_quic,
                    tpu_forwards_quic: node3_tpu_fwd_quic,
                },
            ),
        ]);

        let new_contact_info_vec = vec![info1_prime, info3];

        let mut diffs = detect_tpu_contact_info_diff(&current_mapping, &new_contact_info_vec);
        diffs.sort_by_key(|d| match d {
            Diff::Upsert((pk, _)) => (0, *pk),
            Diff::Remove(pk) => (1, *pk),
        });

        assert_eq!(diffs.len(), 3);

        let actual_first_diff = &diffs[0];
        assert!(matches!(actual_first_diff, Diff::Upsert((pk, _)) if *pk == pk1));

        let actual_second_diff = &diffs[1];
        assert!(matches!(actual_second_diff, Diff::Upsert((pk, _)) if *pk == pk3));

        let actual_third_diff = &diffs[2];
        assert!(matches!(actual_third_diff, Diff::Remove(pk) if *pk == pk2));

        apply_diff_patch(&mut current_mapping, diffs);

        assert_eq!(current_mapping.len(), 2);
        assert!(current_mapping.contains_key(&pk1));
        assert!(current_mapping.contains_key(&pk3));
    }
}

//!
//! Core types and traits for TPU client implementations.
//!
//! # Overview
//!
//! This module contains the core event loop driver and related types for implementing
//! a TPU sender using Tokio and QUIC via the `quinn` library.
//!
//! It defines the main driver struct `TpuSenderDriver`, which manages connections to remote peers,
//! transaction sending workers, and leader prediction.
//!
//! # Connection Eviction
//!
//! The driver supports connection eviction strategies via the [`crate::core::ConnectionEvictionStrategy`] trait.
//! See [`crate::core::StakeBasedEvictionStrategy`] for a basic implementation.
//!
//! # Upcoming Leader Prediction
//!
//! The driver can predict upcoming leaders using the [`crate::core::UpcomingLeaderPredictor`] trait.
//!
//! # Callback Mechanism
//!
//! The driver supports a callback mechanism for notifying the caller about transaction send results.
//! See [`crate::core::TpuSenderResponseCallback`] for details.
//!
//! ## Example
//!
//! ```ignore
//! #[derive(Clone)]
//! struct LoggingCallback;
//!
//! impl TpuSenderResponseCallback for LoggingCallback {
//!     fn call(&self, response: TpuSenderResponse) {
//!         tracing::info!("Received TPU sender response: {:?}", response);
//!     }
//! }
//! ```
//!
//!

mod constants;
mod driver;
mod eviction;
mod identity_update;
mod inlet;
mod leader_fast_path;
mod peer_addr_watcher;
mod quic;
mod response;
mod services;
mod txn;
mod worker;

pub use {
    crate::{
        config::{DEFAULT_UNUSED_CONNECTION_TTL, QUIC_MAX_TIMEOUT},
        tls::{ALPN_TPU_PROTOCOL_ID, crypto_provider},
    },
    constants::{
        DEFAULT_EVICTION_GRACE_DURATION, PACKET_DATA_SIZE, QUIC_KEEP_ALIVE, QUIC_SEND_FAIRNESS,
    },
    driver::spawner::{TpuSenderDriverSpawner, TpuSenderSessionContext},
    eviction::{
        stake_based::StakeBasedEvictionStrategy,
        staked_set::{StakeSortedPeerSet, StakedSortedSet},
        strategy::{
            ActiveConnection, ConnectionEviction, ConnectionEvictionStrategy, MultiplexedPeerGroup,
            RemotePeerAddrMap,
        },
    },
    identity_update::{TpuSenderIdentityUpdater, UpdateIdentity, UpdateIdentityError},
    inlet::{TpuSenderDriverInlet, TpuSenderDriverInletError},
    quic::socket_addr_to_quic_server_name,
    response::{
        Nothing, SendTxError, TpuSenderResponse, TpuSenderResponseCallback, TxDrop, TxDropReason,
        TxFailed, TxSent,
    },
    services::{
        IgnorantLeaderPredictor, LeaderTpuInfoService, OverrideTpuInfoService,
        UpcomingLeaderPredictor, ValidatorStakeInfoService,
    },
    txn::{TXN_INFO_CAP, TpuSenderTxn, TpuSenderTxnInfo},
};

pub const fn module_path_for_test() -> &'static str {
    module_path!()
}

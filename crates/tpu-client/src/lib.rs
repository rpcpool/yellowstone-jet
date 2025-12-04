//!
//! Yellowstone jet-tpu-client
//!
//! This crate is port of the custom TPU-QUIC client used by [Yellowstone Jet](https://github.com/rpcpool/yellowstone-jet)
//! a subsystem of [Cascade-Marketplace](https://triton.one/cascade),
//!
//! This crates expose a generic TPU sender implementation [TpuSender](`crate::sender::TpuSender`) that can be used with different
//! TPU info services, stake info services, eviction strategies, and leader schedule predictors.
//!
//! The cores async event-loop engine uses [quinn] and [tokio] crates to provide a high-performance QUIC-based transport protocol implementation.
//! It is designed to handle the latest Agave network changes and covers all the edge-cases observed in production usage:
//!
//! 1. Automatic leader schedule tracking and slot updates
//! 2. Automatic TPU contact-info handling:
//!    - Contact info discovery using latest gossip information from the network.
//!    - Handles TPU endpoint changes due to leader contact info flapping (e.g. Jito validators)
//! 3. Automic connection manamgent:  reconnect, connection-prediction, failures handling.
//! 4. Rescue transaction on connection dropped (e.g. due to remote peer connection eviction)
//! 5. Stake-aware TPU selection and eviction strategies.
//!
//! ## `YellowstoneTpuSender` : Smart TPU sender implementation
//!
//! This crate come with a _smart_ TPU sender implementation: [YellowstoneTpuSender](`crate::yellowstone_grpc::sender::YellowstoneTpuSender`)
//!
//! This sender implementation supports three different sending strategies:
//!
//! 1. Send transaction to one or more remote peers
//! 2. Send to the current leader
//! 3. Send to the the curent leader AND the next `N-1` leaders in the schedule.
//!
//! The sender automatically tracks the current slot and leader schedule.
//!
//! ## Example
//!
//! See [repository](https://github.com/rpcpool/yellowstone-jet/blob/main/crates/tpu-client/src/bin/test-tpu-send.rs) for more examples.
//!
//! # feature-flag supports
//!
//! - **prometheus**: Enable prometheus metrics exposition module [`crate::prom`]
//! - **yellowstone-grpc**: Enable Yellowstone gRPC based TPU sender implementation [`crate::yellowstone_grpc`]
//! - **bytes** : Enable `bytes` crate based transaction representation support in TPU sender
//!
///
/// module for top-level cnfiguration objects
///
pub mod config;
///
/// module for the core tpu sending driver logic
///
pub mod core;
///
/// module for common tpu sender implementation
///
pub mod sender;

///
/// module to enable prometheus metrics exposition
///
#[cfg(feature = "prometheus")]
pub mod prom;

///
/// module for RPC utilities
///
pub mod rpc;

///
/// module for slot tracking
///
pub mod slot;

///
/// module to host utility that utilize Yellowstone gRPC services
///
#[cfg(feature = "yellowstone-grpc")]
pub mod yellowstone_grpc;

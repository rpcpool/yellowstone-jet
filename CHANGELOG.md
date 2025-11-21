# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note:** Version 0 of Semantic Versioning is handled differently from version 1 and above.
The minor version will be incremented upon a breaking change and the patch version will be incremented for features.

## [Unreleased]


## [14.9.1]

### Fixes

- Fix connection-endpoint allocation [#91](https://github.com/rpcpool/yellowstone-jet/issues/91)

### Misc

- Renamed many tpu-client related objects for more suitable named (replaced "gateway" with tpu-sender).
- Moved out `yellowstone-jet-tpu-client` into its own crate.

## [14.9.0]

### Features

- [@Rhovian](https://github.com/Rhovian) Load keypair locally in set-admin CLI command [#47](https://github.com/rpcpool/yellowstone-jet/issues/47)
- Added optional `prometheus` CLI argument to bind a local a prometheus metrics scrap webpage server.

### Fixes

- Fix `grpc_geyser.rs` missed cancellation token signal during grpc reconnect.

### Misc

- Isolated quic-client in order to move this into a feature crate.

## [14.8.0]

## Changes
- Bump crates to use Agave v3

## [14.7.1]

### Fixes

- Fixed `quic_send_attempts` double-reporting on success (#89)
- Fixed entry cleanup in stake sorted-map (#90)

### Changes

- Removed vixen dependency (#88)

## [14.7.0]

### Features

- Upgraded dependencies (#84)
  - Updated jsonrpsee: 0.24.7 to 0.26.0
  - Updated prometheus, prost, prost-types: 0.13.3 to 0.14.0
  - Updated rand: 0.8.5 to 0.9.0
  - Updated tonic ecosystem: 0.12.3 to 0.14.0
  - Updated tower: 0.4.13 to 0.5.0
  - Updated thiserror: 1.0.58 to 2.0.0
  - Updated yellowstone-grpc-client/proto: 7 to 9.0.0
  - Updated yellowstone-shield-store: ~0.5.2 to 0.7.0
  - Updated Solana public repository patches from v2.2.19-triton-public to v2.3.8-triton-public

### Fixes

- Removed local set from shield store runtime

## [14.6.3]

- Added more details in log during connection error

## [14.6.2]

- Fixed `cluster_identity_stake` prometheus metric that was not getting updated

## [14.6.1]

### Fixes

- `max_streams` set to 0 is not interpreted as `None`
- Fixed SIGINT [#77](https://github.com/rpcpool/yellowstone-jet/issues/77)

## [14.6.0]

### Features

- Added Lewis event tracking support for transaction lifecycle visibility [#53](https://github.com/rpcpool/yellowstone-jet/issues/53)
- Added `lewis-dummy-server` binary for testing
- Added prometheus metrics: `lewis_events_dropped_total`, `lewis_events_sent_total`
- Added `tpu_info_override` config option + `extra_fanout` option [#69](https://github.com/rpcpool/yellowstone-jet/issues/69)

### Fixes

- Bug fix `stake.rs` [#72](https://github.com/rpcpool/yellowstone-jet/issues/72)
- Skip transaction subscription in gRPC Geyser when relay-only mode is enabled [#71](https://github.com/rpcpool/yellowstone-jet/issues/71)

## [14.5.0]

- More metrics for slot timings
- refactoring for geyser subscription
- fix QUIC RETRY_TOKEN server name issue #[66](https://github.com/rpcpool/yellowstone-jet/issues/66)
- remove useless `Arc` #[68](https://github.com/rpcpool/yellowstone-jet/issues/68)
- Supports `getLatestSlot` RPC endpoint
- double permit-per-second calculation in `stake.rs`

## [14.4.0]

- Fixed critical memory leak in rooted transactions where finalized slots were never cleaned up due to incorrect block height comparison.
- Refactored `GrpcRootedTxReceiver` to use a testable state machine pattern, eliminating duplicated state and improving code maintainability.
- Added comprehensive tests for transaction lifecycle and cleanup logic to prevent future regressions.
- No breaking changes to public APIs.

## [14.3.1]

- Fixed connection prediction ignore.

## [14.3.0]

- Improved leader schedule and slot tracking by using `FirstShredReceived` events, resulting in 350-400ms faster TPU routing.
- Refactored `grpc_geyser.rs `for better code clarity and testability.
- Separated slot updates and block metadata into distinct types and channels for improved performance and maintainability.
- Added `SlotStatus` enum to better represent slot states.
- Internal: Renamed config `primary_grpc` and `secondary_grpc` to a single `grpc` field. `primary_grpc` is still supported for backward compatibility.
- Minor internal renames and refactoring to clarify slot/block handling.
- No breaking changes to public APIs.

## [14.2.0]

- Introduce connection prediction in quic-gateway to proactively identify potential leaders before forwarding transaction demands

## [14.1.0]

- Moved away from solana-sdk for individuals solana crates instead.

## [14.0.0]

- Update `yellowstone-shield-store` to `0.5.1` which adjusts public API of `PolicyStore`.

## [13.0.0]

- Removed `ConnectionCache` to use `QuicGateway` that maximise each QUIC connection streaming capability instead of opening new connections.
- Remove retry logic when `relay_mode` is set to `true`.
- Removed `quic.rs` and `quic_solana` modules.
- Refactored `transactions.rs` with simpler retry logic and sending logic.
- Switched to Jemalloc for Global memory allocator

## [12.0.1]

- Updated to Cargo edition 2024 and 1.85 toolchain.

## [12.0.0]

### Breaking Changes

- Remove `shield.enabled` configuration in favor enabling through Jet feature `yellowstone_shield`.

### Features

- Added `YELLOWSTONE_SHIELD` feature flag in proto file definition

## [11.2.0]

### Features

- Added support for Yellowstone Shield transaction filtering. When enabled via `shield.enabled`, applies access control policies to determine which transactions can be forwarded to leader nodes.
- Changed `RpcSendTransactionConfigWithBlocklist` to `JetRpcSendTransactionConfig` and renamed all related fields from `blocklist` to `forwarding_policies`
- Updated protocol buffer definition field from `repeated string blocklist_pdas = 4` to `repeated string forwarding_policies = 4` for consistency
- Added support for base58-encoded public keys in `forwarding_policies` field with automatic validation and filtering of invalid keys
- Made `forwarding_policies` optional with a default empty vector when not specified in JSON RPC requests

## [11.1.3]

### Features

- Version added to metadata to jet-gateway using subscribe request.
- Added support for pushing metrics to vmAgent.

## [11.1.2]

### Fixes

- Added stake-based max_streams formula when creating QUIC connection in `QuicPool` so max stream per connection is derived from current stake weight.
- Remove custom max_stream from configuration options, it should stay dynamic and computed based off validator stake.

## [11.1.1]

### Fixes

- Added missing `LEADER_MTU` metrics registration
- Added missing `LEADER_RTT` metrics registration
- Added missing `SEND_TRANSACTION_E2E_LATENCY` metrics registration
- Added missing `SEND_TRANSACTION_ATTEMPT` metrics registration
- Added missing `GATEWAY_CONNECTED` metric upodate on disconnection logic

## [11.1.0]

### Features

- Added support for block leaders from yellowstone-blocklist smart contract
- Refactor `cluster_tpu_info.rs` and `grpc_geyser.rs`
- Added SelectLeaders struct to handle blocking tpus

### Fixes

- Added tests for blocklist
- Added tests for ClusterTpuInfo
- Fix on QuicClient

## [11.0.0]

### Breaking Changes

- Modified transaction payload structure:
  - Changed base transaction message format
  - Updated protobuf definitions for compatibility
  - Modified legacy payload handling

### Features

- Implemented a comprehensive dual-format transaction system:
  - Legacy format support with base58/base64 encoding options
  - New structured format with enhanced configuration capabilities
  - Automatic format detection and graceful degradation
- Added feature flag system for negotiating capabilities with gateway servers
- Enhanced gRPC connection stability:
  - Improved error handling for disconnections
  - Better detection of problematic connections
  - Added feature flag negotiation with graceful fallback
- Improved empty message handling to prevent protocol errors
- Added comprehensive configuration support:
  - Transaction encoding configuration
  - Extended transaction validation options
  - Support for blocklist PDAs

### Fixes

- Fixed encoding/decoding edge cases in transaction handling
- Improved feature flag compatibility detection
- Enhanced transaction validation reliability
- Fixed connection stability issues

## [10.6.0]

- Added 'IdentityFlusherWaitGroup' struct which function is to control when an identity is updated in Jet.
- Added reset_identity method in RPC.
- Added flush transactions while updating identity.
- Added shutdown connection with Jet-Gateway while updating identity.

### Fixes

- Refactor `SendTransactionsPool`, `SendTransactionsPoolTask`, `RootedTransactions`, `QuicClient`, `ClusterTpuInfo`, `BlockhasQueue`
- Added integration test for `SendTransactionsPool`, `IdentityFlusherWaitGroup` and `RpcServer`.

## [10.5.0]

### Features

- Added `leader_rtt` histogram metric which obseve round-trip-time between jet and remote leader.
- Added `leader_mtu` gauge metric which tracks the each leader connection MTU.
- Added `send_transaction_attempt` counter metrics which count every send attempt (sucessful or failed) to a leader.
- Added `send_transaction_e2e_latency` histogram metrics which measure to end-to-end time it takes from opening a uni-stream sending a transaction and waiting for a acks.

### Fixes

- Added `finish` and `stopped` call in `quic_solana::QuicClientsend_buffer_using_conn` to properly awaits acks making sure transaction are truely handled by
  a remote validator.

## [10.4.0]

### Features

- Updated solana SDK dependencies to 2.1.11 to support version `>=4.1.1` of Dragonsmouth gRPC endpoints.
- Imported `gentx` util to test jet instance.

### Fixes

- Fixes hanging issue during SIGINT shutdown in: `yellowstone_jet::cluster_tpu_info::update_leader_schedule`, `yellowstone-jet::cluster_tpu_info::update_cluster_nodes`, `yellowstone_jet::stake::update_stake`.
- Specifying `keypair` in identity configuration without `expected` caused invalid stake-based message per 100ms calculation (#[5](https://github.com/rpcpool/yellowstone-jet/pull/5))

## [10.3.1]

### Fixes

- In jet, when calling `send_transaction` we now ignore corrupted payload instead of crashing (#[139](https://github.com/rpcpool/solana-yellowstone-jet/pull/139)).

## [10.3.0]

### Features

feat: added consumer/publisher connected status metrics ([#128](https://github.com/rpcpool/solana-yellowstone-jet/pull/124))

## [10.2.4]

### Fixes

- Patch fix transaction size by @leafaar in ([#124](https://github.com/rpcpool/solana-yellowstone-jet/pull/124))

## [10.2.3]

### Fixes

- fix high cardinality metric by @Fatima-yo in ([#122](https://github.com/rpcpool/solana-yellowstone-jet/pull/122))

## [10.2.2]

### Fixes

- Put back `crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()]` in `quic_solana.rs`;

## [10.2.1] (BROKEN)

### Features

- Added metrics to track send payload to jet instance latency (#[117](https://github.com/rpcpool/solana-yellowstone-jet/pull/117))

## [10.2.0] (BROKEN)

### Features

- Added `ConnectionCacheIdentity` to manage the `ConnectionCache`'s `Identity` separatly

### Fixes

- Refactor Quin client
- Added integration test for `ConnectionCache` and `ConnectionCacheIdentity`.
- Fixes the expected identity bug where the jet-gateway client would crash because initially the random identity set wouldn't match the expected one.

### Breaking

## [10.1.0]

### Features

- Added `/publisherList` endpoint for admin interface ([#93](https://github.com/rpcpool/solana-yellowstone-jet/pull/83))
- Added `/auth` endpoint for authenticating pubkey ([#90](https://github.com/rpcpool/solana-yellowstone-jet/pull/90))
- optional pubkey authentication when subscribing ([#85](https://github.com/rpcpool/solana-yellowstone-jet/pull/85))
- prometheus metrics for consumers ([#84](https://github.com/rpcpool/solana-yellowstone-jet/pull/84))
- Added `/subscriberList` endpoint for admin interface ([#83](https://github.com/rpcpool/solana-yellowstone-jet/pull/83))

### Fixes

- Fixes high cardinality metrics ([#81](https://github.com/rpcpool/solana-yellowstone-jet/pull/81))

## [10.0.0]

### Breaking

- proto: add ping/pong message to jet publish/subscribe ([#75](https://github.com/rpcpool/solana-yellowstone-jet/pull/75))

## [9.0.2]

### Features

- metrics: set active gateway in jet ([#69](https://github.com/rpcpool/solana-yellowstone-jet/pull/69))

### Fixes

- grpc: fix TLS/certs connection ([#68](https://github.com/rpcpool/solana-yellowstone-jet/pull/68))

## [9.0.1]

### Fixes

- etcd: fix cancelation of watch client ([#66](https://github.com/rpcpool/solana-yellowstone-jet/pull/66))

## [9.0.0]

### Features

- solana: update to 2.0 ([#64](https://github.com/rpcpool/solana-yellowstone-jet/pull/64))
- rust: bump to 1.82.0 ([#64](https://github.com/rpcpool/solana-yellowstone-jet/pull/64))
- config: remove option leader_schedule_commitment ([#65](https://github.com/rpcpool/solana-yellowstone-jet/pull/65))

### Fixes

- gateway: fix TLS connections ([#64](https://github.com/rpcpool/solana-yellowstone-jet/pull/64))

## [8.3.0]

### Features

- jet: support blocklist from the config ([#59](https://github.com/rpcpool/solana-yellowstone-jet/pull/59))

## [8.2.1]

### Fixes

- quic: remove not required quic stream call ([#57](https://github.com/rpcpool/solana-yellowstone-jet/pull/57))
- grpc: increase max decode message size ([#62](https://github.com/rpcpool/solana-yellowstone-jet/pull/62))

## [8.2.0+solana.1.18.17]

### Features

- gateway: support env variables for listen addresses ([#52](https://github.com/rpcpool/solana-yellowstone-jet/pull/52) [#56](https://github.com/rpcpool/solana-yellowstone-jet/pull/56))
- gateway: add go_package to proto ([#54](https://github.com/rpcpool/solana-yellowstone-jet/pull/54))

## [8.0.0+solana.1.18.17]

### Features

- metrics: add gRPC to lewis ([#47](https://github.com/rpcpool/solana-yellowstone-jet/pull/47))
- gateway: remove kafka ([#48](https://github.com/rpcpool/solana-yellowstone-jet/pull/48))

## [7.0.0+solana.1.18.17]

## Features

- jet-proxy-kafka: acquire lock in `etcd` ([#46](https://github.com/rpcpool/solana-yellowstone-jet/pull/46))

## [6.0.2+solana.1.18.17]

### Features

- fix sanitize error rpc server response ([#42](https://github.com/rpcpool/solana-yellowstone-jet/pull/42))

## [6.0.1+solana.1.18.17]

### Features

- exclude duplicated extra tpu forward ([#41](https://github.com/rpcpool/solana-yellowstone-jet/pull/41))

## [6.0.0+solana.1.18.17]

### Features

- update solana to 1.18.17 ([#38](https://github.com/rpcpool/solana-yellowstone-jet/pull/38))
- add extra_tpu_forward support ([#39](https://github.com/rpcpool/solana-yellowstone-jet/pull/39))
- allow to load keypair from config ([#40](https://github.com/rpcpool/solana-yellowstone-jet/pull/40))

## [5.0.0+solana.1.17.33] - 2024-06-02

### Features

- add jet-proxy-kafka ([#35](https://github.com/rpcpool/solana-yellowstone-jet/pull/35))

## [4.0.0+solana.1.17.33] - 2024-05-22

### Features

- add stake info to metrics ([#33](https://github.com/rpcpool/solana-yellowstone-jet/pull/33))
- add logs to rpc uri middleware ([#34](https://github.com/rpcpool/solana-yellowstone-jet/pull/34))

### Breaking

- deserialize time in config with humantime ([#32](https://github.com/rpcpool/solana-yellowstone-jet/pull/32))

## [3.0.0+solana.1.17.33] - 2024-05-15

### Features

- update solana to 1.17.33 (change default pool size) ([#23](https://github.com/rpcpool/solana-yellowstone-jet/pull/23))
- add `quic.send_timeout` (default 10s) ([#25](https://github.com/rpcpool/solana-yellowstone-jet/pull/25))
- get connection based on number of streams in progress ([#26](https://github.com/rpcpool/solana-yellowstone-jet/pull/26))
- add `skip_sanitize` support ([#28](https://github.com/rpcpool/solana-yellowstone-jet/pull/28))
- add tx to tpu metric ([#29](https://github.com/rpcpool/solana-yellowstone-jet/pull/29))
- add identity check to health endpoint ([#30](https://github.com/rpcpool/solana-yellowstone-jet/pull/30))
- support patched gCN instead of port offset ([#31](https://github.com/rpcpool/solana-yellowstone-jet/pull/31))

### Fixes

- use `deny_unknown_fields` in the config ([#21](https://github.com/rpcpool/solana-yellowstone-jet/pull/21))
- change default value of `quic.send_retry_count` to `1` ([#24](https://github.com/rpcpool/solana-yellowstone-jet/pull/24))

## [2.0.0+solana.1.17.31] - 2024-05-02

### Features

- use jsonrpsee instead of jsonrpc-core ([#16](https://github.com/rpcpool/solana-yellowstone-jet/pull/16))
- proxy simulate and sanitize to rpc ([#17](https://github.com/rpcpool/solana-yellowstone-jet/pull/17))
- add expected identity to config ([#18](https://github.com/rpcpool/solana-yellowstone-jet/pull/18))
- add relay only mode option to config ([#19](https://github.com/rpcpool/solana-yellowstone-jet/pull/19))
- add gRPC reconnect logic ([#20](https://github.com/rpcpool/solana-yellowstone-jet/pull/20))

## [1.3.0+solana.1.17.25] - 2024-04-24

### Features

- add endpoint_port_range to config ([#15](https://github.com/rpcpool/solana-yellowstone-jet/pull/15))

## [1.2.0+solana.1.17.25] - 2024-04-19

### Features

- use lru for connection pools, more quic config options ([#12](https://github.com/rpcpool/solana-yellowstone-jet/pull/12))
- add more metrics to prometheus ([#14](https://github.com/rpcpool/solana-yellowstone-jet/pull/14))

## [1.1.0+solana.1.17.25] - 2024-04-19

### Features

- add more quic options to config ([#8](https://github.com/rpcpool/solana-yellowstone-jet/pull/8))
- do not retry quic timeout error ([#9](https://github.com/rpcpool/solana-yellowstone-jet/pull/9))
- remove warming task ([#10](https://github.com/rpcpool/solana-yellowstone-jet/pull/10))
- add admin interface to cli ([#11](https://github.com/rpcpool/solana-yellowstone-jet/pull/11))

## [1.0.1+solana.1.17.25] - 2024-04-16

### Features

- validate gRPC version on connect ([#7](https://github.com/rpcpool/solana-yellowstone-jet/pull/7))
- solana: update to 1.17.31 ([#7](https://github.com/rpcpool/solana-yellowstone-jet/pull/7))

## [1.0.0+solana.1.17.25] - 2024-04-16

Init

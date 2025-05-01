# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note:** Version 0 of Semantic Versioning is handled differently from version 1 and above.
The minor version will be incremented upon a breaking change and the patch version will be incremented for features.


## [Unreleased]

### Breaking Changes

### Features

### Fixes

## [11.3.0]

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

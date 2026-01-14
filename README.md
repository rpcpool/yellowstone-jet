# Yellowstone Jet

External Solana transaction sender implementation.

Implements the Solana QUIC protocol for sending transactions.

## Features:

- Efficient transaction sending, implementing the Solana TPU client in proxy format
- Detailed configuration of all QUIC related parameters
- Solana JSONRPC support, with an rpc server that supports sendTransaction
- Full support for SwQoS
- Simulation and transaction sanitization support via external RPC
- Prometheus metrics
- Dynamic key loading via getIdentity/setIdentity
- Support for shield policies to determine eligible leaders for transaction forwarding
- Support for connecting to Triton's Cascade delivery network

## Building

```
cargo build --release
```

## Usage

```
yellowstone-jet --config yellowstone.yaml
```

## Config file

A sample configuration file can be found [config.yml](https://github.com/rpcpool/yellowstone-jet/blob/main/apps/jet/config.yml)

### Systemd

Running Jet as a service under SystemD is our recommended approach. A sample systemd file:

```ini
[Unit]
Description=Yellowstone Jet transaction forwarder
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=0

[Service]
Type=simple
DynamicUser=yes
ConfigDirectory=yellowstone-jet

ExecStart=/usr/local/bin/yellowstone-jet --config /etc/yellowstone-jet/config.yml

Environment=RUST_LOG="warn"
Restart=always
RestartSec=5

# Resource Limits
LimitNOFILE=700000
LimitNPROC=700000

# File System Sandboxing
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectHostname=yes
ProtectClock=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectKernelLogs=yes
ProtectControlGroups=yes
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
RestrictNamespaces=yes
LockPersonality=yes
MemoryDenyWriteExecute=yes
RestrictRealtime=yes
RestrictSUIDSGID=yes
RemoveIPC=yes
PrivateUsers=yes

# Privilege Escalation & System Calls
NoNewPrivileges=yes
SystemCallFilter=@system-service
SystemCallErrorNumber=EPERM
SystemCallArchitectures=native

[Install]
WantedBy=multi-user.target
```

## Attribution

Created by the greybeards at [Triton One](https://triton.one)

Copyright (C) 2024 Triton One Ltd

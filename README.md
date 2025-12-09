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

```
[Unit]
Description=Yellowstone Jet transaction forwarder
After=network-online.target
StartLimitInterval=0
StartLimitIntervalSec=0

[Service]
Type=simple
User=yellowstone-jet
Group=yellowstone-jet
PermissionsStartOnly=true
ExecStart=/usr/local/bin/yellowstone-jet --config /etc/yellowstone-jet.yml

Environment=RUST_LOG="warn"

SyslogIdentifier=yellowstone-jet
KillMode=process
Restart=always
RestartSec=5

LimitNOFILE=700000
LimitNPROC=700000

LockPersonality=true
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
RemoveIPC=true
RestrictSUIDSGID=true

ProtectSystem=full

[Install]
WantedBy=multi-user.target
```

## Attribution

Created by the greybeards at [Triton One](https://triton.one)

Copyright (C) 2024 Triton One Ltd

# Yellowstone Jet

External Solana transaction sender implementation.

Implements the Solana QUIC protocol for sending transactions.

## Features:

- Efficient transaction sending, implementing the Solana TPU client in proxy format
- Detailed configuration of all QUIC related parameters
- Solana JSONRPC support, with an rpc server that supports sendTransaction
- HTTP transaction submission API (`POST /api/v1/transactions`) with raw bytes, base58, and base64 support
- Full support for SwQoS
- Simulation and transaction sanitization support via external RPC
- Prometheus metrics
- Dynamic key loading via getIdentity/setIdentity
- Support for shield policies to determine eligible leaders for transaction forwarding
- Support for connecting to Triton's Cascade delivery network

## Building

```
cargo build --release -p yellowstone-jet  

```

Jet binary will be located at `./target/release/jet`

## Usage

```
jet --config yellowstone.yaml
```

## Config file

A sample configuration file can be found [config.yml](https://github.com/rpcpool/yellowstone-jet/blob/main/apps/jet/config.yml)

### Systemd

Running Jet as a service under SystemD is our recommended approach. A sample systemd file is provided at [systemd/yellowstone-jet.service](systemd/yellowstone-jet.service).

To install:

```bash
sudo cp systemd/yellowstone-jet.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now yellowstone-jet
```

## HTTP Transaction API

Jet exposes a lightweight HTTP endpoint for transaction submission alongside the JSON-RPC interface:

```
POST /api/v1/transactions
```

### Optional headers

All per-request configuration for this endpoint is carried in headers.

| Header | Values | Description |
|--------|--------|-------------|
| `x-jet-encoding` | `base58`, `base64` | Encoding of the text body. Ignored for raw bytes |
| `x-jet-max-retries` | integer | Maximum retry attempts |
| `x-jet-forwarding-policies` | comma-separated pubkeys | Restrict forwarding to leaders allowed by these policies |
| `x-jet-response` | `signature`, `none` | What to return on success |

### Content types

| Content-Type | Body format | Description |
|---|---|---|
| `application/octet-stream` | Raw wire bytes | Fastest path — zero encode/decode overhead |
| `text/plain` (or absent) | Encoded text | base58 or base64 encoded transaction string |

### Examples

```bash
# Raw bytes — fastest, no encoding overhead
curl -X POST /api/v1/transactions \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @transaction.bin

# Raw bytes with retry/policy overrides in headers, return signature
curl -X POST /api/v1/transactions \
  -H 'Content-Type: application/octet-stream' \
  -H 'x-jet-max-retries: 3' \
  -H 'x-jet-forwarding-policies: 11111111111111111111111111111111' \
  -H 'x-jet-response: signature' \
  --data-binary @transaction.bin

# Base58 (default encoding), return signature
curl -X POST /api/v1/transactions \
  -H 'x-jet-response: signature' \
  -d '<base58-encoded-tx>'

# Base64 with signature
curl -X POST /api/v1/transactions \
  -H 'x-jet-encoding: base64' \
  -H 'x-jet-response: signature' \
  -d '<base64-encoded-tx>'
```

### Response

- **Success**: `200 OK` — empty body by default, or transaction signature if `x-jet-response: signature`
- **Error**: `400 Bad Request` — plain text error message
- **Wrong method**: `405 Method Not Allowed`

## Attribution

Created by the greybeards at [Triton One](https://triton.one)

Copyright (C) 2024 Triton One Ltd

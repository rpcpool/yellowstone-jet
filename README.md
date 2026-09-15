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

### Query parameters

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `encoding` | `base58`, `base64` | `base58` | Encoding of the text body (ignored for raw bytes) |
| `max_retries` | integer | unset | Maximum retry attempts |
| `response` | `signature`, `none` | `none` | What to return on success |

### Optional headers

These headers provide per-request overrides for retry and policy behavior.

| Header | Values | Description |
|--------|--------|-------------|
| `solana-forwardingpolicies` | comma-separated pubkeys | Restrict forwarding to leaders allowed by these policies |

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

# Raw bytes with retry in query and policy override in header, return signature
curl -X POST '/api/v1/transactions?response=signature&max_retries=3' \
  -H 'Content-Type: application/octet-stream' \
  -H 'solana-forwardingpolicies: 11111111111111111111111111111111' \
  --data-binary @transaction.bin

# Base58 (default encoding), return signature
curl -X POST '/api/v1/transactions?response=signature' \
  -d '<base58-encoded-tx>'

# Base64 with signature
curl -X POST '/api/v1/transactions?encoding=base64&response=signature' \
  -d '<base64-encoded-tx>'
```

### Response

- **Success**: `200 OK` — empty body by default, or transaction signature if `?response=signature`
- **Error**: `400 Bad Request` — plain text error message
- **Wrong method**: `405 Method Not Allowed`

## Attribution

Created by the greybeards at [Triton One](https://triton.one)

Copyright (C) 2024 Triton One Ltd

## TPU forwarding health metric

`jet_tpu_forwarding_stalled` is an unlabelled gauge with one series per Jet. It is 1 when the TPU forwarding stall condition holds and 0 otherwise. Each metrics scrape evaluates the activity window, so the gauge updates even when no new callbacks arrive.

The condition uses the rc6 logic: no successful sends in the trailing 300 buckets of 100 ms, at least one qualifying failure, and failures in at least one fifth of the buckets. A single burst does not satisfy the spread requirement. Startup has no additional grace period. A successful send or expiring failure evidence can clear the condition. Idle expiry allows traffic to retry; it does not prove recovery.

Success counts TPU `TxSent` callbacks. Failure counts other callbacks except drops for remote peer unreachable, invalid packet size, or driver identity changes. The timer wheel and callback classification are unchanged. These measure send outcomes, not transaction landing.

`/health` ignores the forwarding stall condition. It still checks the allowed identity and initialization of gRPC, cluster nodes, and the leader schedule. The HAProxy agent can stop traffic when `jet_tpu_forwarding_stalled == 1` and restore the normal weight decision when it returns to 0.

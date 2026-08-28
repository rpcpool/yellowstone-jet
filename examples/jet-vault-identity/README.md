# jet-vault-identity

Code sample for jet ingesting a **single-use, response-wrapped Vault token**
(provided by its Nomad job) and unwrapping it itself to obtain its identity
keypair, in memory only.

With [Vault response wrapping](https://developer.hashicorp.com/vault/docs/concepts/response-wrapping)
the keypair is never present in the Nomad job spec, template output, or
environment. The task only receives a *wrapping token* — a reference to a
single-use cubbyhole holding the real secret:

- **Single use**: `sys/wrapping/unwrap` succeeds exactly once. If jet's own
  unwrap fails, the token expired or someone else already unwrapped it — a
  built-in interception alarm; rotate the identity.
- **Short-lived**: the `wrap_ttl` bounds the exposure window.
- **Tamper-evident**: before unwrapping, `sys/wrapping/lookup` (which does
  not consume the token) exposes the token's `creation_path`, so jet can
  refuse a token that was not minted by the expected Vault endpoint.

The whole flow lives in `load_identity_from_wrapped_token` in
[`src/main.rs`](src/main.rs), built on the [`vaultrs`](https://docs.rs/vaultrs)
crate, which ships the wrapping endpoints out of the box
(`vaultrs::sys::wrapping::{lookup, unwrap}`): read the wrapping token from the
environment variable the Nomad job rendered, verify its `creation_path`, unwrap it
(authenticated with the wrapping token itself — jet needs no other Vault
credentials), and parse the `keypair` field into a `solana_keypair::Keypair`. In jet proper that keypair
would become `initial_identity` for `JetIdentitySyncGroup`.

## Producer side (sample)

Store the identity and mint a wrapped read of it:

```bash
vault kv put kv/jet/identity keypair=@/path/to/jet-identity.json
vault kv get -wrap-ttl=300s -field=wrapping_token kv/jet/identity
```

In Nomad, a template stanza on the jet task can export only the wrapping token
into the task environment (sketch, not a full job spec):

```hcl
template {
  destination = "${NOMAD_SECRETS_DIR}/identity.env"
  env         = true
  change_mode = "restart"
  data        = "VAULT_WRAPPED_TOKEN={{ with secret \"kv/data/jet/identity\" \"wrap_ttl=5m\" }}{{ .WrapInfo.Token }}{{ end }}"
}
```

Because the token is single-use, every task (re)start needs a freshly minted
wrapping token — `change_mode = "restart"` takes care of that.

The token lives only in the task environment, so it is never written to a
volume — but it does remain readable through `/proc/self/environ` for the
life of the process. Scrubbing it after the unwrap costs an `unsafe` block in
edition 2024 and is only sound before any thread starts, so do it at the very
top of `main` if that matters for your threat model.

## Running the sample

```bash
export VAULT_ADDR=https://vault.service.consul:8200
export VAULT_WRAPPED_TOKEN="$(vault kv get -wrap-ttl=300s -field=wrapping_token kv/jet/identity)"
export VAULT_EXPECTED_CREATION_PATH=kv/data/jet/identity   # optional tamper check
cargo run --bin jet-vault-identity
# jet identity unwrapped: <pubkey>
```

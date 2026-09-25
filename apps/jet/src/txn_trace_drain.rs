use {
    crate::{cluster_tpu_info::ClusterTpuInfo, transactions::JetTxnInfo},
    futures::{Stream, StreamExt},
    serde::Serialize,
    solana_pubkey::Pubkey,
    std::{borrow::Cow, net::SocketAddr, sync::Arc},
    uuid::Uuid,
    yellowstone_jet_tpu_client::core::TpuSenderResponse,
};

pub trait SolanaClientResolver {
    fn get_solana_client(&self, peer_pubkey: &Pubkey) -> Option<String>;
}

impl SolanaClientResolver for ClusterTpuInfo {
    fn get_solana_client(&self, peer_pubkey: &Pubkey) -> Option<String> {
        self.get_solana_client_for_peer(peer_pubkey)
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TxnState {
    Sent,
    Failed,
    Drop,
}

#[derive(Debug, Serialize)]
pub struct TxnTraceEntry<'a> {
    pub signature: Cow<'a, str>,
    pub send_at_slot: u64,
    pub x_request_id: Option<Uuid>,
    pub x_subscription_id: Option<Uuid>,
    pub state: TxnState,
    pub error_msg: Option<&'a str>,
    pub remote_peer_solana_client_id: Option<Cow<'a, str>>,
    pub remote_peer_identity: Option<Cow<'a, str>>,
    pub remote_peer_addr: Option<SocketAddr>,
    pub drop_reason: Option<&'a str>,
    pub drain_id: Option<Cow<'a, str>>,
    pub signer: Option<Cow<'a, str>>,
}

enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

struct OneOrManyIter<'a, T> {
    inner: &'a OneOrMany<T>,
    index: usize,
}

impl<'a, T> Iterator for OneOrManyIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.inner {
            OneOrMany::One(item) => {
                if self.index == 0 {
                    self.index += 1;
                    Some(item)
                } else {
                    None
                }
            }
            OneOrMany::Many(items) => {
                if self.index < items.len() {
                    let item = &items[self.index];
                    self.index += 1;
                    Some(item)
                } else {
                    None
                }
            }
        }
    }
}

impl<T> OneOrMany<T> {
    const fn iter(&self) -> OneOrManyIter<'_, T> {
        OneOrManyIter {
            inner: self,
            index: 0,
        }
    }

    fn map<A>(self, f: impl Fn(T) -> A) -> OneOrMany<A> {
        match self {
            OneOrMany::One(item) => OneOrMany::One(f(item)),
            OneOrMany::Many(items) => OneOrMany::Many(items.into_iter().map(f).collect()),
        }
    }
}

impl<'a, T> IntoIterator for &'a OneOrMany<T> {
    type Item = &'a T;
    type IntoIter = OneOrManyIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

enum OneOrManyIterator<T> {
    One(Option<T>),
    Many(std::vec::IntoIter<T>),
}

impl<T> IntoIterator for OneOrMany<T> {
    type Item = T;
    type IntoIter = OneOrManyIterator<T>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            OneOrMany::One(item) => OneOrManyIterator::One(Some(item)),
            OneOrMany::Many(items) => OneOrManyIterator::Many(items.into_iter()),
        }
    }
}

impl<T> Iterator for OneOrManyIterator<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            OneOrManyIterator::One(opt) => opt.take(),
            OneOrManyIterator::Many(iter) => iter.next(),
        }
    }
}

fn get_txn_info(txn_response: &TpuSenderResponse) -> Option<OneOrMany<&JetTxnInfo>> {
    match txn_response {
        TpuSenderResponse::TxSent(tx_sent) => tx_sent
            .info
            .as_ref()
            .and_then(|info| info.downcast_ref::<JetTxnInfo>())
            .map(OneOrMany::One),
        TpuSenderResponse::TxFailed(tx_failed) => tx_failed
            .info
            .as_ref()
            .and_then(|info| info.downcast_ref::<JetTxnInfo>())
            .map(OneOrMany::One),
        TpuSenderResponse::TxDrop(tx_drop) => {
            let many = tx_drop
                .dropped_tx_vec
                .iter()
                .filter_map(|(dropped_tx, _attempt)| {
                    dropped_tx
                        .info
                        .as_ref()
                        .and_then(|info| info.downcast_ref::<JetTxnInfo>())
                })
                .collect();
            Some(OneOrMany::Many(many))
        }
    }
}

///
/// Builds trace entries for `txn_response`, borrowing from it (and from `remote_peer_solana_client_id`
/// / `drain_id`) rather than allocating new owned strings wherever a borrow will do.
///
/// `remote_peer_solana_client_id` is a plain `Option<&str>` rather than a `&dyn
/// SolanaClientResolver` -- the resolver is only ever consulted once per response (all entries
/// derived from one response share the same remote peer, hence the same resolved id), so the
/// caller resolves it once, up front, and this function just borrows the result. That also makes
/// this field truly zero-copy (`Cow::Borrowed`) instead of unconditionally cloning into an owned
/// `String` on every entry the way going through the resolver here would.
///
fn into_txn_trace_entry<'callsite, 'resp, 'info>(
    txn_response: &'resp TpuSenderResponse,
    one_or_many: OneOrMany<&'info JetTxnInfo>,
    remote_peer_solana_client_id: Option<&'callsite str>,
    drain_id: Option<&'callsite str>,
) -> OneOrMany<TxnTraceEntry<'info>>
where
    'resp: 'info,
    'callsite: 'info,
{
    match txn_response {
        TpuSenderResponse::TxSent(tx_sent) => one_or_many.map(|info| TxnTraceEntry {
            signature: Cow::Owned(info.signature.to_string()),
            send_at_slot: info.send_at_slot,
            x_request_id: info.x_request_id,
            state: TxnState::Sent,
            error_msg: None,
            remote_peer_solana_client_id: remote_peer_solana_client_id.map(Cow::Borrowed),
            remote_peer_identity: Some(Cow::Owned(tx_sent.remote_peer_identity.to_string())),
            remote_peer_addr: Some(tx_sent.remote_peer_addr),
            drop_reason: None,
            drain_id: drain_id.map(Cow::Borrowed),
            x_subscription_id: info.x_subscription_id,
            signer: Some(Cow::Owned(info.signer.to_string())),
        }),
        TpuSenderResponse::TxFailed(tx_failed) => one_or_many.map(|info| TxnTraceEntry {
            signature: Cow::Owned(info.signature.to_string()),
            send_at_slot: info.send_at_slot,
            x_request_id: info.x_request_id,
            state: TxnState::Failed,
            error_msg: Some(&tx_failed.failure_reason),
            remote_peer_solana_client_id: remote_peer_solana_client_id.map(Cow::Borrowed),
            remote_peer_identity: Some(Cow::Owned(tx_failed.remote_peer_identity.to_string())),
            remote_peer_addr: Some(tx_failed.remote_peer_addr),
            drop_reason: None,
            drain_id: drain_id.map(Cow::Borrowed),
            x_subscription_id: info.x_subscription_id,
            signer: Some(Cow::Owned(info.signer.to_string())),
        }),
        TpuSenderResponse::TxDrop(tx_drop) => {
            let many = tx_drop
                .dropped_tx_vec
                .iter()
                .zip(one_or_many)
                .map(|((_dropped_tx, _attempt), info)| TxnTraceEntry {
                    signature: Cow::Owned(info.signature.to_string()),
                    send_at_slot: info.send_at_slot,
                    x_request_id: info.x_request_id,
                    state: TxnState::Drop,
                    error_msg: None,
                    remote_peer_solana_client_id: remote_peer_solana_client_id.map(Cow::Borrowed),
                    remote_peer_identity: Some(Cow::Owned(
                        tx_drop.remote_peer_identity.to_string(),
                    )),
                    remote_peer_addr: None,
                    drop_reason: Some(tx_drop.drop_reason.as_str()),
                    drain_id: drain_id.map(Cow::Borrowed),
                    x_subscription_id: info.x_subscription_id,
                    signer: Some(Cow::Owned(info.signer.to_string())),
                })
                .collect::<Vec<_>>();
            OneOrMany::Many(many)
        }
    }
}

/// The single `Pubkey` a `TpuSenderResponse` carries regardless of variant -- used to resolve
/// `remote_peer_solana_client_id` exactly once per response (see [`into_txn_trace_entry`]).
const fn remote_peer_identity(txn_response: &TpuSenderResponse) -> &Pubkey {
    match txn_response {
        TpuSenderResponse::TxSent(r) => &r.remote_peer_identity,
        TpuSenderResponse::TxFailed(r) => &r.remote_peer_identity,
        TpuSenderResponse::TxDrop(r) => &r.remote_peer_identity,
    }
}

///
/// Adapts a `Stream<Item = TpuSenderResponse>` into a `Stream<Item = SerializeIntoTxnTrace>` --
/// each yielded [`SerializeIntoTxnTrace`] owns one response and everything needed to derive
/// [`TxnTraceEntry`]s from it, without needing `solana_client_resolver` or `drain_id` again
/// (see [`SerializeIntoTxnTrace::new`]).
///
pub struct TxnTraceShaper<St, SCR> {
    source: St,
    solana_client_resolver: SCR,
    drain_id: Option<Arc<str>>,
}

impl<St, SCR> TxnTraceShaper<St, SCR> {
    pub fn new(source: St, solana_client_resolver: SCR, drain_id: Option<String>) -> Self {
        let drain_id = drain_id.map(Arc::from);
        Self {
            source,
            solana_client_resolver,
            drain_id,
        }
    }
}

impl<St, SCR> Stream for TxnTraceShaper<St, SCR>
where
    St: Stream<Item = TpuSenderResponse> + Unpin,
    SCR: SolanaClientResolver + Unpin,
{
    type Item = SerializeIntoTxnTrace;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.source.poll_next_unpin(cx).map(|maybe_response| {
            maybe_response.map(|response| {
                SerializeIntoTxnTrace::new(
                    response,
                    &this.solana_client_resolver,
                    this.drain_id.clone(),
                )
            })
        })
    }
}

///
/// Owns one [`TpuSenderResponse`] plus everything [`into_txn_trace_entry`] needs besides the
/// response itself, so it doesn't need to keep the original `&dyn SolanaClientResolver` around:
/// the resolver is only ever consulted once, right here in [`SerializeIntoTxnTrace::new`] (every
/// entry derived from one response shares the same remote peer, hence the same resolved id).
///
/// This owning-wrapper-plus-borrow-later shape is what lets [`TxnTraceEntry`] stay a borrowed,
/// zero-copy-where-possible type despite `Stream::Item` needing to be an ordinary, fixed
/// (non-lending) associated type: `Stream::poll_next` can't hand back something that borrows
/// from data it only just produced, but it *can* hand back something that *owns* that data --
/// entries are then borrowed from `&self` afterwards, on demand, via
/// `impl IntoIterator for &SerializeIntoTxnTrace` below, exactly like `&Vec<T>: IntoIterator<Item
/// = &T>` borrows from a `Vec` it doesn't consume.
///
pub struct SerializeIntoTxnTrace {
    item: TpuSenderResponse,
    remote_peer_solana_client_id: Option<String>,
    drain_id: Option<Arc<str>>,
}

impl SerializeIntoTxnTrace {
    fn new(
        item: TpuSenderResponse,
        solana_client_resolver: &dyn SolanaClientResolver,
        drain_id: Option<Arc<str>>,
    ) -> Self {
        let remote_peer_solana_client_id =
            solana_client_resolver.get_solana_client(remote_peer_identity(&item));
        Self {
            item,
            remote_peer_solana_client_id,
            drain_id,
        }
    }
}

/// Yields the (possibly zero, for an untraceable response) [`TxnTraceEntry`]s borrowed from one
/// [`SerializeIntoTxnTrace`] -- a thin wrapper around [`OneOrManyIterator`], which already knows
/// how to walk a [`OneOrMany`] by value without ever needing a `Vec` for the common single-entry
/// case.
pub struct TxnTraceIntoIter<'resp> {
    inner: OneOrManyIterator<TxnTraceEntry<'resp>>,
}

impl<'resp> Iterator for TxnTraceIntoIter<'resp> {
    type Item = TxnTraceEntry<'resp>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<'a> IntoIterator for &'a SerializeIntoTxnTrace {
    type Item = TxnTraceEntry<'a>;
    type IntoIter = TxnTraceIntoIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        // `get_txn_info` returning `None` means the response is untraceable (no attached
        // `JetTxnInfo`) -- normalized to an empty `Many` here so `TxnTraceIntoIter` can stay a
        // plain, non-`Option`-wrapped iterator over whatever `OneOrMany` produces.
        let one_or_many = get_txn_info(&self.item)
            .map(|infos| {
                into_txn_trace_entry(
                    &self.item,
                    infos,
                    self.remote_peer_solana_client_id.as_deref(),
                    self.drain_id.as_deref(),
                )
            })
            .unwrap_or(OneOrMany::Many(Vec::new()));
        TxnTraceIntoIter {
            inner: one_or_many.into_iter(),
        }
    }
}
#[cfg(test)]
mod tests {
    use {
        super::*,
        bytes::Bytes,
        futures::stream,
        solana_keypair::Signature,
        solana_pubkey::Pubkey,
        std::{
            collections::HashMap,
            task::{Context, Waker},
        },
        yellowstone_jet_tpu_client::core::{
            TpuSenderTxn, TpuSenderTxnInfo, TxDrop, TxDropReason, TxFailed, TxSent,
        },
    };

    #[derive(Default)]
    struct MockSolanaClientResolver {
        peer_to_client: HashMap<Pubkey, String>,
    }

    impl SolanaClientResolver for MockSolanaClientResolver {
        fn get_solana_client(&self, peer_pubkey: &Pubkey) -> Option<String> {
            self.peer_to_client.get(peer_pubkey).cloned()
        }
    }

    fn addr() -> SocketAddr {
        "127.0.0.1:8001".parse().unwrap()
    }

    fn info(
        signature: Signature,
        x_request_id: Option<Uuid>,
        x_subscription_id: Option<Uuid>,
        signer: Pubkey,
    ) -> TpuSenderTxnInfo {
        TpuSenderTxnInfo::new(JetTxnInfo {
            signature,
            send_at_slot: 1,
            x_request_id,
            x_subscription_id,
            signer,
        })
    }

    fn tx_sent(with_info: bool) -> (TpuSenderResponse, Signature) {
        let sig = Signature::new_unique();
        let signer = Pubkey::new_unique();
        let response = TpuSenderResponse::TxSent(TxSent {
            remote_peer_identity: Pubkey::new_unique(),
            remote_peer_addr: addr(),
            info: with_info.then(|| info(sig, Some(Uuid::new_v4()), Some(Uuid::new_v4()), signer)),
        });
        (response, sig)
    }

    fn tx_failed(with_info: bool) -> (TpuSenderResponse, Signature) {
        let sig = Signature::new_unique();
        let signer = Pubkey::new_unique();
        let response = TpuSenderResponse::TxFailed(TxFailed {
            remote_peer_identity: Pubkey::new_unique(),
            remote_peer_addr: addr(),
            failure_reason: "connection reset".to_string(),
            info: with_info.then(|| info(sig, None, None, signer)),
        });
        (response, sig)
    }

    /// Builds a `TxDrop` response whose `dropped_tx_vec` carries one entry per element of
    /// `infos`: `Some(sig)` attaches trace info to that entry, `None` leaves it untraceable.
    fn tx_drop(infos: Vec<Option<Signature>>) -> TpuSenderResponse {
        let signer = Pubkey::new_unique();
        let dropped_tx_vec = infos
            .into_iter()
            .map(|maybe_sig| {
                let txn_info = maybe_sig.map(|sig| info(sig, None, None, signer));
                (
                    TpuSenderTxn::from_bytes(
                        Pubkey::new_unique(),
                        Bytes::from_static(b"wire"),
                        txn_info,
                    ),
                    0usize,
                )
            })
            .collect();
        TpuSenderResponse::TxDrop(TxDrop {
            remote_peer_identity: Pubkey::new_unique(),
            drop_reason: TxDropReason::RateLimited,
            dropped_tx_vec,
        })
    }

    fn collect_signatures(one_or_many: &OneOrMany<&JetTxnInfo>) -> Vec<Signature> {
        one_or_many.iter().map(|info| info.signature).collect()
    }

    #[test]
    fn get_txn_info_extracts_info_from_tx_sent() {
        let (response, sig) = tx_sent(true);
        let extracted = get_txn_info(&response).expect("info should be present");
        assert_eq!(collect_signatures(&extracted), vec![sig]);
    }

    #[test]
    fn get_txn_info_returns_none_when_tx_sent_has_no_info() {
        let (response, _sig) = tx_sent(false);
        assert!(get_txn_info(&response).is_none());
    }

    #[test]
    fn get_txn_info_extracts_info_from_tx_failed() {
        let (response, sig) = tx_failed(true);
        let extracted = get_txn_info(&response).expect("info should be present");
        assert_eq!(collect_signatures(&extracted), vec![sig]);
    }

    #[test]
    fn get_txn_info_filters_dropped_entries_without_info() {
        let sig = Signature::new_unique();
        let response = tx_drop(vec![Some(sig), None]);
        let extracted = get_txn_info(&response).expect("TxDrop always returns Some(..)");
        assert_eq!(collect_signatures(&extracted), vec![sig]);
    }

    #[test]
    fn get_txn_info_returns_empty_many_when_all_dropped_entries_lack_info() {
        let response = tx_drop(vec![None, None]);
        let extracted = get_txn_info(&response).expect("TxDrop always returns Some(..)");
        assert!(collect_signatures(&extracted).is_empty());
    }

    #[test]
    fn into_txn_trace_entry_maps_tx_sent() {
        let (response, sig) = tx_sent(true);
        let infos = get_txn_info(&response).unwrap();
        let result = into_txn_trace_entry(&response, infos, Some("solana-client"), None);
        let entries: Vec<_> = result.iter().collect();

        assert_eq!(entries.len(), 1);
        let entry = entries[0];
        assert_eq!(entry.signature, sig.to_string());
        assert!(matches!(entry.state, TxnState::Sent));
        assert!(entry.error_msg.is_none());
        assert_eq!(
            entry.remote_peer_solana_client_id.as_deref(),
            Some("solana-client")
        );
        assert!(entry.remote_peer_identity.is_some());
        assert!(entry.remote_peer_addr.is_some());
        assert!(entry.drop_reason.is_none());
    }

    #[test]
    fn into_txn_trace_entry_maps_tx_failed() {
        let (response, sig) = tx_failed(true);
        let infos = get_txn_info(&response).unwrap();
        let result = into_txn_trace_entry(&response, infos, None, None);
        let entries: Vec<_> = result.iter().collect();

        assert_eq!(entries.len(), 1);
        let entry = entries[0];
        assert_eq!(entry.signature, sig.to_string());
        assert!(matches!(entry.state, TxnState::Failed));
        assert_eq!(entry.error_msg, Some("connection reset"));
        assert!(entry.remote_peer_addr.is_some());
    }

    fn poll_cx() -> Context<'static> {
        Context::from_waker(Waker::noop())
    }

    #[test]
    fn serialize_into_txn_trace_resolves_the_client_id_once_and_borrows_it_per_entry() {
        let (response, sig) = tx_sent(true);
        let remote_peer_identity = *remote_peer_identity(&response);
        let mut resolver = MockSolanaClientResolver::default();
        resolver
            .peer_to_client
            .insert(remote_peer_identity, "resolved-client".to_string());

        let batch = SerializeIntoTxnTrace::new(response, &resolver, None);
        let entries: Vec<_> = (&batch).into_iter().collect();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].signature, sig.to_string());
        assert_eq!(
            entries[0].remote_peer_solana_client_id.as_deref(),
            Some("resolved-client")
        );
    }

    #[test]
    fn serialize_into_txn_trace_yields_nothing_for_an_untraceable_response() {
        let (response, _sig) = tx_sent(false);
        let resolver = MockSolanaClientResolver::default();

        let batch = SerializeIntoTxnTrace::new(response, &resolver, None);
        let entries: Vec<_> = (&batch).into_iter().collect();

        assert!(entries.is_empty());
    }

    #[test]
    fn serialize_into_txn_trace_batches_a_tx_drop_into_multiple_entries() {
        let sig1 = Signature::new_unique();
        let sig2 = Signature::new_unique();
        let response = tx_drop(vec![Some(sig1), None, Some(sig2)]);
        let resolver = MockSolanaClientResolver::default();

        let batch = SerializeIntoTxnTrace::new(response, &resolver, None);
        let signatures: Vec<_> = (&batch).into_iter().map(|e| e.signature).collect();

        assert_eq!(signatures, vec![sig1.to_string(), sig2.to_string()]);
    }

    #[test]
    fn serialize_into_txn_trace_stamps_the_configured_drain_id() {
        let (response, _sig) = tx_sent(true);
        let resolver = MockSolanaClientResolver::default();

        let batch = SerializeIntoTxnTrace::new(response, &resolver, Some(Arc::from("jet-1")));
        let entries: Vec<_> = (&batch).into_iter().collect();

        assert_eq!(entries[0].drain_id.as_deref(), Some("jet-1"));
    }

    #[test]
    fn txn_trace_shaper_yields_one_batch_per_response() {
        let (first, sig1) = tx_sent(true);
        let (second, sig2) = tx_failed(true);
        let mut shaper = TxnTraceShaper::new(
            stream::iter(vec![first, second]),
            MockSolanaClientResolver::default(),
            None,
        );

        let mut cx = poll_cx();
        let std::task::Poll::Ready(Some(first_batch)) =
            std::pin::Pin::new(&mut shaper).poll_next(&mut cx)
        else {
            panic!("expected first batch");
        };
        assert_eq!(
            (&first_batch).into_iter().next().unwrap().signature,
            sig1.to_string()
        );

        let std::task::Poll::Ready(Some(second_batch)) =
            std::pin::Pin::new(&mut shaper).poll_next(&mut cx)
        else {
            panic!("expected second batch");
        };
        assert_eq!(
            (&second_batch).into_iter().next().unwrap().signature,
            sig2.to_string()
        );

        assert!(matches!(
            std::pin::Pin::new(&mut shaper).poll_next(&mut cx),
            std::task::Poll::Ready(None)
        ));
    }
}

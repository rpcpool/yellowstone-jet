use {
    futures::future::Either,
    serde::Deserialize,
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_signer::{Signer, SignerError},
    std::{cmp::Ordering, future::Future, sync::Arc},
    tokio::{
        sync::{Mutex, oneshot, watch},
        task::{JoinError, JoinHandle},
        time::{Duration, sleep},
    },
    tonic::{Request, Status, metadata::AsciiMetadataValue},
};

pub mod prom;

/// Creates an x-token interceptor function for gRPC authentication
///
/// This returns a closure that implements the `Interceptor` trait,
/// which can be used with any tonic-generated client's `with_interceptor` method.
/// If x_token is None, returns a no-op interceptor.
pub fn create_x_token_interceptor(
    x_token: Option<String>,
) -> impl tonic::service::Interceptor + Clone {
    let token_value = x_token.and_then(|token| AsciiMetadataValue::try_from(token).ok());

    move |mut request: Request<()>| -> Result<Request<()>, Status> {
        if let Some(ref token) = token_value {
            request.metadata_mut().insert("x-token", token.clone());
        }
        Ok(request)
    }
}

pub type BlockHeight = u64;

// Maitaining CommitmentLevel for compatibility.
// + Because is not the right thing to say that the commitmment of a block is a status of the slot.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CommitmentLevel {
    Processed,
    Confirmed,
    #[default]
    Finalized,
}

impl CommitmentLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Processed => "processed",
            Self::Confirmed => "confirmed",
            Self::Finalized => "finalized",
        }
    }

    const fn as_u8(self) -> u8 {
        match self {
            Self::Processed => 0,
            Self::Confirmed => 1,
            Self::Finalized => 2,
        }
    }
}

impl Ord for CommitmentLevel {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_u8().cmp(&other.as_u8())
    }
}

impl PartialOrd for CommitmentLevel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// SlotStatus is used to represent the status of a slot in the gRPC API.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SlotStatus {
    SlotProcessed,
    SlotConfirmed,
    SlotFinalized,
    #[default]
    SlotFirstShredReceived,
    SlotCompleted,
    SlotCreatedBank,
    SlotDead,
}

impl SlotStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SlotProcessed => "processed",
            Self::SlotConfirmed => "confirmed",
            Self::SlotFinalized => "finalized",
            Self::SlotFirstShredReceived => "first_shred_received",
            Self::SlotCompleted => "completed",
            Self::SlotCreatedBank => "created_bank",
            Self::SlotDead => "dead",
        }
    }
}

impl From<i32> for SlotStatus {
    fn from(status: i32) -> Self {
        match status {
            0 => Self::SlotProcessed,
            1 => Self::SlotConfirmed,
            2 => Self::SlotFinalized,
            3 => Self::SlotFirstShredReceived,
            4 => Self::SlotCompleted,
            5 => Self::SlotCreatedBank,
            6 => Self::SlotDead,
            _ => Self::default(),
        }
    }
}

#[derive(Debug)]
pub struct IncrementalBackoff {
    initial_interval: Duration,
    max_interval: Duration,
    retries: Option<u32>,
}

impl Default for IncrementalBackoff {
    fn default() -> Self {
        Self::new(Duration::from_millis(250), Duration::from_millis(3_000))
    }
}

impl IncrementalBackoff {
    pub const fn new(initial_interval: Duration, max_interval: Duration) -> Self {
        Self {
            initial_interval,
            max_interval,
            retries: None,
        }
    }

    pub const fn reset(&mut self) {
        self.retries = None;
    }

    pub fn init(&mut self) {
        self.retries = self.retries.or(Some(0));
    }

    pub async fn maybe_tick(&mut self) {
        if let Some(retries) = self.retries {
            let next_delay = self.initial_interval * 2u32.pow(retries);
            sleep(self.max_interval.min(next_delay)).await;

            self.retries = Some(retries.checked_add(1).unwrap_or(u32::MAX));
        }
    }
}

pub type WaitShutdownJoinHandle = JoinHandle<anyhow::Result<()>>;
pub type WaitShutdownSharedJoinHandle = Arc<Mutex<WaitShutdownJoinHandle>>;
pub type WaitShutdownJoinHandleResult = Result<anyhow::Result<()>, JoinError>;

pub trait WaitShutdown: Sized {
    fn shutdown(&self);

    fn wait_shutdown_future(self) -> impl Future<Output = WaitShutdownJoinHandleResult>;

    fn wait_shutdown(self) -> impl Future<Output = anyhow::Result<()>> {
        async move {
            match self.wait_shutdown_future().await {
                Ok(result) => result,
                Err(error) => anyhow::bail!("failed to join task: {error:?}"),
            }
        }
    }

    fn spawn<F>(future: F) -> WaitShutdownSharedJoinHandle
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        Arc::new(Mutex::new(tokio::spawn(future)))
    }
}

///
/// A Pubkey that can sign.
///
/// This struct wraps a Keypair and implements the Signer trait.
/// It doesn't expose the private key.
pub struct PubkeySigner(Keypair);

impl Clone for PubkeySigner {
    fn clone(&self) -> Self {
        Self(self.0.insecure_clone())
    }
}

impl PartialEq for PubkeySigner {
    fn eq(&self, other: &Self) -> bool {
        self.0.pubkey() == other.0.pubkey()
    }
}

impl PubkeySigner {
    pub const fn new(keypair: Keypair) -> Self {
        Self(keypair)
    }

    pub fn pubkey(&self) -> Pubkey {
        self.0.pubkey()
    }
}

impl Signer for PubkeySigner {
    fn sign_message(&self, message: &[u8]) -> Signature {
        self.0.sign_message(message)
    }

    fn try_pubkey(&self) -> Result<Pubkey, SignerError> {
        self.0.try_pubkey()
    }

    fn try_sign_message(&self, message: &[u8]) -> Result<Signature, SignerError> {
        self.0.try_sign_message(message)
    }

    fn is_interactive(&self) -> bool {
        self.0.is_interactive()
    }
}

pub fn ms_since_epoch() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("failed to get system time")
        .as_millis() as u64
}

#[derive(Clone)]
pub struct ValueObserver<T> {
    last_val: T,
    rx: watch::Receiver<T>,
}

impl<T: Clone> From<watch::Receiver<T>> for ValueObserver<T> {
    fn from(mut val: watch::Receiver<T>) -> ValueObserver<T> {
        let x = val.borrow_and_update().clone();

        ValueObserver {
            last_val: x,
            rx: val,
        }
    }
}

impl<T: Clone + PartialEq> ValueObserver<T> {
    ///
    /// Get the current identity.
    pub fn get_current(&self) -> T {
        self.last_val.clone()
    }

    ///
    /// Wait for the identity to change and return the new identity.
    pub async fn observe(&mut self) -> T {
        let last_val = self.last_val.clone();
        let new_val = self
            .rx
            .wait_for(|new_val| new_val != &last_val)
            .await
            .expect("sender dropped")
            .clone();
        self.last_val = new_val;
        self.get_current()
    }

    pub async fn until_value_change<F, Fut, O>(&mut self, f: F) -> Either<T, O>
    where
        F: FnOnce(T) -> Fut,
        Fut: Future<Output = O>,
    {
        let current = self.get_current();
        tokio::select! {
            new_val = self.observe() => Either::Left(new_val),
            output = f(current) => Either::Right(output),
        }
    }
}

///
/// Fork a oneshot receiver into two receivers.
///
pub fn fork_oneshot<T>(rx: oneshot::Receiver<T>) -> (oneshot::Receiver<T>, oneshot::Receiver<T>)
where
    T: Clone + Send + 'static,
{
    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    tokio::spawn(async move {
        let x = match rx.await {
            Ok(x) => x,
            Err(_) => return,
        };
        let _ = tx1.send(x.clone());
        let _ = tx2.send(x.clone());
    });
    (rx1, rx2)
}

#[cfg(test)]
mod tests {
    use {super::*, futures::future};

    #[test]
    fn commitment_level_cmp() {
        use CommitmentLevel::*;

        assert!(Processed <= Processed);
        assert!(Processed >= Processed);
        assert!(Confirmed > Processed);
        assert!(Confirmed >= Processed);
        assert!(Finalized > Processed);
        assert!(Finalized >= Processed);

        assert!(Finalized > Confirmed);
        assert!(Finalized >= Confirmed);
    }

    #[tokio::test]
    pub async fn value_observer_should_return_right_when_fut_finish_first() {
        let (tx, rx) = watch::channel(0);
        let mut observer: ValueObserver<i32> = rx.into();

        // Test when custom future finished first
        let result = observer.until_value_change(|_| future::ready(0)).await;
        assert!(matches!(result, Either::Right(0)));

        // Test when value changed first
        let fut = tokio::spawn(async move {
            observer
                .until_value_change(|_| future::pending::<()>())
                .await
        });

        tx.send(1).unwrap();
        let result = fut.await.unwrap();
        assert!(matches!(result, Either::Left(1)));
    }

    #[tokio::test]
    pub async fn value_observer_should_return_left_when_inner_value_change_first() {
        let (tx, rx) = watch::channel(0);
        let mut observer: ValueObserver<i32> = rx.into();

        // Test when value changed first
        let fut = tokio::spawn(async move {
            observer
                .until_value_change(|_| future::pending::<()>())
                .await
        });

        tx.send(1).unwrap();
        let result = fut.await.unwrap();
        assert!(matches!(result, Either::Left(1)));
    }

    #[tokio::test]
    pub async fn value_observer_until_value_change_should_ignore_unchanged_value() {
        let (tx, rx) = watch::channel(0);
        let mut observer: ValueObserver<i32> = rx.into();
        // Test when value changed first
        let fut = tokio::spawn(async move {
            observer
                .until_value_change(|_| future::pending::<()>())
                .await
        });
        // The first value is 0, so it should be ignored
        tx.send(0).unwrap();
        tx.send_replace(10);
        let result = fut.await.unwrap();
        // If the first value has been ignored, the result should be 10
        assert!(matches!(result, Either::Left(10)));
    }

    #[tokio::test]
    pub async fn value_observer_until_value_change_should_error_when_sender_close() {
        let (tx, rx) = watch::channel(0);
        let mut observer: ValueObserver<i32> = rx.into();

        // Test when value changed first
        let fut = tokio::spawn(async move {
            observer
                .until_value_change(|_| future::pending::<()>())
                .await
        });
        // The first value is 0, so it should be ignored
        drop(tx);
        let result = fut.await;
        assert!(result.is_err());
    }
}

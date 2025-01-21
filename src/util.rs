use {
    futures::future::Either,
    serde::Deserialize,
    solana_sdk::{
        pubkey::Pubkey,
        signature::{Keypair, Signature},
        signer::Signer,
    },
    std::{cmp::Ordering, future::Future, sync::Arc},
    tokio::{
        sync::{watch, Mutex},
        task::{JoinError, JoinHandle},
        time::{sleep, Duration},
    },
};

pub type BlockHeight = u64;

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

    pub fn reset(&mut self) {
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

    fn try_pubkey(&self) -> Result<solana_sdk::pubkey::Pubkey, solana_sdk::signer::SignerError> {
        self.0.try_pubkey()
    }

    fn try_sign_message(
        &self,
        message: &[u8],
    ) -> Result<Signature, solana_sdk::signer::SignerError> {
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

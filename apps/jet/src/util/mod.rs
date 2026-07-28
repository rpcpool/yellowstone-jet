use {
    serde::Deserialize,
    std::{cmp::Ordering, future::Future, sync::Arc},
    tokio::{
        sync::Mutex,
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
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Hash)]
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

            self.retries = Some(retries.saturating_add(1));
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

#[cfg(test)]
mod tests {
    #[test]
    fn commitment_level_cmp() {
        use super::CommitmentLevel::*;

        assert!(Processed <= Processed);
        assert!(Processed >= Processed);
        assert!(Confirmed > Processed);
        assert!(Confirmed >= Processed);
        assert!(Finalized > Processed);
        assert!(Finalized >= Processed);

        assert!(Finalized > Confirmed);
        assert!(Finalized >= Confirmed);
    }
}

///
/// THIS FILE HAS BEEN COPIED FROM JET-GATEWAY
/// TODO: CREATE A COMMON LIBRARY
use {
    hyper::StatusCode,
    solana_client::{
        rpc_request::RpcRequest,
        rpc_sender::{RpcSender, RpcTransportStats},
    },
    solana_rpc_client_api::client_error::{Error, ErrorKind},
    std::time::Duration,
};

pub trait SolanaRpcErrorKindExt {
    fn my_error_kind(&self) -> &ErrorKind;

    /// Returns true if the error is transient and the operation can be retried.
    fn is_transient(&self) -> bool {
        match self.my_error_kind() {
            ErrorKind::Io(_) => true,
            ErrorKind::Reqwest(error) => {
                if let Some(status) = error.status() {
                    status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS
                } else {
                    tracing::warn!("Reqwest error without status: {:?}", error);
                    true
                }
            }
            ErrorKind::Middleware(_) => false,
            ErrorKind::RpcError(rpc_error) => {
                !matches!(rpc_error, solana_client::rpc_request::RpcError::ForUser(_))
            }
            ErrorKind::SerdeJson(_) => false,
            ErrorKind::SigningError(_) => false,
            ErrorKind::TransactionError(_) => false,
            ErrorKind::Custom(_) => false,
        }
    }
}

impl SolanaRpcErrorKindExt for Error {
    fn my_error_kind(&self) -> &ErrorKind {
        self.kind()
    }
}

#[derive(Clone, Debug, Copy, PartialEq)]
pub enum RetryRpcSenderStrategy {
    /// Fixed delay between retries
    FixedDelay { delay: Duration, max_retries: usize },
    /// Exponential backoff with a base delay and a factor
    ExponentialBackoff {
        base: Duration,
        exp: f64,
        max_retries: usize,
    },
}

impl IntoIterator for RetryRpcSenderStrategy {
    type Item = Duration;

    type IntoIter = Box<dyn Iterator<Item = Duration> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            RetryRpcSenderStrategy::FixedDelay { delay, max_retries } => Box::new(
                retry::delay::Fixed::from_millis(delay.as_millis() as u64).take(max_retries),
            ),
            RetryRpcSenderStrategy::ExponentialBackoff {
                base,
                exp,
                max_retries,
            } => Box::new(
                retry::delay::Exponential::from_millis_with_factor(base.as_millis() as u64, exp)
                    .take(max_retries),
            ),
        }
    }
}

///
/// RetryRpcSender is a wrapper around RpcSender that retries requests on transient errors
/// according to the provided strategy.
///
pub struct RetryRpcSender<Rpc> {
    /// The RpcSender to wrap
    rpc_sender: Rpc,
    /// The strategy to use for retries
    retry_strategy: RetryRpcSenderStrategy,
}

impl<Rpc> RetryRpcSender<Rpc>
where
    Rpc: RpcSender + Send + Sync + 'static,
{
    ///
    /// Create a new RetryRpcSender with the provided RpcSender and retry strategy.
    ///
    /// # Arguments
    ///
    /// * `rpc_sender` - The RpcSender to wrap
    /// * `retry_strategy` - The strategy to use for retries
    pub const fn new(rpc_sender: Rpc, retry_strategy: RetryRpcSenderStrategy) -> Self {
        Self {
            rpc_sender,
            retry_strategy,
        }
    }
}

#[async_trait::async_trait]
impl<Rpc> RpcSender for RetryRpcSender<Rpc>
where
    Rpc: RpcSender + Send + Sync + 'static,
{
    async fn send(
        &self,
        request: RpcRequest,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, solana_rpc_client_api::client_error::Error> {
        let mut retry_intervals = self.retry_strategy.into_iter();
        loop {
            match self.rpc_sender.send(request, params.clone()).await {
                Ok(response) => return Ok(response),
                Err(error) => {
                    if !error.is_transient() {
                        return Err(error);
                    }
                    match retry_intervals.next() {
                        Some(interval) => {
                            tokio::time::sleep(interval).await;
                        }
                        _ => {
                            return Err(error);
                        }
                    }
                }
            }
        }
    }

    fn get_transport_stats(&self) -> RpcTransportStats {
        self.rpc_sender.get_transport_stats()
    }
    fn url(&self) -> String {
        self.rpc_sender.url()
    }
}

#[cfg(test)]
pub mod testkit {
    ///
    /// MockRpcSender is a mock implementation of RpcSender that allows to set responses for specific RPC requests.
    ///
    use std::{collections::HashMap, sync::Arc};
    use {
        serde::{Deserialize, Serialize},
        solana_client::rpc_sender::{RpcSender, RpcTransportStats},
        solana_epoch_info::EpochInfo,
        solana_rpc_client_api::{
            client_error::{Error, ErrorKind},
            request::RpcRequest,
        },
        std::sync::Mutex,
    };

    type MockRpcResponseBuilder =
        Box<dyn Fn() -> Result<serde_json::Value, Error> + Send + 'static>;

    pub fn return_sucess<T>(constant: T) -> MockRpcResponseBuilder
    where
        T: Serialize + Clone + Send + 'static,
    {
        Box::new(move || Ok(serde_json::to_value(constant.clone()).expect("serde")))
    }

    pub fn return_transient_error() -> MockRpcResponseBuilder {
        Box::new(move || {
            Err(solana_rpc_client_api::client_error::Error {
                request: None,
                kind: Box::new(ErrorKind::Io(std::io::Error::from_raw_os_error(1))),
            })
        })
    }

    pub fn return_fatal_error() -> MockRpcResponseBuilder {
        Box::new(move || {
            Err(solana_rpc_client_api::client_error::Error {
                request: None,
                kind: Box::new(ErrorKind::Custom("testkit fatal error".to_string())),
            })
        })
    }

    struct MockRpcSenderInner {
        return_map: HashMap<RpcRequest, MockRpcResponseBuilder>,
        stats_map: HashMap<RpcRequest, u64>,
    }

    impl Default for MockRpcSenderInner {
        fn default() -> Self {
            let mut return_map = HashMap::default();
            return_map.insert(RpcRequest::GetEpochInfo, return_sucess(epoch_zero()));
            Self {
                return_map,
                stats_map: HashMap::default(),
            }
        }
    }

    impl MockRpcSenderInner {
        pub fn all_fatal_errors() -> Self {
            Self {
                return_map: HashMap::default(),
                stats_map: HashMap::default(),
            }
        }
    }

    #[derive(Clone)]
    pub struct MockRpcSender {
        url: String,
        inner: Arc<Mutex<MockRpcSenderInner>>,
    }

    pub const fn epoch_zero() -> EpochInfo {
        EpochInfo {
            epoch: 0,
            slot_index: 0,
            slots_in_epoch: 0,
            absolute_slot: 0,
            block_height: 0,
            transaction_count: None,
        }
    }

    pub const fn epoch_info_i(i: u64) -> EpochInfo {
        EpochInfo {
            epoch: i,
            slot_index: 0,
            slots_in_epoch: 0,
            absolute_slot: i * 432000,
            block_height: i * 432000,
            transaction_count: None,
        }
    }

    impl MockRpcSender {
        pub fn all_fatal_errors() -> MockRpcSender {
            Self {
                url: "mock".to_string(),
                inner: Arc::new(Mutex::new(MockRpcSenderInner::all_fatal_errors())),
            }
        }

        pub fn set_method_return(&self, method: RpcRequest, response: MockRpcResponseBuilder) {
            self.inner
                .lock()
                .expect("poised")
                .return_map
                .insert(method, response);
        }

        pub fn call_method(&self, method: RpcRequest) -> Result<serde_json::Value, Error> {
            let mut guard = self.inner.lock().expect("poised");

            guard
                .stats_map
                .entry(method)
                .and_modify(|e| *e += 1)
                .or_insert(1);

            guard
                .return_map
                .get(&method)
                .unwrap_or(&return_fatal_error())()
        }

        pub fn get_stats_for_call(&self, method: RpcRequest) -> u64 {
            self.inner
                .lock()
                .expect("poised")
                .stats_map
                .get(&method)
                .copied()
                .unwrap_or(0)
        }

        pub fn get_method_return_as<T>(&self, method: RpcRequest) -> Result<T, Error>
        where
            T: for<'a> Deserialize<'a>,
        {
            let response = self.call_method(method)?;
            Ok(serde_json::from_value(response).expect("serde"))
        }

        ///
        /// Shortcut for setting epoch info when calling `GetEpochInfo` RPC
        ///
        pub fn set_epoch(&self, epoch: u64) {
            self.set_method_return(RpcRequest::GetEpochInfo, return_sucess(epoch_info_i(epoch)));
        }

        pub fn incr_epoch(&self) {
            let curr_epoch = self.get_method_return_as::<EpochInfo>(RpcRequest::GetEpochInfo);
            match curr_epoch {
                Ok(curr_epoch) => {
                    self.set_epoch(curr_epoch.epoch + 1);
                }
                Err(_) => {
                    self.set_epoch(0);
                }
            }
        }
    }

    impl Default for MockRpcSender {
        fn default() -> Self {
            Self {
                url: "mock".to_string(),
                inner: Arc::new(Mutex::new(MockRpcSenderInner::default())),
            }
        }
    }

    #[async_trait::async_trait]
    impl RpcSender for MockRpcSender {
        async fn send(
            &self,
            request: RpcRequest,
            _params: serde_json::Value,
        ) -> Result<serde_json::Value, solana_client::client_error::ClientError> {
            self.call_method(request)
        }
        fn get_transport_stats(&self) -> RpcTransportStats {
            RpcTransportStats::default()
        }
        fn url(&self) -> String {
            self.url.clone()
        }
    }
}

#[cfg(test)]
pub mod tests {

    use {
        super::{RetryRpcSender, RetryRpcSenderStrategy, testkit::MockRpcSender},
        crate::solana_rpc_utils::testkit::{return_fatal_error, return_transient_error},
        solana_client::{
            nonblocking::rpc_client::RpcClient, rpc_client::RpcClientConfig,
            rpc_request::RpcRequest,
        },
        std::time::Duration,
    };

    #[tokio::test]
    pub async fn it_should_retry_on_transient_errors() {
        let mock_sender = MockRpcSender::default();
        let num_retries = 3;
        mock_sender.set_method_return(RpcRequest::GetEpochInfo, return_transient_error());
        let retry_strategy = RetryRpcSenderStrategy::FixedDelay {
            delay: std::time::Duration::from_millis(100),
            max_retries: num_retries,
        };

        let retry_rpc = RetryRpcSender::new(mock_sender.clone(), retry_strategy);

        let rpc = RpcClient::new_sender(retry_rpc, RpcClientConfig::default());
        let t = std::time::Instant::now();
        let result = rpc.get_epoch_info().await;
        let e = t.elapsed();
        assert!(e.as_millis() >= 100 * (num_retries as u128));
        assert!(result.is_err());

        let count = mock_sender.get_stats_for_call(RpcRequest::GetEpochInfo);
        assert_eq!(count, (num_retries + 1) as u64);
    }

    #[tokio::test]
    pub async fn it_should_not_retry_on_fatal_errors() {
        let mock_sender = MockRpcSender::default();
        let num_retries = 3;
        mock_sender.set_method_return(RpcRequest::GetEpochInfo, return_fatal_error());
        let retry_strategy = RetryRpcSenderStrategy::FixedDelay {
            delay: std::time::Duration::from_millis(100),
            max_retries: num_retries,
        };

        let retry_rpc = RetryRpcSender::new(mock_sender.clone(), retry_strategy);

        let rpc = RpcClient::new_sender(retry_rpc, RpcClientConfig::default());
        let result = rpc.get_epoch_info().await;
        assert!(result.is_err());

        let count = mock_sender.get_stats_for_call(RpcRequest::GetEpochInfo);
        assert_eq!(count, 1);
    }

    #[tokio::test]
    pub async fn it_should_not_retry_if_max_retries_equals_zero() {
        let mock_sender = MockRpcSender::default();
        mock_sender.set_method_return(RpcRequest::GetEpochInfo, return_transient_error());
        let retry_strategy = RetryRpcSenderStrategy::FixedDelay {
            delay: std::time::Duration::from_millis(100),
            max_retries: 0,
        };

        let retry_rpc = RetryRpcSender::new(mock_sender.clone(), retry_strategy);

        let rpc = RpcClient::new_sender(retry_rpc, RpcClientConfig::default());
        let result = rpc.get_epoch_info().await;
        assert!(result.is_err());

        let count = mock_sender.get_stats_for_call(RpcRequest::GetEpochInfo);
        assert_eq!(count, 1);
    }

    #[test]
    pub fn test_retry_iterators() {
        let iter = RetryRpcSenderStrategy::FixedDelay {
            delay: Duration::from_millis(100),
            max_retries: 3,
        }
        .into_iter();

        let actuals = iter.collect::<Vec<_>>();
        assert_eq!(
            actuals,
            vec![
                Duration::from_millis(100),
                Duration::from_millis(100),
                Duration::from_millis(100),
            ]
        );

        let iter = RetryRpcSenderStrategy::ExponentialBackoff {
            base: Duration::from_millis(10),
            exp: 10.0,
            max_retries: 3,
        }
        .into_iter();

        let actuals = iter.collect::<Vec<_>>();
        assert_eq!(
            actuals,
            vec![
                Duration::from_millis(10),
                Duration::from_millis(100),
                Duration::from_millis(1000),
            ]
        );

        let iter = RetryRpcSenderStrategy::FixedDelay {
            delay: Duration::from_millis(100),
            max_retries: 0,
        };

        let actuals = iter.into_iter().collect::<Vec<_>>();
        assert!(actuals.is_empty());

        let iter = RetryRpcSenderStrategy::ExponentialBackoff {
            base: Duration::from_millis(10),
            exp: 10.0,
            max_retries: 0,
        };
        let actuals = iter.into_iter().collect::<Vec<_>>();
        assert!(actuals.is_empty());
    }
}

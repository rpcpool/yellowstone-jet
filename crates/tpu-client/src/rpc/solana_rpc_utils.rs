///
/// THIS FILE HAS BEEN COPIED FROM JET-GATEWAY
/// TODO: CREATE A COMMON LIBRARY
use {
    hyper::StatusCode,
    solana_rpc_client_api::client_error::{Error, ErrorKind},
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

        // pub fn get_stats_for_call(&self, method: RpcRequest) -> u64 {
        //     self.inner
        //         .lock()
        //         .expect("poised")
        //         .stats_map
        //         .get(&method)
        //         .copied()
        //         .unwrap_or(0)
        // }

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

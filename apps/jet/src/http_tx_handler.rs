use {
    crate::{
        metrics::jet as metrics, payload::JetRpcSendTransactionConfig,
        transaction_handler::TransactionHandler,
    },
    futures::future::{BoxFuture, FutureExt},
    hyper::{Request, Response, StatusCode},
    jsonrpsee::core::http_helpers::Body,
    solana_rpc_client_api::config::RpcSendTransactionConfig,
    solana_transaction_status_client_types::UiTransactionEncoding,
    std::{
        error::Error,
        task::{Context, Poll},
        time::Instant,
    },
    tower::Service,
    tracing::{debug, warn},
};

const API_TX_PATH: &str = "/api/v1/transactions";

#[derive(Clone)]
pub struct HttpTransactionHandler {
    tx_handler: TransactionHandler,
    log_invalid_txn: bool,
}

impl HttpTransactionHandler {
    pub const fn new(tx_handler: TransactionHandler, log_invalid_txn: bool) -> Self {
        Self {
            tx_handler,
            log_invalid_txn,
        }
    }

    fn parse_encoding(query: Option<&str>) -> Result<UiTransactionEncoding, &'static str> {
        let Some(query) = query else {
            return Ok(UiTransactionEncoding::Base64);
        };
        for pair in query.split('&') {
            let mut kv = pair.splitn(2, '=');
            let key = kv.next().unwrap_or("");
            let value = kv.next().unwrap_or("");
            if key == "encoding" {
                return match value {
                    "base64" => Ok(UiTransactionEncoding::Base64),
                    "base58" => Ok(UiTransactionEncoding::Base58),
                    other => {
                        warn!(encoding = other, "unsupported encoding in HTTP tx request");
                        Err("unsupported encoding: must be 'base64' or 'base58'")
                    }
                };
            }
        }
        Ok(UiTransactionEncoding::Base64)
    }

    fn parse_max_retries(query: Option<&str>) -> Option<usize> {
        let query = query?;
        for pair in query.split('&') {
            let mut kv = pair.splitn(2, '=');
            let key = kv.next().unwrap_or("");
            let value = kv.next().unwrap_or("");
            if key == "max_retries" {
                return value.parse().ok();
            }
        }
        None
    }

    fn encoding_label(encoding: UiTransactionEncoding) -> &'static str {
        match encoding {
            UiTransactionEncoding::Base64 => "base64",
            UiTransactionEncoding::Base58 => "base58",
            _ => "unknown",
        }
    }

    async fn handle_request(self, req: Request<Body>) -> Response<Body> {
        let start = Instant::now();

        if req.method() != hyper::Method::POST {
            metrics::http_tx_requests_inc("error", "unknown");
            return json_response(
                StatusCode::METHOD_NOT_ALLOWED,
                r#"{"error":"method not allowed, use POST"}"#,
            );
        }

        let query = req.uri().query().map(|q| q.to_owned());
        let encoding = match Self::parse_encoding(query.as_deref()) {
            Ok(enc) => enc,
            Err(msg) => {
                metrics::http_tx_requests_inc("error", "unknown");
                return json_error_response(StatusCode::BAD_REQUEST, msg);
            }
        };
        let encoding_label = Self::encoding_label(encoding);
        let max_retries = Self::parse_max_retries(query.as_deref());

        let body_bytes = match http_body_util::BodyExt::collect(req.into_body()).await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                metrics::http_tx_requests_inc("error", encoding_label);
                return json_error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("failed to read request body: {e}"),
                );
            }
        };

        let data = match String::from_utf8(body_bytes.to_vec()) {
            Ok(s) => s.trim().to_owned(),
            Err(_) => {
                metrics::http_tx_requests_inc("error", encoding_label);
                return json_error_response(
                    StatusCode::BAD_REQUEST,
                    "request body must be valid UTF-8",
                );
            }
        };

        if data.is_empty() {
            metrics::http_tx_requests_inc("error", encoding_label);
            return json_error_response(StatusCode::BAD_REQUEST, "empty transaction body");
        }

        let config = JetRpcSendTransactionConfig {
            config: RpcSendTransactionConfig {
                skip_preflight: true,
                encoding: Some(encoding),
                max_retries,
                ..Default::default()
            },
            forwarding_policies: vec![],
        };

        match self.tx_handler.handle_transaction(data, Some(config)).await {
            Ok(signature) => {
                let elapsed = start.elapsed();
                metrics::http_tx_requests_inc("success", encoding_label);
                metrics::http_tx_request_duration(elapsed);
                debug!(
                    signature,
                    encoding = encoding_label,
                    elapsed_ms = elapsed.as_millis(),
                    "HTTP transaction submitted"
                );
                json_response(StatusCode::OK, &format!(r#"{{"signature":"{signature}"}}"#))
            }
            Err(e) => {
                metrics::http_tx_requests_inc("error", encoding_label);
                if self.log_invalid_txn {
                    warn!(error = %e, "HTTP transaction submission failed");
                }
                json_error_response(StatusCode::BAD_REQUEST, &e.to_string())
            }
        }
    }
}

fn json_response(status: StatusCode, body: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::new(body.to_owned()))
        .expect("failed to build response")
}

fn json_error_response(status: StatusCode, message: &str) -> Response<Body> {
    let escaped = message.replace('\\', "\\\\").replace('"', "\\\"");
    json_response(status, &format!(r#"{{"error":"{escaped}"}}"#))
}

/// Tower middleware that intercepts POST requests to /api/v1/transactions
/// and delegates everything else to the inner jsonrpsee service.
#[derive(Clone)]
pub struct HttpTxMiddleware<S> {
    service: S,
    handler: HttpTransactionHandler,
}

impl<S> HttpTxMiddleware<S> {
    pub fn new(service: S, handler: HttpTransactionHandler) -> Self {
        Self { service, handler }
    }
}

impl<S> Service<Request<Body>> for HttpTxMiddleware<S>
where
    S: Service<Request<Body>, Response = Response<Body>>,
    S::Response: 'static,
    S::Error: Into<Box<dyn Error + Send + Sync>> + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = Box<dyn Error + Send + Sync + 'static>;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        if request.uri().path() == API_TX_PATH {
            let handler = self.handler.clone();
            async move { Ok(handler.handle_request(request).await) }.boxed()
        } else {
            let fut = self.service.call(request);
            async move { fut.await.map_err(Into::into) }.boxed()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_encoding_default() {
        assert_eq!(
            HttpTransactionHandler::parse_encoding(None).unwrap(),
            UiTransactionEncoding::Base64
        );
    }

    #[test]
    fn test_parse_encoding_base64() {
        assert_eq!(
            HttpTransactionHandler::parse_encoding(Some("encoding=base64")).unwrap(),
            UiTransactionEncoding::Base64
        );
    }

    #[test]
    fn test_parse_encoding_base58() {
        assert_eq!(
            HttpTransactionHandler::parse_encoding(Some("encoding=base58")).unwrap(),
            UiTransactionEncoding::Base58
        );
    }

    #[test]
    fn test_parse_encoding_invalid() {
        assert!(HttpTransactionHandler::parse_encoding(Some("encoding=json")).is_err());
    }

    #[test]
    fn test_parse_encoding_with_other_params() {
        assert_eq!(
            HttpTransactionHandler::parse_encoding(Some("foo=bar&encoding=base58&baz=1")).unwrap(),
            UiTransactionEncoding::Base58
        );
    }

    #[test]
    fn test_parse_encoding_missing_param() {
        assert_eq!(
            HttpTransactionHandler::parse_encoding(Some("foo=bar")).unwrap(),
            UiTransactionEncoding::Base64
        );
    }

    #[test]
    fn test_parse_max_retries() {
        assert_eq!(HttpTransactionHandler::parse_max_retries(None), None);
        assert_eq!(
            HttpTransactionHandler::parse_max_retries(Some("max_retries=5")),
            Some(5)
        );
        assert_eq!(
            HttpTransactionHandler::parse_max_retries(Some("encoding=base64&max_retries=3")),
            Some(3)
        );
        assert_eq!(
            HttpTransactionHandler::parse_max_retries(Some("encoding=base64")),
            None
        );
        assert_eq!(
            HttpTransactionHandler::parse_max_retries(Some("max_retries=abc")),
            None
        );
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseMode {
    None,
    Signature,
}

struct QueryParams {
    encoding: UiTransactionEncoding,
    max_retries: Option<usize>,
    response: ResponseMode,
}

impl QueryParams {
    fn parse(query: Option<&str>) -> Result<Self, &'static str> {
        let Some(query) = query else {
            return Ok(Self {
                encoding: UiTransactionEncoding::Base58,
                max_retries: None,
                response: ResponseMode::None,
            });
        };

        let mut encoding = UiTransactionEncoding::Base58;
        let mut max_retries = None;
        let mut response = ResponseMode::None;

        for (key, value) in form_urlencoded::parse(query.as_bytes()) {
            match key.as_ref() {
                "encoding" => {
                    encoding = match value.as_ref() {
                        "base64" => UiTransactionEncoding::Base64,
                        "base58" => UiTransactionEncoding::Base58,
                        other => {
                            warn!(encoding = other, "unsupported encoding in HTTP tx request");
                            return Err("unsupported encoding: must be 'base64' or 'base58'");
                        }
                    };
                }
                "max_retries" => {
                    max_retries = value.parse().ok();
                }
                "response" => {
                    response = match value.as_ref() {
                        "signature" => ResponseMode::Signature,
                        "none" => ResponseMode::None,
                        _ => {
                            return Err("unsupported response mode: must be 'signature' or 'none'");
                        }
                    };
                }
                _ => {}
            }
        }

        Ok(Self {
            encoding,
            max_retries,
            response,
        })
    }

    const fn encoding_label(&self) -> &'static str {
        match self.encoding {
            UiTransactionEncoding::Base64 => "base64",
            UiTransactionEncoding::Base58 => "base58",
            _ => "unknown",
        }
    }
}

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

    async fn handle_request(self, req: Request<Body>) -> Response<Body> {
        let start = Instant::now();

        if req.method() != hyper::Method::POST {
            metrics::http_tx_requests_inc("error", "unknown");
            return text_response(
                StatusCode::METHOD_NOT_ALLOWED,
                "method not allowed, use POST",
            );
        }

        let params = match QueryParams::parse(req.uri().query()) {
            Ok(p) => p,
            Err(msg) => {
                metrics::http_tx_requests_inc("error", "unknown");
                return text_response(StatusCode::BAD_REQUEST, msg);
            }
        };

        let is_raw = req
            .headers()
            .get(hyper::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.starts_with("application/octet-stream"));

        let encoding_label = if is_raw {
            "raw"
        } else {
            params.encoding_label()
        };

        let body_bytes = match http_body_util::BodyExt::collect(req.into_body()).await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                metrics::http_tx_requests_inc("error", encoding_label);
                return text_response(
                    StatusCode::BAD_REQUEST,
                    &format!("failed to read request body: {e}"),
                );
            }
        };

        if body_bytes.is_empty() {
            metrics::http_tx_requests_inc("error", encoding_label);
            return text_response(StatusCode::BAD_REQUEST, "empty transaction body");
        }

        let result = if is_raw {
            let config = JetRpcSendTransactionConfig {
                config: RpcSendTransactionConfig {
                    skip_preflight: true,
                    max_retries: params.max_retries,
                    ..Default::default()
                },
                forwarding_policies: vec![],
            };
            self.tx_handler
                .handle_raw_transaction(body_bytes.to_vec(), config)
                .await
        } else {
            let data = match String::from_utf8(body_bytes.to_vec()) {
                Ok(s) => s.trim().to_owned(),
                Err(_) => {
                    metrics::http_tx_requests_inc("error", encoding_label);
                    return text_response(
                        StatusCode::BAD_REQUEST,
                        "request body must be valid UTF-8",
                    );
                }
            };

            if data.is_empty() {
                metrics::http_tx_requests_inc("error", encoding_label);
                return text_response(StatusCode::BAD_REQUEST, "empty transaction body");
            }

            let config = JetRpcSendTransactionConfig {
                config: RpcSendTransactionConfig {
                    skip_preflight: true,
                    encoding: Some(params.encoding),
                    max_retries: params.max_retries,
                    ..Default::default()
                },
                forwarding_policies: vec![],
            };
            self.tx_handler.handle_transaction(data, Some(config)).await
        };

        match result {
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
                match params.response {
                    ResponseMode::Signature => text_response(StatusCode::OK, &signature),
                    ResponseMode::None => empty_response(StatusCode::OK),
                }
            }
            Err(e) => {
                metrics::http_tx_requests_inc("error", encoding_label);
                if self.log_invalid_txn {
                    warn!(error = %e, "HTTP transaction submission failed");
                }
                text_response(StatusCode::BAD_REQUEST, &e.to_string())
            }
        }
    }
}

fn text_response(status: StatusCode, body: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain")
        .body(Body::new(body.to_owned()))
        .expect("failed to build response")
}

fn empty_response(status: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::new(String::new()))
        .expect("failed to build response")
}

/// Tower middleware that intercepts POST requests to /api/v1/transactions
/// and delegates everything else to the inner jsonrpsee service.
#[derive(Clone)]
pub struct HttpTxMiddleware<S> {
    service: S,
    handler: HttpTransactionHandler,
}

impl<S> HttpTxMiddleware<S> {
    pub const fn new(service: S, handler: HttpTransactionHandler) -> Self {
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
    fn test_parse_defaults() {
        let p = QueryParams::parse(None).unwrap();
        assert_eq!(p.encoding, UiTransactionEncoding::Base58);
        assert_eq!(p.max_retries, None);
        assert_eq!(p.response, ResponseMode::None);
    }

    #[test]
    fn test_parse_encoding_base64() {
        let p = QueryParams::parse(Some("encoding=base64")).unwrap();
        assert_eq!(p.encoding, UiTransactionEncoding::Base64);
    }

    #[test]
    fn test_parse_encoding_base58() {
        let p = QueryParams::parse(Some("encoding=base58")).unwrap();
        assert_eq!(p.encoding, UiTransactionEncoding::Base58);
    }

    #[test]
    fn test_parse_encoding_invalid() {
        assert!(QueryParams::parse(Some("encoding=json")).is_err());
    }

    #[test]
    fn test_parse_multiple_params() {
        let p = QueryParams::parse(Some("encoding=base58&max_retries=3")).unwrap();
        assert_eq!(p.encoding, UiTransactionEncoding::Base58);
        assert_eq!(p.max_retries, Some(3));
    }

    #[test]
    fn test_parse_unknown_params_ignored() {
        let p = QueryParams::parse(Some("foo=bar&encoding=base58&baz=1")).unwrap();
        assert_eq!(p.encoding, UiTransactionEncoding::Base58);
    }

    #[test]
    fn test_parse_max_retries_invalid_ignored() {
        let p = QueryParams::parse(Some("max_retries=abc")).unwrap();
        assert_eq!(p.max_retries, None);
    }

    #[test]
    fn test_parse_response_signature() {
        let p = QueryParams::parse(Some("response=signature")).unwrap();
        assert_eq!(p.response, ResponseMode::Signature);
    }

    #[test]
    fn test_parse_response_none() {
        let p = QueryParams::parse(Some("response=none")).unwrap();
        assert_eq!(p.response, ResponseMode::None);
    }

    #[test]
    fn test_parse_response_default_is_none() {
        let p = QueryParams::parse(Some("encoding=base64")).unwrap();
        assert_eq!(p.response, ResponseMode::None);
    }

    #[test]
    fn test_parse_response_invalid() {
        assert!(QueryParams::parse(Some("response=full")).is_err());
    }

    #[test]
    fn test_parse_all_params() {
        let p =
            QueryParams::parse(Some("encoding=base58&max_retries=5&response=signature")).unwrap();
        assert_eq!(p.encoding, UiTransactionEncoding::Base58);
        assert_eq!(p.max_retries, Some(5));
        assert_eq!(p.response, ResponseMode::Signature);
    }
}

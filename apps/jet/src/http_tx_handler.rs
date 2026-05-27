use {
    crate::{
        metrics::jet as metrics, payload::JetRpcSendTransactionConfig,
        transaction_handler::TransactionHandler,
    },
    bytes::Bytes,
    futures::future::{BoxFuture, FutureExt},
    hyper::{HeaderMap, Request, Response, StatusCode},
    jsonrpsee::core::http_helpers::Body,
    solana_pubkey::Pubkey,
    solana_rpc_client_api::config::RpcSendTransactionConfig,
    solana_transaction_status_client_types::UiTransactionEncoding,
    std::{
        error::Error,
        str::FromStr,
        task::{Context, Poll},
        time::Instant,
    },
    tower::Service,
    tracing::{debug, warn},
};

const API_TX_PATH: &str = "/api/v1/transactions";
const X_JET_MAX_RETRIES: &str = "x-jet-max-retries";
const X_JET_FORWARDING_POLICIES: &str = "x-jet-forwarding-policies";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseMode {
    None,
    Signature,
}

struct RequestParams {
    encoding: UiTransactionEncoding,
    encoding_explicit: bool,
    max_retries: Option<usize>,
    forwarding_policies: Vec<Pubkey>,
    response: ResponseMode,
}

impl RequestParams {
    fn parse(query: Option<&str>, headers: &HeaderMap) -> Result<Self, &'static str> {
        let Some(query) = query else {
            let mut params = Self {
                encoding: UiTransactionEncoding::Base58,
                encoding_explicit: false,
                max_retries: None,
                forwarding_policies: vec![],
                response: ResponseMode::None,
            };
            params.apply_headers(headers)?;
            return Ok(params);
        };

        let mut encoding = UiTransactionEncoding::Base58;
        let mut encoding_explicit = false;
        let mut response = ResponseMode::None;

        for (key, value) in form_urlencoded::parse(query.as_bytes()) {
            match key.as_ref() {
                "encoding" => {
                    encoding_explicit = true;
                    encoding = match value.as_ref() {
                        "base64" => UiTransactionEncoding::Base64,
                        "base58" => UiTransactionEncoding::Base58,
                        other => {
                            warn!(encoding = other, "unsupported encoding in HTTP tx request");
                            return Err("unsupported encoding: must be 'base64' or 'base58'");
                        }
                    };
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

        let mut params = Self {
            encoding,
            encoding_explicit,
            max_retries: None,
            forwarding_policies: vec![],
            response,
        };
        params.apply_headers(headers)?;
        Ok(params)
    }

    const fn encoding_label(&self) -> &'static str {
        match self.encoding {
            UiTransactionEncoding::Base64 => "base64",
            UiTransactionEncoding::Base58 => "base58",
            _ => "unknown",
        }
    }

    fn apply_headers(&mut self, headers: &HeaderMap) -> Result<(), &'static str> {
        if let Some(value) = headers.get(X_JET_MAX_RETRIES) {
            self.max_retries = value.to_str().ok().and_then(|v| v.parse().ok());
        }

        if let Some(value) = headers.get(X_JET_FORWARDING_POLICIES) {
            let value = value
                .to_str()
                .map_err(|_| "invalid x-jet-forwarding-policies header: must be valid UTF-8")?;
            self.forwarding_policies = parse_forwarding_policies(value);
        }

        Ok(())
    }
}

fn parse_forwarding_policies(value: &str) -> Vec<Pubkey> {
    value
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .filter_map(|v| Pubkey::from_str(v).ok())
        .collect()
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

        let params = match RequestParams::parse(req.uri().query(), req.headers()) {
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

        if is_raw && params.encoding_explicit {
            metrics::http_tx_requests_inc("error", "raw");
            return text_response(
                StatusCode::BAD_REQUEST,
                "encoding parameter cannot be used with application/octet-stream content type",
            );
        }

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
                forwarding_policies: params.forwarding_policies.clone(),
            };
            self.tx_handler
                .handle_raw_transaction(Bytes::clone(&body_bytes), config)
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
                forwarding_policies: params.forwarding_policies.clone(),
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
    use hyper::header::HeaderValue;

    fn headers(values: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (key, value) in values {
            headers.insert(*key, HeaderValue::from_str(value).unwrap());
        }
        headers
    }

    #[test]
    fn test_parse_defaults() {
        let p = RequestParams::parse(None, &HeaderMap::new()).unwrap();
        assert_eq!(p.encoding, UiTransactionEncoding::Base58);
        assert_eq!(p.max_retries, None);
        assert!(p.forwarding_policies.is_empty());
        assert_eq!(p.response, ResponseMode::None);
    }

    #[test]
    fn test_parse_encoding_base64() {
        let p = RequestParams::parse(Some("encoding=base64"), &HeaderMap::new()).unwrap();
        assert_eq!(p.encoding, UiTransactionEncoding::Base64);
    }

    #[test]
    fn test_parse_encoding_base58() {
        let p = RequestParams::parse(Some("encoding=base58"), &HeaderMap::new()).unwrap();
        assert_eq!(p.encoding, UiTransactionEncoding::Base58);
    }

    #[test]
    fn test_parse_encoding_invalid() {
        assert!(RequestParams::parse(Some("encoding=json"), &HeaderMap::new()).is_err());
    }

    #[test]
    fn test_parse_multiple_headers() {
        let p = RequestParams::parse(
            Some("encoding=base58"),
            &headers(&[(X_JET_MAX_RETRIES, "3")]),
        )
        .unwrap();
        assert_eq!(p.encoding, UiTransactionEncoding::Base58);
        assert_eq!(p.max_retries, Some(3));
    }

    #[test]
    fn test_parse_max_retries_invalid_ignored() {
        let p = RequestParams::parse(None, &headers(&[(X_JET_MAX_RETRIES, "abc")])).unwrap();
        assert_eq!(p.max_retries, None);
    }

    #[test]
    fn test_parse_response_signature() {
        let p = RequestParams::parse(Some("response=signature"), &HeaderMap::new()).unwrap();
        assert_eq!(p.response, ResponseMode::Signature);
    }

    #[test]
    fn test_parse_response_none() {
        let p = RequestParams::parse(Some("response=none"), &HeaderMap::new()).unwrap();
        assert_eq!(p.response, ResponseMode::None);
    }

    #[test]
    fn test_parse_response_default_is_none() {
        let p = RequestParams::parse(Some("encoding=base64"), &HeaderMap::new()).unwrap();
        assert_eq!(p.response, ResponseMode::None);
    }

    #[test]
    fn test_parse_response_invalid() {
        assert!(RequestParams::parse(Some("response=full"), &HeaderMap::new()).is_err());
    }

    #[test]
    fn test_parse_all_params() {
        let p = RequestParams::parse(
            Some("encoding=base58&response=signature"),
            &headers(&[(X_JET_MAX_RETRIES, "5")]),
        )
        .unwrap();
        assert_eq!(p.encoding, UiTransactionEncoding::Base58);
        assert_eq!(p.max_retries, Some(5));
        assert_eq!(p.response, ResponseMode::Signature);
    }

    #[test]
    fn test_encoding_explicit_flag() {
        let p = RequestParams::parse(Some("encoding=base64"), &HeaderMap::new()).unwrap();
        assert!(p.encoding_explicit);

        let p = RequestParams::parse(None, &headers(&[(X_JET_MAX_RETRIES, "3")])).unwrap();
        assert!(!p.encoding_explicit);

        let p = RequestParams::parse(None, &HeaderMap::new()).unwrap();
        assert!(!p.encoding_explicit);
    }

    #[test]
    fn test_parse_forwarding_policies_header() {
        let p = RequestParams::parse(
            None,
            &headers(&[(
                X_JET_FORWARDING_POLICIES,
                "11111111111111111111111111111111,invalid",
            )]),
        )
        .unwrap();
        assert_eq!(p.forwarding_policies.len(), 1);
    }
}

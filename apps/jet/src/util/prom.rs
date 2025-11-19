use {
    bytes::Bytes,
    http_body_util::{BodyExt, Full, combinators::BoxBody},
    hyper::{Request, Response, body, service::service_fn},
    hyper_util::{
        rt::{TokioExecutor, TokioIo},
        server::conn::auto,
    },
    prometheus::TextEncoder,
    std::net::SocketAddr,
    tokio::net::TcpListener,
    tokio_util::sync::CancellationToken,
};

fn metrics_handler(registry: &prometheus::Registry) -> Response<BoxBody<Bytes, hyper::Error>> {
    let metrics = TextEncoder::new()
        .encode_to_string(&registry.gather())
        .unwrap_or_else(|error| {
            tracing::error!("could not encode custom metrics: {}", error);
            String::new()
        });
    let metrics2 = inject_job_label(&metrics, [("job", "jet")]);
    let content = Full::new(metrics2.into())
        .map_err(|never| match never {})
        .boxed();
    Response::builder()
        .header("Content-Type", "text/plain; version=0.0.4")
        .body(content)
        .unwrap()
}

pub async fn serve_prometheus_metric(
    registry: prometheus::Registry,
    listen_addr: SocketAddr,
    cancellation_token: CancellationToken,
) {
    let service_fn_registry = registry.clone();
    let service = service_fn(move |_: Request<body::Incoming>| {
        let registry = service_fn_registry.clone();
        async move {
            let response = metrics_handler(&registry);
            Ok::<_, hyper::Error>(response)
        }
    });

    let tcp_listener = TcpListener::bind(&listen_addr)
        .await
        .expect("prometheus bind");
    let server = auto::Builder::new(TokioExecutor::new());
    loop {
        let (stream, _peer_addr) = tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("Prometheus metrics server is shutting down");
                break;
            }
            result = tcp_listener.accept() => {
                result.expect("tcp listener failed")
            }
        };
        let io = TokioIo::new(stream);
        let conn = server
            .serve_connection_with_upgrades(io, service.clone())
            .into_owned();

        tokio::spawn(async move {
            let result = conn.await;
            if let Err(e) = result {
                tracing::error!("connection error: {}", e);
            }
        });
    }
}

pub fn inject_job_label<'a>(
    metrics: &str,
    labels: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> String {
    let injected = labels
        .into_iter()
        .map(|(k, v)| format!(r#"{k}="{v}""#))
        .collect::<Vec<_>>()
        .join(",");

    if injected.is_empty() {
        return metrics.to_string();
    }

    metrics
        .lines()
        .map(|line| {
            // Don't touch comments
            if line.starts_with("#") {
                line.to_string()
            }
            // Case 1: metric already has `{ existing_labels }`
            else if let Some(pos) = line.find('{') {
                let (metric_name, rest) = line.split_at(pos + 1);
                format!(r#"{metric_name}{injected},{rest}"#,)
            }
            // Case 2: no labels (e.g. `metric 123`)
            else if let Some(pos) = line.find(' ') {
                let (metric_name, value) = line.split_at(pos);
                format!(r#"{metric_name}{{{injected}}}{value}"#)
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

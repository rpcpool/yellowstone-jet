




#[tokio::main]
pub async fn main() {

    let config = JetQuicClientConfig {
        transaction_queue_capacity: 100,
        ..Default::default()
    };

    let svc_discovery = GrpcTritonJetSvcDiscovery::new("https://myserver.com:50051", "xtoken").await.unwrap();

    let quic_client = JetQuicClient::connect(
        svc_discovery
    );


    let quic_client = JetQuicClient::connect_with_config(
        "myserver.com:443", 
        "xtoken",
        config,
    ).await.unwrap();
}
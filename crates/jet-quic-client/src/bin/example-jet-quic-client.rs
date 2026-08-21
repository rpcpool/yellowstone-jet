//! Minimal reference client: connects once to jet's raw QUIC ingress and sends one
//! transaction. One connection, one endpoint, no concurrency -- see the `quinn-client`
//! skill's Rule 3, which is exactly this example's shape.

use {
    clap::Parser,
    jet_quic_client::{
        ClientIdentity, JetQuicEndpoint, JetQuicSender, RootCertStore, ServerAddr,
        ServerVerification, client_config_with_verification, default_client_config, load_cert_pem,
        load_key_pem,
    },
    std::{path::PathBuf, sync::Arc},
};

#[derive(Debug, Parser)]
struct Args {
    /// Address to connect to, e.g. `jet.example.com:443` or `127.0.0.1:8443`.
    #[clap(long)]
    server_addr: String,

    /// TLS server name to validate the certificate against (SNI). Defaults to the host
    /// portion of `server_addr`.
    #[clap(long)]
    server_name: Option<String>,

    /// PEM-encoded client certificate presented for mTLS.
    #[clap(long)]
    client_cert: PathBuf,

    /// PEM-encoded client private key, paired with `client_cert`.
    #[clap(long)]
    client_key: PathBuf,

    /// PEM-encoded CA certificate to validate the server's certificate against, instead
    /// of the OS's native root store (the default when none of --ca-cert/--pinned-cert/
    /// --insecure is given).
    #[clap(long, conflicts_with_all = ["pinned_cert", "insecure"])]
    ca_cert: Option<PathBuf>,

    /// Accept only this exact server certificate (PEM), instead of chain validation.
    #[clap(long, conflicts_with_all = ["ca_cert", "insecure"])]
    pinned_cert: Option<PathBuf>,

    /// Skip server certificate validation entirely. Dev/test only.
    #[clap(long, conflicts_with_all = ["ca_cert", "pinned_cert"])]
    insecure: bool,

    /// File containing the raw wire transaction bytes to send.
    #[clap(long)]
    transaction: PathBuf,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let identity = ClientIdentity {
        cert: load_cert_pem(&std::fs::read(&args.client_cert)?)?,
        key: load_key_pem(&std::fs::read(&args.client_key)?)?,
    };

    let client_config = default_client_config(identity)?;

    let server_name =
        args.server_name
            .clone()
            .unwrap_or_else(|| match args.server_addr.rsplit_once(':') {
                Some((host, _port)) => host.to_owned(),
                None => args.server_addr.clone(),
            });
    let server_addr: ServerAddr = args.server_addr.into();

    // One endpoint for this one connection -- a small, known connection count doesn't
    // need the shared-pool tradeoff (see the `quinn-client` skill's Rule 3).
    let endpoint = JetQuicEndpoint::bind(None)?;
    let connection = endpoint
        .connect(&server_addr, &server_name, client_config)
        .await?;
    let mut sender = JetQuicSender::new(connection);

    let wire_transaction = std::fs::read(&args.transaction)?;
    let len = wire_transaction.len();
    sender.send_transaction(&wire_transaction).await?;

    println!("sent {len} bytes");
    Ok(())
}

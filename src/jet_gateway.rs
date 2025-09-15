use {
    crate::{
        config::ConfigJetGatewayClient, feature_flags::FeatureSet, grpc_jet::GrpcServer,
        identity::JetIdentitySyncMember, rpc::rpc_solana_like::RpcServerImpl, stake::StakeInfoMap,
    },
    futures::FutureExt,
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    std::{future, sync::Arc},
    tokio::sync::mpsc::{self},
    tokio_util::sync::CancellationToken,
};

struct IdentitySyncCommand {
    new_identity: Keypair,
    barrier: Arc<tokio::sync::Barrier>,
}

pub struct JetGatewayIdentityUpdater {
    cnc_tx: mpsc::UnboundedSender<IdentitySyncCommand>,
}

#[async_trait::async_trait]
impl JetIdentitySyncMember for JetGatewayIdentityUpdater {
    async fn pause_for_identity_update(
        &self,
        new_identity: Keypair,
        barrier: Arc<tokio::sync::Barrier>,
    ) {
        let command = IdentitySyncCommand {
            new_identity,
            barrier,
        };
        self.cnc_tx
            .send(command)
            .expect("Failed to send identity update command");
    }
}

pub fn spawn_jet_gw_listener(
    stake_info: StakeInfoMap,
    jet_gw_config: ConfigJetGatewayClient,
    tx_sender: RpcServerImpl,
    expected_identity: Option<Pubkey>,
    features: FeatureSet,
    initial_identity: Keypair,
    cancellation_token: CancellationToken,
) -> (JetGatewayIdentityUpdater, impl Future<Output = ()>) {
    let mut current_identity = initial_identity;
    let (cnc_tx, mut cnc_rx) = mpsc::unbounded_channel::<IdentitySyncCommand>();

    let jet_gw_fut = async move {
        loop {
            let jet_gw_config2 = jet_gw_config.clone();
            let tx_sender2 = tx_sender.clone();
            let features = features.clone();
            let stake_info2 = stake_info.clone();
            let fut = if let Some(expected_identity) = expected_identity {
                if current_identity.pubkey() != expected_identity {
                    let actual_pubkey = current_identity.pubkey();
                    tracing::warn!(
                        "expected identity: {expected_identity}, actual identity: {actual_pubkey}"
                    );
                    tracing::warn!(
                        "will not connect to jet-gateway with identity: {actual_pubkey}, waiting for correct identity to be set..."
                    );
                    future::pending().boxed()
                } else {
                    tracing::info!(
                        "JetGatewayIdentityUpdater: starting grpc server with identity: {}",
                        current_identity.pubkey()
                    );
                    GrpcServer::run_with(
                        Arc::new(current_identity.insecure_clone()),
                        stake_info2,
                        jet_gw_config2.clone(),
                        tx_sender2.clone(),
                        features,
                    )
                    .boxed()
                }
            } else {
                tracing::info!(
                    "JetGatewayIdentityUpdater: starting grpc server with identity: {}",
                    current_identity.pubkey()
                );
                GrpcServer::run_with(
                    Arc::new(current_identity.insecure_clone()),
                    stake_info2,
                    jet_gw_config2.clone(),
                    tx_sender2.clone(),
                    features,
                )
                .boxed()
            };
            tokio::select! {
                _result = fut => {
                    tracing::debug!("JetGatewayIdentityUpdater: grpc server stopped, restarting...");
                },
                maybe = cnc_rx.recv() => {
                    let command = maybe.expect("JetGatewaySubscriberCommand channel closed");
                    current_identity = command.new_identity;
                    tracing::info!("JetGatewayIdentityUpdater: received new identity: {}", current_identity.pubkey());
                    command.barrier.wait().await;
                }
                _ = cancellation_token.cancelled() => {
                    tracing::info!("JetGatewayIdentityUpdater: cancellation token triggered, shutting down...");
                    break;
                }
            }
        }
    };
    (JetGatewayIdentityUpdater { cnc_tx }, jet_gw_fut)
}

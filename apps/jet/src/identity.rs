use {
    crate::rpc::admin::JetIdentityUpdater,
    solana_pubkey::Pubkey,
    yellowstone_jet_tpu_client::{
        core::TpuSenderIdentityUpdater,
        identity::{HardenedKeypair, TpuIdentity},
    },
};

#[async_trait::async_trait]
impl JetIdentityUpdater for TpuSenderIdentityUpdater {
    async fn update_identity(&mut self, new_identity: HardenedKeypair) {
        let identity = TpuIdentity::from_ed25519_signing_key(&new_identity);
        self.update_identity(identity)
            .await
            .expect("update identity")
    }

    fn get_identity(&self) -> Pubkey {
        self.current_identity()
    }
}

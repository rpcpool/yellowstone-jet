use {
    crate::rpc::rpc_admin::JetIdentityUpdater, solana_keypair::Keypair, solana_pubkey::Pubkey,
    solana_signer::Signer, std::sync::Arc,
};

///
/// Syncs the identity across multiple jet components that need to share the same identity.
/// This is useful for components that need to coordinate their identity, such as a jet gateway subscriber and the quic gateway
///
pub struct JetIdentitySyncGroup {
    identity: Keypair,
    members: Vec<Box<dyn JetIdentitySyncMember + Send + Sync + 'static>>,
    watcher: tokio::sync::watch::Sender<Pubkey>,
}

impl JetIdentitySyncGroup {
    pub fn new(
        identity: Keypair,
        members: Vec<Box<dyn JetIdentitySyncMember + Send + Sync + 'static>>,
    ) -> Self {
        let watcher = tokio::sync::watch::channel(identity.pubkey()).0;
        Self {
            identity,
            members,
            watcher,
        }
    }

    pub fn get_identity_watcher(&self) -> tokio::sync::watch::Receiver<Pubkey> {
        self.watcher.subscribe()
    }
}

///
/// A member should support halting its operation waiting for identity updates.
/// then resume operation with the new identity is set.
///
#[async_trait::async_trait]
pub trait JetIdentitySyncMember {
    async fn pause_for_identity_update(
        &self,
        new_identity: Keypair,
        barrier: Arc<tokio::sync::Barrier>,
    );
}

impl JetIdentitySyncGroup {
    pub async fn sync_identity(&mut self, new_identity: Keypair) {
        let member_cnt = self.members.len();
        let barrier = Arc::new(tokio::sync::Barrier::new(member_cnt + 1));
        let members = std::mem::take(&mut self.members);
        for member in members.iter() {
            member
                .pause_for_identity_update(new_identity.insecure_clone(), Arc::clone(&barrier))
                .await;
        }

        // Wait for all members to pause before updating the identity
        barrier.wait().await;
        self.identity = new_identity.insecure_clone();
        self.members = members;
    }

    pub fn get_identity(&mut self) -> Pubkey {
        self.identity.pubkey()
    }
}

#[async_trait::async_trait]
impl JetIdentityUpdater for JetIdentitySyncGroup {
    async fn update_identity(&mut self, new_identity: Keypair) {
        self.sync_identity(new_identity).await;
    }

    async fn get_identity(&self) -> Pubkey {
        self.identity.pubkey()
    }
}

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
        self.watcher.send_replace(self.identity.pubkey());
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

    fn get_identity(&self) -> Pubkey {
        self.identity.pubkey()
    }
}

#[cfg(test)]
mod test {
    use {
        super::*,
        crate::rpc::rpc_admin::JetIdentityUpdater,
        solana_keypair::Keypair,
        std::sync::atomic::{AtomicUsize, Ordering},
    };

    struct TestMember {
        _id: usize,
        pause_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl JetIdentitySyncMember for TestMember {
        async fn pause_for_identity_update(
            &self,
            _new_identity: Keypair,
            barrier: Arc<tokio::sync::Barrier>,
        ) {
            let pause_count = Arc::clone(&self.pause_count);
            tokio::spawn(async move {
                pause_count.fetch_add(1, Ordering::SeqCst);
                barrier.wait().await;
            });
        }
    }

    #[tokio::test]
    async fn test_identity_sync_group() {
        let member_count = 5;
        let pause_count = Arc::new(AtomicUsize::new(0));
        let members: Vec<Box<dyn JetIdentitySyncMember + Send + Sync>> = (0..member_count)
            .map(|i| {
                Box::new(TestMember {
                    _id: i,
                    pause_count: Arc::clone(&pause_count),
                }) as Box<dyn JetIdentitySyncMember + Send + Sync>
            })
            .collect();

        let initial_identity = Keypair::new();
        let mut sync_group = JetIdentitySyncGroup::new(initial_identity.insecure_clone(), members);
        let identity_watcher = sync_group.get_identity_watcher();
        assert_eq!(sync_group.get_identity(), initial_identity.pubkey());
        assert_eq!(*identity_watcher.borrow(), initial_identity.pubkey());
        assert_eq!(pause_count.load(Ordering::SeqCst), 0);

        let new_identity = Keypair::new();
        sync_group
            .update_identity(new_identity.insecure_clone())
            .await;

        assert_eq!(sync_group.get_identity(), new_identity.pubkey());
        assert_eq!(pause_count.load(Ordering::SeqCst), member_count);
        assert_eq!(*identity_watcher.borrow(), new_identity.pubkey());
    }
}

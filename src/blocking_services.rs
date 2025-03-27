use {
    crate::{cluster_tpu_info::TpuInfo, metrics::jet as metrics},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{pubkey::Pubkey, transaction::VersionedTransaction},
    solana_yellowstone_blocklist::state::{AclType, ListState},
    std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    },
    tokio::sync::RwLock,
    tracing::debug,
};

#[derive(Default, Clone)]
pub struct LeadersSelector {
    blocklist: HashSet<Pubkey>,
    deny_lists: Arc<RwLock<HashMap<Pubkey, HashSet<Pubkey>>>>,
    allow_lists: Arc<RwLock<HashMap<Pubkey, HashSet<Pubkey>>>>,
}

impl LeadersSelector {
    pub async fn new_from_blockchain(
        url: String,
        blocklist: HashSet<Pubkey>,
        contract_pubkey: &Option<Pubkey>,
    ) -> Result<Self, anyhow::Error> {
        if let Some(contract_pubkey) = contract_pubkey {
            let rpc = RpcClient::new(url);
            let accounts = rpc.get_program_accounts(contract_pubkey).await?;
            let accounts_parsed: Vec<(Pubkey, HashSet<Pubkey>, AclType)>= accounts
                .iter()
                .filter_map(|account| {
                    match ListState::deserialize(account.1.data.as_slice()) {
                        Ok(state) => Some((
                            account.0,
                            Self::get_hashset(state.list),
                            state.meta.acl_type,
                        )),
                        Err(_) => {
                            tracing::error!("Error parsing account data from {:?}. \n Maybe you should check program pubkey", account.0);
                            None
                        }
                    }
                })
                .collect();

            let mut deny_list = HashMap::new();
            let mut allow_list = HashMap::new();

            accounts_parsed
                .into_iter()
                .for_each(|(key, hash_keys, acl)| match acl {
                    AclType::Allow => {
                        allow_list.insert(key, hash_keys);
                    }
                    AclType::Deny => {
                        deny_list.insert(key, hash_keys);
                    }
                });
            Ok(Self {
                blocklist,
                deny_lists: Arc::new(RwLock::new(deny_list)),
                allow_lists: Arc::new(RwLock::new(allow_list)),
            })
        } else {
            Ok(Self {
                blocklist,
                ..Default::default()
            })
        }
    }

    pub fn new(
        blocklist: HashSet<Pubkey>,
        deny_list: HashMap<Pubkey, HashSet<Pubkey>>,
        allow_list: HashMap<Pubkey, HashSet<Pubkey>>,
    ) -> Self {
        Self {
            blocklist,
            deny_lists: Arc::new(RwLock::new(deny_list)),
            allow_lists: Arc::new(RwLock::new(allow_list)),
        }
    }

    pub fn get_hashset(list: &[Pubkey]) -> HashSet<Pubkey> {
        list.iter().cloned().collect()
    }
}

#[async_trait::async_trait]
pub trait BlocklistUpdater {
    /// Used to update lists from contract Yellowstone-blocklist
    /// It parses the bytes in acc_data into an specified struct
    /// and then insert its data into a collection using key as index
    async fn update_list(&self, acc_data: &[u8], key: Pubkey);
}

#[async_trait::async_trait]
impl BlocklistUpdater for LeadersSelector {
    async fn update_list(&self, acc_data: &[u8], key: Pubkey) {
        match ListState::deserialize(acc_data) {
            Ok(state) => {
                let mut deny_lists = self.deny_lists.write().await;
                let mut allow_lists = self.allow_lists.write().await;

                match state.meta.acl_type {
                    AclType::Allow => {
                        deny_lists.remove(&key);
                        allow_lists.insert(key, Self::get_hashset(state.list));
                    }
                    AclType::Deny => {
                        allow_lists.remove(&key);
                        deny_lists.insert(key, Self::get_hashset(state.list));
                    }
                }
            }
            Err(err) => {
                tracing::error!("Unable to parse account {key:?} data. Maybe you should check program pubkey. Error: {err:?}");
            }
        }
    }
}

#[async_trait::async_trait]
pub trait BlockLeaders {
    /// Takes a mutable reference of a vector containing TpuInfo and retain only those allowed by the
    /// lists indicated by blocklist_keys
    /// # Example
    /// ```
    ///
    /// struct BlocklistYellowstone {
    ///     blocklist: HashMap<Pubkey, HashSet<Pubkey>>
    /// }
    ///
    ///
    /// #[async_trait::async_trait]
    /// impl BlockLeaders for Blocklists {
    ///     async fn block_leaders(&self, tpus: &mut Vec<TpuInfo>, blocklist_keys: &[Pubkey]) {
    ///         let blocklist = self.blocklist;
    ///
    ///         tpus.retain(|info| {
    ///             for key_list in blocklist_keys.iter() {
    ///                if let Some(blocklist_hash) = blocklist.get(key_list) {
    ///                    if blocklist_hash.contains(&info.leader) {
    ///                         return false;
    ///                    }
    ///                }
    ///                true
    ///              }
    ///            }
    ///         )
    ///     }
    /// }
    ///
    ///
    /// async fn main() {
    ///     let key1 = Pubkey::new_unique();
    ///     let mut tpus = vec![TpuInfo {
    ///         leader: key1,
    ///         ...
    ///     }]
    ///     let blocklist_yellowstone = {
    ///         blocklist: hashmap!{key1 => hashset!{key1}}
    ///     }
    ///     blocklist_yellowstone.block_leaders(&mut tpus, &[key1]).await;
    ///     // Now tpus should have a length of zero
    ///     assert!(tpus.is_empty());
    /// }
    /// ```
    async fn block_leaders(&self, tpus: &mut Vec<TpuInfo>, blocklist_keys: &[Pubkey]);
}

#[async_trait::async_trait]

impl BlockLeaders for LeadersSelector {
    async fn block_leaders(&self, tpus: &mut Vec<TpuInfo>, blocklist_keys: &[Pubkey]) {
        let mut blocklisted = 0;
        let allow_lists = self.allow_lists.read().await;
        let deny_lists = self.deny_lists.read().await;

        tpus.retain(|info| {
            let mut is_allow = false;
            let leader = info.leader;

            // Check deny lists first
            for key_list in blocklist_keys {
                if let Some(deny_hash) = deny_lists.get(key_list) {
                    if deny_hash.contains(&leader) {
                        blocklisted += 1;
                        debug!(
                            "Leader {} BLOCKED - found in deny list {}",
                            leader, key_list
                        );
                        return false;
                    }
                }
            }

            // Then check allow lists
            for key_list in blocklist_keys {
                if let Some(allow_hash) = allow_lists.get(key_list) {
                    if !allow_hash.contains(&leader) {
                        blocklisted += 1;
                        debug!(
                            "Leader {} BLOCKED - not found in allow list {}",
                            leader, key_list
                        );
                        return false;
                    } else {
                        is_allow = true;
                    }
                }
            }

            if !is_allow && self.blocklist.contains(&leader) {
                blocklisted += 1;
                debug!("Leader {} BLOCKED - found in global blocklist", leader);
                return false;
            }
            true
        });

        metrics::sts_tpu_blocklisted_inc(blocklisted);
    }
}

#[derive(Clone, Default, Debug)]
pub struct BannedAccounts {
    banned_accounts: HashSet<Pubkey>,
}

impl BannedAccounts {
    pub const fn new(banned_accounts: HashSet<Pubkey>) -> Self {
        Self { banned_accounts }
    }

    pub fn contains_banned_accounts(&self, transaction: &VersionedTransaction) -> bool {
        let transaction_message = &transaction.message;
        let accounts = transaction_message.static_account_keys();
        for account in accounts {
            if self.banned_accounts.contains(account) {
                debug!("Transaction signer {account} banned");
                metrics::increment_banned_transactions_total();
                return true;
            }
        }
        false
    }
}

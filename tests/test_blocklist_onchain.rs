use {
    maplit::{hashmap, hashset},
    solana_sdk::pubkey::Pubkey,
    yellowstone_jet::{
        cluster_tpu_info::{BlockLeaders, LeadersSelector, TpuInfo},
        config::ConfigBlocklist,
    },
};

const fn get_tpu_info(leader: &Pubkey) -> TpuInfo {
    TpuInfo {
        leader: *leader,
        quic: None,
        quic_forwards: None,
        slots: [0u64; 4],
    }
}

#[tokio::test]
async fn test_blocklist_onchain_blocks_every_tpu_leaders() {
    let leader1 = Pubkey::new_unique();
    let leader2 = Pubkey::new_unique();
    let tpu1 = get_tpu_info(&leader1);
    let tpu2 = get_tpu_info(&leader2);
    let mut tpus = vec![tpu1, tpu2];

    let deny_lists = hashmap! { leader1 => hashset! {leader1, leader2}};
    let leaders_selector = LeadersSelector::new(hashset! {}, deny_lists, hashmap! {});

    leaders_selector.block_leaders(&mut tpus, &[leader1]).await;

    assert!(tpus.is_empty());
}

#[tokio::test]
async fn test_blocklist_onchain_blocks_only_one_tpu_leader() {
    let leader1 = Pubkey::new_unique();
    let leader2 = Pubkey::new_unique();
    let tpu1 = get_tpu_info(&leader1);
    let tpu2 = get_tpu_info(&leader2);
    let mut tpus = vec![tpu1, tpu2];

    let deny_lists = hashmap! { leader1 => hashset! {leader1}};
    let leaders_selector = LeadersSelector::new(hashset! {}, deny_lists, hashmap! {});

    leaders_selector.block_leaders(&mut tpus, &[leader1]).await;

    assert!(tpus.len() == 1);
    assert!(tpus[0].leader == leader2);
}

#[tokio::test]
async fn test_blocklist_onchain_tpu_in_every_allowlist() {
    let leader1 = Pubkey::new_unique();
    let leader2 = Pubkey::new_unique();
    let tpu1 = get_tpu_info(&leader1);
    let tpu2 = get_tpu_info(&leader2);
    let mut tpus = vec![tpu1, tpu2];

    let allow_lists = hashmap! { leader1 => hashset! {leader1, leader2}};
    let leaders_selector = LeadersSelector::new(hashset! {}, hashmap! {}, allow_lists);

    leaders_selector.block_leaders(&mut tpus, &[leader1]).await;

    assert!(tpus.len() == 2);
    assert!(tpus[0].leader == leader1);
    assert!(tpus[1].leader == leader2);
}

#[tokio::test]
async fn test_blocklist_onchain_tpu_in_one_allowlist() {
    let leader1 = Pubkey::new_unique();
    let leader2 = Pubkey::new_unique();
    let tpu1 = get_tpu_info(&leader1);
    let tpu2 = get_tpu_info(&leader2);
    let mut tpus = vec![tpu1, tpu2];

    let allow_lists = hashmap! { leader1 => hashset! {leader1}};
    let leaders_selector = LeadersSelector::new(hashset! {}, hashmap! {}, allow_lists);

    leaders_selector.block_leaders(&mut tpus, &[leader1]).await;

    assert!(tpus.len() == 1);
    assert!(tpus[0].leader == leader1);
}

#[tokio::test]
async fn test_blocklist_onchain_no_tpu_in_any_allowlist() {
    let leader1 = Pubkey::new_unique();
    let leader2 = Pubkey::new_unique();
    let tpu1 = get_tpu_info(&leader1);
    let tpu2 = get_tpu_info(&leader2);
    let mut tpus = vec![tpu1, tpu2];

    let allow_lists = hashmap! { leader1 => hashset! {}, leader2 => hashset! {}};
    let leaders_selector = LeadersSelector::new(hashset! {}, hashmap! {}, allow_lists);

    leaders_selector
        .block_leaders(&mut tpus, &[leader1, leader2])
        .await;

    assert!(tpus.is_empty());
}

#[tokio::test]
async fn test_blocklist_onchain_allowlist_denylist_block_one_tpu() {
    let leader1 = Pubkey::new_unique();
    let leader2 = Pubkey::new_unique();
    let tpu1 = get_tpu_info(&leader1);
    let tpu2 = get_tpu_info(&leader2);
    let mut tpus = vec![tpu1, tpu2];

    let allow_lists = hashmap! { leader2 => hashset! {leader1, leader2}};
    let deny_lists = hashmap! { leader1 => hashset! {leader1}};
    let leaders_selector = LeadersSelector::new(hashset! {}, deny_lists, allow_lists);

    leaders_selector
        .block_leaders(&mut tpus, &[leader1, leader2])
        .await;

    assert!(tpus.len() == 1);
    assert!(tpus[0].leader == leader2);
}

#[tokio::test]
async fn blocklist_onchain_takes_prescendence_over_config_blocklist() {
    let leader1 = Pubkey::new_unique();
    let leader2 = Pubkey::new_unique();
    let tpu1 = get_tpu_info(&leader1);
    let tpu2 = get_tpu_info(&leader2);
    let mut tpus = vec![tpu1, tpu2];

    let mut config_blocklist = ConfigBlocklist::default();
    config_blocklist.leaders.insert(leader1);

    let allow_lists = hashmap! { leader2 => hashset! {leader1}};
    let leaders_selector = LeadersSelector::new(hashset! {leader1}, hashmap! {}, allow_lists);

    leaders_selector.block_leaders(&mut tpus, &[leader2]).await;

    assert!(tpus.len() == 1);
    assert!(tpus[0].leader == leader1);
}

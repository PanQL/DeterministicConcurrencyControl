#![cfg(madsim)]

use anyhow::{bail, Result};
use calvinfs_demo::checker::{
    assert_scc_reorders_consistent, assert_state_matches, assert_tx_results_consistent,
    merge_shard_states, reference_execute_batches, reference_execute_scc_reordered_batches,
};
use calvinfs_demo::convert::{
    fs_op_to_proto, inode_entries_from_proto, scc_reorder_records_from_proto, tx_result_from_i32,
    tx_result_records_from_proto,
};
use calvinfs_demo::engine::{
    SchedulerKind, SequencerConfig, SequencerRuntime, ShardConfig, ShardRuntime,
};
use calvinfs_demo::executor::derive_read_write_set;
use calvinfs_demo::model::{Batch, FsOp, Key, OrderedTx, ShardId, TxResult, TxResultRecord};
use calvinfs_demo::proto::pb;
use calvinfs_demo::router::ShardLayout;
use calvinfs_demo::service::{sequencer_service, shard_service};
use calvinfs_demo::workload::metadata_workload;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::transport::Server;

const SHARD_COUNT: u64 = 4;
const BATCH_COUNT: usize = 16;
const BATCH_SIZE: usize = 512;
const MDTEST_DEFAULT_CLIENT_COUNT: usize = 8;
const MDTEST_DEFAULT_DIRS_PER_CLIENT: usize = 4;
const MDTEST_DEFAULT_FILES_PER_CLIENT: usize = 64;
const MDTEST_PRIVATE_ROOT: &str = "/mdtest_private";
const MDTEST_PUBLIC_ROOT: &str = "/mdtest_public";
const SHARD_PORT: u16 = 50_051;
const SEQUENCER_PORT: u16 = 50_052;

#[madsim::test]
async fn four_shards_full_metadata_workload() -> Result<()> {
    let mut shard_endpoints = BTreeMap::new();
    for shard_id in 0..SHARD_COUNT {
        let ip = shard_ip(shard_id);
        shard_endpoints.insert(shard_id, endpoint(ip, SHARD_PORT));
    }
    for shard_id in 0..SHARD_COUNT {
        let ip = shard_ip(shard_id);
        start_shard_node(shard_id, ip, shard_endpoints.clone());
    }

    let sequencer_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 100));
    let sequencer_endpoint = endpoint(sequencer_ip, SEQUENCER_PORT);
    start_sequencer_node(sequencer_ip, shard_endpoints.clone());

    let driver = madsim::runtime::Handle::current()
        .create_node()
        .name("driver")
        .ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 200)))
        .build();
    driver
        .spawn(run_driver(sequencer_endpoint, shard_endpoints))
        .await
        .expect("driver task panicked")?;

    Ok(())
}

#[madsim::test]
async fn submit_tx_get_result_client_api() -> Result<()> {
    let mut shard_endpoints = BTreeMap::new();
    for shard_id in 0..SHARD_COUNT {
        let ip = shard_ip(shard_id);
        shard_endpoints.insert(shard_id, endpoint(ip, SHARD_PORT));
    }
    for shard_id in 0..SHARD_COUNT {
        let ip = shard_ip(shard_id);
        start_shard_node(shard_id, ip, shard_endpoints.clone());
    }

    let sequencer_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 100));
    let sequencer_endpoint = endpoint(sequencer_ip, SEQUENCER_PORT);
    start_sequencer_node(sequencer_ip, shard_endpoints.clone());

    let driver = madsim::runtime::Handle::current()
        .create_node()
        .name("submit-tx-driver")
        .ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 201)))
        .build();
    driver
        .spawn(run_submit_tx_driver(sequencer_endpoint, shard_endpoints))
        .await
        .expect("submit tx driver task panicked")?;

    Ok(())
}

#[madsim::test]
async fn scc_same_parent_create_batch() -> Result<()> {
    let mut shard_endpoints = BTreeMap::new();
    for shard_id in 0..SHARD_COUNT {
        let ip = shard_ip(shard_id);
        shard_endpoints.insert(shard_id, endpoint(ip, SHARD_PORT));
    }
    for shard_id in 0..SHARD_COUNT {
        let ip = shard_ip(shard_id);
        start_shard_node_with_scheduler(
            shard_id,
            ip,
            shard_endpoints.clone(),
            SchedulerKind::SccOnline,
        );
    }

    let sequencer_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 100));
    let sequencer_endpoint = endpoint(sequencer_ip, SEQUENCER_PORT);
    start_sequencer_node(sequencer_ip, shard_endpoints.clone());

    let driver = madsim::runtime::Handle::current()
        .create_node()
        .name("scc-driver")
        .ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 202)))
        .build();
    driver
        .spawn(run_scc_same_parent_create_driver(
            sequencer_endpoint,
            shard_endpoints,
        ))
        .await
        .expect("scc driver task panicked")?;

    Ok(())
}

#[madsim::test]
async fn scc_prediction_failure_fallback() -> Result<()> {
    let mut shard_endpoints = BTreeMap::new();
    for shard_id in 0..SHARD_COUNT {
        let ip = shard_ip(shard_id);
        shard_endpoints.insert(shard_id, endpoint(ip, SHARD_PORT));
    }
    for shard_id in 0..SHARD_COUNT {
        let ip = shard_ip(shard_id);
        start_shard_node_with_scheduler(
            shard_id,
            ip,
            shard_endpoints.clone(),
            SchedulerKind::SccOnline,
        );
    }

    let sequencer_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 100));
    let sequencer_endpoint = endpoint(sequencer_ip, SEQUENCER_PORT);
    start_sequencer_node(sequencer_ip, shard_endpoints.clone());

    let driver = madsim::runtime::Handle::current()
        .create_node()
        .name("scc-fallback-driver")
        .ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 203)))
        .build();
    driver
        .spawn(run_scc_prediction_failure_driver(
            sequencer_endpoint,
            shard_endpoints,
        ))
        .await
        .expect("scc fallback driver task panicked")?;

    Ok(())
}

#[madsim::test]
async fn scc_mixed_metadata_success_batch() -> Result<()> {
    let mut shard_endpoints = BTreeMap::new();
    for shard_id in 0..SHARD_COUNT {
        let ip = shard_ip(shard_id);
        shard_endpoints.insert(shard_id, endpoint(ip, SHARD_PORT));
    }
    for shard_id in 0..SHARD_COUNT {
        let ip = shard_ip(shard_id);
        start_shard_node_with_scheduler(
            shard_id,
            ip,
            shard_endpoints.clone(),
            SchedulerKind::SccOnline,
        );
    }

    let sequencer_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 100));
    let sequencer_endpoint = endpoint(sequencer_ip, SEQUENCER_PORT);
    start_sequencer_node(sequencer_ip, shard_endpoints.clone());

    let driver = madsim::runtime::Handle::current()
        .create_node()
        .name("scc-mixed-driver")
        .ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 204)))
        .build();
    driver
        .spawn(run_scc_mixed_metadata_driver(
            sequencer_endpoint,
            shard_endpoints,
        ))
        .await
        .expect("scc mixed driver task panicked")?;

    Ok(())
}

#[madsim::test]
async fn mdtest_like_client_workload() -> Result<()> {
    let config = MdtestConfig::from_env()?;
    println!(
        "\nCalvinFS mdtest-like workload: clients={} dirs/client={} files/client={} batch_size={}",
        config.client_count, config.dirs_per_client, config.files_per_client, config.batch_size
    );

    let calvin_private_summary =
        run_mdtest_like_cluster(BenchmarkScheduler::Calvin, MdtestMode::Private, config, 20)
            .await?;
    let calvin_public_summary =
        run_mdtest_like_cluster(BenchmarkScheduler::Calvin, MdtestMode::Public, config, 30).await?;
    let scc_private_summary =
        run_mdtest_like_cluster(BenchmarkScheduler::Scc, MdtestMode::Private, config, 40).await?;
    let scc_public_summary =
        run_mdtest_like_cluster(BenchmarkScheduler::Scc, MdtestMode::Public, config, 50).await?;

    print_mode_summary(&calvin_private_summary, config.show_ranks);
    print_mode_summary(&calvin_public_summary, config.show_ranks);
    print_mode_summary(&scc_private_summary, config.show_ranks);
    print_mode_summary(&scc_public_summary, config.show_ranks);
    print_private_public_comparison(
        BenchmarkScheduler::Calvin,
        &calvin_private_summary,
        &calvin_public_summary,
    );
    print_private_public_comparison(
        BenchmarkScheduler::Scc,
        &scc_private_summary,
        &scc_public_summary,
    );
    print_scheduler_comparison(
        MdtestMode::Private,
        &calvin_private_summary,
        &scc_private_summary,
    );
    print_scheduler_comparison(
        MdtestMode::Public,
        &calvin_public_summary,
        &scc_public_summary,
    );

    Ok(())
}

async fn run_scc_prediction_failure_driver(
    sequencer_endpoint: String,
    shard_endpoints: BTreeMap<ShardId, String>,
) -> Result<()> {
    wait_for_shards(&shard_endpoints).await?;
    wait_for_sequencer(&sequencer_endpoint).await?;

    let mut sequencer = pb::sequencer_client::SequencerClient::connect(sequencer_endpoint).await?;
    let mut reference_batches = Vec::new();
    let mut all_records = Vec::new();

    submit_expected_batch(
        &mut sequencer,
        vec![expect("scc_fallback:mkdir_root", mkdir("/")?, TxResult::Ok)],
        &mut reference_batches,
        &mut all_records,
    )
    .await?;
    submit_expected_batch(
        &mut sequencer,
        vec![expect(
            "scc_fallback:mkdir_d",
            mkdir("/scc_fb")?,
            TxResult::Ok,
        )],
        &mut reference_batches,
        &mut all_records,
    )
    .await?;
    submit_expected_batch(
        &mut sequencer,
        vec![
            expect(
                "scc_fallback:create_first",
                create("/scc_fb/x")?,
                TxResult::Ok,
            ),
            expect(
                "scc_fallback:create_duplicate",
                create("/scc_fb/x")?,
                TxResult::AlreadyExists,
            ),
        ],
        &mut reference_batches,
        &mut all_records,
    )
    .await?;
    let fallback_batch_id = reference_batches
        .last()
        .expect("fallback batch was just submitted")
        .batch_id;

    let mut shard_states = Vec::new();
    let mut shard_reorders = Vec::new();
    for (shard_id, endpoint) in &shard_endpoints {
        let mut client = pb::shard_client::ShardClient::connect(endpoint.clone()).await?;
        let response = client
            .dump_state(pb::DumpStateRequest {})
            .await?
            .into_inner();
        shard_states.push((*shard_id, inode_entries_from_proto(response.entries)?));
        shard_reorders.push((
            *shard_id,
            scc_reorder_records_from_proto(response.scc_reorders),
        ));
    }

    let layout = ShardLayout::new(SHARD_COUNT);
    let scc_reorders = assert_scc_reorders_consistent(shard_reorders)?;
    let fallback_reorder = scc_reorders
        .get(&fallback_batch_id)
        .ok_or_else(|| anyhow::anyhow!("missing SCC fallback reorder record"))?;
    if fallback_reorder.speculative_success_indices != vec![0]
        || fallback_reorder.fallback_indices != vec![1]
    {
        bail!(
            "expected duplicate-create batch reorder success=[0] fallback=[1], got {:?}",
            fallback_reorder
        );
    }
    let expected = reference_execute_scc_reordered_batches(&reference_batches, &scc_reorders)?;
    let actual = merge_shard_states(&layout, shard_states)?;
    assert_state_matches(&expected, &actual)?;
    assert_tx_results_consistent(&layout, &reference_batches, &all_records)?;

    Ok(())
}

async fn run_scc_same_parent_create_driver(
    sequencer_endpoint: String,
    shard_endpoints: BTreeMap<ShardId, String>,
) -> Result<()> {
    wait_for_shards(&shard_endpoints).await?;
    wait_for_sequencer(&sequencer_endpoint).await?;

    let mut sequencer = pb::sequencer_client::SequencerClient::connect(sequencer_endpoint).await?;
    let mut reference_batches = Vec::new();
    let mut all_records = Vec::new();

    submit_expected_batch(
        &mut sequencer,
        vec![expect("scc_setup:mkdir_root", mkdir("/")?, TxResult::Ok)],
        &mut reference_batches,
        &mut all_records,
    )
    .await?;
    submit_expected_batch(
        &mut sequencer,
        vec![expect(
            "scc_setup:mkdir_public",
            mkdir("/scc_public")?,
            TxResult::Ok,
        )],
        &mut reference_batches,
        &mut all_records,
    )
    .await?;

    let create_ops = (0..64)
        .map(|index| {
            Ok(expect(
                format!("scc_create:file_{index}"),
                create(&format!("/scc_public/file_{index}"))?,
                TxResult::Ok,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    submit_expected_batch(
        &mut sequencer,
        create_ops,
        &mut reference_batches,
        &mut all_records,
    )
    .await?;

    let mut shard_states = Vec::new();
    let mut shard_reorders = Vec::new();
    for (shard_id, endpoint) in &shard_endpoints {
        let mut client = pb::shard_client::ShardClient::connect(endpoint.clone()).await?;
        let response = client
            .dump_state(pb::DumpStateRequest {})
            .await?
            .into_inner();
        shard_states.push((*shard_id, inode_entries_from_proto(response.entries)?));
        shard_reorders.push((
            *shard_id,
            scc_reorder_records_from_proto(response.scc_reorders),
        ));
    }

    let layout = ShardLayout::new(SHARD_COUNT);
    let scc_reorders = assert_scc_reorders_consistent(shard_reorders)?;
    if scc_reorders
        .values()
        .any(|record| !record.fallback_indices.is_empty())
    {
        bail!(
            "same-parent create SCC test should not use fallback, got {:?}",
            scc_reorders
        );
    }
    let expected = reference_execute_scc_reordered_batches(&reference_batches, &scc_reorders)?;
    let actual = merge_shard_states(&layout, shard_states)?;
    assert_state_matches(&expected, &actual)?;
    assert_tx_results_consistent(&layout, &reference_batches, &all_records)?;
    let public = actual
        .get(&Key::new("/scc_public")?)
        .ok_or_else(|| anyhow::anyhow!("missing /scc_public"))?;
    assert_eq!(public.child_count, 64);

    Ok(())
}

async fn run_scc_mixed_metadata_driver(
    sequencer_endpoint: String,
    shard_endpoints: BTreeMap<ShardId, String>,
) -> Result<()> {
    wait_for_shards(&shard_endpoints).await?;
    wait_for_sequencer(&sequencer_endpoint).await?;

    let mut sequencer = pb::sequencer_client::SequencerClient::connect(sequencer_endpoint).await?;
    let mut reference_batches = Vec::new();
    let mut all_records = Vec::new();

    submit_expected_batch(
        &mut sequencer,
        vec![
            expect("scc_mixed:mkdir_root", mkdir("/")?, TxResult::Ok),
            expect("scc_mixed:mkdir_root_dir", mkdir("/scc_mix")?, TxResult::Ok),
            expect("scc_mixed:mkdir_src", mkdir("/scc_mix/src")?, TxResult::Ok),
            expect("scc_mixed:mkdir_dst", mkdir("/scc_mix/dst")?, TxResult::Ok),
        ],
        &mut reference_batches,
        &mut all_records,
    )
    .await?;
    submit_expected_batch(
        &mut sequencer,
        vec![
            expect(
                "scc_mixed:create_file_old",
                create("/scc_mix/src/file_old")?,
                TxResult::Ok,
            ),
            expect(
                "scc_mixed:mkdir_empty_dir",
                mkdir("/scc_mix/src/empty_dir")?,
                TxResult::Ok,
            ),
            expect(
                "scc_mixed:create_delete_file",
                create("/scc_mix/src/delete_file")?,
                TxResult::Ok,
            ),
            expect(
                "scc_mixed:mkdir_delete_dir",
                mkdir("/scc_mix/src/delete_dir")?,
                TxResult::Ok,
            ),
        ],
        &mut reference_batches,
        &mut all_records,
    )
    .await?;
    submit_expected_batch(
        &mut sequencer,
        vec![
            expect(
                "scc_mixed:create_new_file",
                create("/scc_mix/src/new_file")?,
                TxResult::Ok,
            ),
            expect(
                "scc_mixed:unlink_delete_file",
                unlink("/scc_mix/src/delete_file")?,
                TxResult::Ok,
            ),
            expect(
                "scc_mixed:rmdir_delete_dir",
                rmdir("/scc_mix/src/delete_dir")?,
                TxResult::Ok,
            ),
            expect(
                "scc_mixed:rename_file",
                rename("/scc_mix/src/file_old", "/scc_mix/dst/file_new")?,
                TxResult::Ok,
            ),
            expect(
                "scc_mixed:rename_empty_dir",
                rename("/scc_mix/src/empty_dir", "/scc_mix/dst/empty_dir_new")?,
                TxResult::Ok,
            ),
            expect(
                "scc_mixed:stat_renamed_file",
                stat("/scc_mix/dst/file_new")?,
                TxResult::Ok,
            ),
            expect(
                "scc_mixed:stat_new_file",
                stat("/scc_mix/src/new_file")?,
                TxResult::Ok,
            ),
        ],
        &mut reference_batches,
        &mut all_records,
    )
    .await?;

    let mut shard_states = Vec::new();
    let mut shard_reorders = Vec::new();
    for (shard_id, endpoint) in &shard_endpoints {
        let mut client = pb::shard_client::ShardClient::connect(endpoint.clone()).await?;
        let response = client
            .dump_state(pb::DumpStateRequest {})
            .await?
            .into_inner();
        shard_states.push((*shard_id, inode_entries_from_proto(response.entries)?));
        shard_reorders.push((
            *shard_id,
            scc_reorder_records_from_proto(response.scc_reorders),
        ));
    }

    let layout = ShardLayout::new(SHARD_COUNT);
    let scc_reorders = assert_scc_reorders_consistent(shard_reorders)?;
    if scc_reorders
        .values()
        .any(|record| !record.fallback_indices.is_empty())
    {
        bail!(
            "mixed SCC success test should not use fallback, got {:?}",
            scc_reorders
        );
    }
    let expected = reference_execute_scc_reordered_batches(&reference_batches, &scc_reorders)?;
    let actual = merge_shard_states(&layout, shard_states)?;
    assert_state_matches(&expected, &actual)?;
    assert_tx_results_consistent(&layout, &reference_batches, &all_records)?;
    if actual.contains_key(&Key::new("/scc_mix/src/file_old")?) {
        bail!("renamed source file still exists");
    }
    if !actual.contains_key(&Key::new("/scc_mix/dst/file_new")?) {
        bail!("renamed destination file missing");
    }
    if actual.contains_key(&Key::new("/scc_mix/src/delete_file")?) {
        bail!("unlinked file still exists");
    }
    if actual.contains_key(&Key::new("/scc_mix/src/delete_dir")?) {
        bail!("removed directory still exists");
    }

    Ok(())
}

async fn run_driver(
    sequencer_endpoint: String,
    shard_endpoints: BTreeMap<ShardId, String>,
) -> Result<()> {
    wait_for_shards(&shard_endpoints).await?;
    wait_for_sequencer(&sequencer_endpoint).await?;

    let initialization_workload = metadata_workload(BATCH_COUNT, BATCH_SIZE)?;
    let mut sequencer = pb::sequencer_client::SequencerClient::connect(sequencer_endpoint).await?;
    let mut reference_batches = Vec::with_capacity(BATCH_COUNT + 4);
    let mut all_records = Vec::new();

    for (batch_index, ops) in initialization_workload.into_iter().enumerate() {
        let expected_ops = ops
            .into_iter()
            .enumerate()
            .map(|(op_index, op)| {
                ExpectedOp::new(format!("init:{batch_index}:{op_index}"), op, TxResult::Ok)
            })
            .collect();
        submit_expected_batch(
            &mut sequencer,
            expected_ops,
            &mut reference_batches,
            &mut all_records,
        )
        .await?;
    }

    for expected_ops in small_semantic_batches()? {
        submit_expected_batch(
            &mut sequencer,
            expected_ops,
            &mut reference_batches,
            &mut all_records,
        )
        .await?;
    }

    submit_expected_batch(
        &mut sequencer,
        large_mixed_batch()?,
        &mut reference_batches,
        &mut all_records,
    )
    .await?;

    let mut shard_states = Vec::new();
    for (shard_id, endpoint) in &shard_endpoints {
        let mut client = pb::shard_client::ShardClient::connect(endpoint.clone()).await?;
        let response = client
            .dump_state(pb::DumpStateRequest {})
            .await?
            .into_inner();
        shard_states.push((*shard_id, inode_entries_from_proto(response.entries)?));
    }

    let layout = ShardLayout::new(SHARD_COUNT);
    let expected = reference_execute_batches(&reference_batches);
    let actual = merge_shard_states(&layout, shard_states)?;
    assert_state_matches(&expected, &actual)?;
    assert_tx_results_consistent(&layout, &reference_batches, &all_records)?;

    Ok(())
}

async fn run_submit_tx_driver(
    sequencer_endpoint: String,
    shard_endpoints: BTreeMap<ShardId, String>,
) -> Result<()> {
    wait_for_shards(&shard_endpoints).await?;
    wait_for_sequencer(&sequencer_endpoint).await?;

    let expected_ops = vec![
        expect("submit_tx:mkdir_root", mkdir("/")?, TxResult::Ok),
        expect("submit_tx:mkdir_client", mkdir("/client")?, TxResult::Ok),
        expect(
            "submit_tx:create_file",
            create("/client/file")?,
            TxResult::Ok,
        ),
        expect("submit_tx:stat_file", stat("/client/file")?, TxResult::Ok),
        expect(
            "submit_tx:create_duplicate",
            create("/client/file")?,
            TxResult::AlreadyExists,
        ),
        expect(
            "submit_tx:unlink_file",
            unlink("/client/file")?,
            TxResult::Ok,
        ),
        expect("submit_tx:rmdir_client", rmdir("/client")?, TxResult::Ok),
        expect("submit_tx:final_stat_root", stat("/")?, TxResult::Ok),
    ];

    let layout = ShardLayout::new(SHARD_COUNT);
    let mut sequencer = pb::sequencer_client::SequencerClient::connect(sequencer_endpoint).await?;
    let mut tx_ids = Vec::with_capacity(expected_ops.len());
    let mut ops = Vec::with_capacity(expected_ops.len());

    for (index, expected) in expected_ops.iter().enumerate() {
        let response = sequencer
            .submit_tx(pb::SubmitTxRequest {
                op: Some(fs_op_to_proto(&expected.op)),
            })
            .await?
            .into_inner();
        let expected_tx_id = index as u64 + 1;
        if response.tx_id != expected_tx_id {
            bail!(
                "expected tx_id {}, got {} for {}",
                expected_tx_id,
                response.tx_id,
                expected.label
            );
        }

        let (read_set, write_set) = derive_read_write_set(&expected.op)?;
        let expected_result_shard = layout
            .result_shard_for_sets(&read_set, &write_set)
            .expect("test transaction has a result shard");
        if response.result_shard != expected_result_shard {
            bail!(
                "tx {} ({}) expected result_shard {}, got {}",
                response.tx_id,
                expected.label,
                expected_result_shard,
                response.result_shard
            );
        }
        if matches!(expected.op, FsOp::Stat { .. }) {
            let coordinator = layout
                .read_only_coordinator(&read_set)
                .expect("stat read set has coordinator");
            if response.result_shard != coordinator {
                bail!(
                    "read-only tx {} expected coordinator {}, got {}",
                    response.tx_id,
                    coordinator,
                    response.result_shard
                );
            }
        }

        let mut shard =
            pb::shard_client::ShardClient::connect(shard_endpoints[&response.result_shard].clone())
                .await?;
        let result = shard
            .get_tx_result(pb::GetTxResultRequest {
                tx_id: response.tx_id,
            })
            .await?
            .into_inner();
        assert_ready_result(&expected.label, &result, expected.expected)?;

        let non_result_shard = (response.result_shard + 1) % SHARD_COUNT;
        let mut non_result_client =
            pb::shard_client::ShardClient::connect(shard_endpoints[&non_result_shard].clone())
                .await?;
        let non_result = non_result_client
            .get_tx_result(pb::GetTxResultRequest {
                tx_id: response.tx_id,
            })
            .await?
            .into_inner();
        if non_result.status != pb::TxResultStatus::NotResponsible as i32 {
            bail!(
                "tx {} ({}) expected NOT_RESPONSIBLE from shard {}, got status {}",
                response.tx_id,
                expected.label,
                non_result_shard,
                non_result.status
            );
        }

        tx_ids.push(response.tx_id);
        ops.push(expected.op.clone());
    }

    let mut shard_states = Vec::new();
    for (shard_id, endpoint) in &shard_endpoints {
        let mut client = pb::shard_client::ShardClient::connect(endpoint.clone()).await?;
        let response = client
            .dump_state(pb::DumpStateRequest {})
            .await?
            .into_inner();
        shard_states.push((*shard_id, inode_entries_from_proto(response.entries)?));
    }

    let reference_batches = vec![reference_batch(1, tx_ids, ops)?];
    let expected = reference_execute_batches(&reference_batches);
    let actual = merge_shard_states(&layout, shard_states)?;
    assert_state_matches(&expected, &actual)?;

    Ok(())
}

async fn run_mdtest_like_cluster(
    scheduler: BenchmarkScheduler,
    mode: MdtestMode,
    config: MdtestConfig,
    network: u8,
) -> Result<ModeSummary> {
    let mut shard_endpoints = BTreeMap::new();
    for shard_id in 0..SHARD_COUNT {
        let ip = mdtest_shard_ip(network, shard_id);
        shard_endpoints.insert(shard_id, endpoint(ip, SHARD_PORT));
    }
    for shard_id in 0..SHARD_COUNT {
        let ip = mdtest_shard_ip(network, shard_id);
        start_shard_node_with_name(
            format!(
                "mdtest-{}-{}-shard-{shard_id}",
                scheduler.name(),
                mode.name()
            ),
            shard_id,
            ip,
            shard_endpoints.clone(),
            scheduler.scheduler_kind(),
        );
    }

    let sequencer_ip = mdtest_node_ip(network, 100);
    let sequencer_endpoint = endpoint(sequencer_ip, SEQUENCER_PORT);
    start_sequencer_node_with_config_and_name(
        format!("mdtest-{}-{}-sequencer", scheduler.name(), mode.name()),
        sequencer_ip,
        shard_endpoints.clone(),
        config.batch_size,
        SequencerConfig::default_batch_flush_interval(),
    );

    let driver = madsim::runtime::Handle::current()
        .create_node()
        .name(format!(
            "mdtest-{}-{}-driver",
            scheduler.name(),
            mode.name()
        ))
        .ip(mdtest_node_ip(network, 200))
        .build();
    driver
        .spawn(run_mdtest_like_client_driver(
            scheduler,
            mode,
            sequencer_endpoint,
            shard_endpoints,
            config,
        ))
        .await
        .expect("mdtest-like driver task panicked")
}

async fn run_mdtest_like_client_driver(
    scheduler: BenchmarkScheduler,
    mode: MdtestMode,
    sequencer_endpoint: String,
    shard_endpoints: BTreeMap<ShardId, String>,
    config: MdtestConfig,
) -> Result<ModeSummary> {
    wait_for_shards(&shard_endpoints).await?;
    wait_for_sequencer(&sequencer_endpoint).await?;

    let mut sequencer =
        pb::sequencer_client::SequencerClient::connect(sequencer_endpoint.clone()).await?;
    submit_client_tx(
        &mut sequencer,
        &shard_endpoints,
        "mdtest:setup:mkdir_root",
        mkdir("/")?,
        TxResult::Ok,
    )
    .await?;

    let summary = run_mdtest_mode(
        scheduler,
        mode,
        &config,
        &sequencer_endpoint,
        &shard_endpoints,
    )
    .await?;

    submit_client_tx(
        &mut sequencer,
        &shard_endpoints,
        "mdtest:sanity:stat_root",
        stat("/")?,
        TxResult::Ok,
    )
    .await?;
    submit_client_tx(
        &mut sequencer,
        &shard_endpoints,
        &format!("mdtest:sanity:stat_{}_removed", mode.name()),
        stat(mode.root())?,
        TxResult::NotFound,
    )
    .await?;

    Ok(summary)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BenchmarkScheduler {
    Calvin,
    Scc,
}

impl BenchmarkScheduler {
    fn name(self) -> &'static str {
        match self {
            Self::Calvin => "calvin",
            Self::Scc => "scc",
        }
    }

    fn display_name(self) -> &'static str {
        match self {
            Self::Calvin => "Calvin",
            Self::Scc => "SCC",
        }
    }

    fn scheduler_kind(self) -> SchedulerKind {
        match self {
            Self::Calvin => SchedulerKind::CalvinLocking,
            Self::Scc => SchedulerKind::SccOnline,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MdtestMode {
    Private,
    Public,
}

impl MdtestMode {
    fn name(self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::Public => "public",
        }
    }

    fn root(self) -> &'static str {
        match self {
            Self::Private => MDTEST_PRIVATE_ROOT,
            Self::Public => MDTEST_PUBLIC_ROOT,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct MdtestConfig {
    client_count: usize,
    dirs_per_client: usize,
    files_per_client: usize,
    batch_size: usize,
    show_ranks: bool,
}

impl MdtestConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            client_count: read_positive_usize_env(
                "CALVINFS_MDTEST_CLIENTS",
                MDTEST_DEFAULT_CLIENT_COUNT,
            )?,
            dirs_per_client: read_positive_usize_env(
                "CALVINFS_MDTEST_DIRS_PER_CLIENT",
                MDTEST_DEFAULT_DIRS_PER_CLIENT,
            )?,
            files_per_client: read_positive_usize_env(
                "CALVINFS_MDTEST_FILES_PER_CLIENT",
                MDTEST_DEFAULT_FILES_PER_CLIENT,
            )?,
            batch_size: read_positive_usize_env("CALVINFS_MDTEST_BATCH_SIZE", BATCH_SIZE)?,
            show_ranks: read_bool_env("CALVINFS_MDTEST_SHOW_RANKS", false)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MdtestPhase {
    DirectoryCreation,
    DirectoryStat,
    FileCreation,
    FileStat,
    FileRemoval,
    DirectoryRemoval,
}

impl MdtestPhase {
    fn all() -> [Self; 6] {
        [
            Self::DirectoryCreation,
            Self::DirectoryStat,
            Self::FileCreation,
            Self::FileStat,
            Self::FileRemoval,
            Self::DirectoryRemoval,
        ]
    }

    fn name(self) -> &'static str {
        match self {
            Self::DirectoryCreation => "Directory creation",
            Self::DirectoryStat => "Directory stat",
            Self::FileCreation => "File creation",
            Self::FileStat => "File stat",
            Self::FileRemoval => "File removal",
            Self::DirectoryRemoval => "Directory removal",
        }
    }

    fn operations_for_rank(
        self,
        mode: MdtestMode,
        config: &MdtestConfig,
        rank: usize,
    ) -> Result<Vec<FsOp>> {
        let mut ops = Vec::new();
        match self {
            Self::DirectoryCreation => {
                ops.reserve(config.dirs_per_client);
                for item in 0..config.dirs_per_client {
                    ops.push(mkdir(&mdtest_dir_item(mode, rank, item))?);
                }
            }
            Self::DirectoryStat => {
                ops.reserve(config.dirs_per_client);
                for item in 0..config.dirs_per_client {
                    ops.push(stat(&mdtest_dir_item(mode, rank, item))?);
                }
            }
            Self::DirectoryRemoval => {
                ops.reserve(config.dirs_per_client);
                for item in 0..config.dirs_per_client {
                    ops.push(rmdir(&mdtest_dir_item(mode, rank, item))?);
                }
            }
            Self::FileCreation => {
                ops.reserve(config.files_per_client);
                for item in 0..config.files_per_client {
                    ops.push(create(&mdtest_file_item(mode, rank, item))?);
                }
            }
            Self::FileStat => {
                ops.reserve(config.files_per_client);
                for item in 0..config.files_per_client {
                    ops.push(stat(&mdtest_file_item(mode, rank, item))?);
                }
            }
            Self::FileRemoval => {
                ops.reserve(config.files_per_client);
                for item in 0..config.files_per_client {
                    ops.push(unlink(&mdtest_file_item(mode, rank, item))?);
                }
            }
        }
        Ok(ops)
    }
}

#[derive(Clone, Debug)]
struct SubmittedClientTx {
    label: String,
    expected: TxResult,
    tx_id: u64,
    result_shard: ShardId,
}

#[derive(Clone, Debug)]
struct RankPhaseResult {
    rank: usize,
    items: usize,
    time_before_barrier: Duration,
    time: Duration,
}

#[derive(Clone, Debug)]
struct PhaseSummary {
    phase: MdtestPhase,
    ranks: Vec<RankPhaseResult>,
    aggregate_ops_per_sec: f64,
    aggregate_ms_per_op: f64,
}

#[derive(Clone, Debug)]
struct ModeSummary {
    scheduler: BenchmarkScheduler,
    mode: MdtestMode,
    phases: Vec<PhaseSummary>,
}

#[derive(Clone, Copy, Debug)]
struct FloatStats {
    max: f64,
    min: f64,
    mean: f64,
}

async fn run_mdtest_mode(
    scheduler: BenchmarkScheduler,
    mode: MdtestMode,
    config: &MdtestConfig,
    sequencer_endpoint: &str,
    shard_endpoints: &BTreeMap<ShardId, String>,
) -> Result<ModeSummary> {
    let mut sequencer =
        pb::sequencer_client::SequencerClient::connect(sequencer_endpoint.to_string()).await?;
    setup_mdtest_mode(&mut sequencer, shard_endpoints, mode, config).await?;

    let mut phases = Vec::new();
    for phase in MdtestPhase::all() {
        let ranks =
            run_mdtest_phase(phase, mode, config, sequencer_endpoint, shard_endpoints).await?;
        phases.push(PhaseSummary::new(phase, ranks));
    }

    cleanup_mdtest_mode(&mut sequencer, shard_endpoints, mode, config).await?;
    Ok(ModeSummary {
        scheduler,
        mode,
        phases,
    })
}

async fn run_mdtest_phase(
    phase: MdtestPhase,
    mode: MdtestMode,
    config: &MdtestConfig,
    sequencer_endpoint: &str,
    shard_endpoints: &BTreeMap<ShardId, String>,
) -> Result<Vec<RankPhaseResult>> {
    let start_barrier = Arc::new(tokio::sync::Barrier::new(config.client_count));
    let end_barrier = Arc::new(tokio::sync::Barrier::new(config.client_count));
    let mut handles = Vec::with_capacity(config.client_count);

    for rank in 0..config.client_count {
        let config = *config;
        let sequencer_endpoint = sequencer_endpoint.to_string();
        let shard_endpoints = shard_endpoints.clone();
        let start_barrier = start_barrier.clone();
        let end_barrier = end_barrier.clone();
        handles.push(tokio::spawn(async move {
            run_mdtest_rank_phase(
                phase,
                mode,
                config,
                rank,
                sequencer_endpoint,
                shard_endpoints,
                start_barrier,
                end_barrier,
            )
            .await
        }));
    }

    let mut results = Vec::with_capacity(config.client_count);
    for handle in handles {
        results.push(handle.await.expect("mdtest client task panicked")?);
    }
    results.sort_by_key(|result| result.rank);
    Ok(results)
}

async fn run_mdtest_rank_phase(
    phase: MdtestPhase,
    mode: MdtestMode,
    config: MdtestConfig,
    rank: usize,
    sequencer_endpoint: String,
    shard_endpoints: BTreeMap<ShardId, String>,
    start_barrier: Arc<tokio::sync::Barrier>,
    end_barrier: Arc<tokio::sync::Barrier>,
) -> Result<RankPhaseResult> {
    let ops = phase.operations_for_rank(mode, &config, rank)?;
    let mut sequencer = pb::sequencer_client::SequencerClient::connect(sequencer_endpoint).await?;
    let mut shard_clients = connect_shard_clients(&shard_endpoints).await?;

    start_barrier.wait().await;
    let started = Instant::now();

    let mut submitted = Vec::with_capacity(ops.len());
    for (op_index, op) in ops.into_iter().enumerate() {
        let label = format!(
            "mdtest:{}:{}:rank_{rank}:op_{op_index}",
            mode.name(),
            phase.name()
        );
        let response = sequencer
            .submit_tx(pb::SubmitTxRequest {
                op: Some(fs_op_to_proto(&op)),
            })
            .await?
            .into_inner();
        submitted.push(SubmittedClientTx {
            label,
            expected: TxResult::Ok,
            tx_id: response.tx_id,
            result_shard: response.result_shard,
        });
    }

    for tx in &submitted {
        let shard = shard_clients
            .get_mut(&tx.result_shard)
            .expect("result shard client should be connected");
        let result = shard
            .get_tx_result(pb::GetTxResultRequest { tx_id: tx.tx_id })
            .await?
            .into_inner();
        assert_ready_result(&tx.label, &result, tx.expected)?;
    }

    let time_before_barrier = started.elapsed();
    end_barrier.wait().await;
    let time = started.elapsed();

    Ok(RankPhaseResult {
        rank,
        items: submitted.len(),
        time_before_barrier,
        time,
    })
}

async fn setup_mdtest_mode(
    sequencer: &mut pb::sequencer_client::SequencerClient<tonic::transport::Channel>,
    shard_endpoints: &BTreeMap<ShardId, String>,
    mode: MdtestMode,
    config: &MdtestConfig,
) -> Result<()> {
    submit_client_tx(
        sequencer,
        shard_endpoints,
        &format!("mdtest:{}:setup:mkdir_root", mode.name()),
        mkdir(mode.root())?,
        TxResult::Ok,
    )
    .await?;

    match mode {
        MdtestMode::Private => {
            for rank in 0..config.client_count {
                submit_client_tx(
                    sequencer,
                    shard_endpoints,
                    &format!("mdtest:private:setup:mkdir_client_{rank}"),
                    mkdir(&private_client_root(rank))?,
                    TxResult::Ok,
                )
                .await?;
                submit_client_tx(
                    sequencer,
                    shard_endpoints,
                    &format!("mdtest:private:setup:mkdir_client_{rank}_dirs"),
                    mkdir(&private_dir_parent(rank))?,
                    TxResult::Ok,
                )
                .await?;
                submit_client_tx(
                    sequencer,
                    shard_endpoints,
                    &format!("mdtest:private:setup:mkdir_client_{rank}_files"),
                    mkdir(&private_file_parent(rank))?,
                    TxResult::Ok,
                )
                .await?;
            }
        }
        MdtestMode::Public => {
            submit_client_tx(
                sequencer,
                shard_endpoints,
                "mdtest:public:setup:mkdir_dirs",
                mkdir(&public_dir_parent())?,
                TxResult::Ok,
            )
            .await?;
            submit_client_tx(
                sequencer,
                shard_endpoints,
                "mdtest:public:setup:mkdir_files",
                mkdir(&public_file_parent())?,
                TxResult::Ok,
            )
            .await?;
        }
    }

    Ok(())
}

async fn cleanup_mdtest_mode(
    sequencer: &mut pb::sequencer_client::SequencerClient<tonic::transport::Channel>,
    shard_endpoints: &BTreeMap<ShardId, String>,
    mode: MdtestMode,
    config: &MdtestConfig,
) -> Result<()> {
    match mode {
        MdtestMode::Private => {
            for rank in 0..config.client_count {
                submit_client_tx(
                    sequencer,
                    shard_endpoints,
                    &format!("mdtest:private:cleanup:rmdir_client_{rank}_files"),
                    rmdir(&private_file_parent(rank))?,
                    TxResult::Ok,
                )
                .await?;
                submit_client_tx(
                    sequencer,
                    shard_endpoints,
                    &format!("mdtest:private:cleanup:rmdir_client_{rank}_dirs"),
                    rmdir(&private_dir_parent(rank))?,
                    TxResult::Ok,
                )
                .await?;
                submit_client_tx(
                    sequencer,
                    shard_endpoints,
                    &format!("mdtest:private:cleanup:rmdir_client_{rank}"),
                    rmdir(&private_client_root(rank))?,
                    TxResult::Ok,
                )
                .await?;
            }
        }
        MdtestMode::Public => {
            submit_client_tx(
                sequencer,
                shard_endpoints,
                "mdtest:public:cleanup:rmdir_files",
                rmdir(&public_file_parent())?,
                TxResult::Ok,
            )
            .await?;
            submit_client_tx(
                sequencer,
                shard_endpoints,
                "mdtest:public:cleanup:rmdir_dirs",
                rmdir(&public_dir_parent())?,
                TxResult::Ok,
            )
            .await?;
        }
    }

    submit_client_tx(
        sequencer,
        shard_endpoints,
        &format!("mdtest:{}:cleanup:rmdir_root", mode.name()),
        rmdir(mode.root())?,
        TxResult::Ok,
    )
    .await?;
    Ok(())
}

async fn submit_client_tx(
    sequencer: &mut pb::sequencer_client::SequencerClient<tonic::transport::Channel>,
    shard_endpoints: &BTreeMap<ShardId, String>,
    label: &str,
    op: FsOp,
    expected: TxResult,
) -> Result<SubmittedClientTx> {
    let response = sequencer
        .submit_tx(pb::SubmitTxRequest {
            op: Some(fs_op_to_proto(&op)),
        })
        .await?
        .into_inner();
    let mut shard =
        pb::shard_client::ShardClient::connect(shard_endpoints[&response.result_shard].clone())
            .await?;
    let result = shard
        .get_tx_result(pb::GetTxResultRequest {
            tx_id: response.tx_id,
        })
        .await?
        .into_inner();
    assert_ready_result(label, &result, expected)?;
    Ok(SubmittedClientTx {
        label: label.to_string(),
        expected,
        tx_id: response.tx_id,
        result_shard: response.result_shard,
    })
}

async fn connect_shard_clients(
    shard_endpoints: &BTreeMap<ShardId, String>,
) -> Result<BTreeMap<ShardId, pb::shard_client::ShardClient<tonic::transport::Channel>>> {
    let mut clients = BTreeMap::new();
    for (shard_id, endpoint) in shard_endpoints {
        clients.insert(
            *shard_id,
            pb::shard_client::ShardClient::connect(endpoint.clone()).await?,
        );
    }
    Ok(clients)
}

impl PhaseSummary {
    fn new(phase: MdtestPhase, ranks: Vec<RankPhaseResult>) -> Self {
        let total_items: usize = ranks.iter().map(|rank| rank.items).sum();
        let max_time = ranks
            .iter()
            .map(|rank| rank.time.as_secs_f64())
            .fold(0.0, f64::max);
        let aggregate_ops_per_sec = if total_items == 0 || max_time == 0.0 {
            0.0
        } else {
            total_items as f64 / max_time
        };
        let aggregate_ms_per_op = if aggregate_ops_per_sec == 0.0 {
            0.0
        } else {
            1000.0 / aggregate_ops_per_sec
        };
        Self {
            phase,
            ranks,
            aggregate_ops_per_sec,
            aggregate_ms_per_op,
        }
    }

    fn rank_rate_stats(&self) -> FloatStats {
        float_stats(
            self.ranks
                .iter()
                .map(|rank| ops_per_sec(rank.items, rank.time_before_barrier)),
        )
    }

    fn rank_time_stats(&self) -> FloatStats {
        float_stats(
            self.ranks
                .iter()
                .map(|rank| ms_per_op(rank.items, rank.time_before_barrier)),
        )
    }
}

fn print_mode_summary(summary: &ModeSummary, show_ranks: bool) {
    println!(
        "\n{} {} mode SUMMARY rate (in ops/sec):",
        summary.scheduler.display_name(),
        summary.mode.name().to_uppercase()
    );
    println!(
        "{:<22} {:>14} {:>14} {:>14}    {:>14} {:>14} {:>14} {:>14}",
        "Operation",
        "Rank Max",
        "Rank Min",
        "Rank Mean",
        "Iter Max",
        "Iter Min",
        "Iter Mean",
        "Std Dev"
    );
    for phase in &summary.phases {
        let stats = phase.rank_rate_stats();
        print_summary_row(
            phase.phase.name(),
            stats,
            phase.aggregate_ops_per_sec,
            phase.aggregate_ops_per_sec,
            phase.aggregate_ops_per_sec,
            0.0,
        );
    }

    println!(
        "\n{} {} mode SUMMARY time (in ms/op):",
        summary.scheduler.display_name(),
        summary.mode.name().to_uppercase()
    );
    println!(
        "{:<22} {:>14} {:>14} {:>14}    {:>14} {:>14} {:>14} {:>14}",
        "Operation",
        "Rank Max",
        "Rank Min",
        "Rank Mean",
        "Iter Max",
        "Iter Min",
        "Iter Mean",
        "Std Dev"
    );
    for phase in &summary.phases {
        let stats = phase.rank_time_stats();
        print_summary_row(
            phase.phase.name(),
            stats,
            phase.aggregate_ms_per_op,
            phase.aggregate_ms_per_op,
            phase.aggregate_ms_per_op,
            0.0,
        );
    }

    if show_ranks {
        println!(
            "\n{} {} mode per-rank details:",
            summary.scheduler.display_name(),
            summary.mode.name().to_uppercase()
        );
        println!(
            "{:<22} {:>6} {:>8} {:>14} {:>14} {:>14}",
            "Operation", "Rank", "Items", "Ops/sec", "ms/op", "Elapsed ms"
        );
        for phase in &summary.phases {
            for rank in &phase.ranks {
                println!(
                    "{:<22} {:>6} {:>8} {:>14.3} {:>14.6} {:>14.3}",
                    phase.phase.name(),
                    rank.rank,
                    rank.items,
                    ops_per_sec(rank.items, rank.time_before_barrier),
                    ms_per_op(rank.items, rank.time_before_barrier),
                    rank.time_before_barrier.as_secs_f64() * 1000.0
                );
            }
        }
    }
}

fn print_summary_row(
    operation: &str,
    rank_stats: FloatStats,
    iter_max: f64,
    iter_min: f64,
    iter_mean: f64,
    std_dev: f64,
) {
    println!(
        "{:<22} {:>14.3} {:>14.3} {:>14.3}    {:>14.3} {:>14.3} {:>14.3} {:>14.3}",
        operation,
        rank_stats.max,
        rank_stats.min,
        rank_stats.mean,
        iter_max,
        iter_min,
        iter_mean,
        std_dev
    );
}

fn print_private_public_comparison(
    scheduler: BenchmarkScheduler,
    private_summary: &ModeSummary,
    public_summary: &ModeSummary,
) {
    println!("\n{} Private/Public comparison:", scheduler.display_name());
    println!(
        "{:<22} {:>16} {:>16} {:>16}",
        "Operation", "Private ops/s", "Public ops/s", "Public/Private"
    );
    for private_phase in &private_summary.phases {
        let public_phase = public_summary
            .phases
            .iter()
            .find(|phase| phase.phase == private_phase.phase)
            .expect("public summary should contain same phase");
        let ratio = if private_phase.aggregate_ops_per_sec == 0.0 {
            0.0
        } else {
            public_phase.aggregate_ops_per_sec / private_phase.aggregate_ops_per_sec
        };
        println!(
            "{:<22} {:>16.3} {:>16.3} {:>16.3}",
            private_phase.phase.name(),
            private_phase.aggregate_ops_per_sec,
            public_phase.aggregate_ops_per_sec,
            ratio
        );
    }
}

fn print_scheduler_comparison(
    mode: MdtestMode,
    calvin_summary: &ModeSummary,
    scc_summary: &ModeSummary,
) {
    println!(
        "\n{} SCC/Calvin comparison:",
        mode.name().to_ascii_uppercase()
    );
    println!(
        "{:<22} {:>16} {:>16} {:>16}",
        "Operation", "Calvin ops/s", "SCC ops/s", "SCC/Calvin"
    );
    for calvin_phase in &calvin_summary.phases {
        let scc_phase = scc_summary
            .phases
            .iter()
            .find(|phase| phase.phase == calvin_phase.phase)
            .expect("SCC summary should contain same phase");
        let ratio = if calvin_phase.aggregate_ops_per_sec == 0.0 {
            0.0
        } else {
            scc_phase.aggregate_ops_per_sec / calvin_phase.aggregate_ops_per_sec
        };
        println!(
            "{:<22} {:>16.3} {:>16.3} {:>16.3}",
            calvin_phase.phase.name(),
            calvin_phase.aggregate_ops_per_sec,
            scc_phase.aggregate_ops_per_sec,
            ratio
        );
    }
}

fn read_positive_usize_env(name: &str, default: usize) -> Result<usize> {
    let Some(value) = env::var_os(name) else {
        return Ok(default);
    };
    let value = value.to_string_lossy();
    let parsed = value
        .parse::<usize>()
        .map_err(|err| anyhow::anyhow!("{name} must be a positive integer: {err}"))?;
    if parsed == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(parsed)
}

fn read_bool_env(name: &str, default: bool) -> Result<bool> {
    let Some(value) = env::var_os(name) else {
        return Ok(default);
    };
    match value.to_string_lossy().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => bail!("{name} must be one of 1/0/true/false/yes/no/on/off, got {other}"),
    }
}

fn float_stats(values: impl IntoIterator<Item = f64>) -> FloatStats {
    let mut count = 0usize;
    let mut sum = 0.0;
    let mut max = f64::NEG_INFINITY;
    let mut min = f64::INFINITY;
    for value in values {
        count += 1;
        sum += value;
        max = max.max(value);
        min = min.min(value);
    }
    if count == 0 {
        return FloatStats {
            max: 0.0,
            min: 0.0,
            mean: 0.0,
        };
    }
    FloatStats {
        max,
        min,
        mean: sum / count as f64,
    }
}

fn ops_per_sec(items: usize, duration: Duration) -> f64 {
    let seconds = duration.as_secs_f64();
    if items == 0 || seconds == 0.0 {
        0.0
    } else {
        items as f64 / seconds
    }
}

fn ms_per_op(items: usize, duration: Duration) -> f64 {
    let seconds = duration.as_secs_f64();
    if items == 0 {
        0.0
    } else {
        seconds * 1000.0 / items as f64
    }
}

fn private_client_root(rank: usize) -> String {
    format!("{MDTEST_PRIVATE_ROOT}/client_{rank}")
}

fn private_dir_parent(rank: usize) -> String {
    format!("{}/dirs", private_client_root(rank))
}

fn private_file_parent(rank: usize) -> String {
    format!("{}/files", private_client_root(rank))
}

fn public_dir_parent() -> String {
    format!("{MDTEST_PUBLIC_ROOT}/dirs")
}

fn public_file_parent() -> String {
    format!("{MDTEST_PUBLIC_ROOT}/files")
}

fn mdtest_dir_item(mode: MdtestMode, rank: usize, item: usize) -> String {
    match mode {
        MdtestMode::Private => format!("{}/dir_{item}", private_dir_parent(rank)),
        MdtestMode::Public => format!("{}/dir_c{rank}_{item}", public_dir_parent()),
    }
}

fn mdtest_file_item(mode: MdtestMode, rank: usize, item: usize) -> String {
    match mode {
        MdtestMode::Private => format!("{}/file_{item}", private_file_parent(rank)),
        MdtestMode::Public => format!("{}/file_c{rank}_{item}", public_file_parent()),
    }
}

fn assert_ready_result(
    label: &str,
    response: &pb::GetTxResultResponse,
    expected: TxResult,
) -> Result<()> {
    if response.status != pb::TxResultStatus::Ready as i32 {
        bail!(
            "tx {} ({}) expected READY, got status {}",
            response.tx_id,
            label,
            response.status
        );
    }
    let actual = tx_result_from_i32(response.result)?;
    if actual != expected {
        bail!(
            "tx {} ({}) expected {:?}, got {:?}",
            response.tx_id,
            label,
            expected,
            actual
        );
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct ExpectedOp {
    op: FsOp,
    expected: TxResult,
    label: String,
}

impl ExpectedOp {
    fn new(label: impl Into<String>, op: FsOp, expected: TxResult) -> Self {
        Self {
            op,
            expected,
            label: label.into(),
        }
    }
}

async fn submit_expected_batch(
    sequencer: &mut pb::sequencer_client::SequencerClient<tonic::transport::Channel>,
    expected_ops: Vec<ExpectedOp>,
    reference_batches: &mut Vec<Batch>,
    all_records: &mut Vec<TxResultRecord>,
) -> Result<()> {
    let ops: Vec<FsOp> = expected_ops
        .iter()
        .map(|expected| expected.op.clone())
        .collect();
    let response = sequencer
        .submit_batch(pb::SubmitBatchRequest {
            ops: ops.iter().map(fs_op_to_proto).collect(),
        })
        .await?
        .into_inner();
    assert_eq!(response.tx_ids.len(), expected_ops.len());

    let mut expected_by_tx = BTreeMap::new();
    for (tx_id, expected) in response.tx_ids.iter().copied().zip(expected_ops.iter()) {
        expected_by_tx.insert(tx_id, (&expected.label, expected.expected));
    }

    let records = tx_result_records_from_proto(response.tx_results)?;
    let mut seen = BTreeSet::new();
    for record in &records {
        let Some((label, expected)) = expected_by_tx.get(&record.tx_id) else {
            bail!(
                "unexpected tx result for tx {} on shard {}",
                record.tx_id,
                record.shard_id
            );
        };
        if record.result != *expected {
            bail!(
                "tx {} ({}) on shard {} expected {:?}, got {:?}",
                record.tx_id,
                label,
                record.shard_id,
                expected,
                record.result
            );
        }
        seen.insert(record.tx_id);
    }
    for tx_id in &response.tx_ids {
        if !seen.contains(tx_id) {
            let (label, _) = expected_by_tx
                .get(tx_id)
                .expect("tx_id should have expected result");
            bail!("tx {} ({}) returned no active result", tx_id, label);
        }
    }

    all_records.extend(records);
    reference_batches.push(reference_batch(response.batch_id, response.tx_ids, ops)?);
    Ok(())
}

fn small_semantic_batches() -> Result<Vec<Vec<ExpectedOp>>> {
    Ok(vec![
        vec![
            expect(
                "small_setup:mkdir_semantic",
                mkdir("/semantic")?,
                TxResult::Ok,
            ),
            expect("small_setup:mkdir_a", mkdir("/semantic/a")?, TxResult::Ok),
            expect("small_setup:mkdir_b", mkdir("/semantic/b")?, TxResult::Ok),
            expect(
                "small_setup:create_file",
                create("/semantic/a/file")?,
                TxResult::Ok,
            ),
            expect(
                "small_setup:create_other",
                create("/semantic/a/other")?,
                TxResult::Ok,
            ),
            expect(
                "small_setup:create_parent_file",
                create("/semantic/a/parent_file")?,
                TxResult::Ok,
            ),
            expect(
                "small_setup:mkdir_empty_dir",
                mkdir("/semantic/a/empty_dir")?,
                TxResult::Ok,
            ),
            expect(
                "small_setup:mkdir_nonempty_dir",
                mkdir("/semantic/a/nonempty_dir")?,
                TxResult::Ok,
            ),
            expect(
                "small_setup:create_nonempty_child",
                create("/semantic/a/nonempty_dir/child")?,
                TxResult::Ok,
            ),
            expect(
                "small_setup:stat_file",
                stat("/semantic/a/file")?,
                TxResult::Ok,
            ),
            expect(
                "small_setup:stat_missing",
                stat("/semantic/missing")?,
                TxResult::NotFound,
            ),
        ],
        vec![
            expect(
                "small_errors:mkdir_duplicate",
                mkdir("/semantic/a")?,
                TxResult::AlreadyExists,
            ),
            expect(
                "small_errors:mkdir_missing_parent",
                mkdir("/semantic/missing_parent/child")?,
                TxResult::NotFound,
            ),
            expect(
                "small_errors:mkdir_parent_is_file",
                mkdir("/semantic/a/parent_file/child_dir")?,
                TxResult::NotDirectory,
            ),
            expect(
                "small_errors:create_duplicate",
                create("/semantic/a/file")?,
                TxResult::AlreadyExists,
            ),
            expect(
                "small_errors:create_missing_parent",
                create("/semantic/missing_parent/file")?,
                TxResult::NotFound,
            ),
            expect(
                "small_errors:create_parent_is_file",
                create("/semantic/a/parent_file/child")?,
                TxResult::NotDirectory,
            ),
            expect("small_errors:create_root", create("/")?, TxResult::Invalid),
            expect(
                "small_errors:stat_existing",
                stat("/semantic/a/file")?,
                TxResult::Ok,
            ),
            expect(
                "small_errors:stat_missing",
                stat("/semantic/nope")?,
                TxResult::NotFound,
            ),
            expect(
                "small_errors:unlink_file",
                unlink("/semantic/a/other")?,
                TxResult::Ok,
            ),
            expect(
                "small_errors:unlink_missing",
                unlink("/semantic/a/other")?,
                TxResult::NotFound,
            ),
            expect(
                "small_errors:unlink_directory",
                unlink("/semantic/a/nonempty_dir")?,
                TxResult::Invalid,
            ),
            expect("small_errors:unlink_root", unlink("/")?, TxResult::Invalid),
            expect(
                "small_errors:rmdir_empty",
                rmdir("/semantic/a/empty_dir")?,
                TxResult::Ok,
            ),
            expect(
                "small_errors:rmdir_nonempty",
                rmdir("/semantic/a/nonempty_dir")?,
                TxResult::DirectoryNotEmpty,
            ),
            expect(
                "small_errors:rmdir_file",
                rmdir("/semantic/a/file")?,
                TxResult::NotDirectory,
            ),
            expect("small_errors:rmdir_root", rmdir("/")?, TxResult::Invalid),
        ],
        vec![
            expect(
                "small_rename:create_to_rename",
                create("/semantic/a/to_rename")?,
                TxResult::Ok,
            ),
            expect(
                "small_rename:rename_file",
                rename("/semantic/a/to_rename", "/semantic/b/renamed_file")?,
                TxResult::Ok,
            ),
            expect(
                "small_rename:stat_renamed_file",
                stat("/semantic/b/renamed_file")?,
                TxResult::Ok,
            ),
            expect(
                "small_rename:rename_dst_exists",
                rename("/semantic/b/renamed_file", "/semantic/a/file")?,
                TxResult::AlreadyExists,
            ),
            expect(
                "small_rename:rename_missing_src",
                rename("/semantic/missing_src", "/semantic/b/missing_dst")?,
                TxResult::NotFound,
            ),
            expect(
                "small_rename:rename_nonempty_dir",
                rename("/semantic/a/nonempty_dir", "/semantic/b/nonempty_moved")?,
                TxResult::DirectoryNotEmpty,
            ),
            expect(
                "small_rename:mkdir_empty_rename_src",
                mkdir("/semantic/a/rename_empty_dir")?,
                TxResult::Ok,
            ),
            expect(
                "small_rename:rename_empty_dir",
                rename(
                    "/semantic/a/rename_empty_dir",
                    "/semantic/b/renamed_empty_dir",
                )?,
                TxResult::Ok,
            ),
            expect(
                "small_rename:rename_self",
                rename(
                    "/semantic/b/renamed_empty_dir",
                    "/semantic/b/renamed_empty_dir",
                )?,
                TxResult::Invalid,
            ),
            expect(
                "small_rename:rename_root_src",
                rename("/", "/semantic/root_move")?,
                TxResult::Invalid,
            ),
            expect(
                "small_rename:rename_root_dst",
                rename("/semantic/a/file", "/")?,
                TxResult::Invalid,
            ),
            expect(
                "small_rename:rename_dst_parent_is_file",
                rename("/semantic/a/file", "/semantic/a/parent_file/child_dst")?,
                TxResult::NotDirectory,
            ),
        ],
    ])
}

fn large_mixed_batch() -> Result<Vec<ExpectedOp>> {
    let mut ops = Vec::with_capacity(BATCH_SIZE);
    for case_id in 0..32 {
        ops.extend(mixed_case(case_id)?);
    }
    assert_eq!(ops.len(), BATCH_SIZE);
    Ok(ops)
}

fn mixed_case(case_id: usize) -> Result<Vec<ExpectedOp>> {
    let src_dir = case_id;
    let dst_dir = case_id + 128;
    let base = format!("/dir_{src_dir}/mix_case_{case_id}");
    let dst = format!("/dir_{dst_dir}/mix_case_{case_id}_renamed");
    Ok(vec![
        expect(
            format!("mixed:{case_id}:mkdir_base"),
            mkdir(&base)?,
            TxResult::Ok,
        ),
        expect(
            format!("mixed:{case_id}:create_file"),
            create(&format!("{base}/file"))?,
            TxResult::Ok,
        ),
        expect(
            format!("mixed:{case_id}:stat_file"),
            stat(&format!("{base}/file"))?,
            TxResult::Ok,
        ),
        expect(
            format!("mixed:{case_id}:create_duplicate"),
            create(&format!("{base}/file"))?,
            TxResult::AlreadyExists,
        ),
        expect(
            format!("mixed:{case_id}:mkdir_parent_is_file"),
            mkdir(&format!("{base}/file/child_dir"))?,
            TxResult::NotDirectory,
        ),
        expect(
            format!("mixed:{case_id}:create_missing_parent"),
            create(&format!("{base}/missing_parent/file"))?,
            TxResult::NotFound,
        ),
        expect(
            format!("mixed:{case_id}:mkdir_empty_dir"),
            mkdir(&format!("{base}/empty_dir"))?,
            TxResult::Ok,
        ),
        expect(
            format!("mixed:{case_id}:rmdir_empty_dir"),
            rmdir(&format!("{base}/empty_dir"))?,
            TxResult::Ok,
        ),
        expect(
            format!("mixed:{case_id}:mkdir_nonempty_dir"),
            mkdir(&format!("{base}/nonempty_dir"))?,
            TxResult::Ok,
        ),
        expect(
            format!("mixed:{case_id}:create_nonempty_child"),
            create(&format!("{base}/nonempty_dir/child"))?,
            TxResult::Ok,
        ),
        expect(
            format!("mixed:{case_id}:rmdir_nonempty_dir"),
            rmdir(&format!("{base}/nonempty_dir"))?,
            TxResult::DirectoryNotEmpty,
        ),
        expect(
            format!("mixed:{case_id}:unlink_nonempty_child"),
            unlink(&format!("{base}/nonempty_dir/child"))?,
            TxResult::Ok,
        ),
        expect(
            format!("mixed:{case_id}:rmdir_now_empty_dir"),
            rmdir(&format!("{base}/nonempty_dir"))?,
            TxResult::Ok,
        ),
        expect(
            format!("mixed:{case_id}:create_rename_src"),
            create(&format!("{base}/to_rename"))?,
            TxResult::Ok,
        ),
        expect(
            format!("mixed:{case_id}:rename_file"),
            rename(&format!("{base}/to_rename"), &dst)?,
            TxResult::Ok,
        ),
        expect(
            format!("mixed:{case_id}:rename_self_invalid"),
            rename(&format!("{base}/file"), &format!("{base}/file"))?,
            TxResult::Invalid,
        ),
    ])
}

fn expect(label: impl Into<String>, op: FsOp, expected: TxResult) -> ExpectedOp {
    ExpectedOp::new(label, op, expected)
}

fn mkdir(path: &str) -> Result<FsOp> {
    Ok(FsOp::Mkdir {
        path: Key::new(path)?,
    })
}

fn create(path: &str) -> Result<FsOp> {
    Ok(FsOp::Create {
        path: Key::new(path)?,
    })
}

fn unlink(path: &str) -> Result<FsOp> {
    Ok(FsOp::Unlink {
        path: Key::new(path)?,
    })
}

fn rmdir(path: &str) -> Result<FsOp> {
    Ok(FsOp::Rmdir {
        path: Key::new(path)?,
    })
}

fn rename(src: &str, dst: &str) -> Result<FsOp> {
    Ok(FsOp::Rename {
        src: Key::new(src)?,
        dst: Key::new(dst)?,
    })
}

fn stat(path: &str) -> Result<FsOp> {
    Ok(FsOp::Stat {
        path: Key::new(path)?,
    })
}

fn start_shard_node(shard_id: ShardId, ip: IpAddr, peer_endpoints: BTreeMap<ShardId, String>) {
    start_shard_node_with_scheduler(shard_id, ip, peer_endpoints, SchedulerKind::CalvinLocking);
}

fn start_shard_node_with_scheduler(
    shard_id: ShardId,
    ip: IpAddr,
    peer_endpoints: BTreeMap<ShardId, String>,
    scheduler: SchedulerKind,
) {
    start_shard_node_with_name(
        format!("shard-{shard_id}"),
        shard_id,
        ip,
        peer_endpoints,
        scheduler,
    );
}

fn start_shard_node_with_name(
    node_name: String,
    shard_id: ShardId,
    ip: IpAddr,
    peer_endpoints: BTreeMap<ShardId, String>,
    scheduler: SchedulerKind,
) {
    let node = madsim::runtime::Handle::current()
        .create_node()
        .name(node_name)
        .ip(ip)
        .build();
    node.spawn(async move {
        let runtime = Arc::new(
            ShardRuntime::new(ShardConfig {
                node_id: format!("shard-{shard_id}"),
                shard_id,
                shard_count: SHARD_COUNT,
                peer_endpoints,
                scheduler,
            })
            .expect("create shard runtime"),
        );
        let addr = SocketAddr::new(ip, SHARD_PORT);
        Server::builder()
            .add_service(pb::shard_server::ShardServer::new(shard_service(runtime)))
            .serve(addr)
            .await
            .expect("serve shard");
    });
}

fn start_sequencer_node(ip: IpAddr, shard_endpoints: BTreeMap<ShardId, String>) {
    start_sequencer_node_with_flush_interval(
        ip,
        shard_endpoints,
        SequencerConfig::default_batch_flush_interval(),
    );
}

fn start_sequencer_node_with_flush_interval(
    ip: IpAddr,
    shard_endpoints: BTreeMap<ShardId, String>,
    batch_flush_interval: Duration,
) {
    start_sequencer_node_with_config(ip, shard_endpoints, BATCH_SIZE, batch_flush_interval);
}

fn start_sequencer_node_with_config(
    ip: IpAddr,
    shard_endpoints: BTreeMap<ShardId, String>,
    max_batch_size: usize,
    batch_flush_interval: Duration,
) {
    start_sequencer_node_with_config_and_name(
        "sequencer".to_string(),
        ip,
        shard_endpoints,
        max_batch_size,
        batch_flush_interval,
    );
}

fn start_sequencer_node_with_config_and_name(
    node_name: String,
    ip: IpAddr,
    shard_endpoints: BTreeMap<ShardId, String>,
    max_batch_size: usize,
    batch_flush_interval: Duration,
) {
    let node = madsim::runtime::Handle::current()
        .create_node()
        .name(node_name)
        .ip(ip)
        .build();
    node.spawn(async move {
        let runtime = Arc::new(SequencerRuntime::new(SequencerConfig {
            node_id: "sequencer".to_string(),
            shard_count: SHARD_COUNT,
            shard_endpoints,
            max_batch_size,
            batch_flush_interval,
        }));
        let addr = SocketAddr::new(ip, SEQUENCER_PORT);
        Server::builder()
            .add_service(pb::sequencer_server::SequencerServer::new(
                sequencer_service(runtime),
            ))
            .serve(addr)
            .await
            .expect("serve sequencer");
    });
}

async fn wait_for_shards(shard_endpoints: &BTreeMap<ShardId, String>) -> Result<()> {
    for (shard_id, endpoint) in shard_endpoints {
        let mut ready = false;
        for _ in 0..100 {
            match pb::shard_client::ShardClient::connect(endpoint.clone()).await {
                Ok(mut client) => {
                    let response = client.ping(pb::PingRequest {}).await?.into_inner();
                    if response.shard_id == *shard_id {
                        ready = true;
                        break;
                    }
                    bail!(
                        "unexpected shard id from {}: expected {}, got {}",
                        endpoint,
                        shard_id,
                        response.shard_id
                    );
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(1)).await,
            }
        }
        if !ready {
            bail!("shard {} did not become ready at {}", shard_id, endpoint);
        }
    }
    Ok(())
}

async fn wait_for_sequencer(endpoint: &str) -> Result<()> {
    for _ in 0..100 {
        match pb::sequencer_client::SequencerClient::connect(endpoint.to_string()).await {
            Ok(mut client) => {
                client.ping(pb::PingRequest {}).await?;
                return Ok(());
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(1)).await,
        }
    }
    bail!("sequencer did not become ready at {}", endpoint)
}

fn reference_batch(batch_id: u64, tx_ids: Vec<u64>, ops: Vec<FsOp>) -> Result<Batch> {
    let mut txs = Vec::with_capacity(ops.len());
    for (batch_index, (tx_id, op)) in tx_ids.into_iter().zip(ops).enumerate() {
        let (read_set, write_set) = derive_read_write_set(&op)?;
        txs.push(OrderedTx {
            tx_id,
            batch_index: batch_index as u32,
            op,
            read_set,
            write_set,
        });
    }
    Ok(Batch { batch_id, txs })
}

fn shard_ip(shard_id: ShardId) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(10, 0, 0, shard_id as u8 + 1))
}

fn mdtest_shard_ip(network: u8, shard_id: ShardId) -> IpAddr {
    mdtest_node_ip(network, shard_id as u8 + 1)
}

fn mdtest_node_ip(network: u8, host: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(10, network, 0, host))
}

fn endpoint(ip: IpAddr, port: u16) -> String {
    format!("http://{ip}:{port}")
}

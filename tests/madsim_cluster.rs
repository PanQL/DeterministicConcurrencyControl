#![cfg(madsim)]

use anyhow::{bail, Result};
use calvinfs_demo::checker::{
    assert_state_matches, assert_tx_results_consistent, merge_shard_states,
    reference_execute_batches,
};
use calvinfs_demo::convert::{
    fs_op_to_proto, inode_entries_from_proto, tx_result_from_i32, tx_result_records_from_proto,
};
use calvinfs_demo::engine::{SequencerConfig, SequencerRuntime, ShardConfig, ShardRuntime};
use calvinfs_demo::executor::derive_read_write_set;
use calvinfs_demo::model::{Batch, FsOp, Key, OrderedTx, ShardId, TxResult, TxResultRecord};
use calvinfs_demo::proto::pb;
use calvinfs_demo::router::ShardLayout;
use calvinfs_demo::service::{sequencer_service, shard_service};
use calvinfs_demo::workload::metadata_workload;
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;

const SHARD_COUNT: u64 = 4;
const BATCH_COUNT: usize = 16;
const BATCH_SIZE: usize = 512;
const PARALLEL_CLIENT_COUNT: usize = 16;
const CREATES_PER_PARALLEL_CLIENT: usize = BATCH_SIZE;
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
async fn submit_tx_parallel_clients_create_full_batches() -> Result<()> {
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
    start_sequencer_node_with_flush_interval(
        sequencer_ip,
        shard_endpoints.clone(),
        Duration::from_secs(60),
    );

    let driver = madsim::runtime::Handle::current()
        .create_node()
        .name("parallel-submit-tx-driver")
        .ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 202)))
        .build();
    driver
        .spawn(run_parallel_submit_tx_driver(
            sequencer_endpoint,
            shard_endpoints,
        ))
        .await
        .expect("parallel submit tx driver task panicked")?;

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

async fn run_parallel_submit_tx_driver(
    sequencer_endpoint: String,
    shard_endpoints: BTreeMap<ShardId, String>,
) -> Result<()> {
    wait_for_shards(&shard_endpoints).await?;
    wait_for_sequencer(&sequencer_endpoint).await?;

    let layout = ShardLayout::new(SHARD_COUNT);
    let mut sequencer =
        pb::sequencer_client::SequencerClient::connect(sequencer_endpoint.clone()).await?;
    let mut reference_batches = Vec::new();
    let mut all_records = Vec::new();

    submit_expected_batch(
        &mut sequencer,
        vec![
            expect("parallel_setup:mkdir_root", mkdir("/")?, TxResult::Ok),
            expect(
                "parallel_setup:mkdir_parallel",
                mkdir("/parallel")?,
                TxResult::Ok,
            ),
        ],
        &mut reference_batches,
        &mut all_records,
    )
    .await?;

    let mut handles = Vec::new();
    for client_id in 0..PARALLEL_CLIENT_COUNT {
        let endpoint = sequencer_endpoint.clone();
        handles.push(tokio::spawn(async move {
            let mut client = pb::sequencer_client::SequencerClient::connect(endpoint).await?;
            let mut submitted = Vec::with_capacity(CREATES_PER_PARALLEL_CLIENT);
            for file_id in 0..CREATES_PER_PARALLEL_CLIENT {
                let label = format!("parallel_client:{client_id}:{file_id}");
                let op = create(&format!("/parallel/client_{client_id}_file_{file_id}"))?;
                let response = client
                    .submit_tx(pb::SubmitTxRequest {
                        op: Some(fs_op_to_proto(&op)),
                    })
                    .await?
                    .into_inner();
                submitted.push(SubmittedClientTx {
                    label,
                    op,
                    expected: TxResult::Ok,
                    tx_id: response.tx_id,
                    result_shard: response.result_shard,
                });
            }
            Ok::<_, anyhow::Error>(submitted)
        }));
    }

    let mut submitted = Vec::with_capacity(PARALLEL_CLIENT_COUNT * CREATES_PER_PARALLEL_CLIENT);
    for handle in handles {
        submitted.extend(handle.await.expect("parallel client task panicked")?);
    }
    submitted.sort_by_key(|tx| tx.tx_id);

    let expected_parallel_count = PARALLEL_CLIENT_COUNT * CREATES_PER_PARALLEL_CLIENT;
    assert_eq!(submitted.len(), expected_parallel_count);
    assert_eq!(expected_parallel_count % BATCH_SIZE, 0);

    let first_parallel_tx = reference_batches
        .last()
        .and_then(|batch| batch.txs.last())
        .map(|tx| tx.tx_id + 1)
        .expect("setup batch contains transactions");
    for (index, tx) in submitted.iter().enumerate() {
        let expected_tx_id = first_parallel_tx + index as u64;
        if tx.tx_id != expected_tx_id {
            bail!(
                "{} expected tx_id {}, got {}",
                tx.label,
                expected_tx_id,
                tx.tx_id
            );
        }
        let (read_set, write_set) = derive_read_write_set(&tx.op)?;
        let expected_result_shard = layout
            .result_shard_for_sets(&read_set, &write_set)
            .expect("parallel create has a result shard");
        if tx.result_shard != expected_result_shard {
            bail!(
                "{} expected result_shard {}, got {}",
                tx.label,
                expected_result_shard,
                tx.result_shard
            );
        }
    }

    for tx in &submitted {
        let mut shard =
            pb::shard_client::ShardClient::connect(shard_endpoints[&tx.result_shard].clone())
                .await?;
        let result = shard
            .get_tx_result(pb::GetTxResultRequest { tx_id: tx.tx_id })
            .await?
            .into_inner();
        assert_ready_result(&tx.label, &result, tx.expected)?;
    }

    for (batch_index, chunk) in submitted.chunks(BATCH_SIZE).enumerate() {
        let batch_id = 2 + batch_index as u64;
        let tx_ids = chunk.iter().map(|tx| tx.tx_id).collect();
        let ops = chunk.iter().map(|tx| tx.op.clone()).collect();
        reference_batches.push(reference_batch(batch_id, tx_ids, ops)?);
    }

    submit_expected_batch(
        &mut sequencer,
        vec![expect(
            "parallel_barrier:stat_root",
            stat("/")?,
            TxResult::Ok,
        )],
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

    let expected = reference_execute_batches(&reference_batches);
    let actual = merge_shard_states(&layout, shard_states)?;
    assert_state_matches(&expected, &actual)?;

    Ok(())
}

#[derive(Clone, Debug)]
struct SubmittedClientTx {
    label: String,
    op: FsOp,
    expected: TxResult,
    tx_id: u64,
    result_shard: ShardId,
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
    let node = madsim::runtime::Handle::current()
        .create_node()
        .name(format!("shard-{shard_id}"))
        .ip(ip)
        .build();
    node.spawn(async move {
        let runtime = Arc::new(
            ShardRuntime::new(ShardConfig {
                node_id: format!("shard-{shard_id}"),
                shard_id,
                shard_count: SHARD_COUNT,
                peer_endpoints,
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
    let node = madsim::runtime::Handle::current()
        .create_node()
        .name("sequencer")
        .ip(ip)
        .build();
    node.spawn(async move {
        let runtime = Arc::new(SequencerRuntime::new(SequencerConfig {
            node_id: "sequencer".to_string(),
            shard_count: SHARD_COUNT,
            shard_endpoints,
            max_batch_size: BATCH_SIZE,
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

fn endpoint(ip: IpAddr, port: u16) -> String {
    format!("http://{ip}:{port}")
}

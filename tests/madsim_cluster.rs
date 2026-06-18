#![cfg(madsim)]

use anyhow::{bail, Result};
use calvinfs_demo::checker::{
    assert_state_matches, assert_tx_results_consistent, merge_shard_states,
    reference_execute_batches,
};
use calvinfs_demo::convert::{
    fs_op_to_proto, inode_entries_from_proto, tx_result_records_from_proto,
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

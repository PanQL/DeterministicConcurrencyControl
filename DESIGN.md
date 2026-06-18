# CalvinFS Metadata Transaction Demo Design

状态：Draft v2，Milestone 1 batch execution core 已实现，Milestone 2 增加 client-facing 单事务 API。

本文档描述一个基于 Rust、tokio、tonic 和 madsim 的纯内存 CalvinFS-style metadata execution engine。目标是在 madsim 模拟环境中验证 Calvin-style metadata transaction 的核心正确性：Sequencer 生成事务 batch、所有 shard 按 batch 内确定性并发控制执行、跨分片读结果交换、active participant 执行一致性，以及最终分片状态与串行参考执行一致。

## 1. 目标

实现一个可在 madsim 中运行的多节点、纯内存 metadata execution engine：

- 业务代码直接写成普通 tokio/tonic 服务。
- simulation build 中由 madsim 替换 tokio/tonic runtime 和 network。
- test harness 在 madsim 中启动一个 Sequencer tonic server 和多个 shard tonic server。
- test driver 可通过内部 `SubmitBatch` 提交一批 FsOp；client-facing API 使用 `SubmitTx` 提交单个事务。
- shard 对 Sequencer 内部 batch log 无感知，只按收到的 batch 内顺序做 deterministic local locking。
- passive participant 通过 RPC 向 active participants 转发 local read result。
- active participants 收齐 full read set 后执行同一份 deterministic transaction logic。
- 每个 active participant 只应用本 shard 拥有的 local writes。
- checker 合并所有 shard state，并与 serial reference execution 对比。

## 2. 架构原则

### 2.1 tokio/tonic-first

项目从第一天开始写 tonic gRPC service，而不是先写一套进程内 channel/runtime 抽象。

原因：

- CalvinFS metadata service 本质上是远端分布式服务。
- shard 间 read-result exchange 本来就是网络通信。
- Sequencer 到 shard 的 batch 下发本来就是网络通信。
- madsim 已经支持模拟 tokio/tonic 相关基础设施。
- 提前使用 tonic 可以尽早稳定协议边界和跨节点交互形态。

### 2.2 madsim-backed simulation

测试代码使用 `#[madsim::test]` 在单机进程内创建 simulated nodes、DNS records、tonic server/client。第一阶段使用 madsim 的主要目的，是快速开发和运行多节点功能闭环，而不是优先做故障、乱序或网络扰动测试。

运行方式：

```bash
RUSTFLAGS="--cfg madsim" cargo test
```

Cargo 依赖版本在实际实现时以当前 madsim tonic example 为准。设计层面只固定原则：业务代码保持普通 tokio/tonic 风格，simulation build 使用 madsim 相关替代 crate。

### 2.3 deterministic core 可复用

事务语义本身不应依赖 tonic、madsim 或网络。核心执行逻辑应抽成纯函数或近似纯函数：

```rust
pub fn execute_deterministic(
    tx: &OrderedTx,
    full_reads: &BTreeMap<Key, ReadValue>,
) -> TxExecutionOutput;
```

这样同一份逻辑可以用于：

- madsim sharded execution。
- serial reference execution。
- 单元测试。
- 后续真实 tokio/tonic runtime。

### 2.4 batch-oriented execution

Calvin 论文中的 sequencing layer 会把输入事务组织成 batch/epoch，scheduler 根据 sequencer 指定的顺序使用 deterministic locking 执行事务。CalvinFS 论文中的 log/front-end 也会批量收集请求，并把事务请求转发给相关 metadata shards。

本 demo 做一个更简单的单 Sequencer 版本：

- client-facing API 是 `Sequencer.SubmitTx(FsOp)`，Sequencer 内部把在线请求组织成 `Batch`。
- `Sequencer.SubmitBatch` 保留为集成测试/内部 API，不作为 client-facing API。
- Sequencer 将同一个 `Batch` 发送给所有 shards。
- shard 不保存、也不感知 Sequencer 内部 batch log。
- shard 只知道当前 batch 的 ordered transactions，并在 batch 内做 deterministic concurrency control。
- Sequencer 等待所有 shards 报告该 batch 完成后，才发送下一个 batch。
- open batch 通过 `max_batch_size` 或 `batch_flush_interval` 双条件关闭，低负载时未满 batch 也会发送给 shards。
- Sequencer 内部可以保留 batch log，供 checker 和 debug 使用；这是测试辅助状态，不是 shard 执行协议的一部分。
- 初始 root directory 由第一个 batch 显式创建，测试不预装隐藏状态。

## 3. 模块划分

### 3.1 `model`

内部领域模型。不要直接把 prost generated types 当作业务模型。

职责：

- 定义 metadata KV 的 key/value。
- 定义事务操作、read/write set、ordered transaction。
- 定义执行结果和 write intent。
- 提供稳定排序和校验逻辑。

KV 约定：

- `Key` 使用规范化后的绝对路径，例如 `/`、`/a`、`/a/x`。
- 路径必须以 `/` 开头，不能包含空 segment、`.`、`..` 或尾随 `/`，根目录 `/` 除外。
- `Value` 是该路径对应的 `Inode`。
- 本 demo 不存储文件数据；`Inode` 只保存元数据，不保存 file blocks 或 file content。
- 目录 inode 维护 `child_count`，用于判断 `rmdir` 是否允许执行；第一版不支持删除非空目录，也不扫描目录子树。

核心类型：

```rust
pub type TxId = u64;
pub type BatchId = u64;
pub type ShardId = u64;

pub struct Key(pub String);

pub struct Inode {
    pub kind: NodeKind,
    pub child_count: u64,
}

pub enum NodeKind {
    File,
    Directory,
}

pub enum FsOp {
    Create { path: Key },
    Mkdir { path: Key },
    Unlink { path: Key },
    Rmdir { path: Key },
    Rename { src: Key, dst: Key },
    Stat { path: Key },
}

pub struct Batch {
    pub batch_id: BatchId,
    pub txs: Vec<OrderedTx>,
}

pub struct OrderedTx {
    pub tx_id: TxId,
    pub batch_index: u32,
    pub op: FsOp,
    pub read_set: BTreeSet<Key>,
    pub write_set: BTreeSet<Key>,
}

pub enum TxResult {
    Ok,
    NotFound,
    AlreadyExists,
    NotDirectory,
    DirectoryNotEmpty,
    Invalid,
}

pub enum ReadValue {
    Present(Inode),
    Missing,
}

pub enum WriteOp {
    Put { key: Key, value: Inode },
    Delete { key: Key },
}

pub struct TxExecutionOutput {
    pub result: TxResult,
    pub writes: Vec<WriteOp>,
}
```

### 3.2 `proto`

tonic/prost wire format。

职责：

- 维护 `proto/calvinfs.proto`。
- 生成 tonic client/server stubs。
- 提供 proto model 与 internal model 转换。

原则：

- proto 只是 wire format。
- internal model 承载业务不变量。
- conversion 层负责字段缺失、非法 enum、重复 key、乱序集合等校验。

接口：

```rust
impl TryFrom<proto::OrderedTx> for model::OrderedTx { /* ... */ }
impl From<model::OrderedTx> for proto::OrderedTx { /* ... */ }
impl TryFrom<proto::FsOp> for model::FsOp { /* ... */ }
impl From<model::TxResult> for proto::TxResult { /* ... */ }
```

### 3.3 `storage`

metadata KV 存储层。

第一阶段使用 `redb` 的 `InMemoryBackend`，不手写 `BTreeMap` 版 KV。`redb` 是纯 Rust 嵌入式 KV，提供 ACID transaction、BTreeMap-style API、MVCC，并包含 in-memory backend。参考：

- `redb` crate docs: <https://docs.rs/redb/latest/redb/>
- `redb::backends::InMemoryBackend`: <https://docs.rs/redb/latest/redb/backends/struct.InMemoryBackend.html>

调研结论：

- 选 `redb::backends::InMemoryBackend` 作为第一阶段默认实现，因为它是明确的进程内临时内存 backend。
- `sled::Config::temporary(true)` 可作为备选，但语义是临时 DB，drop 后删除；未设置 path 时 Linux 下使用 `/dev/shm`，不如 redb 的 in-memory backend 直接。参考：<https://docs.rs/sled/latest/sled/struct.Config.html#method.temporary>
- cache 类 crate 不作为默认选择，因为这里需要 KV table 和事务边界。

KV layout：

- table name: `inodes`
- key: normalized absolute path string
- value: encoded `Inode`
- encoding: first implementation may use `bincode` or `prost` bytes; keep encoding inside storage adapter, not in transaction logic

接口：

```rust
use redb::{Database, TableDefinition};
use redb::backends::InMemoryBackend;

const INODE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("inodes");

pub struct RedbInMemoryInodeStore {
    db: redb::Database,
}

impl RedbInMemoryInodeStore {
    pub fn new() -> Result<Self, StoreError> {
        let db = Database::builder().create_with_backend(InMemoryBackend::new())?;
        Ok(Self { db })
    }

    pub fn read_many(&self, keys: &BTreeSet<Key>) -> Result<BTreeMap<Key, ReadValue>, StoreError>;
    pub fn apply_writes_atomically(&self, writes: &[WriteOp]) -> Result<(), StoreError>;
    pub fn dump(&self) -> Result<BTreeMap<Key, Inode>, StoreError>;
}
```

`dump()` 返回 `BTreeMap` 只是为了 deterministic checker 输出，不代表存储实现使用手写 map。

`read_many()` 在一个 read transaction 中读取 worker 已持有本地 locks 的 keys。`apply_writes_atomically()` 在一个 write transaction 中应用 worker 已校验过的 local writes；它是 worker 写入 store 的唯一入口。

本设计只保留纯内存 store，后续开发也先围绕内存模拟环境推进。

### 3.4 `router`

负责 key 到 shard 的稳定映射，以及 participant 计算。

第一版使用具体 `ShardLayout`，不做 trait 抽象：

```rust
pub struct ShardLayout {
    pub shard_count: u64,
}

impl ShardLayout {
    pub fn shard_for_key(&self, key: &Key) -> ShardId;
    pub fn participants_for_sets(
        &self,
        read_set: &BTreeSet<Key>,
        write_set: &BTreeSet<Key>,
    ) -> Participants;
    pub fn read_only_coordinator(&self, read_set: &BTreeSet<Key>) -> ShardId;
}

pub struct Participants {
    pub all: BTreeSet<ShardId>,
    pub active: BTreeSet<ShardId>,
    pub passive: BTreeSet<ShardId>,
}
```

第一阶段使用确定性 hash mapping，不使用 `std::collections::hash_map::DefaultHasher` 或任何带随机种子的 hasher。

具体规则：

- `shard_for_key(key) = stable_hash(normalized_path_bytes) % shard_count`。
- `stable_hash` 固定为 FNV-1a 64-bit，offset basis `14695981039346656037`，prime `1099511628211`。
- Sequencer 生成 `OrderedTx` 时只写入 read/write sets，不写入 participant 字段。
- 每个 shard 收到 batch 后也使用同一 `ShardLayout` 独立复算自己是否参与某个 tx，以及该 tx 的其他 participants 是哪些。
- shard 不依赖 Sequencer 告诉它“我该联系谁”；participants 完全由 `read_set`、`write_set` 和 `ShardLayout` 推导。
- `read_only_coordinator(read_set)` 选择 read set 中排序后第一个 key 的 owner shard。

这样每个 shard 都能基于 batch 里的 keys 独立确定跨分片 read-result exchange 的目标，协议更接近 Calvin-style 分片执行模型。

第一阶段 participant model：

- shard 收到 batch 后按确定性 hash 独立复算 participants，用于决定自己是否参与以及要向哪些 active participants 发送 read results。
- 对含写事务，active participants 是 `write_set` 的 owner shards。
- 对 read-only tx，各 shard 使用 `ShardLayout::read_only_coordinator(read_set)` 选择排序后第一个 read key 的 owner shard 作为唯一 active participant。
- shard membership 在测试启动时固定。
- participants 在一次 batch 执行期间一直存活。
- 不设计 participant timeout、failure detector、reconfiguration 或 partial retry。

### 3.5 `executor`

deterministic transaction logic。

Sequencer 必须按下面规则生成 read/write set：

```text
Mkdir(path):
  if path == "/":
    read  = { "/" }
    write = { "/" }
  else:
    read  = { parent(path), path }
    write = { parent(path), path }

Create(path):
  read  = { parent(path), path }
  write = { parent(path), path }

Unlink(path):
  read  = { parent(path), path }
  write = { parent(path), path }

Rmdir(path):
  read  = { parent(path), path }
  write = { parent(path), path }

Rename(src, dst):
  read  = { parent(src), parent(dst), src, dst }
  write = { parent(src), parent(dst), src, dst }

Stat(path):
  read  = { path }
  write = {}
```

这些集合是协议的一部分，不是优化提示。checker、Sequencer 和 shard 都按这些集合解释事务。

`read_set`/`write_set` 虽然可以由 `FsOp` 推导，但仍作为 `OrderedTx` 的 wire fields 保留，因为它们代表 Sequencer 的 transaction planning 输出。shard 和 checker 必须使用同一份 `derive_read_write_set(op)` 重新推导并校验：

- 若 `OrderedTx.read_set` 与推导结果不同，拒绝该 batch。
- 若 `OrderedTx.write_set` 与推导结果不同，拒绝该 batch。
- conversion 层应先完成 path normalization，再进行集合校验。

职责：

- 基于 full read set 判断事务结果。
- 生成完整 write intents。
- 不直接修改 shard store。
- 不依赖本地 shard id。

接口：

```rust
pub fn execute_deterministic(
    tx: &OrderedTx,
    full_reads: &BTreeMap<Key, ReadValue>,
) -> TxExecutionOutput;

pub fn filter_local_writes(
    writes: Vec<WriteOp>,
    local_shard: ShardId,
    layout: &ShardLayout,
) -> Vec<WriteOp>;
```

约束：

- 所有 active participants 对同一 `OrderedTx` 和同一 `full_reads` 必须得到同一个 `TxResult`。
- write filtering 只能影响应用哪些 writes，不能影响事务结果。
- `Mkdir("/")` 是 root bootstrap 操作：若 `/` missing，则创建 directory inode；若 `/` 已存在，则返回 `AlreadyExists`。
- `Rmdir` 只允许删除空目录：若目标 inode 是 directory 且 `child_count == 0`，生成 delete；若 `child_count > 0`，返回 `DirectoryNotEmpty`。
- `Create`/`Mkdir` 增加 parent directory 的 `child_count`；`Unlink`/成功的 `Rmdir` 减少 parent directory 的 `child_count`；`Rename` 移动同一个 path value，并在跨 parent rename 时更新两个 parent directories 的 `child_count`。
- `Rename` 严格检查：src parent 存在且是 directory，dst parent 存在且是 directory，src 存在，dst missing；否则返回对应错误。
- 同一 batch 内允许后序事务读取前序事务写入的结果；Sequencer 生成 read/write set 时必须包含 parent、src、dst 等所有相关 keys。

错误语义表：

| Op | 条件 | TxResult | Writes |
| --- | --- | --- | --- |
| 任意 op | path 非规范化，或包含空 segment、`.`、`..`、非法尾随 `/` | `Invalid` | none |
| `Mkdir("/")` | `/` missing | `Ok` | put `/` directory |
| `Mkdir("/")` | `/` present | `AlreadyExists` | none |
| `Mkdir(path != "/")` | parent missing | `NotFound` | none |
| `Mkdir(path != "/")` | parent present but not directory | `NotDirectory` | none |
| `Mkdir(path != "/")` | target present | `AlreadyExists` | none |
| `Mkdir(path != "/")` | parent directory exists, target missing | `Ok` | put target directory, parent `child_count += 1` |
| `Create(path)` | path is `/` | `Invalid` | none |
| `Create(path)` | parent missing | `NotFound` | none |
| `Create(path)` | parent present but not directory | `NotDirectory` | none |
| `Create(path)` | target present | `AlreadyExists` | none |
| `Create(path)` | parent directory exists, target missing | `Ok` | put target file, parent `child_count += 1` |
| `Stat(path)` | target missing | `NotFound` | none |
| `Stat(path)` | target present | `Ok` | none |
| `Unlink(path)` | path is `/` | `Invalid` | none |
| `Unlink(path)` | parent missing | `NotFound` | none |
| `Unlink(path)` | parent present but not directory | `NotDirectory` | none |
| `Unlink(path)` | target missing | `NotFound` | none |
| `Unlink(path)` | target is directory | `Invalid` | none |
| `Unlink(path)` | target is file | `Ok` | delete target, parent `child_count -= 1` |
| `Rmdir(path)` | path is `/` | `Invalid` | none |
| `Rmdir(path)` | parent missing | `NotFound` | none |
| `Rmdir(path)` | parent present but not directory | `NotDirectory` | none |
| `Rmdir(path)` | target missing | `NotFound` | none |
| `Rmdir(path)` | target is file | `NotDirectory` | none |
| `Rmdir(path)` | target directory has `child_count > 0` | `DirectoryNotEmpty` | none |
| `Rmdir(path)` | target empty directory | `Ok` | delete target, parent `child_count -= 1` |
| `Rename(src, dst)` | `src == dst`, `src == "/"`, or `dst == "/"` | `Invalid` | none |
| `Rename(src, dst)` | src parent missing or dst parent missing | `NotFound` | none |
| `Rename(src, dst)` | src parent or dst parent present but not directory | `NotDirectory` | none |
| `Rename(src, dst)` | src missing | `NotFound` | none |
| `Rename(src, dst)` | dst present | `AlreadyExists` | none |
| `Rename(src, dst)` | src is non-empty directory | `DirectoryNotEmpty` | none |
| `Rename(src, dst)` | src is file or empty directory, dst missing | `Ok` | delete src, put dst with same inode, update parent `child_count` only if parent changes |

### 3.6 `shard`

tonic Shard service 实现。

运行时状态：

```rust
pub struct ShardRuntime {
    pub shard_id: ShardId,
    pub store: Arc<RedbInMemoryInodeStore>,
    pub peers: BTreeMap<ShardId, String>,
    pub batch_tx: mpsc::Sender<BatchJob>,
    pub read_result_mailboxes: Arc<Mutex<ReadResultMailboxRegistry>>,
    pub client_results: TxResultRegistry,
}

pub struct ReadResultMailboxRegistry {
    pub mailboxes: BTreeMap<(BatchId, TxId), ReadResultMailbox>,
}

impl ReadResultMailboxRegistry {
    pub fn get_or_create_sender(&mut self, key: (BatchId, TxId)) -> mpsc::Sender<LocalReadResult>;
    pub fn take_or_create_receiver(&mut self, key: (BatchId, TxId)) -> mpsc::Receiver<LocalReadResult>;
    pub fn remove(&mut self, key: (BatchId, TxId));
}

pub struct ReadResultMailbox {
    pub sender: mpsc::Sender<LocalReadResult>,
    pub receiver: Option<mpsc::Receiver<LocalReadResult>>,
}

pub struct BatchExecutionState {
    pub batch_id: BatchId,
    pub lock_table: LockTable,
    pub tx_results: BTreeMap<TxId, TxResult>,
}

pub struct TxResultRegistry {
    pub results: BTreeMap<TxId, watch::Sender<ClientTxResultState>>,
}

pub enum ClientTxResultState {
    Pending,
    Ready(TxResult),
    NotResponsible,
}
```

职责：

- 接收 Sequencer 发送的 `Batch`。
- 对 batch 内事务计算本 shard 的 local read/write keys。
- 按 `(batch_id, batch_index)` 排锁。
- 为所有本 shard 相关事务创建 lock-grant channel 并 spawn worker。
- executor 只向每个 lock queue 的队首发送 grant signal。
- worker 收齐本地 lock grants 后执行本地 reads。
- worker 向 active participants 转发 local read results。
- active worker 收齐 full read set 后执行 deterministic logic。
- active worker 过滤并应用本 shard local writes。
- 记录 tx result。
- 若本 shard 是该 tx 的 client-facing `result_shard`，将 `TxResultRegistry` 中的状态更新为 `Ready(result)`，唤醒正在等待的 `GetTxResult` RPC。
- 对非 `result_shard` 的 tx，更新为 `NotResponsible`，避免误查该 shard 的 client 长时间等待。
- batch 内本 shard 相关事务全部完成后，向 Sequencer 返回 batch 完成结果。
- 提供 blocking `GetTxResult` 供 client 获取事务结果。
- 提供 DumpState 供测试使用。

### 3.7 `sequencer`

Sequencer 是 client-facing 提交入口。Milestone 1 的 `SubmitBatch` 保留为集成测试/内部 API；Milestone 2 增加真正面向 client 的 `SubmitTx`。

职责：

- 接收 client 提交的单个 `FsOp`。
- 使用单 actor 管理在线提交、open batch、flush timer 和 dispatch queue。
- `SubmitTx` RPC handler 通过 `oneshot` reply channel 等待 actor 返回 `tx_id` 和 `result_shard`。
- `SubmitTx` 返回点是 Sequencer 接受事务、分配 `tx_id` 并放入内部 batch；不等待 batch flush 或 shard 执行完成。
- 接收 test driver 提交的内部 `FsOp` batch。
- 校验 batch size 不超过配置上限，默认 `max_batch_size = 512`。
- 通过 `batch_flush_interval` 关闭未满 batch，默认 1ms，可配置。
- 分配递增 batch_id。
- 分配递增 tx_id。
- 计算 read/write set。
- 计算 client-facing `result_shard`：write tx 取 active participants 中最小 shard id；read-only tx 取 `read_only_coordinator`。
- 生成 `Batch { batch_id, txs }`，其中每个 tx 带有 batch_index。
- 将同一个 `Batch` 发送给所有 shards。
- 等待所有 shards 返回 `ExecuteBatchResponse`。
- 保存 batch log 和 batch result，供 checker/debug 查询。
- `SubmitBatch` 同步返回 batch result，但只作为 internal/test API。

核心内部命令：

```rust
pub struct SubmitTxCommand {
    pub op: FsOp,
    pub reply: oneshot::Sender<Result<SubmitTxAck>>,
}

pub struct SubmitTxAck {
    pub tx_id: TxId,
    pub result_shard: ShardId,
}
```

assumptions：

- shard membership 在测试启动时明确。
- `max_batch_size` 是 Sequencer 配置项，默认 512；超过上限的 `SubmitBatch` 返回 `InvalidArgument`，不生成 `Batch`。
- `batch_flush_interval` 是 Sequencer 配置项，默认 1ms；open batch 只要非空，达到 size 或 timeout 任一条件都会 seal。
- 每个 tx 的 participants 由 batch 内 read/write set 和 `ShardLayout` 确定，并在 batch 执行期间一直存活。
- Sequencer 不处理 participant failure、retry、membership change。
- Sequencer 的 batch log 只存在于 Sequencer/test driver，用于 checker 和 debug；shards 对它无感知。

### 3.8 `sim`

madsim 测试和模拟集群工具。

职责：

- 创建 madsim nodes。
- 配置 shard IP、端口和 DNS。
- 启动 tonic Shard server。
- 启动 tonic Sequencer server。
- 创建 tonic clients。
- 通过内部 `SubmitBatch` 运行大规模集成 workload。
- 通过 client-facing `SubmitTx` + blocking `GetTxResult` 验证在线单事务 API。
- 运行多节点功能测试；第一阶段不主动注入乱序、网络故障或 participant crash。
- 调用 checker。

### 3.9 `checker`

checker 是测试 oracle，不是线上模块。

职责：

- 按 Sequencer batch log 顺序在单机内存中做 serial reference execution。
- 调用各 shard 的 DumpState。
- 合并分片 state。
- 比较 sharded final state 与 reference final state。
- 使用 Sequencer 聚合的 `ExecuteBatchResponse` 检查每个 tx 的 active participant `TxResult` 是否一致。

## 4. RPC 定义

初版 proto 文件路径：

```text
proto/calvinfs.proto
```

proto 草案：

```proto
syntax = "proto3";

package calvinfs;

service Shard {
  rpc Ping(PingRequest) returns (PingResponse);
  rpc ExecuteBatch(ExecuteBatchRequest) returns (ExecuteBatchResponse);
  rpc LocalReadResult(LocalReadResultRequest) returns (LocalReadResultResponse);
  rpc GetTxResult(GetTxResultRequest) returns (GetTxResultResponse);
  rpc DumpState(DumpStateRequest) returns (DumpStateResponse);
}

service Sequencer {
  rpc Ping(PingRequest) returns (PingResponse);
  rpc SubmitTx(SubmitTxRequest) returns (SubmitTxResponse);
  rpc SubmitBatch(SubmitBatchRequest) returns (SubmitBatchResponse);
}

message PingRequest {}

message PingResponse {
  string node_id = 1;
  uint64 shard_id = 2;
}

message ExecuteBatchRequest {
  Batch batch = 1;
}

message ExecuteBatchResponse {
  uint64 batch_id = 1;
  uint64 shard_id = 2;
  repeated TxResultRecord tx_results = 3;
}

message LocalReadResultRequest {
  uint64 batch_id = 1;
  uint64 tx_id = 2;
  uint64 from_shard = 3;
  repeated ReadEntry reads = 4;
}

message LocalReadResultResponse {}

message GetTxResultRequest {
  uint64 tx_id = 1;
}

message GetTxResultResponse {
  uint64 tx_id = 1;
  uint64 shard_id = 2;
  TxResultStatus status = 3;
  TxResult result = 4;
}

message DumpStateRequest {}

message DumpStateResponse {
  repeated InodeEntry entries = 1;
}

message Batch {
  uint64 batch_id = 1;
  repeated OrderedTx txs = 2;
}

message OrderedTx {
  uint64 tx_id = 1;
  uint32 batch_index = 2;
  FsOp op = 3;
  repeated string read_set = 4;
  repeated string write_set = 5;
}

message FsOp {
  oneof op {
    Create create = 1;
    Mkdir mkdir = 2;
    Unlink unlink = 3;
    Rmdir rmdir = 4;
    Rename rename = 5;
    Stat stat = 6;
  }
}

message Create {
  string path = 1;
}

message Mkdir {
  string path = 1;
}

message Unlink {
  string path = 1;
}

message Rmdir {
  string path = 1;
}

message Rename {
  string src = 1;
  string dst = 2;
}

message Stat {
  string path = 1;
}

message InodeEntry {
  string key = 1;
  Inode inode = 2;
}

message Inode {
  NodeKind kind = 1;
  uint64 child_count = 2;
}

message ReadEntry {
  string key = 1;
  oneof value {
    Inode inode = 2;
    Missing missing = 3;
  }
}

message Missing {}

enum NodeKind {
  NODE_KIND_UNSPECIFIED = 0;
  NODE_KIND_FILE = 1;
  NODE_KIND_DIRECTORY = 2;
}

enum TxResult {
  TX_RESULT_UNSPECIFIED = 0;
  TX_RESULT_OK = 1;
  TX_RESULT_NOT_FOUND = 2;
  TX_RESULT_ALREADY_EXISTS = 3;
  TX_RESULT_NOT_DIRECTORY = 4;
  TX_RESULT_DIRECTORY_NOT_EMPTY = 5;
  TX_RESULT_INVALID = 6;
}

enum TxResultStatus {
  TX_RESULT_STATUS_UNSPECIFIED = 0;
  TX_RESULT_STATUS_READY = 1;
  TX_RESULT_STATUS_NOT_RESPONSIBLE = 2;
}

message TxResultRecord {
  uint64 tx_id = 1;
  uint64 shard_id = 2;
  TxResult result = 3;
}

message SubmitTxRequest {
  FsOp op = 1;
}

message SubmitTxResponse {
  uint64 tx_id = 1;
  uint64 result_shard = 2;
}

message SubmitBatchRequest {
  repeated FsOp ops = 1;
}

message SubmitBatchResponse {
  uint64 batch_id = 1;
  repeated uint64 tx_ids = 2;
  repeated TxResultRecord tx_results = 3;
}

```

## 5. 事务执行流程

### 5.1 Sequencer 生成并发送 Batch

client-facing 路径由 client 调用 Sequencer 的 `SubmitTx`：

1. `SubmitTx` RPC handler 解析 `FsOp`。
2. handler 创建 `oneshot` reply channel，并向 Sequencer actor 发送 `SubmitTxCommand`。
3. actor 串行分配递增 `tx_id`，计算 read/write set 和 `result_shard`。
4. actor 将 tx 放入当前 open batch，并通过 `oneshot` 返回 `SubmitTxAck { tx_id, result_shard }`。
5. handler 立即向 client 返回 `SubmitTxResponse`，不等待 batch flush 或 shard 执行完成。
6. open batch 在达到 `max_batch_size` 或 `batch_flush_interval` 后 seal。
7. dispatcher 将 sealed batch 发送给所有 shards 的 `ExecuteBatch`。
8. dispatcher 等待所有 shards 返回 `ExecuteBatchResponse` 后才发送下一个 batch。

internal/test 路径由 test driver 调用 Sequencer 的 `SubmitBatch`：

1. Sequencer 接收一批 `FsOp`。
2. Sequencer 校验 batch size 不超过 `max_batch_size`。
3. Sequencer 分配递增 `batch_id`。
4. Sequencer 为 batch 内每个 op 分配递增 `tx_id` 和 `batch_index`。
5. Sequencer 基于 path key 计算 read/write set；participants 和 active participants 由所有 shard 使用同一 `ShardLayout` 独立复算。
6. Sequencer 生成 `Batch { batch_id, txs }`。
7. Sequencer 将同一个 `Batch` 发送给所有 shards 的 `ExecuteBatch`。
8. Sequencer 等待所有 shards 返回 `ExecuteBatchResponse`。
9. Sequencer 聚合 batch result，并向 caller 返回 `SubmitBatchResponse`。
10. `SubmitBatch` 仅作为 internal/test API；client-facing API 不暴露 `batch_id`。

Sequencer 内部可以保存 `Vec<Batch>` 作为测试和 debug 用的 batch log。这个 log 不下发、不安装到 shard，shard 对它无感知。

第一阶段假设所有 participants 在 batch 开始前已知且一直存活。RPC 失败视为测试失败，不做重试、补发或 membership repair。

### 5.2 ExecuteBatch

每个 shard 收到 `ExecuteBatch` 后：

1. 保存当前 `batch_id` 到本地 batch execution context。
2. 遍历 batch 内所有 tx，计算本 shard 对每个 tx 的角色：non-participant、passive participant 或 active participant。
3. 对本 shard 参与的 tx，按 `(batch_id, batch_index)` 把本地 read/write keys 加入 lock queues。
4. shard 内部可以并发推进多个 tx，但所有 lock granting 必须由 batch 内顺序确定。
5. executor 为每个本 shard 相关 tx 创建一个 lock-grant channel，并把该 channel 的 sender clone 放入该 tx 所有本地 key 的 lock queue entry 中。
6. executor 一次性 spawn 所有本 shard 相关 tx 的 worker；worker 启动后先等待自己的 lock-grant receiver。
7. executor 对每个非空 lock queue 的队首发送 grant signal。
8. worker 收到自己所有本地 key 的 grant 后，从 store 读取本 shard 的 local reads。
9. worker 向 active participants 发送 local read result。
10. active worker 收齐所有 participants 的 read results 后执行 deterministic logic，并计算本 shard 的 local writes。
11. active worker 在持有本地 locks 的情况下应用 local writes，并在写入完成后返回执行结果。
12. executor 记录 `TxResult`，release lock queues，并向新队首发送 grant signal。
13. 当前 batch 内本 shard 相关 tx 全部完成后，shard 返回 `ExecuteBatchResponse`。

收到 batch 的 shard 即使不是某些 tx 的 participant，也不需要为这些 tx 执行任何 storage 操作。发送给所有 shards 是为了简化第一阶段协议和测试拓扑。

Sequencer 等待全体 shards 返回 batch ack。每个 shard 只执行自己相关的事务，忽略无关事务；完全无关的 shard 仍要对该 batch 返回空 `ExecuteBatchResponse`，作为 batch barrier 的一部分。

batch completion 对不同角色的含义：

- non-participant shard：该 tx 无本地工作。
- passive participant：worker 收到所有本地 lock grants、读取 local reads、发送 local read result 后返回，executor release lock queues；不记录 final result 或 read-only trace。
- active participant：worker 收到所有本地 lock grants、读取 local reads、收齐 full reads、执行 deterministic logic、过滤并应用本 shard local writes，然后返回 tx result；executor 记录 tx result、release lock queues，并通知后续 queue heads。

### 5.3 LocalReadResult

`LocalReadResult` RPC handler 不聚合 read set，也不执行事务。它直接根据 `(batch_id, tx_id)` 把消息投递到对应 read-result mailbox。

remote read result 可能早于 active worker 创建到达，甚至早于本 shard 开始处理对应 `ExecuteBatch`。shard runtime 对每个 `(batch_id, tx_id)` 使用一个共享 mailbox channel：

```rust
pub struct ReadResultMailbox {
    pub sender: mpsc::Sender<LocalReadResult>,
    pub receiver: Option<mpsc::Receiver<LocalReadResult>>,
}
```

当 `LocalReadResult` 到达时，RPC handler 加锁访问 `ReadResultMailboxRegistry`。如果还没有 mailbox，就创建一个 `(sender, receiver)`，clone 出 `sender` 后立即释放 registry lock，然后把消息 send 到 `sender`。如果 active worker 还没创建，消息自然缓存在 channel 中；如果 worker 已经创建，消息直接进入 worker 正在持有的 receiver。

创建 active worker 时，executor 加锁访问同一个 `ReadResultMailboxRegistry`，创建或找到 mailbox，取出 `receiver` 并交给 worker。`receiver` 只能被取走一次；worker 完成后 executor 删除该 `(batch_id, tx_id)` 的 mailbox。mailbox 只解决消息到达时序问题，不参与 full read set 聚合。

mailbox channel 使用 bounded `mpsc`，容量固定为 `shard_count`。第一阶段没有 duplicate/retry，每个 participant 对一个 active participant 最多发送一条 `LocalReadResult`，因此该容量足以容纳 worker 创建前所有可能早到的 remote read results。

实现约束：

- `ReadResultMailboxRegistry` 需要由 RPC handler 和 shard executor 共享，使用 `Arc<Mutex<_>>`。
- 不允许在持有 registry lock 时 `.await`。handler 只能在锁内 get-or-create mailbox 并 clone `sender`，然后释放锁再 send。
- 如果 mailbox 的 `receiver` 已被取走，后续 `LocalReadResult` 仍可通过已有 `sender` 进入 worker。
- 如果 worker 完成并删除 mailbox 后又收到同一 `(batch_id, tx_id)` 的 read result，视为重复或迟到消息；第一阶段无 retry/duplicate，直接返回 internal error 让测试失败。
- batch 结束时，executor 清理该 batch 的剩余 mailboxes；若存在没有被 active worker 取走 receiver 的 mailbox，说明收到了发往非 active participant 的 read result，应让测试失败。

active worker 聚合 remote reads 时必须校验：

- `from_shard` 必须属于该 tx 的 participants。
- 同一个 `from_shard` 只能出现一次。
- `reads` 中的 keys 必须等于该 `from_shard` 根据 `read_set` 和 `ShardLayout` 计算出的 local read keys。
- 每个 read key 必须只有一个 `ReadValue`：`Present(Inode)` 或 `Missing`。
- 收齐所有 participant read results 后，合并得到的 full read set 必须覆盖 `OrderedTx.read_set`，且不能包含额外 key。

active worker 启动时就持有 mailbox receiver。remote read result 可以在 worker 拿齐本地 lock grants 前到达，并自然缓存在 mailbox channel 中。

active worker 的执行顺序：

1. 从 lock-grant receiver 等待本 shard 所有 local keys 的 grant signals。
2. 从 store 读取本 shard local reads，并放入自己的聚合状态；本地 reads 不需要通过 self-RPC 回环。
3. 向其他 active participants 发送本 shard local read result。
4. 从 mailbox receiver 聚合来自其他 participants 的 read results。
5. 每个 read key 都必须有一个 `ReadValue`：`Present(Inode)` 或 `Missing`。
6. 检查是否收齐该 tx 的 participant results。
7. 合并成 full read set。
8. 调用 `execute_deterministic`。
9. 调用 `filter_local_writes` 计算本 shard 的 local writes。
10. 校验 local writes 的 key 都属于本 shard，且都在本 tx 的 local write keys 内。
11. 在一个 store write transaction 中应用 local writes。
12. 若本 shard 是该 tx 的 `result_shard`，写入完成后更新 `TxResultRegistry` 为 `Ready(result)`。
13. 将 tx result 返回给 executor。
14. executor 记录 tx result。
15. executor release lock queues，并通知每个被 release queue 的新队首。

### 5.4 GetTxResult 和 DumpState

`GetTxResult` 是 client-facing 查询接口，但语义是 blocking wait，不是 polling：

- client 只能依赖 `SubmitTxResponse.result_shard` 查询事务结果。
- `GetTxResult(tx_id)` 进入 shard 后，订阅或创建该 `tx_id` 的 `TxResultRegistry` entry。
- 若状态已是 `Ready(result)`，立即返回 `TX_RESULT_STATUS_READY`。
- 若状态是 `NotResponsible`，立即返回 `TX_RESULT_STATUS_NOT_RESPONSIBLE`。
- 若状态是 `Pending`，RPC handler await `watch::Receiver::changed()`，直到状态进入终态。
- response 不包含 `Pending`；未完成时 RPC 不返回。
- 任意错误 shard 或无效 tx_id 查询不作为第一版用户语义保证；client 应设置 RPC deadline。

`DumpState` 是测试/调试接口：

- `DumpState` 用于 checker 拉取 shard local state。

Sequencer 仍通过等待所有 shards 的 `ExecuteBatchResponse` 判断 batch barrier 完成；这和 client 通过 `GetTxResult` 获取单事务结果是两条不同路径。

### 5.5 Shard 内部 tokio 执行模型

`ExecuteBatch` RPC handler 不直接执行整个 batch。它只做参数校验，然后创建一个 `oneshot` response channel，向 shard 内部 batch executor 提交任务，并 `await` batch 完成。

结构：

```rust
pub struct ShardRuntime {
    shard_id: ShardId,
    store: Arc<RedbInMemoryInodeStore>,
    peers: BTreeMap<ShardId, ShardClient>,
    batch_tx: mpsc::Sender<BatchJob>,
    read_result_mailboxes: Arc<Mutex<ReadResultMailboxRegistry>>,
    client_results: TxResultRegistry,
}

pub struct BatchJob {
    batch: Batch,
    respond_to: oneshot::Sender<ExecuteBatchResponse>,
}

pub struct LockGrant {
    key: Key,
}

pub enum WorkerOutcome {
    PassiveDone {
        batch_id: BatchId,
        tx_id: TxId,
    },
    ActiveDone {
        batch_id: BatchId,
        tx_id: TxId,
        result: TxResult,
    },
}
```

启动 shard server 时，同时启动一个 shard-local executor task：

```text
Shard tonic server
  ExecuteBatch RPC -> batch_tx.send(BatchJob) -> await oneshot
  LocalReadResult RPC -> route result into read-result mailbox

Shard executor task
  顺序接收 BatchJob
  构造 batch-local LockTable
  为所有本 shard 相关 tx 创建 lock-grant channel
  一次性 spawn 所有本 shard 相关 tx worker
  对每个 lock queue 的队首发送 grant signal
  收集 WorkerOutcome
  release lock queues 并向新队首发送 grant signal
  respond ExecuteBatchResponse
```

并发控制规则：

- 同一个 shard 一次只执行一个 batch；batch 之间没有并发。
- batch 开始时一次性 spawn 所有本 shard 相关 tx 的 worker；无关 tx 不 spawn worker。
- shard executor task 是唯一修改 `LockTable` 和 batch-local tx state 的任务。
- worker 不能自己决定锁顺序；worker 只能等待 executor 通过 lock-grant channel 发来的 grant signals。
- 对每个 relevant tx，executor 创建一个 bounded `mpsc` lock-grant channel。该 tx 在每个本地 key lock queue 中的 entry 都保存同一个 sender 的 clone；worker 只持有一个 receiver，并等待 `local_keys.len()` 个 grant signals。
- 对 active tx，executor 必须先取得该 `(batch_id, tx_id)` read-result mailbox 的 receiver，再启动 worker，并把 receiver 一起交给 worker。
- worker 收齐本地 lock grants 后，从 store 读取 local reads。
- passive worker 负责向 active participants 发送 local read result，发送完成后返回 `PassiveDone`。
- active worker 负责把本 shard local reads 放入自己的聚合状态，并向其他 active participants 发送本 shard local read result；它同时从 mailbox receiver 接收 remote read results，收齐 full read set 后调用 `execute_deterministic`，再调用 `filter_local_writes` 算出本 shard local writes。
- active worker 在持有本地 lock grants 的情况下应用 local writes；写入完成后才能返回 `ActiveDone`。
- `LocalReadResult` RPC handler 直接做 `(batch_id, tx_id)` mailbox 路由，不聚合 read set。
- executor 不应用 local writes。executor 只记录 active worker 返回的 tx result，并在 worker 完成后 release lock queues。

worker 写本地 store 的约束：

- worker 只能在收齐本 shard 全部 local key grants 后读取和写入 store。
- worker 只能写 `filter_local_writes` 产生的 local writes。
- 每个 local write 的 key 必须满足 `layout.shard_for_key(key) == local_shard`。
- 每个 local write 的 key 必须属于该 tx 的 local write keys；否则视为 executor bug 或 transaction logic bug，让测试失败。
- active worker 必须先完成 local writes 的 store transaction，再返回 `ActiveDone`。
- executor 收到 `ActiveDone` 后才能 release lock queues；因此后续 worker 拿到 grant 并读取同一 key 时，一定能看到前序 tx 已提交的 local writes。

这样做的目标是避免 tonic RPC handler 之间互相等待导致死锁，同时把所有会影响确定性的调度决策集中在 shard executor task 中。

## 6. Deterministic Local Locking

### 6.1 基本规则

Calvin-style lock order 来自 Sequencer 指定的 batch 内顺序。第一阶段每个 shard 收到同一个 `Batch`，并按 `(batch_id, batch_index)` 组织本地 lock queues。

第一阶段可以简化为：

- 每个 key 有一个 FIFO lock queue。
- queue 顺序由 `(batch_id, batch_index)` 决定。
- 所有锁都是 exclusive lock，不实现 shared/read lock。
- tx 必须等待自己所需全部本地 read/write keys 都轮到自己。
- active/passive participant 都通过同一套确定性锁表管理本地 read/write keys。

### 6.2 死锁避免

禁止把 RPC 到达顺序或本地 task 调度顺序作为锁顺序。即使第一阶段不做故障或乱序测试，锁顺序也应统一来自 batch 内顺序。

实现接口：

```rust
pub struct LockTable {
    queues: BTreeMap<Key, VecDeque<LockQueueEntry>>,
}

pub struct LockQueueEntry {
    pub tx_id: TxId,
    pub grant_tx: mpsc::Sender<LockGrant>,
}

impl LockTable {
    pub fn enqueue(&mut self, key: Key, tx_id: TxId, grant_tx: mpsc::Sender<LockGrant>);
    pub fn grant_initial_heads(&self);
    pub fn release_and_grant_next(&mut self, tx_id: TxId, local_keys: &BTreeSet<Key>);
}
```

`LockTable` 是 batch-local 数据结构，由 shard executor task 独占访问，不用暴露给 worker tasks，也不需要额外 `Mutex<LockTable>`。

### 6.3 Grant、Release 和队列推进

第一版的 lock table 不保存真实锁对象；所谓加锁/放锁就是维护每个 key 的 deterministic queue head。

batch 初始化时，executor 为每个本 shard 相关 tx 创建一个 lock-grant channel：

```rust
let (grant_tx, grant_rx) = mpsc::channel::<LockGrant>(local_keys.len());
```

worker 只持有一个 `grant_rx`。executor 把同一个 `grant_tx` clone 放入该 tx 每个本地 key 的 queue entry：

```text
T2 local keys = {/a, /a/z}

/a   queue entry for T2 stores grant_tx.clone()
/a/z queue entry for T2 stores grant_tx.clone()
T2 worker owns one grant_rx and waits for 2 grants
```

这里不为每个 local key 创建一个 receiver。原因是多 receiver 会把 worker 侧的等待逻辑拆散；一个 receiver 加多个 sender clone 可以把“等待 N 个本地 key grant”表达成同一个 mailbox 上的计数问题。`LockGrant { key }` 带 key 字段，worker 可以用 `BTreeSet<Key>` 去重并校验是否收齐自己的全部 local keys。

队列 grant 规则：

- `grant_initial_heads()` 在 batch worker 全部 spawn 后调用。
- 对每个非空 key queue，只向队首 entry 的 `grant_tx` 发送一条 `LockGrant { key }`。
- 同一个 tx 如果是多个 key queue 的队首，会在同一个 receiver 上收到多条 grant。
- worker 等到收到 `local_keys.len()` 条 grant，并确认 grant keys 等于自己的 local key set 后，才读取 local reads。
- 如果一个 tx 还涉及其他 shard，本 shard 上收齐 local lock grants 不代表该 tx 已经收齐 full read set；active worker 仍必须等待其他 participants 的 `LocalReadResult`。

`release_and_grant_next(tx_id, local_keys)` 的定义：

- 对 `local_keys` 中每个 key，检查 `queues[key].front() == Some(tx_id)`。
- 若不是队首，说明 executor 或 worker 状态机有 bug，直接返回 internal error 或 panic 让测试失败。
- 若是队首，执行 `pop_front()`。
- 如果该 key queue 还有新队首，立即向新队首 entry 的 `grant_tx` 发送 `LockGrant { key }`。
- 如果某个 key 的 queue 变空，从 `queues` 中删除该 key。

release 必须由 shard executor task 在处理 `WorkerOutcome` 时一次性完成。worker 不直接修改 lock table。

batch executor 为每个本 shard 相关 tx 维护状态：

```rust
pub enum LocalTxState {
    Running,
    Done,
}
```

由于所有本 shard 相关 workers 在 batch 开始时已经 spawn，`Running` 表示 worker 已创建，可能正在等待 lock grants、等待 remote reads 或执行 deterministic logic。

```text
batch setup:
  for tx in batch.txs sorted by batch_index:
    if tx is relevant to this shard:
      create one lock-grant channel for tx
      enqueue one LockQueueEntry per local key, each with grant_tx.clone()
      create/take read-result mailbox receiver if tx is active
      spawn worker(tx, grant_rx, read_result_rx_if_active)
      state[tx_id] = Running

  lock_table.grant_initial_heads()
```

worker 完成后通过 channel 返回 `WorkerOutcome`。executor 的事件循环处理 outcome：

```text
on WorkerOutcome(tx_id):
  if ActiveDone:
    record tx result
  if PassiveDone:
    record no final result
  lock_table.release_and_grant_next(tx_id, local_keys[tx_id])
  state[tx_id] = Done
  if all relevant tx states are Done:
    respond ExecuteBatchResponse
```

因此，“唤醒后续事务”不是依赖 condition variable，也不是 worker 主动唤醒。唯一的推进点是 executor 在 release queue head 后，向每个被 release 的 key queue 新队首发送 grant signal。worker 自己通过同一个 receiver 收集来自多个 key queue 的 grants。

判断某个 tx 是否可以开始本地 reads，不再由 executor 扫描判断，而由 worker 的 grant 计数自然决定：

```text
T2 local keys = {k1, k2, k3}
T2 can read local state iff
  T2 worker has received grants for k1, k2, and k3
```

如果任意一个本地 key 的队首仍是其他未完成 tx，T2 worker 就收不到该 key 的 grant，并继续 await。executor 不需要全表扫描，也不需要显式判断 T2 是否 ready。如果 T2 的某些 read/write keys 属于其他 shard，本 shard 不检查那些 remote keys；remote shard 会独立做同样的 local lock grant，active worker 通过 read-result exchange 等待 full reads。

## 7. 正确性检查

### 7.1 Serial reference execution

checker 在单机内存中按 Sequencer batch log 顺序执行所有 tx：

```text
initial_state + Vec<Batch> -> reference_final_state
```

reference execution 使用同一个 `execute_deterministic`，但直接应用完整 writes。

### 7.2 Sharded state merge

checker 调用每个 shard 的 `DumpState`，合并成：

```text
all_shards_dump -> sharded_final_state
```

每个 key 只能出现在 owner shard。若同一 key 出现在多个 shard，checker 应失败。

### 7.3 TxResult consistency

对每个 tx：

- 通过 `write_set` 的 owner shards 找到 active participants；read-only tx 通过 `read_only_coordinator(read_set)` 找到唯一 active participant。
- 从 Sequencer 聚合的 `ExecuteBatchResponse` 中读取各 active participant 的 `TxResultRecord`。
- 要求所有 active participants 的 result 相同。
- passive participants 不记录 final result 或 read-only trace。

## 8. madsim 测试计划

### 8.1 `sim_ping_shards`

目标：

- 创建 1 个 simulated sequencer node。
- 创建 3-4 个 simulated shard nodes。
- 为每个 node 注册 DNS。
- 启动 tonic Sequencer server。
- 启动 tonic Shard server。
- test driver 调用 Sequencer 和 Shard 的 `Ping`。

验收：

- Sequencer 和所有 shard 返回正确 id。

### 8.2 `sim_submit_batch_basic`

事务：

- `mkdir /`
- `mkdir /a`
- `create /a/x`
- `stat /a/x`
- `unlink /a/x`

验收：

- test driver 只向 Sequencer 调用 `SubmitBatch`。
- Sequencer 生成一个 `Batch`，并通过 `ExecuteBatch` 发送给所有 shards。
- 所有 tx result 符合预期。
- final sharded state 等于 serial reference state。

### 8.3 `sim_cross_shard_rename`

事务：

- 构造 `/a/x -> /b/y`，让 src 和 dst 落到不同 shards。

验收：

- active participants result 一致。
- src 被删除。
- dst 被创建。
- 每个 key 只在 owner shard 出现。

### 8.4 `sim_error_semantics`

事务：

- `mkdir /`
- `stat /missing`
- `mkdir /a`
- `create /a/x`
- `create /a/x`
- `rmdir /a`

验收：

- 返回 `Ok`、`NotFound`、`Ok`、`Ok`、`AlreadyExists`、`DirectoryNotEmpty` 等预期结果。
- final sharded state 等于 serial reference state。
- active participants 的 `TxResult` 一致。

## 9. Roadmap

### Milestone 1：sequencer-driven batch Calvin core under madsim

范围：

- Cargo 项目骨架。
- proto + tonic build。
- Sequencer service。
- Shard service。
- madsim multi-node test harness。
- internal model + proto conversion。
- redb in-memory metadata store。
- stable router。
- Sequencer-generated batches。
- deterministic local locking。
- read-result forwarding。
- active participant execution。
- DumpState checker。

完成标准：

- `RUSTFLAGS="--cfg madsim" cargo test` 通过核心 simulation tests。
- final state 与 serial reference 对齐。
- active participant tx results 一致。

### Milestone 2：online SubmitTx and blocking GetTxResult

范围：

- 在 Milestone 1 的 Sequencer 基础上支持 client-facing 单事务 `SubmitTx`。
- Sequencer 使用 actor + `oneshot` reply 管理 online submit、open batch、flush timer 和 dispatch queue。
- open batch 使用 `max_batch_size = 512` 和可配置 `batch_flush_interval` 双条件 flush。
- `SubmitTx` 返回 `tx_id` 和单个 `result_shard`，不暴露 `batch_id`。
- Shard 提供 blocking `GetTxResult`，通过 awaitable `TxResultRegistry` 等待结果完成。
- `SubmitBatch` 保留为 internal/test API。
- 保持不处理 participant failure。

完成标准：

- client 只调用 `SubmitTx` 和 `GetTxResult` 即可完成基本元数据事务。
- `GetTxResult` 未完成时在服务端 await，不需要 client polling。
- read-only `Stat` 的 `result_shard` 等于 deterministic read-only coordinator。
- madsim client API test 通过，并且 final state 与 serial reference execution 一致。

### Milestone 3：engine workload and validation

范围：

- 面向 engine 的随机 metadata operation workload。
- 多 simulated client nodes。
- 多 batch 顺序执行和结果查询。
- checker 覆盖更多路径和错误语义。
- latency/throughput/trace summary 仅作为模拟测试输出。

## 10. 收敛状态

当前 v2 设计选择已收敛：client-facing API 固定为 `SubmitTx` + blocking `GetTxResult`，batch 仍是 Sequencer 和 shard 之间的内部执行单位。后续讨论应以实现过程中暴露出的具体 bug、测试缺口或性能瓶颈为准。

## 11. 术语

- Batch：Sequencer 一次发送给所有 shards 的 ordered transactions 集合。
- Batch log：Sequencer/test driver 内部保存的 `Vec<Batch>`，用于 checker 和 debug；shards 对它无感知。
- OrderedTx：带 tx_id、batch_index 和 read/write set 的事务。
- participant：持有该 tx read/write key 的 shard。
- active participant：持有该 tx write key 的 shard，需要执行 deterministic logic 并记录 final result。
- passive participant：只持有 read key 的 shard，只负责 local read 和 forwarding。
- local reads：某 shard 本地拥有的 read set 子集。
- full reads：执行事务逻辑需要的完整 read set。
- local writes：某 shard 负责实际应用的 write intent 子集。
- checker：madsim 测试中的 correctness oracle，不参与线上执行。

# SCC Online Scheduler Design

状态：v0 已收敛设计，用于实现博士论文研究内容二 4.3.2 的在线调度器。

本文档描述如何在现有 CalvinFS metadata execution engine 上新增一个 SCC-style semantic concurrency control scheduler。目标不是替换已有 Calvin deterministic-locking demo，而是在同一套 client API、Sequencer batching、Shard RPC 和 madsim 测试环境下，提供第二种 batch execution strategy，用于正确性验证和性能对比。

v0 只实现在线调度器，不实现离线分析。语义冲突表、路径编号和路径条件检查规则由代码手写。实现完成后，原 CalvinFS demo 必须仍能以默认配置运行。

## 1. 目标

新增一个在线语义调度器实现，作为 `CalvinLocking` 之外的第二种 shard scheduler：

- 保留现有 `Sequencer.SubmitTx`、`Sequencer.SubmitBatch`、`Shard.ExecuteBatch`、`Shard.GetTxResult` RPC 语义。
- Sequencer 仍负责把在线事务组织成 ordered `Batch`。
- 所有 shard 仍收到完整 batch，并独立执行确定性调度。
- Shard 通过配置选择 `CalvinLocking` 或 `SccOnline`。
- `CalvinLocking` 保持现有 deterministic local locking 实现不变。
- `SccOnline` 使用两个独立 DAG：
  - `GraphEffect`：约束 delta 计算前必须观察到的副作用依赖。
  - `GraphCondition`：约束路径条件复查前必须观察到的条件依赖。
- `SccOnline` 使用 commit sequence 和 delta merge 替代互斥锁。
- 语义冲突表手写，不实现静态/离线分析。
- v0 预测路径固定为成功路径。若实际执行没有走成功路径，则触发确定性重排序 fallback；最终状态保证可串行化，但串行化顺序允许不同于 Sequencer 给出的 batch 原始顺序。

## 2. 核心原则

### 2.1 与 Calvin baseline 共存

SCC 是 shard 内部的 batch execution strategy，不是新的客户端协议。

配置类型：

```rust
pub enum SchedulerKind {
    CalvinLocking,
    SccOnline,
}

pub struct ShardConfig {
    pub node_id: String,
    pub shard_id: ShardId,
    pub shard_count: u64,
    pub peer_endpoints: BTreeMap<ShardId, String>,
    pub scheduler: SchedulerKind,
}
```

默认值必须是 `CalvinLocking`，保证现有测试和 demo 不需要修改即可运行。madsim cluster helper 通过显式传入 `SchedulerKind::SccOnline` 启动对比集群。

### 2.2 Shard 本地独立确定性

每个 shard 对同一个 batch 独立构造完全相同的语义依赖图。图构造只能依赖：

- batch 内事务顺序。
- 每个事务的 `FsOp`、`read_set`、`write_set`。
- 确定性的路径分类函数。
- 确定性的 conflict mask 计算。
- 手写语义冲突表。

图构造不能依赖 RPC 到达顺序、任务调度顺序、本 shard 本地状态、随机数或 wall-clock。

batch 中每个 `OrderedTx` 必须满足两个集合不变量：

- `participant_shards == owners(read_set ∪ write_set)`。不允许只是子集或超集。
- `result_shard ∈ participant_shards`。write tx 的 `result_shard` 固定为 write participant 中最小 shard id；read-only tx 的 `result_shard` 固定为 `participant_shards` 中最小 shard id。

### 2.3 Delta 是必要接口

现有 executor 输出的是绝对写入：

```rust
WriteOp::Put { key, value }
WriteOp::Delete { key }
```

这对 Calvin locking 是正确的，因为同一 key 上的事务不会并发进入临界区。但 SCC 要允许语义上不冲突的事务并发更新同一目录 inode，例如同一父目录下并发创建不同文件。若两个 create 都从 `child_count = 0` 读出并输出 `Put(parent, child_count = 1)`，最终会丢一次更新。

因此 SCC 必须把执行结果转换为可合并 delta：

```rust
pub enum DeltaOp {
    Put { key: Key, value: Inode },
    Delete { key: Key },
    AddIntegerField {
        key: Key,
        field: InodeIntegerField,
        delta: i64,
    },
}

pub enum InodeIntegerField {
    ChildCount,
}

pub struct TxDelta {
    pub ops: Vec<DeltaOp>,
}
```

delta merge 规则：

- `Put` 创建或覆盖完整 inode。
- `Delete` 删除 key。
- `AddIntegerField` 对整型字段做有符号增量。v0 只定义并使用 `ChildCount`。
- 对不存在、非目录或下溢的 `AddIntegerField(ChildCount)` 视为调度器 bug，返回 internal error。

`TxDelta` 不重复保存 write keys。事务的写集合以 `OrderedTx.write_set` 为唯一来源；`SccBatchPlan` 会把每个 tx 的 `write_set` 投影成本 shard 的 `local_write_keys_by_tx`，供物化读判断是否需要等待 pending slot。delta ops 也从 op 语义和 `write_set` 构造出来。

### 2.4 两个 DAG 不能合并

`GraphEffect` 和 `GraphCondition` 是两个独立 DAG，不能用一张保守 DAG 替代。

原因：

- effect dependency 描述 “前序事务的副作用对后序事务 delta 计算的影响关系”。
- condition dependency 描述 “前序事务的副作用对后序事务已预测路径成立条件的影响关系”。
- 两者是独立关系。合并会牺牲论文算法要表达的调度结构，也不利于分析性能差异。

v0 分别构造、分别维护、分别等待两张 DAG，并在建图完成后分别预计算物化读所需的 prefix bound。

## 3. 模块划分

### 3.1 `scheduler`

新增调度器入口层。

职责：

- 定义 `SchedulerKind`。
- 在 `ShardRuntime` batch executor 中选择 Calvin 或 SCC。
- 保持 `BatchExecutionSummary`、client result registry、DumpState 行为一致。

v0 不抽 trait。shard batch executor 直接按 `SchedulerKind` 分支调用 `execute_calvin_batch` 或 `execute_scc_batch`。

### 3.2 `scc`

新增 SCC 在线调度核心模块。

职责：

- 路径分类。
- conflict mask 计算。
- 手写语义冲突表。
- `GraphEffect` 和 `GraphCondition` 构造。
- commit sequence 管理。
- materialized local read。
- SCC worker 生命周期。
- 推测失败记录和 fallback 协调。

核心类型：

```rust
pub type PathId = u16;

pub enum DagKind {
    Effect,
    Condition,
}

pub struct SccBatchPlan {
    pub effect: SemanticDag,
    pub condition: SemanticDag,
    pub tx_plans: Vec<SccTxPlan>,
}

pub struct SccTxPlan {
    pub predicted_path: PathId,
    pub effect_max_pred_index: Option<usize>,
    pub condition_max_pred_index: Option<usize>,
}

pub struct SemanticDag {
    pub nodes: Vec<DagNode>,
}

pub struct DagNode {
    pub successors: BTreeSet<usize>,
    #[cfg(debug_assertions)]
    pub predecessors: BTreeSet<usize>,
}

pub struct CommitSequence {
    slots: Vec<CommitSlotCell>,
}

pub struct CommitSlotCell {
    state_tx: watch::Sender<CommitSlotState>,
}

#[derive(Clone)]
pub enum CommitSlotState {
    Pending,
    NoOp,
    Delta(Arc<TxDelta>),
    Failed,
}
```

`SccTxPlan` 的事务位置由 `tx_plans` 的数组下标表示；spawn worker 时把该下标作为独立 `tx_index` 参数传入。

`effect_max_pred_index` 和 `condition_max_pred_index` 是建图阶段预计算出的物化读 prefix bound。`None` 表示该阶段没有前驱；否则物化本地读需要按 commit sequence 前缀检查失败并重放相关 delta 到该 index。

worker 启动时直接拿到自己的 `SccTxPlan`，不在运行时重新扫描 DAG。`SemanticDag` 在 release 执行路径只保留 successor 关系；predecessor 集合只在 debug build 中保留，用于断言和调试输出，调度逻辑不得读取 predecessor 集合。prefix bound 是 builder 的输出，不在 runtime DAG 中重复保存。

`SccTxPlan` 还提供确定性优化判断：若 `effect_max_pred_index` 覆盖 `condition_max_pred_index`，condition read view 已被 effect read view 覆盖。比较规则是 `None` 表示无前缀；`(_, None)` 覆盖成立，`(Some(effect), Some(condition))` 在 `effect >= condition` 时覆盖成立，`(None, Some(_))` 覆盖不成立。

`CommitSequence` 是 batch-local append-once slot 数组。每个 tx 对应一个 slot，每个 slot 独立使用 `watch` 发布 terminal state。没有全局 commit-sequence mutex；物化读等待哪个 slot，就订阅哪个 slot。

固定接口：

```rust
impl CommitSequence {
    pub fn new(batch_len: usize) -> Self;

    pub async fn wait_terminal(&self, index: usize) -> Result<CommitSlotState>;

    pub fn set_terminal_once(
        &self,
        index: usize,
        state: CommitSlotState,
    ) -> Result<()>;

    pub fn terminal_snapshot(&self) -> Vec<CommitSlotState>;
}
```

约束：

- `set_terminal_once` 只允许 `Pending -> NoOp | Delta | Failed`，重复写同一个 slot 是调度器 bug。
- 每个 slot 的 terminal writer 由调度流程唯一确定：相关 tx 的 worker 写自己的 slot；本 shard 完全无关 tx 由 coordinator 写 `NoOp`。
- `wait_terminal` 先读当前 state；若仍是 `Pending`，再订阅该 slot 的 watch receiver 并 await。
- `Delta` 使用 `Arc<TxDelta>`，多个物化读共享同一份 delta。

### 3.3 `dag_runtime`

SCC 不使用 Calvin 的 per-key 授锁邮箱。Calvin 的 `LockTable` 和 lock grant channel 只服务 `CalvinLocking` baseline；`SccOnline` 使用两个 DAG 通知邮箱。

职责：

- 为 `GraphEffect` 和 `GraphCondition` 分别维护运行时入度。
- 为每个 `(dag_kind, tx_index)` 创建一个 ready mailbox。
- batch 开始时通知所有入度为 0 的顶点。
- vertex terminal 后删除两张图中的出边，递减后继入度。
- 当某个后继入度降为 0 时，向该后继的 ready mailbox 发送 ready。

运行时类型：

```rust
pub struct SccDagRuntime {
    effect: DagRuntime,
    condition: DagRuntime,
}

pub struct DagRuntime {
    nodes: Vec<DagRuntimeNode>,
}

pub struct DagRuntimeNode {
    indegree: usize,
    successors: BTreeSet<usize>,
    ready_tx: Option<oneshot::Sender<()>>,
}

pub struct TxDagWaiters {
    pub effect_ready: oneshot::Receiver<()>,
    pub condition_ready: oneshot::Receiver<()>,
}
```

每个 SCC worker 只拿到自己的 `TxDagWaiters`，不直接修改 DAG。worker 完成 terminal slot 后向 batch coordinator 报告 `TxTerminal { tx_index }`；coordinator 串行调用 `SccDagRuntime::finish_vertex(tx_index)`，从而保证 DAG 维护逻辑简单、确定且没有并发写图。

入度为 0 的通知是一次性语义，固定使用 `oneshot`。coordinator 在 batch start 时先为所有 vertex 创建 sender/receiver，再发送初始 ready；即使 ready 早于 worker await 发生，`oneshot::Receiver` 也能正常收到。

### 3.4 `semantic_table`

手写语义表模块。v0 不做离线分析。

职责：

- 给 `FsOp` 分配成功路径编号。
- 根据两笔事务的路径参数计算 conflict mask。
- 维护两张只读语义冲突表：effect conflict table 和 condition conflict table。
- 运行时只通过 `(Path1, Path2, ConflictMask)` 查表，不把表内容写成散落的分支逻辑。

接口：

```rust
pub fn predicted_path(op: &FsOp) -> PathId;

pub fn conflict_mask(lhs: &FsOp, rhs: &FsOp) -> ConflictMask;

pub struct SemanticKey {
    pub path1: PathId,
    pub path2: PathId,
    pub conflict_mask: ConflictMask,
}

pub struct SemanticTable {
    entries: BTreeMap<SemanticKey, bool>,
    default_conflict: bool,
}

impl SemanticTable {
    pub fn has_conflict(&self, key: &SemanticKey) -> bool;
}

pub struct SemanticTables {
    pub effect: SemanticTable,
    pub condition: SemanticTable,
}
```

表值使用 conflict 语义：`true` 表示该 `(Path1, Path2, ConflictMask)` 冲突，需要在对应 DAG 中加边；`false` 表示不冲突，不加边。v0 手动构造表项，但调度器必须像查表一样使用这些表项。

`default_conflict` v0 固定为 `true`。没有明确表项的路径组合默认冲突，保证手写表保守。

v0 路径预测固定为成功路径，不枚举失败路径：

- `MkdirSuccess`
- `CreateSuccess`
- `UnlinkSuccess`
- `RmdirSuccess`
- `RenameSuccess`
- `StatSuccess`

实际执行结果与预测路径不一致时，视为推测失败。例如 `CreateSuccess` 实际得到 `AlreadyExists`、`NotFound` 或 `NotDirectory`，都不在 SCC 推测路径内处理，而是进入 fallback。

### 3.5 `delta`

新增 delta 计算和 merge 模块。

职责：

- 将 `execute_deterministic` 的 `TxExecutionOutput` 转换为 `TxDelta`。
- 在 materialized state 上应用 delta。
- 在真实 store 上按 deterministic reorder 顺序安装 speculative success delta。

接口：

```rust
pub fn output_to_delta(
    tx: &OrderedTx,
    reads: &BTreeMap<Key, ReadValue>,
    output: TxExecutionOutput,
) -> Result<TxDelta>;

pub fn apply_delta_to_state(
    state: &mut BTreeMap<Key, Inode>,
    delta: &TxDelta,
) -> Result<()>;

pub fn apply_delta_to_store(
    store: &RedbInMemoryInodeStore,
    delta: &TxDelta,
) -> Result<()>;
```

`output_to_delta` 只在 actual path 与预测成功路径匹配后调用。v0 预测路径固定为成功路径，因此 `TxResult != Ok` 不进入 delta conversion，而是写 `CommitSlotState::Failed` 并触发 fallback。成功但本 shard 没有 local write 的事务才写 `CommitSlotState::NoOp`。

delta conversion 是确定性字段 diff，不按操作名散落编写临时代码。v0 只支持当前 inode 模型实际存在的可合并字段：

- integer additive：整型字段用 `new - old` 生成 additive delta，安装时累加。当前 demo 中 `child_count` 属于这一类。

字段规则：

- `kind`：创建后不可变。`None -> Some` 用 `Put`，`Some -> None` 用 `Delete`；已有 inode 的 `kind` 变化只能退化为 `Put`，对应路径必须由语义表保守串行化。
- `child_count`：目录 inode 的 integer additive 字段。

1. 以 `OrderedTx.write_set` 作为唯一允许产生 delta 的 key 集合；`output` 中出现不属于 `write_set` 的 key 是 executor bug。
2. 对每个本 shard 拥有的 write key，取 `reads[key]` 作为 old value，取 `output` 中该 key 的最终写入作为 new value。
3. 若 `old = None` 且 `new = Some(inode)`，生成 `DeltaOp::Put { key, value: inode }`。
4. 若 `old = Some(_)` 且 `new = None`，生成 `DeltaOp::Delete { key }`。
5. 若 `old = Some(dir_old)` 且 `new = Some(dir_new)`，并且二者都是目录 inode，除 additive 字段外其它字段相同，则对变化的 `child_count` 生成 `DeltaOp::AddIntegerField { field: ChildCount, delta }`。`delta = 0` 时不产生 op。
6. 其它 `Some(old) -> Some(new)` 变化生成 `DeltaOp::Put { key, value: new }`。
7. 对 `write_set` 中未被 `output` 写入、且 old/new 等价的 key，不产生 delta。

该规则同时覆盖 `CreateSuccess`、`MkdirSuccess`、`UnlinkSuccess`、`RmdirSuccess` 和 v0 受限的 `RenameSuccess`。例如同父目录下多个 create 对父目录 inode 产生多个 `AddIntegerField(ChildCount, +1)`，安装时按 delta merge 累加，不会覆盖彼此。

## 4. RPC 变化

v0 保持 client-facing API 不变，只修改 shard 内部 wire format。

Shard 内部 read-result exchange 增加 phase 字段：

```proto
enum ReadPhase {
  READ_PHASE_UNSPECIFIED = 0;
  READ_PHASE_CALVIN = 1;
  READ_PHASE_SCC_EFFECT = 2;
  READ_PHASE_SCC_CONDITION = 3;
}

message LocalReadResultRequest {
  uint64 batch_id = 1;
  uint64 tx_id = 2;
  uint64 from_shard = 3;
  repeated ReadEntry reads = 4;
  ReadPhase phase = 5;
  LocalReadStatus status = 6;
}

enum LocalReadStatus {
  LOCAL_READ_STATUS_UNSPECIFIED = 0;
  LOCAL_READ_STATUS_OK = 1;
  LOCAL_READ_STATUS_SPECULATION_FAILED = 2;
}

message SccReorderRecord {
  uint64 batch_id = 1;
  repeated uint32 speculative_success_indices = 2;
  repeated uint32 fallback_indices = 3;
}

message DumpStateResponse {
  repeated InodeEntry entries = 1;
  repeated SccReorderRecord scc_reorders = 2;
}
```

兼容规则：

- Calvin worker 发送 `READ_PHASE_CALVIN`。
- SCC effect 阶段发送 `READ_PHASE_SCC_EFFECT`。
- SCC condition 阶段发送 `READ_PHASE_SCC_CONDITION`。
- SCC deterministic reorder fallback 直接复用 Calvin deterministic-locking worker，因此 fallback read-result exchange 使用 `READ_PHASE_CALVIN`。
- materialized read 成功时发送 `LOCAL_READ_STATUS_OK`。
- materialized read 观察到 `Failed` slot 时，发送 `LOCAL_READ_STATUS_SPECULATION_FAILED`，不携带 reads。
- mailbox key 从 `(batch_id, tx_id)` 改为 `(batch_id, tx_id, phase)`。
- `SubmitTx`、`SubmitBatch`、`ExecuteBatch`、`GetTxResult` 不变。
- SCC speculative phase 结束后不再做 completion report 通信；fallback 只使用本 shard commit sequence 中确定性推导出的 failed tx index 集合。
- `DumpState` 是 test/debug RPC；SCC 模式下额外返回每个 batch 的 shard-local deterministic reorder summary。`fallback_indices` 表示该 shard 本地 failed tx indices；`speculative_success_indices` 表示本 shard 不需要 fallback 的 tx indices，包括 non-participant 的本地 `NoOp`。

## 5. 语义图构造

对 batch 中任意 `i < j` 的两笔事务：

1. 计算 `path_i = predicted_path(tx_i.op)`。
2. 计算 `path_j = predicted_path(tx_j.op)`。
3. 计算 `mask = conflict_mask(tx_i.op, tx_j.op)`。
4. 构造 `SemanticKey { path1: path_i, path2: path_j, conflict_mask: mask }`。
5. 查询 `SemanticTables.effect.has_conflict(key)`；若返回 `true`，在 `GraphEffect` 添加边 `i -> j`。
6. 查询 `SemanticTables.condition.has_conflict(key)`；若返回 `true`，在 `GraphCondition` 添加边 `i -> j`。
7. 添加边时在 builder 内更新目标节点的临时 `max_pred_index = max(max_pred_index, i)`。

DAG 边只从小 index 指向大 index，因此天然无环。

建图完成后，为每个 tx 固化一个 `SccTxPlan`：

- `predicted_path`
- `effect_max_pred_index`
- `condition_max_pred_index`

运行期 worker 和 materialized read 只读取 `SccTxPlan`，不再动态计算前驱最大序号。

### 5.1 Conflict mask

v0 只建模成功路径之间使用的路径关系：

```rust
pub struct ConflictMask(u64);

const SAME_TARGET: u64 = 1 << 0;
const SAME_PARENT: u64 = 1 << 1;
const DIFFERENT_TARGET: u64 = 1 << 2;
const LHS_TARGET_IS_RHS_PARENT: u64 = 1 << 3;
const ANCESTOR_DESCENDANT: u64 = 1 << 4;
const ROOT_INVOLVED: u64 = 1 << 5;
const INDEPENDENT_SUBTREE: u64 = 1 << 6;
const RENAME_INVOLVED: u64 = 1 << 7;
```

对 `Rename`，需要同时考虑 `src`、`dst`、`parent(src)`、`parent(dst)` 与另一事务目标路径之间的关系。v0 对任何 `RENAME_INVOLVED` 直接判定 effect 和 condition 都冲突。

`conflict_mask(lhs, rhs)` 必须返回 canonical mask。也就是说，同一对路径关系只能映射到一个稳定 bitset，不能同时带上无关位。优先级：

1. `ROOT_INVOLVED`
2. `RENAME_INVOLVED`
3. `SAME_TARGET`
4. `LHS_TARGET_IS_RHS_PARENT`
5. `ANCESTOR_DESCENDANT`
6. `SAME_PARENT | DIFFERENT_TARGET`
7. `INDEPENDENT_SUBTREE`

表初始化和查表都使用 canonical mask。扩展更细关系时，必须新增明确 bit 或枚举值，而不是让查询端组合临时 mask。

### 5.2 初始手写语义表

v0 语义表只处理成功路径，具体路径为 `CreateSuccess`、`MkdirSuccess`、`UnlinkSuccess`、`RmdirSuccess`、`RenameSuccess` 和 `StatSuccess`。运行时语义信息是只读键值表，键是 `(Path1, Path2, ConflictMask)`，值是 `conflict: bool`。

v0 `PathId`：

```rust
pub enum PathId {
    CreateSuccess,
    MkdirSuccess,
    UnlinkSuccess,
    RmdirSuccess,
    RenameSuccess,
    StatSuccess,
}
```

`StatSuccess` 表示 `Stat` 的成功路径：目标 path 存在。当前 client-facing `Stat` 只返回 `TxResult`，不返回 inode metadata，因此它不观察 `child_count`、version 等 inode 内容。

表项构造规则：

- `default_conflict = true`。
- 所有未列出的 `(Path1, Path2, ConflictMask)` 默认冲突。
- `SemanticKey { path1, path2, conflict_mask }` 是有序 key。`(CreateSuccess, MkdirSuccess, mask)` 和 `(MkdirSuccess, CreateSuccess, mask)` 是两个不同表项。
- 下表中的 `Path1 values` / `Path2 values` 是初始化期展开规则。实现必须在表初始化时把它们展开为具体 `(PathId, PathId, ConflictMask) -> false` 条目；运行时 `has_conflict` 只能做精确 key 查找，不能再判断分组。

v0 effect table 的 `conflict = false` 表项：

| Path1 values | Path2 values | ConflictMask | 展开后条目数 | 语义 |
| --- | --- | --- | --- | --- |
| `CreateSuccess`, `MkdirSuccess`, `UnlinkSuccess`, `RmdirSuccess` | `CreateSuccess`, `MkdirSuccess`, `UnlinkSuccess`, `RmdirSuccess` | `SAME_PARENT | DIFFERENT_TARGET` | 16 | 同一父目录下不同 target 的 namespace mutation 不互相覆盖，父目录 `child_count` 通过 delta 合并 |
| `StatSuccess` | `StatSuccess` | `SAME_TARGET` | 1 | read-only stat 之间无状态转移冲突 |
| `StatSuccess` | `StatSuccess` | `SAME_PARENT | DIFFERENT_TARGET` | 1 | read-only stat 之间无状态转移冲突 |
| `StatSuccess` | `StatSuccess` | `LHS_TARGET_IS_RHS_PARENT` | 1 | read-only stat 之间无状态转移冲突 |
| `StatSuccess` | `StatSuccess` | `ANCESTOR_DESCENDANT` | 1 | read-only stat 之间无状态转移冲突 |
| `CreateSuccess`, `MkdirSuccess`, `UnlinkSuccess`, `RmdirSuccess` | `StatSuccess` | `SAME_PARENT | DIFFERENT_TARGET` | 4 | `Stat` 只返回 `TxResult`，同父不同 target 的 namespace mutation 不影响该 stat 结果 |
| `StatSuccess` | `CreateSuccess`, `MkdirSuccess`, `UnlinkSuccess`, `RmdirSuccess` | `SAME_PARENT | DIFFERENT_TARGET` | 4 | 同上，另一个有序方向 |
| `CreateSuccess`, `MkdirSuccess`, `UnlinkSuccess`, `RmdirSuccess`, `StatSuccess` | `CreateSuccess`, `MkdirSuccess`, `UnlinkSuccess`, `RmdirSuccess`, `StatSuccess` | `INDEPENDENT_SUBTREE` | 25 | 两个非 rename 成功路径位于独立子树，不共享 target 或 parent inode，状态转移不冲突 |

v0 condition table 的 `conflict = false` 表项：

| Path1 values | Path2 values | ConflictMask | 展开后条目数 | 语义 |
| --- | --- | --- | --- | --- |
| `CreateSuccess`, `MkdirSuccess`, `UnlinkSuccess`, `RmdirSuccess` | `CreateSuccess`, `MkdirSuccess`, `UnlinkSuccess`, `RmdirSuccess` | `SAME_PARENT | DIFFERENT_TARGET` | 16 | 同一父目录下不同 target 的 missing/existing/type/empty 条件互不影响 |
| `StatSuccess` | `StatSuccess` | `SAME_TARGET` | 1 | read-only stat 条件互不影响 |
| `StatSuccess` | `StatSuccess` | `SAME_PARENT | DIFFERENT_TARGET` | 1 | read-only stat 条件互不影响 |
| `StatSuccess` | `StatSuccess` | `LHS_TARGET_IS_RHS_PARENT` | 1 | read-only stat 条件互不影响 |
| `StatSuccess` | `StatSuccess` | `ANCESTOR_DESCENDANT` | 1 | read-only stat 条件互不影响 |
| `CreateSuccess`, `MkdirSuccess`, `UnlinkSuccess`, `RmdirSuccess` | `StatSuccess` | `SAME_PARENT | DIFFERENT_TARGET` | 4 | 同父不同 target 的 namespace mutation 不影响该 stat 目标是否存在 |
| `StatSuccess` | `CreateSuccess`, `MkdirSuccess`, `UnlinkSuccess`, `RmdirSuccess` | `SAME_PARENT | DIFFERENT_TARGET` | 4 | 同上，另一个有序方向 |
| `CreateSuccess`, `MkdirSuccess`, `UnlinkSuccess`, `RmdirSuccess`, `StatSuccess` | `CreateSuccess`, `MkdirSuccess`, `UnlinkSuccess`, `RmdirSuccess`, `StatSuccess` | `INDEPENDENT_SUBTREE` | 25 | 两个非 rename 成功路径位于独立子树，路径条件互不影响 |

对未在上表列出的 path 组合，以下 mask 默认冲突，不需要显式列出 `conflict = true` 表项：

- `SAME_TARGET`
- `LHS_TARGET_IS_RHS_PARENT`
- `ANCESTOR_DESCENDANT`
- `ROOT_INVOLVED`
- `RENAME_INVOLVED`

`INDEPENDENT_SUBTREE` 对两个非 rename 成功路径明确不冲突；任何包含 `RenameSuccess` 的 `INDEPENDENT_SUBTREE` 组合仍由 `RENAME_INVOLVED` 优先级归入保守冲突路径。

## 6. SCC Batch 执行流程

### 6.1 Batch start

Shard 收到 `ExecuteBatch` 后：

1. 校验 batch order、read/write set、`participant_shards == owners(read_set ∪ write_set)`，以及 `result_shard ∈ participant_shards`。
2. 构造 `SccBatchPlan`，包含 `GraphEffect`、`GraphCondition` 和 predicted paths。
3. 创建本 batch 的 commit sequence：`CommitSequence::new(batch.txs.len())`，所有 slot 初始为 `Pending`。
4. 创建 `SccDagRuntime`，并为每个 tx 创建两个 DAG ready mailbox。
5. 初始化 DAG runtime，向所有 effect/condition 入度为 0 的 vertex 发送 ready。
6. 对本 shard 参与的 tx 创建两个 read-result mailbox。参与定义是该 tx 在本 shard 上有 local read key 或 local write key；等价于本 shard 属于该 tx 的 `participant_shards`。local read result 允许为空集合：
   - `(batch_id, tx_id, SCC_EFFECT)`
   - `(batch_id, tx_id, SCC_CONDITION)`
7. 为 client result registry 标记 `Pending` 或 `NotResponsible`，与 Calvin 模式一致。
8. 对本 shard 相关的 tx 全部 spawn worker，并把对应 `TxDagWaiters` 交给 worker。
9. 对本 shard 完全无关的 tx，coordinator 将本地 commit slot 视为 `NoOp`，并在 DAG runtime 中完成该 vertex，避免本 shard 的本地 DAG 等待一个不会 spawn 的 worker。

本 shard 相关 tx 定义为：该事务在本 shard 上有 local read key 或 local write key。只有 read/write projection 都为空的 tx 才是本 shard 无关 tx，由 coordinator 直接置为 `NoOp`。

### 6.2 DAG notification flow

SCC 的等待和唤醒流程替代 Calvin 授锁流程：

1. worker 启动后等待 `effect_ready`。
2. effect phase 完成后，worker 不唤醒后继；若发现本地 speculative failure，直接写 `Failed` terminal slot，否则保存 effect 阶段暂存 delta，暂存 delta 允许为空。
3. condition phase 完成后，worker 写入本 shard 的 terminal commit slot，并向 batch coordinator 发送 `TxTerminal`。
4. batch coordinator 收到 `TxTerminal` 后，串行调用 `finish_vertex(tx_index)`：
   - 在 `GraphEffect` 删除该 vertex 出边。
   - 在 `GraphCondition` 删除该 vertex 出边。
   - 对两个 DAG 中入度变成 0 的后继发送 ready。
5. 后继 worker 从自己的 ready mailbox 被唤醒，继续对应 phase。

正确性约束：

- worker 不直接递减 DAG 入度，避免多个 worker 并发修改图。
- 一个事务必须先写入本 shard 的 commit sequence terminal slot，才能唤醒它在两张 DAG 上的后继。effect phase 产生暂存 delta 还不算该 vertex 完成；只有 worker 写入 `Delta/NoOp/Failed` 后，coordinator 才删除该 vertex 在 `GraphEffect` 和 `GraphCondition` 中的出边。
- commit slot 是本 shard 上的本地状态贡献，不是全局事务决策。read/write projection 都为空的 tx 在本 shard 上直接提交为 `NoOp`；只要 read/write projection 至少一个非空，就必须走本地读、远程读交换、执行、条件检查和本地写入流程。
- ready mailbox 只表示 “该 DAG 上所有前驱 vertex 已 terminal”，不表示 commit sequence prefix 已经全部可读。
- materialized read 仍必须根据 `max_pred_index` 扫描 commit sequence prefix；prefix 中每个 slot 都要等到 terminal 并检查 `Failed`，但只有前序 tx 的本地 write set 与当前 local read keys 相交时，才按 index 顺序应用对应 delta。

### 6.3 Effect phase

每个 worker：

1. 等待自己的 `effect_ready` mailbox。
2. 调用 `materialized_local_read(tx, GraphEffect)`。
3. 将本地读通过 `LocalReadResult(phase = SCC_EFFECT)` 发给所有 participant shards；如果本 shard 的 local read keys 为空，也发送空 read result。
4. participant worker 收齐 effect phase 的 full reads。
5. participant worker 调用 `execute_deterministic(tx, effect_full_reads)`，生成 effect 阶段暂存 output，并根据本 shard local write keys 生成 local staged delta；local staged delta 允许为空。
6. 根据执行结果计算 `actual_path`。
7. 若 `actual_path != predicted_path`，worker 写 `CommitSlotState::Failed`，向 coordinator 发送 `TxTerminal`，不进入 condition phase。
8. 若 `actual_path == predicted_path`，worker 保存本 shard local staged delta，但暂不安装到真实 store，也不唤醒后继。

没有 local write 的 participant 也必须执行 deterministic logic。若该 shard 没有 local delta，它最终写 `NoOp`，但不能只转发 local reads 后提前结束。

### 6.4 Condition phase

本地 effect phase 成功的 participant worker 进入 condition phase：

1. 等待自己的 `condition_ready` mailbox。
2. 如果 `effect_max_pred_index` 覆盖 `condition_max_pred_index`，跳过 condition materialized read、`SCC_CONDITION` read exchange 和 `check_success_path_condition`，直接根据 effect phase 暂存 delta 写 terminal slot。
3. 如果覆盖不成立，调用 `materialized_local_read(tx, GraphCondition)`。
4. 将本地读通过 `LocalReadResult(phase = SCC_CONDITION)` 发给所有 participant shards；如果本 shard 的 local read keys 为空，也发送空 read result。
5. participant worker 收齐 condition phase 的 full reads。
6. participant worker 在本地调用 `check_success_path_condition(tx, predicted_path, condition_full_reads)`。
7. 若 condition path condition 不成立，写 `CommitSlotState::Failed`。
8. 若 condition 成立且本 shard 有 local staged delta，写 `CommitSlotState::Delta(delta)`。
9. 若 condition 成立且本 shard 没有 local staged delta，写 `CommitSlotState::NoOp`。
10. worker 向 coordinator 发送 `TxTerminal`，coordinator 再从 `GraphEffect` 和 `GraphCondition` 中 finish vertex，唤醒后继。

覆盖成立时仍然等待 `condition_ready` mailbox。这不是为了重做条件检查，而是为了维护 `DagRuntime` 的入度和后继唤醒顺序：worker 只能在对应 condition vertex 已 ready 后写 terminal slot 并让 coordinator finish 该 vertex。

`check_success_path_condition` 是 worker 内部执行的手写路径条件函数，不是 RPC，也不由 coordinator 执行。它不重新执行事务，也不重新生成 delta，而是按 `predicted_path` 检查成功路径条件：

- `CreateSuccess` / `MkdirSuccess`：parent 存在且是目录，target 不存在。
- `UnlinkSuccess`：parent 存在且是目录，target 存在且是文件，target 不是 root。
- `RmdirSuccess`：parent 存在且是目录，target 存在且是空目录，target 不是 root。
- `RenameSuccess`：src parent 和 dst parent 都存在且是目录；src 存在且是文件或空目录；dst 不存在；src/dst 不是 root；src != dst；不存在 ancestor/self rename。v0 不支持非空目录 rename；非空目录 rename 返回失败，并进入 fallback 后按同一 executor 语义失败。
- `StatSuccess`：target 存在。

条件不成立时，本 shard 写 `Failed`；条件成立时，本 shard 根据是否有 local delta 写 `Delta` 或 `NoOp`。成功路径条件函数是 SCC semantic layer 的一部分，不能通过再次调用 `execute_deterministic` 替代。

### 6.5 Commit sequence slot 状态

participant：

- 成功且有 local delta：写 `CommitSlotState::Delta(delta)`。
- 成功但本 shard 无 local write：写 `CommitSlotState::NoOp`。
- 推测失败：写 `CommitSlotState::Failed`。

non-participant：

- 如果该 tx 在本 shard 上既没有 local read key，也没有 local write key，不 spawn worker。
- coordinator 直接把该 tx 的本地 commit slot 置为 terminal `NoOp`，并完成两个 DAG 中对应 vertex。这样 materialization 能读取该 slot，DAG 也不会等待一个不存在的 worker。

### 6.6 End-to-end execution order

SCC batch 在每个 shard 上按以下固定顺序推进：

1. coordinator 构造 `SccBatchPlan`、`CommitSequence`、两个 `DagRuntime`、read-result mailboxes 和 result registry entries。
2. coordinator 为本 shard 相关事务 spawn worker；无关事务由 coordinator 直接写 `NoOp` terminal slot，并完成对应 DAG vertex。
3. worker 等待自己的 `effect_ready`。
4. worker 做 effect materialized local read，将本地读结果发送给所有 participant shards，并收齐 `SCC_EFFECT` full reads。
5. worker 执行 `execute_deterministic` 得到 effect 阶段暂存 output。
6. 若 effect actual path 与 predicted path 不一致，worker 写 `Failed` terminal slot 并报告 `TxTerminal`，不进入 condition phase。
7. 若 effect 成功，worker 把 output 转成本 shard local staged delta；如果本 shard 没有 local write，staged delta 为空。staged delta 不安装到真实 store，也不唤醒后继。
8. worker 等待自己的 `condition_ready`。
9. 若 effect prefix 覆盖 condition prefix，worker 跳过 condition read exchange 和条件函数；否则做 condition materialized local read，将本地读结果发送给所有 participant shards，并收齐 `SCC_CONDITION` full reads，然后在本地执行 `check_success_path_condition`。
10. worker 写自己的本地 terminal slot：本地成功且有 local delta 写 `Delta`，本地成功但无 local delta 写 `NoOp`，本地 speculative failure 写 `Failed`。
11. worker 向 coordinator 报告 `TxTerminal`。
12. coordinator 收到 `TxTerminal` 后，串行 finish 该 tx 在 `GraphEffect` 和 `GraphCondition` 中的 vertex，并通知 newly-ready 后继。
13. 如果该 tx 在本 shard 上成功 terminal，且本 shard 是该 tx 的 `result_shard`，coordinator 立即发布 speculative `TxResult::Ok`。
14. speculative phase 结束后，coordinator 直接从本 shard commit sequence 计算 `failed_indices`。
15. coordinator 记录 shard-local reorder summary：`fallback_indices = failed_indices`，`speculative_success_indices = all_indices - failed_indices`。
16. coordinator 按原 batch index 顺序安装所有不在 `failed_indices` 中的本 shard local delta。
17. coordinator 按本地 `failed_indices` 的升序构造 fallback 重执行序列，并对这些事务执行 Calvin deterministic-locking fallback。

关键不变量：

- DAG 后继只能在前驱写入 commit sequence terminal slot 后被唤醒。
- commit sequence slot 只表示本 shard 的本地状态贡献；non-participant 直接写 `NoOp`。participant 必须完成 effect read exchange 和本地执行；condition read exchange 在 effect prefix 覆盖 condition prefix 时确定性跳过，否则必须完成。
- materialized read 只从 batch base read cache 和 commit sequence terminal slot 构造读视图，不读取当前 store 的最新状态。
- 真实 store 只由 coordinator 安装 delta，worker 不直接修改 store。
- speculative 成功事务的 client result 在 terminal slot 写入后立即发布；failed 事务的 result 只由 fallback 执行发布。
- deterministic reorder fallback 的串行化顺序是：所有 speculative 成功事务按原 batch index 顺序安装，然后所有 failed 事务按原 batch index 顺序重执行。
- 对任意 tx，所有 participating shards 必须基于相同 full read set 和相同条件检查确定性地得到相同 success/fail 判定；non-participant shard 不需要知道该 tx 是否 fallback，本地保持 `NoOp` 即可。
- Sequencer batch barrier 不变：即使某个 client 提前通过 `GetTxResult` 看到 speculative `Ok`，Sequencer 也必须等当前 batch 的所有 shard `ExecuteBatchResponse` 返回后，才能 dispatch 下一个 batch。因此下一批事务不会读取尚未安装到 store 的 speculative delta。
- 如果 `T_i` 在任一 DAG 中可达 `T_j`，且 `T_i` 在某 shard 上 terminal 为 `Failed`，则 `T_j` 不能在该 shard 上提交为 `Delta/NoOp`。由于 DAG 边只从小 index 指向大 index，`T_i` 的 index 必然不大于 `T_j` 在该 DAG 上的 `max_pred_index`；物化读扫描 `0..=max_pred_index` 并检查任意 `Failed` slot，因此该失败一定会被观察到。

## 7. Materialized Local Read

materialized local read 不直接读取当前 store 的最新状态，而是基于 batch 起始 read cache 和预计算 prefix bound 构造一个确定性本地版本：

```text
base_read_cache_on_this_shard + commit_seq[0..=max_pred_index] 中本 shard 的 delta
```

其中 `max_pred_index` 不在 materialized read 内动态计算，而是在 `SccBatchPlan` 构造时预先写入 `SccTxPlan`：

- effect phase 使用 `tx_plan.effect_max_pred_index`。
- condition phase 使用 `tx_plan.condition_max_pred_index`。

`local_write_keys_by_tx` 也在 `SccBatchPlan` 构造时预先生成：对 batch 中每个 tx，取 `OrderedTx.write_set` 中 owner 为本 shard 的 key。worker 启动后只读这份数组，不在物化读过程中重新计算 write set。

固定接口：

```rust
pub async fn materialized_local_read(
    tx_plan: &SccTxPlan,
    phase: SccPhase,
    local_read_keys: &BTreeSet<Key>,
    local_write_keys_by_tx: &[BTreeSet<Key>],
    base_read_cache: &BTreeMap<Key, ReadValue>,
    commit_seq: &CommitSequence,
) -> Result<BTreeMap<Key, ReadValue>>;
```

流程：

1. 在 batch start 预先计算 `local_base_read_keys = union(local_read_keys(tx))`，并调用一次 `store.read_many(&local_base_read_keys)` 得到 `base_read_cache`。
2. 从 `base_read_cache` 投影出本事务 `local_read_keys` 的起始值。
3. 根据 `phase` 选择预计算好的 `max_pred_index`。
4. 如果 `max_pred_index = None`，直接返回 base projection。
5. 否则按 batch index 从小到大遍历 commit sequence 前缀 `0..=max_pred_index`。
6. 对每个 slot：
   - 如果 slot 仍是 `Pending`，await 该 slot 完成。按 DAG ready 语义，真正的 DAG 前驱此时应已 terminal；这里等待是为了保持 prefix materialization 语义确定。
   - `Failed`：返回 speculation error。失败检查不受 write set 是否相交影响。
   - `NoOp`：跳过。
   - `Delta`：只有当 `local_write_keys_by_tx[index]` 与当前 `local_read_keys` 有交集时，才把 delta 投影到本事务 local read keys 上并应用；没有交集则跳过 delta 应用。
7. 返回 `BTreeMap<Key, ReadValue>`。

v0 不缓存中间 materialized version。

实现要点：

- 每个 commit slot 必须能被多个后继事务 await，固定使用 `watch::Sender<CommitSlotState>`。
- slot 更新只能发生一次，从 `Pending` 到 terminal state。
- materialization 必须按 batch index 顺序应用 delta，不能按 worker 完成顺序。
- materialization 只投影并应用影响 `local_read_keys` 的 delta，避免为每个读请求复制全量 shard state。
- materialization 的失败检查和 delta 应用是两层逻辑：`0..=max_pred_index` 前缀中的任意 `Failed` 都必须被观察；delta 应用只对 write set 与 `local_read_keys` 相交的前序 slot 生效。
- DAG ready mailbox 和 materialized read 等待不是同一件事：ready mailbox 表示该 DAG 上的直接/间接语义前驱已经 terminal；materialized read 仍必须按 `max_pred_index` 扫描 commit sequence prefix，以构造确定性读版本。
- 如果 materialized read 看到 `Failed`，必须通过 read-result exchange 把 speculation failure 传播给该 tx 的其他 participant shards，避免远端 worker 永久等待。

### 7.1 Commit sequence concurrency

提交序列的并发性来自 slot 独立性，而不是全局队列锁：

- `CommitSequence` 创建后长度固定，不在执行过程中 push/pop。
- 每个 worker 只写自己 tx_index 对应的 slot。
- 每个 slot 独立持有一个 `watch::Sender<CommitSlotState>`。
- materialized read 扫描 `0..=max_pred_index` 前缀，并等待每个 prefix slot 进入 terminal 状态。
- prefix 中任意 `Failed` 都会让当前事务推测失败。
- prefix 中的 `Delta` 只有在对应 write set 与当前 local read keys 相交时才会被投影并应用。
- slot terminal 后，该 slot 的所有等待者被一次广播唤醒。
- `Delta` 存为 `Arc<TxDelta>`，多个 materialized read 共享同一份 delta。
- 没有保护整个 commit sequence 的 async mutex；只允许按 slot 访问。

因此，多个并发 materialized local read 不会排队竞争一个全局 commit-sequence mutex。它们的成本是：

- 对 `max_pred_index = None` 的 tx：不访问 commit sequence，直接返回 base projection。
- 对有依赖的 tx：按 batch index 扫描并等待 `0..=max_pred_index` 中的 slot terminal。
- 对 prefix 中的 `Failed`：本事务推测失败。
- 对 prefix 中的 `Delta`：只有 write set 与当前 local read keys 相交时，才按 delta ops 投影到 local read keys 后应用。

v0 不实现 prefix snapshot cache 或 per-key delta log。当前 batch size 上限为 512，线性 prefix scan 是确定性成本；扩展 prefix cache 或 per-key delta log 必须由 profiling 证明该 scan 已成为瓶颈。

## 8. 推测失败与 Deterministic Reorder Fallback

因为 v0 有 condition graph，就必须定义推测失败的确定性处理。本文档采用博士论文中的确定性重排序 fallback：已经产生的 `Delta/NoOp` 不被丢弃；推测失败的事务在 batch 末尾按确定性顺序重执行。

失败来源：

- effect phase actual path 与 predicted path 不一致。
- condition phase path condition 不成立。
- materialized read 观察到前序 `Failed`。
- remote read exchange 收到 `LOCAL_READ_STATUS_SPECULATION_FAILED`。

处理规则：

1. worker 观察到任一失败来源后，将自己的 commit slot 标记为 `Failed`。
2. worker 向 batch coordinator 发送 `TxTerminal`。
3. coordinator 串行 finish 该 tx 在两个 DAG 中的 vertex，继续唤醒 DAG 后继，避免后继永久阻塞。
4. DAG 后继事务在 materialized local read 的 prefix failure check 中看到该 `Failed` 后，也将自身标记为 `Failed`。
5. 如果某个 participant 在 read exchange 中收到远端 `SPECULATION_FAILED`，它同样把当前事务标记为 `Failed`，发送 `TxTerminal`，并由 coordinator 继续唤醒后继。

失败传播不要求一次性把所有传递后继直接标成 `Failed`。后继通过自己的 prefix failure check 或远程读交换确定性地观察失败，并按相同规则退出。

### 8.1 Speculative success publication

一个事务的 speculative result 只有在 condition phase 通过、并且本 shard 写入 terminal `Delta` 或 `NoOp` 后才算产生。effect phase 的暂存 output 不是可发布结果。

v0 预测路径全是成功路径，所以 speculative success 的 client-facing result 固定为 `TxResult::Ok`：

- 如果本 shard 是该 tx 的 `result_shard`，coordinator 在 terminal `Delta/NoOp` 写入后立即 `mark_ready(tx_id, TxResult::Ok)`。
- 如果 tx 写入 `Failed`，不发布 client result；该 tx 的 result 由 deterministic fallback 执行后发布。
- speculative success 的 delta 暂存在 commit sequence 中，不由 worker 直接安装到真实 store。

这个规则成立的原因是 deterministic reorder fallback 不丢弃已经产生的 delta。已经发布 `Ok` 的事务会进入最终串行化顺序，只是该顺序允许不同于 Sequencer 的原 batch 顺序。

### 8.2 Shard-local failed set

SCC 不再在 speculative phase 末尾交换 failed set。每个 shard 只维护：

- `failed_indices`：本 shard commit sequence 中状态为 `Failed` 的 tx index，按 batch index 升序排列。

新不变量是：对任意 tx，所有 participating shards 会确定性地得到相同 success/fail 判定。原因是 participating shards 会交换同一 full read set，并执行同一 deterministic executor、path classification 和 condition check。non-participant shard 没有参与该 tx 的计算，本地 slot 是 `NoOp`；它不需要知道该 tx 是否在其它 participating shards fallback。

因此，fallback 顺序定义为每个 shard 按本地 `failed_indices` 的原 batch index 升序执行。这个规则避免了一轮全 shard completion report，不改变 Sequencer 的 batch dispatch 模型。Sequencer 仍然只等待每个 shard 的 `ExecuteBatchResponse`。

### 8.3 Deterministic reorder fallback

每个 shard 用本地 `failed_indices` 按同一确定性规则完成 batch：

1. 按原 batch index 升序扫描 commit sequence。
2. 对不在 `failed_indices` 中的 slot：
   - `Delta(delta)`：安装到本 shard store。
   - `NoOp`：跳过。
   - `Failed`：如果该 index 不在 `failed_indices` 中，说明本地 failed set 推导出错，是 internal error。
3. 对在 `failed_indices` 中的 slot，不安装 speculative delta。
4. 用 `failed_indices` 按升序构造 fallback 重执行序列。
5. 对 fallback 重执行序列使用 Calvin deterministic-locking fallback 执行。

最终串行化顺序固定为：

```text
speculative_successes_in_original_batch_order
then failed_transactions_in_original_batch_order
```

因此，SCC 保证可串行化，而不是保证等价于 Sequencer 原始 batch 顺序。checker 为 SCC 模式执行 reorder-aware reference execution：先应用 speculative success 集合，再按 fallback order 执行 failed 集合。

fallback 直接调用 `execute_calvin_batch`，复用 `ReadPhase::Calvin` 的 read-result mailbox。SCC speculative 阶段只使用 `ReadPhase::SccEffect` 和 `ReadPhase::SccCondition`，因此 fallback 不会与 speculative read-result mailbox 混淆。

## 9. Client Result 与 TxResultRecord

SCC 对外结果必须与 Calvin 模式使用同一套 RPC，但发布时机不同：

- `result_shard` 仍由 `ShardLayout` 根据 read/write set 计算。
- `GetTxResult` 仍只在 result shard 返回 `READY(result)`。
- participant shards 仍返回 `TxResultRecord`，checker 仍检查 participant result 一致。
- speculative success 的 result 是 `TxResult::Ok`，在 terminal `Delta/NoOp` 产生后可立即发布。
- failed tx 的 result 由 fallback 执行发布。

`ExecuteBatchResponse.tx_results` 包含最终结果：

- speculative success tx 的 `Ok` records。
- fallback 重执行 tx 的 records。

不存在 “被 fallback 丢弃的 speculative records”。如果某个 tx 已经产生 `Delta/NoOp` 并发布 `Ok`，它必须进入最终串行化顺序；如果某个 tx 写入 `Failed`，它不能在 fallback 前发布 client result。

checker 不重新实现 SCC 调度来猜测最终串行化顺序。SCC shard 必须通过 `DumpState.scc_reorders` 暴露每个 batch 的 shard-local reorder：

- `speculative_success_indices`
- `fallback_indices`

checker 先检查每个 shard-local record 自洽。然后对每个 batch 从所有 shard-local `fallback_indices` 取 union，得到 reference fallback order：不在 union fallback 中的 tx 按原 batch index 执行，然后 union fallback tx 按原 batch index 执行。这个 union 只用于测试 reference execution，不是 SCC 运行时协议。

## 10. 与现有实现的集成点

### 10.1 ShardRuntime

`ShardCore` 增加 scheduler kind：

```rust
struct ShardCore {
    scheduler: SchedulerKind,
    // existing fields...
}
```

batch executor：

```rust
match core.scheduler {
    SchedulerKind::CalvinLocking => execute_calvin_batch(core, batch).await,
    SchedulerKind::SccOnline => execute_scc_batch(core, batch).await,
}
```

现有 `execute_batch_on_shard_inner` 可重命名为 `execute_calvin_batch`。

### 10.2 Mailbox registry

read-result mailbox key 增加 phase：

```rust
type MailboxKey = (BatchId, TxId, ReadPhase);
```

Calvin 模式使用 `ReadPhase::Calvin`。SCC speculative 模式使用 `SccEffect` 和 `SccCondition`；SCC fallback 复用 Calvin deterministic-locking path，因此使用 `ReadPhase::Calvin`。

### 10.3 Store

`RedbInMemoryInodeStore` 新增两个能力：

- batch start 时读取本 shard 需要的 base read cache。
- atomically apply delta。

v0 不使用 `store.dump()` 作为物化读快照。`execute_scc_batch` 开始时计算本 shard 在该 batch 中会读取的 key 集合，并调用一次 `store.read_many()` 得到 `base_read_cache`。由于 Sequencer 严格一次只发送一个 batch，batch 内没有其它 writer，这个 base read cache 是确定的。

### 10.4 Executor

现有 `execute_deterministic` 保留。

新增：

```rust
pub fn classify_actual_path(
    tx: &OrderedTx,
    reads: &BTreeMap<Key, ReadValue>,
    output: &TxExecutionOutput,
) -> PathId;

pub fn check_success_path_condition(
    tx: &OrderedTx,
    predicted: PathId,
    reads: &BTreeMap<Key, ReadValue>,
) -> Result<bool>;
```

v0 actual path 固定按 `(FsOp kind, TxResult)` 分类。只有 `TxResult::Ok` 对应成功路径；所有非 `Ok` 结果都视为 “不是预测路径”，用于触发 fallback。

`check_success_path_condition` 是手写路径条件表，不调用 `execute_deterministic`。delta 由 `output_to_delta` 根据 op 成功路径和 `OrderedTx.write_set` 构造，`write_set` 是 delta key 的唯一来源。

## 11. 测试计划

### 11.1 Unit tests

- `conflict_mask_same_parent_different_target`
- `conflict_mask_same_target`
- `conflict_mask_parent_created_before_child_create`
- `effect_and_condition_tables_are_independent`
- `same_parent_different_name_create_has_no_effect_or_condition_edge`
- `same_parent_different_name_delete_has_no_effect_or_condition_edge`
- `same_parent_different_name_create_delete_has_no_effect_or_condition_edge`
- `parent_create_before_child_create_has_effect_and_condition_edges`
- `delta_create_same_parent_merges_child_count`
- `delta_unlink_same_parent_merges_child_count`
- `delta_rmdir_same_parent_merges_child_count`
- `condition_check_detects_prediction_failure`
- `materialized_read_waits_prefix_but_applies_only_intersecting_deltas`
- `materialized_read_prefix_failed_slot_forces_failure_without_write_intersection`
- `shard_local_failed_indices_drive_reorder_fallback`

### 11.2 SCC madsim correctness tests

新增 fresh cluster，所有 shards 使用 `SchedulerKind::SccOnline`。

测试 1：同父目录并发 create

- setup: `mkdir /`, `mkdir /public`
- batch: 创建 `/public/file_0..file_511`
- 期望：全部 `Ok`
- checker：最终 `/public.child_count == 512`，所有文件存在，状态等于 reorder-aware reference。该场景无失败，reference 顺序等价于原 batch 顺序。

测试 2：父目录创建与子文件创建

- setup: `mkdir /`
- batch:
  - `mkdir /public`
  - `create /public/file_0`
- 期望：全部 `Ok`
- checker：`create /public/file_0` 必须通过 `GraphEffect` 和 `GraphCondition` 等待 `mkdir /public`，最终状态等于 reorder-aware reference。该场景无失败，reference 顺序等价于原 batch 顺序。

测试 3：预测失败触发完整 fallback

- setup: `mkdir /`, `mkdir /d`
- batch:
  - `create /d/x`
  - `create /d/x`
- 预测都为 `CreateSuccess`。
- 第二个事务实际结果不是成功路径，触发全 shard failed set exchange 和 deterministic reorder fallback。
- 最终结果必须等价于 reorder-aware reference：第一个 `Ok`，第二个由 fallback 产生 `AlreadyExists`。
- checker 通过 `DumpState.scc_reorders` 验证所有 shard 报告相同的 `speculative_success_indices` 和 `fallback_indices`。

测试 4：Rename 保守串行

- 包含 rename 与 create/stat/unlink 混合 batch。
- 验证 SCC 状态仍等于 reorder-aware reference。
- 不要求比 Calvin 更高并发。

测试 5：同父目录并发 unlink/rmdir

- setup: `mkdir /`, `mkdir /public`，创建 `/public/file_0..file_255` 和空目录 `/public/dir_0..dir_255`。
- batch: `unlink /public/file_0..file_255` 与 `rmdir /public/dir_0..dir_255`。
- 期望：全部 `Ok`。
- checker：最终 `/public.child_count == 0`，所有 target 被删除，状态等于 reorder-aware reference。该场景无失败，reference 顺序等价于原 batch 顺序。

测试 6：同父目录 create/delete 混合并发

- setup: `mkdir /`, `mkdir /public`，创建一组待删除文件或空目录。
- batch: 对不同 target 混合执行 `create`、`mkdir`、`unlink`、`rmdir`。
- 期望：全部 `Ok`。
- checker：parent `child_count` 等于 reorder-aware reference，状态等于 reorder-aware reference。

### 11.3 Performance comparison

保留现有 mdtest-like workload，并增加 scheduler 维度。SCC v0 覆盖 mdtest 的 directory/file creation 和 removal phase；stat phase 因当前 `Stat` 只返回 `TxResult`，按语义表获得有限并发，但不是主要优化目标。

- Calvin private/public。
- SCC private/public。
- 输出每个 phase 的 ops/sec。
- 输出 SCC/Calvin ratio。

每次比较必须使用 fresh cluster，避免 result registry、store 或 batch log 历史状态影响结果。

## 12. Roadmap

### Step 1：重构出 scheduler 分支

- 添加 `SchedulerKind`。
- 保持默认 Calvin。
- 重命名现有 Calvin batch executor。
- 确认所有现有测试不变通过。

### Step 2：支持内部 RPC 和 phase mailbox

- proto 增加 `ReadPhase`。
- proto 增加 `LocalReadStatus`。
- mailbox key 增加 phase。
- Calvin worker 使用 `ReadPhase::Calvin`。
- `DumpState` 增加 SCC reorder summary debug 字段。
- 确认现有测试不变通过。

### Step 3：实现语义表和双 DAG

- 手写 path classification。
- 手写 conflict mask。
- 手写 effect/condition conflict table。
- 构造 `SccBatchPlan`。
- 添加 DAG 单元测试。

### Step 4：实现 commit sequence 和 delta

- 定义 `DeltaOp`、`TxDelta`、`CommitSequence`、`CommitSlotState`。
- 实现 output-to-delta。
- 实现 materialized read。
- 实现 delta apply。

### Step 5：实现 SCC worker 和 fallback

- 实现两个 DAG ready mailbox。
- effect phase read/exchange/execute/delta。
- condition phase read/exchange/check。
- coordinator 串行维护两个 DAG 的 vertex finish 和 ready 通知。
- failed slot 传播。
- shard-local failed set 和 deterministic fallback。
- 按 deterministic reorder 顺序安装 speculative success delta。
- 对 failed tx index 集合执行 Calvin fallback。
- speculative success 立即发布 `TxResult::Ok`，failed tx 由 fallback 发布 result。

### Step 6：正确性和性能测试

- SCC correctness tests。
- condition failure fallback test。
- mdtest-like Calvin/SCC comparison。
- 更新 `DESIGN.md` 或在其中链接 `SCC_DESIGN.md`。

## 13. 后续扩展

1. `RenameSuccess` 在 v0 中保守串行。扩展同父目录或跨父目录 rename 的并发规则时，必须新增精确 `ConflictMask` bit 和对应 effect/condition 表项。
2. `Stat` 在 v0 中固定只返回 `TxResult`，不返回 inode 内容。扩展 `Stat` 使其返回 inode metadata 时，必须重新定义 stat 与 parent `child_count`、target inode 字段变化之间的语义冲突表。

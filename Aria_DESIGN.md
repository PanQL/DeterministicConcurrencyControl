# Aria Scheduler Design

状态：v0 设计与当前实现保持一致，用于审阅在 CalvinFS demo 中新增的第三个调度器。

本文档描述第三个 shard scheduler：`Aria`。它基于论文 *Aria: A Fast and Practical Deterministic OLTP Database* 的两阶段思想：先在 batch snapshot 上 optimistic execution，由 read owner 记录实际服务过的 reads，并根据 executor output 记录 actual writes/write reservation；再在 commit phase 中判断 WAW/RAW conflict，成功事务安装 staged writes，失败事务在同一个 batch 末尾走 deterministic locking fallback。

当前版本选择让 Aria 不信任 Sequencer 提供的 `read_set/write_set`：Sequencer 仍然可以在 batch 里保留这些字段以兼容 Calvin/SCC 和现有工具；Aria 在 shard 侧根据 `FsOp` 类型重新推导 read keys，动态路由 snapshot read；write reservation 使用 executor output 中的 actual writes。Aria 的 result shard / execution coordinator 由 `tx_id` 决定。

## 1. 目标和非目标

### 1.1 目标

- 新增第三个 shard scheduler：`Aria`。
- 默认调度器仍为 `CalvinLocking`，现有 Calvin/SCC 功能不因 Aria 改变。
- Sequencer 继续保留 batch 中的 `OrderedTx.read_set/write_set`，但 Aria 忽略它们。
- Aria 使用 `tx_id % shard_count` 作为 deterministic `result_shard` 和 execution coordinator。
- 每个事务只由一个 execution coordinator 执行事务逻辑。
- Execution phase 不加锁，所有读都来自 batch start snapshot。
- Aria shard 侧根据 `FsOp` 类型推导 read keys，再动态确定对应 key owner 获取 full read set。
- 写 key 来自 executor output；owner 动态接收 staged writes，并记录 actual writers/write reservation。
- Commit phase 使用基础版 Aria Rule 1：
  - 如果事务有 WAW conflict，失败。
  - 如果事务有 RAW conflict，失败。
  - 否则提交。
- 失败事务在同一个 batch 内走 Aria 专用 deterministic locking fallback。该 fallback fork 自 Calvin locking 思路，但定制 active set，把 `tx_id % shard_count` 强制纳入 active executors。
- 性能测试可以比较 Calvin、SCC、Aria。

### 1.2 非目标

- v0 不实现 deterministic reordering，因此不做 read reservation，也不检测 WAR。
- v0 不实现跨 batch retry。
- v0 不引入通用 async transaction context / DSL，不做 context-driven `ctx.read()/ctx.write()` executor。
- v0 的 read keys 由 Aria shard 侧根据 `FsOp` 类型推导；actual write set 来自 executor output。
- v0 不处理 range query / phantom。当前文件系统元数据事务只读写具体 path key。
- v0 不让 Aria 依赖 Sequencer 填入的 `read_set/write_set`。

## 2. 第一性原理约束

Aria v0 需要满足五个最小条件：

1. 每个 tx 有唯一 deterministic executor/result owner，否则客户端不知道从哪里取结果。
2. Execution phase 中所有读必须来自同一个 batch snapshot，否则 phase 语义不成立。
3. Write reservation 必须基于 actual writes，而不是 arrival order。
4. Commit decision 必须对所有实际读写过的 key 生效。
5. 所有 shard 对同一个 tx 必须得到相同 commit/abort 结论，否则会 partial commit。

因为 v0 的 read set 由 Aria shard 侧重新推导并通过 owner-read RPC 记录，write set 来自 actual executor output，failed set 不再是 Sequencer batch 内容的纯函数。一个 shard 只知道自己拥有的 keys 上发生了哪些 readers/writers，所以需要在 execution barrier 后做一次 local failed-set exchange，取 union 后再 install/fallback。这个 exchange 在动态版本中不是冗余。

## 3. Sequencer 和 Result Shard

### 3.1 Batch 字段

Sequencer 继续生成 `OrderedTx.read_set/write_set`：

- Calvin 和 SCC 继续使用这些字段。
- 现有 checker、benchmark、dump 工具可以继续读取它们。
- Aria 不使用这些字段决定 coordinator、read participants、write participants、reservation 或 failed set。

如果后续要完全移除静态集合，需要同步改 Calvin/SCC 和测试；这不是 Aria v0 的目标。

### 3.2 Aria result shard

Aria 使用：

```text
result_shard(tx) = tx.tx_id % shard_count
execution_coordinator(tx) = result_shard(tx)
```

这个规则只依赖 `tx_id` 和 cluster layout，所有 shard 都能独立重算。Sequencer 在 Aria 模式下返回这个 `result_shard` 给 client。

为了不影响 Calvin/SCC，实现上采用一个小的 Sequencer result policy 配置：

```rust
enum SequencerResultPolicy {
    StaticReadWriteSet,
    TxIdModulo,
}
```

Calvin/SCC 保持 `StaticReadWriteSet`；Aria benchmark/cluster 使用 `TxIdModulo`。这不是 batch shape 的大改，只是 result routing policy 必须和 shard scheduler 一致。

### 3.3 Aria fallback coordinator

Aria fallback 也必须使用 `tx_id % shard_count` 发布 client-visible result。否则 optimistic success 和 fallback success 可能发布到不同 shard，client 会查不到失败后重跑的结果。

不能只给现有 `execute_calvin_batch` 增加 result-shard override。`tx_id % shard_count` 不一定拥有 fallback 阶段的任何 read/write key；在现有 Calvin worker 模型里，它可能不会 spawn worker，也就无法发布结果。

Aria 需要 fork 一套 fallback implementation：

- fallback conservative `read_set/write_set` 仍由 shard 侧从 `FsOp` 推导；
- fallback key owners 仍按 deterministic locking 顺序提供 reads / 安装 writes；
- `aria_result_shard = tx_id % shard_count` 强制加入 fallback active set；
- 如果 `aria_result_shard` 没有本地 lock key，它仍然 spawn active worker、创建 read mailbox、收齐 full read set、执行事务并发布 client result；
- 现有 Calvin/SCC fallback 逻辑不改，避免影响已有调度器。

## 4. 新增公共接口

### 4.1 Scheduler kind

新增：

```rust
pub enum SchedulerKind {
    CalvinLocking,
    SccOnline,
    Aria,
}
```

### 4.2 Profile scheduler

新增：

```rust
pub enum SchedulerProfileScheduler {
    CalvinLocking,
    SccOnline,
    Aria,
}
```

`proto/calvinfs.proto` 中对应增加：

```proto
SCHEDULER_PROFILE_SCHEDULER_ARIA = 3;
```

现有 profile counters/timings 先复用以下字段：

- `completion_publish_ns` / `completion_collect_ns`：execution barrier 和 failed-set exchange。
- `install_successes_ns`：optimistic success install。
- `fallback_ns` / `fallback_tx_count`：Aria deterministic locking fallback。
- `local_failed_count` / `global_failed_count`：local conflict failures 和 union 后 failures。
- `result_records_produced` / `speculative_success_count`：result shard 输出和 optimistic success 数量。

当前 v0 没有为 optimistic dynamic read/execute 单独填充 per-worker timing；如果需要细分 Aria 的 dynamic read RPC、snapshot read 和 execution compute cost，应后续增加 Aria 专用 worker profile，而不是复用 Calvin/SCC 的语义不完全一致字段。

### 4.3 Dynamic read RPC

新增 owner-read RPC：

```proto
rpc AriaReadSnapshot(AriaReadSnapshotRequest)
    returns (AriaReadSnapshotResponse);

message AriaReadSnapshotRequest {
  uint64 batch_id = 1;
  uint32 tx_index = 2;
  uint64 tx_id = 3;
  uint64 from_shard = 4;
  string key = 5;
}

message AriaReadSnapshotResponse {
  ReadEntry read = 1;
}
```

Key owner 收到请求后：

1. 校验 `layout.shard_for_key(key) == self_shard`。
2. 从 batch snapshot 读取 key。
3. 记录 `readers_by_key[key].insert(tx_index)`。
4. 返回 read value。

Execution phase 期间不安装任何 optimistic writes。v0 明确依赖当前 engine 的 per-shard batch serial execution：同一 shard 上不会有后续 batch 的 mutation 与本 batch execution phase 交错。因此 owner 在收到 `AriaReadSnapshot` 时直接读当前 store，并把读值视为本 batch snapshot value。v0 不实现 per-batch snapshot cache；如果未来允许 batch overlap，必须先引入 snapshot cache 后才能保持 Aria phase 语义。

### 4.4 Staged outcome / write reservation RPC

Coordinator 执行结束后得到 actual writes。新增或扩展 staged outcome RPC：

```proto
rpc AriaStageOutcome(AriaStageOutcomeRequest)
    returns (AriaStageOutcomeResponse);

message AriaStageOutcomeRequest {
  uint64 batch_id = 1;
  uint32 tx_index = 2;
  uint64 tx_id = 3;
  uint64 from_shard = 4;
  TxResult result = 5;
  repeated WriteEntry writes = 6;
  bool is_result_shard = 7;
}

message AriaStageOutcomeResponse {}
```

Coordinator 发送规则：

- 对每个 actual write owner 发送该 owner 的 writes。
- 如果 `result_shard` 是 actual write owner，该 staged outcome 同时携带 tx result，并标记为 result shard outcome。
- 如果 `result_shard` 不是 actual write owner，也向 `result_shard` 发送空 writes 的 result-only staged outcome。
- 如果事务没有 actual writes，只向 `result_shard` 发送 result-only staged outcome。

Receiver 收到后：

1. 校验 `from_shard == tx_id % shard_count`。
2. 校验 `is_result_shard == (self_shard == tx_id % shard_count)`。
3. 校验每个 write key 属于本 shard。
4. 暂存本 shard writes；只有 `is_result_shard` 时才暂存 result。
5. 对每个 write key 记录 `writers_by_key[key].insert(tx_index)`；不维护独立 `write_reservation` map，reservation winner 在 commit decision 时由 `min(writers_by_key[key])` 派生。
6. ack coordinator。

### 4.5 Execution-done RPC

```proto
rpc ReportAriaExecutionDone(AriaExecutionDoneRequest)
    returns (AriaExecutionDoneResponse);

message AriaExecutionDoneRequest {
  uint64 batch_id = 1;
  uint64 from_shard = 2;
}

message AriaExecutionDoneResponse {}
```

本 shard 作为 coordinator 的所有 tx 都完成，并且所有 dynamic read/staged outcome RPC 都已收到 ack 后，才能发送 `ExecutionDone`。

### 4.6 Local failed-set RPC

基于 owner-recorded reads 和 actual writes 的版本需要 failed-set exchange：

```proto
rpc ReportAriaLocalFailures(AriaLocalFailuresRequest)
    returns (AriaLocalFailuresResponse);

message AriaLocalFailuresRequest {
  uint64 batch_id = 1;
  uint64 from_shard = 2;
  repeated uint32 failed_indices = 3;
}

message AriaLocalFailuresResponse {}
```

每个 shard 收齐所有 `ExecutionDone` 后，基于本 shard owner keys 上的 served readers 和 actual writers 计算 local failed set，然后广播。所有 shard 收齐 local failures 后取 union，得到 global failed set。

## 5. 数据结构

每个 shard 对每个 Aria batch 保存：

```rust
struct AriaBatchState {
    staged_outcomes: BTreeMap<usize, AriaStagedOutcome>,
    readers_by_key: BTreeMap<Key, BTreeSet<usize>>,
    writers_by_key: BTreeMap<Key, BTreeSet<usize>>,
    execution_done_reports: BTreeSet<ShardId>,
    failure_reports: BTreeMap<ShardId, BTreeSet<usize>>,
}

struct AriaStagedOutcome {
    tx_index: usize,
    tx_id: TxId,
    result: Option<TxResult>,
    local_writes: Vec<WriteOp>,
    is_result_shard: bool,
}
```

`staged_outcomes` 是 optimistic install 和 result publishing 所需的唯一事务 outcome 状态。只有 `is_result_shard` 的 staged outcome 保存 `result`，并负责产生 `TxResultRecord` 和 client-visible result。Write owner shard 只保存和安装 `local_writes`。Coordinator 执行结果在发送 staged outcome 并收到 ack 后丢弃。

`readers_by_key/writers_by_key` 只记录本 shard 拥有的 keys。`write_reservation[key]` 不作为核心持久状态保存；它在 local commit decision 中由 `min(writers_by_key[key])` 派生。Commit decision 通过 failed-set union 获得全局一致结果。

## 6. Protocol

### 6.1 Batch start

`execute_batch_on_shard` 按 `SchedulerKind::Aria` 分支调用 `execute_aria_batch(core, batch, profile_enabled)`。

Aria batch start：

1. `validate_batch_order(&batch)`。
2. 不调用 `validate_sets(tx)` 作为 Aria correctness 前置条件。
3. 对每个 tx 计算 `result_shard = tx_id % shard_count`。
4. result shard 上 `ensure_pending(tx_id)`，其他 shard 上 `mark_not_responsible(tx_id)`。
5. 初始化 Aria batch state、execution-done registry、failed-set registry。
6. 如果本 shard 是某 tx 的 coordinator，spawn coordinator worker。

### 6.2 Execution phase

每个 tx 只在 `tx_id % shard_count` 对应 shard 执行。

Coordinator worker：

1. 调用 Aria shard 侧 helper 根据 `FsOp` 类型推导 read keys。
   - 这个 helper 复用 `derive_read_write_set(&op)` 中的读集合逻辑，但输入只能是 `tx.op`，不能读取或校验 batch 中 Sequencer 填入的 `tx.read_set`。
   - 所有 shard 使用同一个 helper；只有 coordinator 负责据此获取 full read set 并执行事务。
2. 对每个 read key：
   - 如果 key 属于本 shard，读本地 batch snapshot，并记录 reader。
   - 否则调用 `AriaReadSnapshot`。
3. 收齐 full reads 后调用 `execute_deterministic(&tx, &full_reads)`。
4. 从 output writes 得到 actual write set。
5. 按 actual write owner 分组发送 `AriaStageOutcome`。
6. 如果需要，向 result shard 发送 result-only staged outcome。
7. 等所有 read/stage RPC ack 后，worker 完成。

Execution phase 不使用 lock table。所有 worker 读到的都是 batch start snapshot。

### 6.3 Execution barrier

本 shard 满足以下条件后广播：

```text
ReportAriaExecutionDone(batch_id, from_shard)
```

条件：

- 本 shard 作为 coordinator 的所有 worker 完成。
- 这些 worker 发出的所有 dynamic read RPC 都已返回。
- 这些 worker 发出的所有 staged outcome RPC 都已 ack。

非 coordinator shard 允许较早发送自己的 `ExecutionDone`。这是安全的：只有当所有 shards 的 `ExecutionDone` 都收齐时，才能推出所有 coordinator 的 dynamic reads/staged outcomes 都已被 key owners ack，因此不会再有新的 readers/writers 记录到达。

### 6.4 Local commit decision

每个 shard 收齐所有 `ExecutionDone` 后，基于本 shard owner keys 计算 local failed set。

对每个 key：

1. `writers = writers_by_key[key]`。
2. 如果 `writers` 非空，`winner = min(writers)`。
3. WAW：`writers - {winner}` 中所有 tx failed。
4. RAW：对每个 `reader in readers_by_key[key]`，如果 `winner < reader`，则 reader failed。

这等价于基础版 Aria Rule 1：

```text
HasConflicts(TID, served_RS union actual_WS, derived_write_reservations)
```

v0 不检测 WAR，因此不需要 read reservation。

### 6.5 Failed-set exchange

每个 shard 广播：

```text
ReportAriaLocalFailures(batch_id, from_shard, local_failed_indices)
```

所有 shard 收齐后：

```text
global_failed_indices = union(all local_failed_indices)
```

之后才允许 install optimistic successes 或启动 fallback。这个 exchange 防止 partial commit：只要某个 key owner 发现 tx failed，所有 shard 都必须跳过该 tx 的 staged writes/result。

### 6.6 Install optimistic successes

每个 shard 按 batch index 升序处理 staged outcomes：

1. 如果 `tx_index in global_failed_indices`，丢弃 staged outcome。
2. 否则安装 `local_writes`。
3. 如果 `is_result_shard`，向 batch summary 追加唯一的 `TxResultRecord`，并发布 client-visible result。

安装顺序使用原 batch order。基础版 Aria committed transactions 没有 WAW/RAW conflict，因此 committed writes 不应互相覆盖同一 key；按 batch order 安装最容易检查和调试。

### 6.7 Deterministic locking fallback

如果 `global_failed_indices` 非空：

1. 构造 fallback batch：
   - tx 来自原 batch 的 failed indices。
   - 保留原 `tx_id` 和 `op`。
   - 重新设置 fallback batch 内 `batch_index = 0..n-1`。
   - `batch_id` 保持原 batch id。
2. 对 fallback tx 在 shard 侧重新从 `op` 推导 conservative `read_set/write_set`，不要复用 Sequencer batch 字段。
3. 使用 Aria 专用 fallback participants：
   - `key_participants = owners(read_set ∪ write_set)`。
   - `read_sources = owners(read_set)`。
   - `normal_active = owners(write_set)`；如果 `write_set` 为空，则 `normal_active = {}`。
   - `aria_result_shard = tx_id % shard_count`。
   - `active = normal_active ∪ {aria_result_shard}`。
   - `all = key_participants ∪ active`。
4. `key_participants` 按 fallback batch order 获取本地 deterministic locks；没有本地 lock key、但属于 `active` 的 shard 不参与 lock table，但必须 spawn active worker。
5. 每个 `read_sources` shard 在拿到本地 locks 后读取本地 keys，并把 local reads 发送给所有 `active` shards。
6. 每个 `active` shard 收齐来自 `read_sources` 的 full reads 后调用 `execute_deterministic`。
7. 属于 `owners(write_set)` 的 active shards 安装本 shard local writes，但不产生 `TxResultRecord`，除非它同时是 `aria_result_shard`。
8. `aria_result_shard` 发布 client-visible result，并向 batch summary 追加该 tx 唯一的 `TxResultRecord`。
9. fallback 在 optimistic successes 已安装后的真实 store 上执行。
10. fallback result record 合并进 Aria batch summary。

这个 fallback 是确定性的：所有 shard 使用相同 global failed set、相同 fallback order、相同 shard-side set derivation 和相同 active-set rule。它复用 Calvin locking 的核心思想，但不直接调用现有 `execute_calvin_batch`，避免改变 Calvin/SCC 语义。

注意：`aria_result_shard` 被强制加入 active set 后，远程读 fanout 会比普通 Calvin fallback 更大。这个代价是有意接受的，因为它让 client-visible result owner 在 optimistic phase 和 fallback phase 完全一致。

Aria fallback 允许多个 active shards 冗余执行同一个 tx。生产语义只使用 `aria_result_shard` 的 result；其他 active shard 的 execution result 不产生 `TxResultRecord`，也不影响 client-visible result。v0 不增加跨 shard result-digest exchange；如果后续需要 debug 校验 active executors 是否一致，应新增诊断 RPC 或 profile/debug record，不能通过多发 `TxResultRecord` 实现。

## 7. Result Records 和 Checker

Aria 的 batch summary 不能再用静态 `layout.participants(tx).active` 校验，因为 Aria 不信任 batch 中的 static read/write set，且 actual write owners 取决于 execution output。

checker 必须按 scheduler 分两层：

1. 对 Calvin/SCC 保留现有 active participant equality 检查。
2. 对 Aria：
   - 每个 tx 恰好有一个 result record。
   - 该 result record 必须来自 `tx_id % shard_count`。
   - final state 使用 Aria reorder record 回放校验。

Aria optimistic success 的 result record 只写在 result shard。Actual write owners 只负责安装本 shard writes，不产生 `TxResultRecord`。

失败事务的最终 result 由 fallback path 发布，同样必须包含 `tx_id % shard_count` result shard。

Aria fallback 的 active set 可能包含多个 executors，但 result record 仍只来自 `tx_id % shard_count`。因此 checker 对 Aria 不能要求 result shard set 等于静态 participants；只要求唯一 result record 来自 result shard，并且 final state/reorder 正确。

## 8. 正确性约束

### 8.1 已确定工程约束

Aria v0 固定采用以下工程约束：

1. Read set 是 shard-side conservative served read set。Aria 根据 `FsOp` 类型重新推导 read keys 并请求 key owner；凡 owner 实际服务过的 read 都记录为 reader。它不是 context-driven executor 内部的分支实际 read set，因此可能比理论 actual read set 更保守，并可能带来更多 fallback。
2. Snapshot 依赖 per-shard batch serial execution。v0 不实现 per-batch snapshot cache；同一 shard 不允许后续 batch mutation 与当前 Aria execution phase 交错。如果未来支持 batch overlap，必须先引入 snapshot cache。
3. Aria fallback 使用 expanded active set 冗余执行。`tx_id % shard_count` 必须作为 active executor 发布唯一 result record；write owners 负责安装 local writes。v0 不做跨 shard active result-digest exchange，result shard 的 result 是唯一对外语义。
4. Write reservation 是派生值。核心状态只保存 `writers_by_key`；`winner = min(writers_by_key[key])` 在 local commit decision 时计算。不得维护会影响 correctness 的第二份 reservation 状态。

### 8.2 Snapshot read

Aria execution phase 不能读到同 batch optimistic writes。所有 install 必须发生在 execution barrier 和 failed-set exchange 之后。

### 8.3 Reservation determinism

Write reservation 使用 `min(tx_index)`，不能依赖 RPC arrival order。实现不保存独立的 core `write_reservations` map；local commit decision 直接从 `writers_by_key[key]` 计算 `winner = min(writers)`。如果 profile/debug 需要 reservation table，只能从 `writers_by_key` 派生，不能维护第二份会影响 correctness 的状态。

### 8.4 Failed-set union

基于 owner-recorded reads 和 actual writes 时，failed-set exchange 必须保留。任何 shard 发现的 failed tx 都必须进入 global failed set。

### 8.5 Client result

事务 result 只能发布一次：

- optimistic success：result shard 在 install success 后发布 optimistic result。
- failed tx：optimistic staged result 被丢弃，由 Aria fallback path 发布最终 result。

### 8.6 Fallback visibility

Fallback 必须在 optimistic successes 安装后运行。参考执行顺序是：

```text
all optimistic successes in batch order
then all failed txs in fallback batch order
```

checker 应按这个 deterministic reorder 顺序构造参考执行。

## 9. 测试计划

### 9.1 Unit tests

- `aria_result_shard_uses_tx_id_modulo`。
- `aria_ignores_batch_static_sets`：构造错误/空 `read_set/write_set`，Aria 仍按 op 动态读写执行。
- `aria_owner_records_dynamic_reads`。
- `aria_served_read_set_is_conservative`：即使 executor 分支可能提前失败，Aria 仍按 `FsOp` helper 请求并记录 served reads。
- `aria_stage_outcome_records_dynamic_writers`。
- `aria_rule1_detects_waw_from_actual_writes`。
- `aria_rule1_detects_raw_from_actual_reads_and_writes`。
- `aria_failed_set_union_prevents_partial_commit`。
- `aria_result_records_only_on_result_shard`。
- `aria_reservation_is_derived_from_writers_by_key`。

### 9.2 madsim tests

- `aria_duplicate_create_fallback`：
  - 两个 `Create` 同一路径。
  - 第一个 optimistic success。
  - 第二个 WAW failed，fallback 后返回 `AlreadyExists`。
- `aria_create_then_stat_fallback`：
  - `Create /d/a` 后 `Stat /d/a`。
  - `Stat` 因 RAW failed。
  - fallback 后返回 `Ok`。
- `aria_cross_shard_dynamic_read_write`：
  - 读 key 和 actual write key 分属不同 shard。
  - 验证 read owner/writer owner 都参与 local conflict decision。
- `aria_fallback_publishes_to_tx_id_result_shard`。
- `aria_fallback_result_shard_without_local_keys_executes`：
  - 构造 `tx_id % shard_count` 不属于 fallback conservative read/write owner 的 failed tx。
  - 验证该 shard 仍作为 active fallback executor 收齐 reads、执行并发布 result。
- `aria_fallback_active_executor_results_agree`：
  - 构造多个 active fallback executors。
  - 验证 debug/test 路径能检查它们的 execution result 一致。
- `aria_scheduler_profiles_dump_state`。
- 现有 benchmark 输出增加 Aria 对比。

### 9.3 Regression

- 现有 Calvin tests 不需要修改语义。
- 现有 SCC tests 不因 Aria 引入的 RPC/model enum 变化而改变行为。
- `cargo check` 必须通过。
- madsim test target 必须通过。

## 10. 实现步骤

1. 增加 `SchedulerKind::Aria`、profile enum、proto enum。
2. 给 Sequencer 增加 result policy，小范围支持 Aria 的 `tx_id % shard_count`。
3. 增加 Aria dynamic read RPC、staged outcome RPC、execution-done RPC、local failed-set RPC。
4. 增加 Aria batch state registry：staged outcomes、readers/writers、execution-done、failed-set。
5. 实现 Aria shard-side read-key derivation、coordinator dynamic read pull 和 actual write staging。
6. 实现 owner-local WAW/RAW failed-set 计算和 failed-set union。
7. 实现 optimistic install。
8. fork Aria deterministic locking fallback：重新从 op 推导 lock sets，强制 `tx_id % shard_count` 加入 active set，并保持现有 Calvin/SCC fallback 不变。
9. 将现有 `SccReorderRecord` 泛化为 `BatchReorderRecord`，SCC 和 Aria 共用 “optimistic successes + fallback txs” replay 结构。
10. 增加 unit tests、madsim tests 和 benchmark Aria 输出。

## 11. 已确定设计决策

1. 不引入通用 transaction execution context。Aria 只在 shard 侧根据 `FsOp` 类型推导 read keys，再动态路由到 key owner 获取 full read set；事务逻辑仍调用现有 `execute_deterministic`。
2. `SccReorderRecord` 泛化为 `BatchReorderRecord`，用于 SCC 和 Aria 的 deterministic replay/checker。
3. Aria 的 failed set 由一次 local failed-set exchange 得到：各 shard 先基于 owner keys 上的 readers/writers 算 local failed set，再 union 成 global failed set。
4. Aria fallback 不无脑复用 SCC/Calvin fallback。它 fork 一套 fallback implementation，定制 active set，让 `tx_id % shard_count` 总能作为 fallback active executor 发布 client-visible result。

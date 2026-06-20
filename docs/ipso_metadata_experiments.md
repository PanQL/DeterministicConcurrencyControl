# IPSO 元数据实验

本文档定义用于 IPSO 博士论文的 CalvinFS 元数据实验。文档说明实验目标、工作负载、实验矩阵、指标和脚本使用方式，但不绑定某一次具体生成的 CSV 或 PDF 数据快照。

## 实验目标

元数据实验比较三个调度器：

- Calvin：悲观的记录级调度。
- Aria：带回退路径的乐观记录级调度。
- SCC：语义指导调度。

这组实验回答三个问题：

- 在低冲突和共享父目录两类基础元数据场景下，三个调度器的扩展性如何？
- 在 md-workbench-like 工作负载中，随着父目录热点增强，三个调度器的性能如何变化？
- 在选定的高吞吐 mdtest 点上，三个调度器向客户端暴露的端到端延迟分布是什么样？

## 工作负载

### mdtest 扩展性

mdtest 扩展性实验使用 madsim cluster test 中实现的 mdtest-like 工作负载，包含 private 和 public 两种目录布局。

- Private 模式：每个 client 在自己的目录树下操作，跨 client 父目录冲突较低。
- Public 模式：多个 client 在共享父目录下操作，但对象名保持 client 唯一，从而提高父目录写冲突。
- 图中展示的操作：文件创建、属性查询、文件删除。
- client 数量：`1,2,4,8,16,32,64`。

图形为 2 行 x 3 列折线图：

- 行：private 和 public。
- 列：create、stat、unlink。
- 线条：Calvin、Aria、SCC。
- 纵轴：吞吐量，单位为千次操作每秒。

### mdtest 延迟

mdtest 延迟实验与 mdtest 扩展性实验使用相同的 workload 语义，不定义单独的 mdtest-lat workload。

每个 client 提交 operation 后，会把已提交事务的元数据通过有界 channel 发送给该 client 自己的 result collector。collector 并发等待事务结果，每个 client 最多同时维护 `CALVINFS_MDTEST_RESULT_INFLIGHT` 个 in-flight result request。

这个设计让请求提交和结果取回并行，同时限制挂起的 result waiter 数量。延迟指标定义为客户端可观测的端到端延迟：

```text
latency_ms = result_complete_time - submit_start_time
```

该指标不使用 client 时间戳与 server 时间戳相减，因此在 madsim 和真实分布式环境中都有明确语义。

延迟 CDF 使用扩展性曲线中高吞吐区域的固定 client 数。默认脚本使用 `32` 个 client。

图形为 2 行 x 3 列 CDF 图：

- 行：private 和 public。
- 列：create、stat、unlink。
- 线条：Calvin、Aria、SCC。
- 横轴：客户端可观测延迟，单位为毫秒。
- 纵轴：累积分布概率。

### mdworkbench 父目录热点

mdworkbench 实验保留 md-workbench 风格的 offset 访问模式，但不再把 offset scan 作为独立实验。

实验固定：

- client 数量 `N = 8`。
- offset：`CALVINFS_MDWB_OFFSET = 1`。
- 每个 client 的 data set 数：`CALVINFS_MDWB_DATA_SETS = 4`。
- 每个 data set 的预创建对象数：`CALVINFS_MDWB_PRECREATE_PER_SET = 32`。
- 每个 data set 的 benchmark 操作数：`CALVINFS_MDWB_OPS_PER_SET = 16`。
- 迭代次数：`CALVINFS_MDWB_ITERATIONS = 2`。

实验改变共享父目录 bucket 数 `M`：

```text
M = 8,4,2,1
fan_in = N / M = 1,2,4,8
```

每个 client 映射到某个共享父目录 bucket。随着 `fan_in` 增大，更多 client 收敛到同一个父目录，父目录写冲突压力增强。对象名保持唯一，因此该实验关注的是父目录元数据冲突，而不是对象名碰撞。

图形包含上下两个面板：

- 上半部分：benchmark 吞吐量，单位为千次操作每秒。
- 下半部分：Aria 回退事务数。
- 横轴：父目录扇入度 `N/M`。
- 线条：Calvin、Aria、SCC。

## 指标定义

### 吞吐量

`ops_per_sec` 表示某个 workload phase 的吞吐量。图中将其除以 `1000`，显示为千次操作每秒。

对于 mdtest，吞吐量按 phase 汇总，phase 完成时间取所有 client 中最慢的完成时间。

对于 mdworkbench，吞吐量只统计 benchmark phase。setup 和 cleanup phase 不纳入图中 benchmark 吞吐量。

### 延迟

`latency_ms` 表示从 client 开始提交请求到收到对应事务结果之间的客户端可观测时长。

对于 mdtest latency，result collector 属于 client-side benchmark driver。collector 与提交路径并行运行，并使用 `CALVINFS_MDTEST_RESULT_INFLIGHT` 限制 result wait 并发。

### 父目录热点

`fan_in` 定义为：

```text
fan_in = client_count / parent_count
```

它是 mdworkbench bucket-hotness 图的横轴。`fan_in` 越高，表示每个共享父目录 bucket 被越多 client 同时访问。

### Aria 回退

`fallback_tx_count` 表示 Aria 中从乐观路径进入 fallback 路径的事务数量。它用于解释 mdworkbench bucket-hotness 中的冲突压力。

### 冲突计数器

CSV 还包含一些冲突和 profile 计数器，例如：

- `key_conflicts`
- `conflicts_per_tx`
- `local_failed_count`
- `global_failed_count`
- `fallback_tx_count`

这些指标主要用于解释和 sanity check，不全部进入默认图形。

## 运行实验

madsim runner 为：

```bash
python3 scripts/run_ipso_metadata_madsim.py
```

默认运行：

- mdtest 扩展性实验。
- mdtest 延迟实验。
- mdworkbench 父目录热点实验。

常用参数示例：

```bash
python3 scripts/run_ipso_metadata_madsim.py \
  --trials 1 \
  --mdtest-clients 1,2,4,8,16,32,64 \
  --mdtest-latency-clients 32 \
  --mdtest-result-inflight 64 \
  --mdwb-parent-buckets 8,4,2,1
```

快速 smoke test 示例：

```bash
python3 scripts/run_ipso_metadata_madsim.py \
  --trials 1 \
  --mdtest-clients 1,2 \
  --mdtest-latency-clients 16 \
  --skip-mdworkbench \
  --output /tmp/ipso-metadata-smoke.csv \
  --latency-output /tmp/ipso-metadata-latency-smoke.csv \
  --log-dir /tmp/ipso-metadata-logs
```

## 绘制图形

绘图脚本为：

```bash
python3 scripts/plot_ipso_metadata.py
```

脚本读取 benchmark CSV 和可选的 latency CSV，并生成：

- `mdtest-scalability.pdf`
- `mdtest-latency-cdf.pdf`
- `mdwb-bucket-hotness.pdf`

显式指定输入文件的示例：

```bash
python3 scripts/plot_ipso_metadata.py \
  results/ipso_metadata/ipso-metadata-madsim.csv \
  --latency-csv results/ipso_metadata/ipso-metadata-latency-madsim.csv \
  --latency-clients 32 \
  --out-dir results/ipso_metadata/figures
```

## 版本管理约定

runner 和 plotter 应随源码纳入版本控制。生成的 CSV 和 PDF 如果作为论文稳定引用的实验产物，可以纳入版本控制。临时 stdout 日志和重复 trial 原始日志应放在 `results/ipso_metadata/run_logs/` 下，并由 git 忽略。

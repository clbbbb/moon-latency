# MoonLatency

MoonLatency 是纯 MoonBit 的整数延迟直方图。每个工作节点可以用单个直方图汇总请求耗时，随后合并节点结果，查询全局分位数以及满足延迟阈值的请求数。调用方统一时间单位，例如毫秒或微秒。

```sh
moon run cmd/main --release
moon test --release --deny-warn
```

演示将两个节点的 200 条观测合并，得到 p99 区间 `[400, 415]` ms，以及不超过 400 ms 的请求数量区间 `[194, 200]`。如果业务要求至少 198 条请求达标，这个摘要只能判断证据不足，因为该阈值落在一个包含观测的桶内。

## 接口

用 `Histogram::new()` 创建摘要。调用 `record(value, count=...)` 记录非负 Int64 延迟或一批相同延迟。用 `merge` 合并相同单位的摘要。非法值、非法权重或计数溢出返回 false，现有数据保留。

`quantile(p)` 使用百万分比，`990000` 表示 p99。分位数按 nearest-rank 定义，0 返回最小值所在区间，1000000 返回最大值所在区间。空摘要或超出范围的参数返回 None。`count_at_most(threshold)` 返回包含真实计数的闭区间。

## 算法和边界

0～31 逐值分桶。从 32 开始，每个二次幂区间均分为 16 桶，覆盖到 Int64 最大值。每个摘要有 960 个计数槽，桶界在模块内共享。录入用二分查找，合并和查询最多扫描 960 桶。合并逐桶相加，同一批观测的分桶结果与分片方式无关。

对精确分位数，目标秩对应的所有候选值都在返回桶内，桶宽至多为下界的 1/16。对阈值，完全落在阈值内的桶构成计数下界，再加上跨越阈值的桶得到上界。在桶右边界上，计数为精确值。

跨进程交换通过 `snapshot()` 和 `Histogram::from_snapshot()` 完成。MLH1 以四字节 ASCII 标识开头，随后每个非空桶以大端 u16 桶号和正 i64 计数组成十字节记录。桶号必须严格递增。空摘要四字节，最密摘要 9,604 字节。调用方需统一度量单位，并负责传输和数据完整性。

`assess_slo(threshold, target)` 返回达标、未达标、证据不足或无样本。阈值使用原延迟单位，目标比例使用百万分比。分类依据通过计数的上下界，只有整个区间都满足目标时才标记达标。

`Window::new(width, slots)` 汇总最近若干对齐的时间桶。以 `merge_at(time, shard)` 提交分片摘要，通过 `advance(time)` 推进空闲期，使用 `summary()` 取得独立快照。窗口允许同一时间桶多次汇入，拒绝更早的时间桶。过期桶从总摘要中扣除。调用方应在同一时间桶内汇总同批数据，并处理去重。

`Histogram::distribution()` 为同一批数据一次性构建前缀计数。多个分位数与阈值查询可用这个不可变视图进行二分查找，也可查询闭区间内的计数上下界。

`Shard` 用 MLS1 格式携带延迟单位、来源编号、序号、时间戳及 MLH1 内容。`Collector` 按来源与序号去重，校验单位与身份冲突，接受仍在窗口内的乱序分片。记录过期后回收去重状态，生产者应为不同分片分配不同身份。所有分片时间戳必须采用统一时间轴。

```sh
moon build --target js --release --deny-warn
node _build/js/release/build/cmd/collect/collect.js --help
python scripts/check_collect.py
```

文件汇总命令使用 Node.js。参数依次为延迟单位、时间桶宽度、桶数、延迟阈值、目标百万分比和 MLS1 文件路径。输出 JSON 中的 Int64 计数使用十进制字符串。

项目独立实现，遵循 Apache-2.0。相关近似分位技术可参阅 [Apache DataSketches KLL](https://datasketches.apache.org/docs/KLL/KLLSketch.html)。

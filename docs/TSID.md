# TSID / SeriesID 设计笔记

> 一条 series 的身份 = 它的完整 label set。本文记录 SeriesID 的设计推理：
> 为什么需要它、为什么必须先 canonicalize、为什么 hash 不够用、
> 以及 MVP 为什么选「计数器 + 注册表」而不是「纯 hash」。
>
> VM 对照源码：`lib/storage/tsid.go`、`lib/storage/index_db.go`

## 0. 一句话定义

```
SeriesID = H( canonicalize( {__name__="http_requests_total", job="api", instance="h1:9090", ...} ) )
```

**除 `(timestamp, value)` 之外的一切，统一参与身份计算。**
metric name 不是特例 —— 它只是名为 `__name__` 的普通 label
（Prometheus/VM 内部就是这么建模的）。

为什么需要它：样本点只有 16 字节，label set 字符串 100+ 字节。
若每个样本都带完整标签落盘，1M 点就是 100MB+ 冗余；且查询合并、
compaction 去重都是 series 对齐操作，整数比较比字符串比较快几个
数量级。磁盘上的 part 必须只存 `SeriesID → [(t, v)]`。

（概念定位：这不是 RDBMS 的 3NF normalization —— 时序数据
append-only 不可变，没有更新异常要防。它更像 star schema 的维度表 +
列存的 dictionary encoding：逻辑模型不变，纯物理优化。）

## 1. 先 canonicalize，再 hash

`{job="api",path="/foo"}` 和 `{path="/foo",job="api"}` 是**同一条**
series。必须先把 label 按 key 排序、序列化成唯一形式，再参与身份计算。
否则同一条逻辑 series 裂成多个 ID，数据直接碎片化。

这是这一步唯一的"硬骨头"，也是最容易写错的地方。

## 2. Hash 不可逆 → 它只解决"身份"，不解决"查找"

光有 hash，查询 `job="api"` 时**什么也做不了** —— 无法从 hash 反推
标签，也无法按单个 label 找 series。注册表实际需要两个方向：

```
正排 names.json:     SeriesID → 完整 label set     （hash 不可逆，必须存）
倒排 inverted.json:  "job=api" → [SeriesID, ...]   （查询入口）
```

Hash 只是"判等"的手段，这两个映射才是让系统能工作的本体。

## 3. 碰撞怎么办 —— 真实的分叉

64 位 hash 在百万级 series 下碰撞概率约 10⁻⁸，极小，但一旦发生就是
**两条 series 静默合并、数据损坏**。两种解法：

| 方案 | 做法 | 代价 |
|---|---|---|
| 纯 hash + 验证（VM 思路） | hash 出候选 ID，读注册表核对真实标签，撞了就换 ID | 无论如何都得存 ID→labels 来验证 |
| 计数器 + 注册表（MVP 思路） | canonical string 做 map key，新 key 分配递增 ID | 注册表必须持久化、重启要 reload |

**反直觉的点**：纯 hash 方案并不能省掉注册表（碰撞验证需要它），
所以"无状态 hash"的优势其实不成立。MVP 选计数器方案 —— 用 Go map
的字符串判等天然处理碰撞，逻辑上等于"hash 无限长、永不碰撞"，
代码还更简单。

## 4. 计数器方案的性能：不是 O(N) 全量对比

疑问：每次写入是否要用 canonical string 跟所有 existing string 逐一
对比？—— 不用，这正是 hash map 存在的意义：

```
canonical string
    │
    ▼  ① 算一次 hash：O(字符串长度)，100 字节 ≈ 几纳秒
  bucket #417
    │
    ▼  ② 只在该 bucket 内做字符串全等比较：期望 ~1 次
  命中 → 返回已有 ID ／ 未命中 → 分配新 ID
```

hash 用来**分区**（决定去哪个桶），字符串全等比较只用来在桶内
**验证**。桶数量随 key 总数增长，每桶平均只有几个 key —— 1 万条和
100 万条 series，单次查找成本基本一样：**O(1)，与已有 series 数量无关**。

注意这和上一节是同一件事的两个层面：map 内部就是"hash 碰撞 →
全字符串验证"机制，Go runtime 替我们实现了碰撞处理，我们拿到的语义
是"无限长 hash，永不碰撞"，代价为零。

### 工程上的真实瓶颈（不在查找）

1. **锁竞争**：`RWMutex` 下老 series 查找（读锁）可并发；**新 series
   注册**要写锁。高基数 churn（大量新 label 组合，如把用户 ID 当
   label）时注册才是痛点。VM 的对策是按 hash 把 map 分片（shard）
   摊薄锁竞争 —— MVP 不需要。
2. **Canonicalize 本身的成本**：每次写入排序 labels + 拼字符串，
   O(L log L)（L = label 个数），比 map 查找还贵。VM 在 hot path 上
   缓存"原始字节 → TSID"跳过重复解析 —— 也是后话。

## 5. MVP 决策

```
canonical string 做 map key + 单调递增计数器分配 SeriesID
```

- 正确性：靠字符串判等，碰撞问题从"检测并修复"变成"根本不存在"
- 成本：每次写入 ≈ 一次字符串 hash + 一次 map 查找
- 代价：注册表（`series/names.json`）需持久化，重启 reload ——
  这正好也是查询路径（§2）本来就需要的正排映射，一份数据两处用

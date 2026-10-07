# cluster-resource-gc-go

集群遗留资源（garbage）的识别与安全回收工具。从可替换的数据源读取对象及其归属/引用关系，
识别从存活根不可达的资源，先生成确定性的干跑计划，再按计划分批、并发、限速地安全回收，
支持中断恢复、越权拦截与全程审计。本地运行与测试完全不依赖真实集群、凭据或网络。

## 快速开始

```bash
go build ./...
go test ./...

# 干跑：只输出回收计划，不删除任何对象
go run ./cmd/gc -snapshot examples/snapshot.json -config examples/config.json -dry-run

# 执行回收（本地模拟删除落点，不触碰真实集群）
go run ./cmd/gc -snapshot examples/snapshot.json -config examples/config.json -dry-run=false
```

## 架构

```
Source(可替换) ──> Snapshot ──> Graph/Classify ──> ProtectPolicy ──> Plan(干跑,确定性)
                                                                      │
                                            Auditor <── Executor(分批/并发/限速/重试/断点续跑)
```

| 文件 | 职责 |
|---|---|
| `pkg/gc/types.go` | 资源、快照、分类、计划等核心类型 |
| `pkg/gc/source.go` | `Source`/`Deleter` 抽象；文件快照源、内存模拟源、幂等内存删除器 |
| `pkg/gc/graph.go` | 引用图构建与可达性遍历（迭代式，环安全） |
| `pkg/gc/classify.go` | 垃圾判定与原因细分 |
| `pkg/gc/protect.go` | 保护策略（标签 / 宽限期 / 自定义规则），判定可解释 |
| `pkg/gc/plan.go` | 确定性干跑计划（内容指纹 ID、排序、分批） |
| `pkg/gc/executor.go` | 执行器：分批、并发度、速率限制、幂等重试、断点续跑 |
| `pkg/gc/checkpoint.go` | 检查点持久化（文件原子写 / 内存实现） |
| `pkg/gc/audit.go` | 审计记录与授权拦截 |
| `pkg/gc/gc.go` | 编排入口：`DryRun` / `Run` |

## 回收语义

### 可达性与垃圾判定

- 从根集合（`root_kinds` + `root_ids`）出发，沿**引用边**与**归属边**（owner → 归属对象）正向遍历；
  不可达对象判为垃圾。遍历使用 visited 集合，**环、互相引用、重复引用都不会导致死循环**。
- 垃圾原因细分为四种，体现在计划项的 `reason` 字段：
  - `unreachable`：普通不可达；
  - `dangling-reference`：对象引用了不存在的目标（悬空引用）→ **需人工确认**（`category=manual`），默认不进执行计划；
  - `garbage-only-referenced`：仅被其他垃圾对象引用的中间对象 → 可自动回收；
  - `idle`：不可达且超过 `IdleThreshold` 未被使用 → 可自动回收。
- 每个判定都带 `detail` 说明（如缺失的引用目标、引用者列表、空闲时长），可解释。

### 保护策略（最高优先级，任何情况下不回收）

按顺序判定，命中即保护并记录原因：

1. **保护标签**：`protected_label` 指定的标签值为 `"true"`；
2. **宽限期**：对象年龄 < `grace_period`；
3. **自定义规则**：按 `kind` / `namespace` / `labels` 匹配（AND 语义），可配置多条。

被保护对象出现在计划的 `skipped` 列表中并附带原因。

### 干跑计划与确定性

- `DryRun` 只生成计划不删除；`Run` 先生成同一份计划再执行，**实际删除严格对齐计划内容，无计划外删除**。
- 计划项按 ID 排序，计划 ID 是规范化内容的 SHA-256 指纹：**同一份输入多次运行得到的计划完全一致**。

### 执行：分批、并发、限速、取消

- `batch_size` 控制每批对象数；批与批之间串行，批内按 `concurrency` 并发。
- `rate_per_second` 限制每秒发起的删除数（令牌间隔）。
- `max_retries` + `retry_backoff` 做幂等重试；`Deleter.Delete` 对不存在对象返回成功，重试不会重复计数。
- ctx 取消/超时：正在进行的删除会等待返回并提交，未开始的批次不再启动；**对象不会停在"删除中途"状态**——
  要么已提交（committed），要么保持待处理（pending），重启后继续。

### 中断恢复

- 每个对象删除成功后立即将检查点原子落盘（临时文件 + rename）。
- 重启后用相同输入重新生成计划（ID 相同），执行器加载检查点：
  - `committed`：跳过，不重复删除（结果中计入 `resumed`）；
  - `pending` / `failed`：重新执行（删除幂等，不会重复计数）。
- 已提交与未完成状态在检查点文件中明确区分。

### 授权与审计

- 每次执行前进行授权检查；未授权请求被拒绝、不产生任何删除，并写入 `denied` 审计记录（可追溯）。
- 每次删除尝试、计划生成、执行完成都会写入审计记录（操作者、对象、计划 ID、结果）。

## 配置项

`examples/config.json` 对应 `gc.Config`：

| 字段 | 说明 | 默认 |
|---|---|---|
| `roots.root_kinds` | 视为存活根的对象 Kind | 无 |
| `roots.root_ids` | 显式指定的根对象 ID | 无 |
| `policy.protected_label` | 保护标签名（值 `"true"` 生效） | 禁用 |
| `policy.grace_period` | 创建宽限期（ns 或 Go duration） | 0（禁用） |
| `policy.rules[]` | 自定义保护规则（name/kind/namespace/labels） | 无 |
| `plan.BatchSize` / CLI `-batch-size` | 每批对象数 | 50 |
| `plan.IncludeManual` | 是否将需人工确认的对象纳入计划 | false |
| `plan.Classify.IdleThreshold` | 空闲判定阈值 | 0（禁用） |
| `executor.Concurrency` / `-concurrency` | 批内并发度 | 4 |
| `executor.RatePerSecond` / `-rate` | 每秒删除速率上限，0 不限 | 0 |
| `executor.MaxRetries` | 单对象失败重试次数 | 0 |
| `executor.RetryBackoff` | 重试间隔 | 0 |
| `-checkpoint-dir` | 检查点目录 | `.gc-checkpoints` |
| `-actor` | 操作者标识（授权与审计） | `cli` |
| `-timeout` | 整体执行超时 | 10m |

## 接入真实集群

实现两个接口即可替换数据源与删除落点，核心识别/计划/执行/恢复逻辑不变：

```go
type Source interface  { Load(ctx context.Context) (*Snapshot, error) }
type Deleter interface { Delete(ctx context.Context, id ObjectID) error } // 必须幂等
```

## 测试覆盖

```bash
go test -race ./...
```

- 环引用 / 互相引用（不死循环、不误判可达对象）
- 悬空引用（判为 manual，默认不进计划）
- 仅被垃圾引用的中间对象、空闲对象
- 保护标签 / 宽限期 / 自定义规则
- 计划确定性与"执行结果对齐计划"
- 并发度上限、速率限制、幂等重试
- 取消后无中间状态、进程重启后从检查点恢复（不跳过、不重复）
- 越权拒绝与审计留痕

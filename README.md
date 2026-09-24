# 多设备生命体征流处理管线（Go）

一套**完全本地、确定性、可复现**的多设备多通道生命体征流处理管线。它把带时钟
差异、乱序、重复、迟到的设备采样归一到统一主时间轴，区分信号质量，按连续时间
窗派生指标并进行带迟滞的告警判定/恢复。**不依赖任何外部服务或真实凭据**。

- 语言：Go（module `github.com/h3yow/biosignal-pipeline-go`，go 1.25）
- 运行示例：`go run ./cmd/demo`
- 测试：`go test ./...`；竞态：`go test -race ./...`

---

## 1. 总体结构与数据流

```
设备数据包 Sample
   │
   ▼
[校准 CalibrationRegistry]  量纲线性换算，记录校准版本
   │
   ▼
[对齐 Aligner]              时钟建模(偏移+漂移) → 去重 → 乱序重排 → 迟到标记
   │  只输出 ActionAccept（有序） / ActionDuplicate / ActionLate
   ▼
[质量 WindowAssessor]       统一时间网格；合成缺失点；点级 GOOD/UNAVAILABLE/ABNORMAL；标记中断 GapAfter
   │
   ├──► [指标 SegmentAccumulator]  固定翻滚窗聚合，缺失/不可用/异常/中断使窗口失效
   └──► [告警 Engine]              连续时间窗迟滞状态机，仅 GOOD 点驱动，只在跳变时出事件
```

`Pipeline`（`pipeline.go`）用一把互斥锁串行化全部内部状态，`Push/Advance/Close`
可被多 goroutine 并发调用。下游处理是「按接收时间排序的输入流 → 结论流」的纯
函数，因此**在线处理与历史重放收敛到完全一致的结果**。

包划分：

| 包/文件 | 职责 |
| --- | --- |
| `calibration.go` | 量纲线性换算注册表，版本可追溯 |
| `timesync.go` | 设备时钟模型、去重、乱序重排、迟到判定 |
| `quality.go` | 点级三分类 + 时间网格缺失/中断评估 |
| `metrics.go` | 翻滚窗派生指标，禁止跨中断拼接 |
| `alarm.go` | 连续窗告警触发/恢复状态机 |
| `pipeline.go` | 线程安全总装编排 |
| `simulate/` | 固定种子的本地确定性数据生成器（注入各类异常） |
| `cmd/demo/` | 本地可运行的端到端示例 |

---

## 2. 时间对齐与时钟漂移处理

### 2.1 主时间轴与时钟模型

- 主时间轴以**管线所在主机的接收时钟**为基准。
- 每台设备维护一个时钟模型：`masterTime = slope * deviceTime + intercept`。
  - `intercept` 覆盖固定偏移；`slope` 覆盖线性漂移（如 1200 PPM）。
  - 用**带指数衰减权重的最小二乘**在线拟合，权重
    `w_i = 2^(-(x_last - x_i)/DriftHalfLife)`，越近的观测权重越高。
  - 拟合在「相对首观测的中心化秒」上进行，避免在 ~1.79e9 的绝对 Unix 秒上
    平方导致浮点灾难性抵消。
  - 权重只依赖观测内容，不依赖墙钟或随机量，因此**同一输入序列必得同一估计**。
- 只有**被接受的非重复、非迟到点**才更新时钟模型；重复点与迟到点不污染估计。
  每台设备的第一个接受点用于初始化模型。

> 可观测性限制：纯网络传输延迟与设备时钟偏移在数学上无法仅靠
> （设备时间, 接收时间）对区分。本模拟以接收时钟为主时间轴（无额外传输延迟），
> 使对齐结果即真实主时间。

### 2.2 去重

- 去重键为 **(设备, 通道, 序号 Seq)**。序号由设备单调递增。
- 相同键的包一律产出 `ActionDuplicate`（原因 `DUP_SEQ`），**绝不重复计入**，
  也不更新任何状态（前沿、时钟模型、质量、指标、告警均不受影响）。

### 2.3 乱序重排

- 每通道维护一个重排缓冲与「前沿 frontier」（已接受点中的最大主时间戳）。
- 新点先入缓冲；**仅当该点主时间戳推进了前沿时**才释放到期点，避免乱序补到的
  较早点被错误地放到已释放序列之后。
- 释放条件：点在缓冲中待满整个容忍窗口，即
  `master + ReorderWindow <= frontier`（闭区间）。
- 释放时按主时间戳排序（相同时间戳按 Seq 排序），输出对每个 (设备,通道)
  **严格按主时间有序**。

### 2.4 迟到与固定处置策略

- 迟到判定（闭区间边界）：`master + MaxLateWindow <= frontier`。
- 迟到点产出 **`ActionLate`（原因 `LATE_BEYOND_WINDOW`）并即时输出**：
  - **不静默丢弃**：作为明确事件出现，可被记录、统计、审计；
  - **不回插**到已经释放的有序序列，不会插到错误位置；
  - 不推进前沿、不更新时钟模型；其序号仍登记（其后到达的同序号重复包仍判重）。
- 等间隔采样点恰好落在边界时仍属于「可接受延迟」（闭区间）。
- `Flush` 按设备、通道字典序释放，保证输出顺序确定、可复现。

### 2.5 校准与量纲可追溯

- 换算为线性形式：`to = value * Scale + Offset`（如 `bpmK → bpm`，Scale=1000）。
- 每条换算带不可为空的 **版本号 Version**；对齐点携带 `CalVersion`，使每个数值
  都可追溯到具体校准配置。上报量纲已是规范量纲时使用恒等换算（版本 `identity`）。
- 两类接入错误严格区分：`ErrNoCalibration`（设备/通道从未注册）与
  `ErrUnknownUnit`（通道已知但来源量纲未注册）。同一来源量纲以不同版本重复注册
  会被当作配置冲突拒绝。

---

## 3. 信号质量与可用性语义

评估在统一时间网格（`BucketWidth`，按纪元整除对齐）上进行，每网格恰好一个判定点。

| 结论 `QualityClass` | 原因码 | 语义 | 是否进入数值聚合/推进告警 |
| --- | --- | --- | --- |
| `GOOD` | `ACCEPTED` | 有采样、信号可用、数值在合理范围 | 是 |
| `MISSING` | `MISSING_WINDOW` | **数据缺失**：该网格没有任何采样 | 否 |
| `UNAVAILABLE` | `ARTIFACT_HINT` | **信号不可用/受伪迹影响**：有采样但设备报告脱落/伪迹 | 否 |
| `ABNORMAL` | `VALUE_OUT_OF_RANGE` | **数值本身异常**：信号可用但值超出通道 [Min,Max] | 否 |

关键规则：

- **先判可用性、再判数值异常**：受伪迹影响的越限值归为 `UNAVAILABLE` 而非
  `ABNORMAL`，避免一个伪迹尖峰同时触发质量与数值两类结论。
- **缺失由网格确定性合成**：相邻真实点之间的空网格、以及 `Advance(now)` 到期仍
  无采样的网格，都合成 `MISSING` 点，位置完全由时间戳整除决定。
- **中断边界 `GapAfter`**：相邻真实点间隔超过 `MaxGap` 即标记一次中断。
  中断期间的网格全部是 `MISSING`。

### 指标不被污染、不跨中断拼接

- `SegmentAccumulator` 按固定翻滚窗（`Metric`，纪元对齐）聚合 `mean/min/max`。
- 只有 `GOOD` 点进入数值聚合；缺失/不可用/异常点不参与。
- 窗口一旦包含 `GapAfter`（或缺失/不可用/异常），**整个窗口判失效**
  （`Metric.Valid=false`），失效原因按固定优先级给出，可区分：
  `GAP_INTERRUPTED > MISSING_WINDOW > UNAVAILABLE_INPUTS > VALUE_OUT_OF_RANGE`。
- 失效窗口**仍会输出**，让「为什么没有指标」本身可解释，而不是静默消失。
  因此任何派生指标都不可能跨越中断把两段采样拼成「连续」的虚假结果。

---

## 4. 告警判定与恢复

`Engine` 是一个按 `(规则, 设备)` 维护的迟滞状态机（IDLE / ACTIVE）。

- 规则 `Rule{High, Low, EnterWindow, ExitWindow, Version}`：越界定义为
  `value > High` 或 `value < Low`（闭区间 `[Low,High]` 为安全）。
- **连续时间窗**：只有连续的 `GOOD` 越界点，其首末时间跨度
  `last - first >= EnterWindow` 才触发（1s 采样下需要 3 个点覆盖整个 2s 窗口）。
  因此单个采样点、甚至两帧的抖动**不会**触发告警。
- **迟滞恢复**：进入 ACTIVE 后，需要连续 `GOOD` 安全点跨度达到 `ExitWindow`
  才恢复，避免在阈值附近反复抖动。
- **缺失/不可用/异常/中断不触发也不恢复**：这些点清零连续段计数但**保持当前
  状态**。设备断连期间告警保持 ACTIVE，重连后必须重新积累足够连续安全点才恢复，
  不会因断线而漏恢复或误清除，也不会重复告警。
- **事件只在状态跳变时产生一次**（IDLE→ACTIVE、ACTIVE→IDLE），同一越界无论
  持续多久、被重放多少次，都只产生一条触发和一条恢复。
- 每个事件带 **`RuleVersion`**：规则/配置版本变化后，历史结论可按其当时版本复现。
- 引擎是「有序判定点流 → 事件流」的纯函数；**在线与重放逐字一致**。
- 时间跨度用 `UnixNano` 差值计算，不依赖 `time.Time` 的 Location。

---

## 5. 确定性、并发与重放收敛

- 数据生成器（`simulate`）的所有「随机」噪声来自固定种子 LCG；场景规格显式
  声明时钟偏移/漂移、断连区间、伪迹区间、强制越界值，以及网络层的
  重复 / 相邻交换（乱序）/ 延迟（迟到）注入。同规格同种子永远生成同一批包。
- 管线内部不使用随机量与墙钟；缺失网格位置由整除决定，缓冲释放由时间戳比较
  决定，所有 map 的对外遍历都按固定键序排序。
- `Pipeline` 单锁串行化；`go test -race` 下多设备 goroutine 并发推送干净通过，
  且两次并发运行的接受/重复/迟到计数完全一致。
- `TestReplayConvergesWithOnline` 断言：同一批包（保持接收顺序）在两个全新管线
  上分别「在线处理」与「重放」，对齐事件、判定点、指标、告警逐项相等。
- `Advance(maxMasterTime)` 是在线与重放共用的缺失驱动入口：在线由墙钟定时驱动，
  重放传入数据覆盖的最大主时间戳即可得到相同终态。

### 失败与拒绝原因可区分

- 硬错误（接入失败，返回 `error`）：`ErrNoCalibration` / `ErrUnknownUnit`。
- 正常的拒绝/降级事件（出现在结论中，带原因码）：
  `DUP_SEQ`、`LATE_BEYOND_WINDOW`、`MISSING_WINDOW`、`ARTIFACT_HINT`、
  `VALUE_OUT_OF_RANGE`、`GAP_INTERRUPTED`、`UNAVAILABLE_INPUTS`。

---

## 6. 本地验证方法

无需联网、无需任何凭据或外部服务。

```bash
# 1) 编译
go build ./...

# 2) 运行确定性端到端示例（打印告警与汇总结论）
go run ./cmd/demo

# 3) 单元 + 端到端测试（日志打印输入、判定依据与最终结论）
go test -v ./...

# 4) 竞态检测 + 重复运行（验证并发与确定性）
go test -race -count=3 ./...
```

测试日志对每个包打印 `INPUT ...`（输入）、`ALIGN/JUDGE/METRIC/ALARM ...`
（判定依据与结论）、`CONCLUSION ...`（汇总结论）。

### 测试覆盖的场景

- 多设备（`wrist-A`、`chest-B`）多通道（HR、SpO2），不同量纲与校准版本；
- 固定时钟偏移与正/负线性漂移（+1200 / -800 PPM）；
- 数据包重复（重复序号去重）、相邻交换乱序（容忍窗口内重排）、延迟迟到
  （超出窗口明确标记、不回插）；
- 断连区间（缺失网格 + 中断边界 + 指标失效 + 告警保持）与重连恢复；
- 伪迹区间（UNAVAILABLE）、孤立数值尖峰（ABNORMAL，不触发告警）、
  持续越界段（触发一次 → 恢复一次的完整告警生命周期）；
- 在线 vs 重放逐项一致；多 goroutine 并发在 `-race` 下通过。

### 关键测试索引

| 测试 | 验证点 |
| --- | --- |
| `TestAlignerDuplicate/Reorder/Late/ClockOffsetAndDrift` | 去重、乱序、迟到、漂移对齐 |
| `TestClassifierThreeClasses` / `TestAssessorMissingAndGap` | 三分类、缺失合成、中断标记 |
| `TestMetricsNoCrossGap` / `TestMetricsUnavailableExcluded` | 不跨中断拼接、失效原因区分 |
| `TestAlarmSingleSpikeNoTrigger` 等 | 抖动抑制、触发/恢复、缺失不恢复、重放一致 |
| `TestCalibrationConvertAndTrace` / `...ErrorDistinction` | 校准追溯、错误可区分 |
| `TestE2EDeterminismAndClassification` / `TestReplayConvergesWithOnline` | 端到端分类与重放收敛 |
| `TestConcurrentRaceFree` | 并发竞态与计数确定性 |

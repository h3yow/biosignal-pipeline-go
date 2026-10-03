# 多设备生命体征流处理管线（本地、确定性、可复现）

一套**完全本地运行**的多设备 / 多通道生命体征采样处理管线。用确定性数据模拟真实
佩戴场景，验证设备时钟差异、乱序、重复、迟到、信号中断、断连重连与历史重放等情况
下，时间对齐、质量判定与告警结论的**一致性与可解释性**。

- 纯 Go 标准库，无任何外部服务、网络调用或真实凭据。
- 所有时间来自固定基准，不读取墙钟；相同输入永远得到相同输出。
- 每个被接受 / 拒绝的包都带可区分的状态与人类可读原因。

## 目录结构

| 文件 | 职责 |
| --- | --- |
| `types.go` | 核心类型：原始包、校准、时钟参数、对齐点、质量分类、告警状态/事件 |
| `timesync.go` | 设备时钟 → 统一时间轴映射（固定偏移 + 线性漂移） |
| `reorder.go` | 乱序重排、序列号去重、迟到拒绝、校准换算 |
| `quality.go` | 信号质量三分类与缺失槽填充 |
| `metrics.go` | 不跨越中断边界的派生指标聚合 |
| `alert.go` | 基于连续时间窗的告警触发/恢复迟滞状态机 |
| `incident.go` | 持续异常事件（Incident）跟踪：稳定编号、阶段时间线 |
| `pipeline.go` | 串联全部阶段的并发安全编排器（含尾段静默与配置快照） |
| `simulate.go` | 确定性本地数据生成器（乱序/重复/丢包/中断/伪迹/漂移） |
| `cmd/demo` | 本地可运行的端到端演示 |
| `*_test.go` | 覆盖全部场景的确定性测试，日志含输入、依据、结论 |

## 1. 时间对齐与漂移处理

### 1.1 时钟模型

每个设备注册一个 `ClockParams`，映射到统一时间轴：

```
unified = Epoch + round((deviceTime - Epoch) * (1 + DriftPPM*1e-6)) + Offset
```

- `Offset`：固定相位偏移（设备时钟与统一时间的恒定差）。
- `DriftPPM`：线性时钟漂移，百万分率（例如 200 ppm = 0.02%）。
- `Epoch`：所有设备共享的漂移基准时刻（取采集起点）。漂移是相对基准的**一阶项**，
  因此基准必须是真实起点；用公元元年作基准会把微小漂移放大成天文数字。
- 结果四舍五入到纳秒，消除浮点亚纳秒噪声，保证映射逐位可复现。
- **未注册时钟的设备**直接返回 `StatusUnknownDevice` 拒绝，绝不静默假设零偏移。

### 1.2 乱序、重复与迟到（固定策略，状态可区分）

`ReorderBuffer.Ingest` 对每个原始包按固定顺序判定：

1. **去重**：键为 `(device, channel, seq)`。已见过的序列号判 `StatusDuplicate`，
   不重复计入任何统计，也不推进水位。
2. **时钟映射**：未注册 → `StatusUnknownDevice`。
3. **迟到判定（先于去重之外的一切内容判定）**：
   水位 `watermark_d = maxUnifiedSeen_d - MaxDelay`，按**设备独立**计算，
   避免不同设备时钟互相干扰。统一时间早于水位的包判 `StatusLateRejected`：
   - 不静默丢弃：返回明确状态与可读原因（含实际时间、水位时间）；
   - 不插入错误位置：迟到包绝不混入已经 `Drain` 出去的早期批次。
4. **校准查找**：未注册通道/校准 → `StatusUnknownChannel`。
5. **量纲换算**：`physical = raw*Scale + Offset`，换算公式与单位记录在接受原因中，
   可追溯到 `ChannelSpec.Calibration`。

`Drain(cutoff)` 输出统一时间 `<= cutoff` 的点，按
`(channel, time, seq)` 稳定排序——**结果只取决于数据内容，与到达顺序无关**。

> 关于确定性的边界：水位依赖“已观测到的最大时间”，因此在**给定到达序列**下结果
> 完全确定；不同的到达顺序（如不可控的并发调度）下，哪些包算迟到本身会不同，这是
> 流式处理的固有语义。本管线保证：①相同到达序列 ⇒ 相同结论；②每个迟到包都被
> 明确标记而非丢弃；③重复包在任何交错下都不会重复计入。

## 2. 信号质量分类语义

四个互斥类别（`QualityClass`），严格区分三类问题：

| 类别 | 含义 | 典型依据 |
| --- | --- | --- |
| `QGood` | 信号正常、数值可信 | 无硬件异常标志，值落在可信生理范围 |
| `QMissing` | **数据缺失**：该有数据但没有 | 相邻点间隔 `> ExpectedInterval + GapTolerance` |
| `QArtifactUnusable` | **信号不可用 / 受伪迹影响**：有数据但不可信 | `FlagSensorOff`（传感器脱落）、`FlagSaturation`（ADC 饱和削顶）、`FlagLeadsOff`（导联脱落） |
| `QValueAbnormal` | **数值本身异常**：信号可信但超出生理范围 | 校准值 `< PhysMin` 或 `> PhysMax` |

判定优先级：**硬件伪迹标志 > 缺失（仅由时间空洞产生）> 数值范围**。
即“数值 250 且带饱和标志”判为不可用，而不是数值异常——因为该读数本身不可信。

缺失槽在真正的时间空洞内按 `SlotStep` 生成（时间戳严格位于前后两点之间），
正常抖动（容差内）不会误报；每个判定都带 `Reasons` 说明依据。

### 指标不跨中断拼接

`MetricAggregator` 逐点聚合 `Mean/Min/Max/Count`：

- `QGood` 与 `QValueAbnormal` 计入当前窗；**数值异常仍参与统计**，但在
  `AbnormalCount` 中单独计数，与缺失/不可用严格区分。
- 遇到 `QMissing` 或 `QArtifactUnusable` **立即关闭当前窗**；中断后的有效数据
  开启新窗，并在新窗 `GapBefore` 记录中断时长。因此指标永远不会跨越空洞拼接出
  虚假连续段。

## 3. 告警判定与恢复逻辑

每条规则（`AlertRule`）是一个独立状态机，带 `ID` 与 `Version`：

```
Inactive ── 连续 TriggerN 个越限槽 ──────────────► Active(触发事件)
Active   ── 连续 RecoverN 个非越限槽 ────────────► Inactive(恢复事件)
Active   ── 出现 QMissing / QArtifactUnusable ──► Inactive(强制恢复事件)
```

- **连续时间窗**：评估以等宽槽进行，连续 `TriggerN` 个槽越限才触发；
  中途反向立即清零。单个采样点抖动既不会触发，也不会让活跃告警反复翻转。
- **迟滞恢复**：活跃后需连续 `RecoverN` 个槽回到范围内才恢复，防止临界点抖动。
- **断连/中断强制恢复**：活跃告警期间数据缺失或信号不可用，立刻产生一次
  强制恢复——设备断连不允许告警滞留；未处于告警态时不产生事件，杜绝重复告警。
- **规则版本**：每个 `AlertEvent` 固化 `RuleVersion`。规则或配置变化后，用旧版本
  配置对历史数据重放即可复现当时结论（测试 `TestRuleVersionReproducibility` 验证）。
- 支持高限（`>= HighThreshold`）与低限（`<= LowThreshold`），可分别启用。

### 在线处理与历史重放收敛

管线不使用接收墙钟，结论只由**设备时间（映射到统一时间轴）与序列号**决定。
因此：

- 在线逐包进入 + 按 `unified - MaxDelay` 推进水位；
- 离线把同一批包一次性灌入后统一 `Advance`；

两条路径产出的指标窗与告警事件**逐条相同**，断连重连后也收敛到同一状态，
不出现重复告警或漏报（见 `TestOnlineEqualsReplay`）。

## 4. 持续异常事件（Incident）管理

`IncidentTracker` 把每个通道的质量判定流折叠成“事件”：一段持续异常从首次
数值越限开始，到**连续正常满 `IncidentRecoverAfter`** 才结束，全程只占一条档案。

- **稳定编号**：`INC-<channel>-<序号>`（如 `INC-HR-0001`），同一次异常全程不变，
  事后可按编号把 `Timeline` 里的阶段迁移串起来。
- **断连不结束事件**：事件进行中遇到 `QMissing` / `QArtifactUnusable`
  （设备摘下、伪迹遮挡、丢包），进入 `PhaseUnavailable`——**数据不可用、结论待定**，
  事件不关闭、不新增；数据恢复后若仍异常回到 `PhaseAbnormal`，若正常则进入
  `PhaseRecovering` 重新计时。
- **关闭条件**：只有连续正常满 `IncidentRecoverAfter`（`<=0` 表示首个正常槽即关闭）
  才进入 `PhaseClosed`。中间任何异常或不可用都会打断恢复计时。
- **在线 = 重放**：事件只由质量判定流决定，而判定流在在线逐包与历史重放下逐条一致，
  因此两条路径产出的事件**条数、编号、时间线完全对齐**（`TestIncidentDisconnectReconnect`）。
- 查询：`Pipeline.Incidents()` 返回全部事件档案（含进行中的），
  `AdvanceFull` 返回的 `AdvanceResult.Incidents` 是本次推进产生的阶段迁移。

```
无事件  ── QValueAbnormal ──► ABNORMAL ── 缺失/不可用 ──► UNAVAILABLE(待定)
ABNORMAL/UNAVAILABLE ── 正常 ──► RECOVERING ── 连续正常满 RecoverAfter ──► CLOSED
RECOVERING ── 再次异常 ──► ABNORMAL        RECOVERING ── 不可用 ──► UNAVAILABLE
```

## 5. 会话收尾与尾段静默

设备摘下放在一边、或一次采集结束后，**最后一条数据之后的时间不是“正常”**。
开启 `MaxSilence`（>0）后，每次 `Advance` 都会对“最后实际点之后没有数据”的通道
按 `SlotStep` 补 `QMissing` 判定：

- **有界**：任一静默跨度（点间空洞或尾段）最多补 `MaxSilence` 时长；
  静默超过上限时，在“静默起点 + MaxSilence”处打一条 `Truncated=true` 的
  截断标记槽（机器可读，位置即截断点），之后的静默不再逐槽展开，结论待定。
- **幂等**：重复推进同一时间段不会产生重复判定（按 through 水位去重），
  在线逐步推进与一次性重放的结论一致。
- **可配置、可复现**：`MaxSilence` 由使用方按场景设定；同一配置重跑结论逐位一致
  （`TestSilenceCapConfigurable`）。`MaxSilence=0` 关闭该语义，行为与旧版完全一致。
- 截断后事件仍开着的一律停在 `PhaseUnavailable`（结论待定），不会被误判为已恢复。

> 调用约束（在线路径）：`Advance` 的 cutoff 不应越过“已见最大统一时间 - MaxDelay”
> 这一安全水位。越过水位推进会让后到但合法的包被水位判定丢弃——这与迟到拒绝
> 是同一类流式固有语义，按水位推进即可避免。

## 6. 配置快照与版本兼容

每条可查询/导出的结论都携带**机器可读**的 `ConfigSnapshot`（不是文字说明）：

| 结论类型 | 快照位置 |
| --- | --- |
| `QualityVerdict` | `Snapshot`（含截断标记槽） |
| `MetricWindow` | `Snapshot` |
| `AlertEvent` | `RuleVersion` + `Snapshot` |
| `Incident` | `Snapshot`（事件开启时固化） |

`ConfigSnapshot` 含：`ConfigVersion`（`Config.Version`）、该通道当时生效的
`Cal`（Scale/Offset/Unit）、`QualityRuleVersion`（`QualityRule.Version`）、
以及该通道全部告警规则的 `ID -> Version` 映射。

- **历史结论不被改写**：快照在建管线时固化；之后改换算参数或规则版本，
  已产出的结论仍指向旧版本。
- **旧版本可复现**：用旧版本配置对同一批原始数据重放，可逐条还原相同结论
  （判定、告警事件、指标窗、事件档案），见 `TestConfigSnapshotPinnedAndReplay`。
- **兼容范围**：新增字段（`Version`/`MaxSilence`/`IncidentRecoverAfter`/
  `QualityRule.Version`）均为可选；不设置时行为与旧版逐位一致
  （`MaxSilence=0` 不补尾段、快照版本字段为空串）。旧配置不需要迁移。

## 7. 本地验证方法

前置：Go 1.25+，无需任何外部服务或凭据。

```bash
# 端到端演示（含处置计数、指标窗、告警事件与判定原因）
go run ./cmd/demo

# 全部单元测试（日志中打印输入、判定依据与最终结论）
go test -v .

# 并发 + 竞态检测（确定性多 goroutine 压测）
go test -race -count=1 ./...

# 覆盖率
go test -cover .
```

### 测试覆盖的场景

| 测试 | 覆盖点 |
| --- | --- |
| `TestClockMapping` | 偏移 + 漂移换算精确、未注册设备拒绝 |
| `TestReorderDedupLateCalibration` | 乱序升序输出、重复去重、超窗迟到拒绝、校准公式 |
| `TestUnknownDeviceChannel` | 两类拒绝原因可区分 |
| `TestQualityClasses` | 正常 / 数值异常 / 伪迹不可用三类互斥判定与优先级 |
| `TestMissingFillAndNoGapCrossing` | 缺失槽位置与数量、容差内抖动不误报 |
| `TestMetricsWindowSplitting` | 缺失与不可用切窗、异常值计数、GapBefore |
| `TestAlertTriggerHysteresisJitter` | 连续窗触发/恢复、单点抖动不翻转、事件时间与版本 |
| `TestAlertForcedRecoveryOnGap` | 中断强制恢复、非活跃态不产生重复事件 |
| `TestOnlineEqualsReplay` | 在线 vs 重放逐条一致；两设备漂移、乱序、重复、丢包、断连重连；恰好 1 次触发 1 次恢复 |
| `TestRuleVersionReproducibility` | 事件固化规则版本，历史结论可复现 |
| `TestConcurrentIngestRace` | `-race` 并发安全；每个包处置总和守恒、无静默丢弃 |
| `TestIncidentDisconnectReconnect` | 断连重连不拆事件：编号稳定、不可用段待定、连续正常才关闭；在线=重放 |
| `TestTailSilenceVisibleAndTruncated` | 尾段静默显式标缺失、超上限截断、重复推进幂等 |
| `TestSilenceCapConfigurable` | 上限可配置（5s/15s/0），结论各自稳定可复现 |
| `TestIncidentPendingWhenDeviceRemoved` | 异常中设备摘下：事件不关闭、停在最末待定阶段、可见截断位置 |
| `TestConfigSnapshotPinnedAndReplay` | 结论固化换算参数与规则版本；改版不改写历史；旧版重放逐条还原 |
| `TestConcurrentNewPathsRace` | `-race` 下并发 AdvanceFull/Incidents/Events 无竞争，结论与串行一致 |

所有随机化均来自固定种子的确定性 LCG（仅用于制造固定的乱序与抖动），
不读取时间或环境，测试可重复运行并得到字节级一致的结论。

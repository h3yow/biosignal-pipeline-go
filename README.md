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
| `quality.go` | 信号质量三分类与缺失槽填充（受静默上限约束） |
| `metrics.go` | 不跨越中断边界的派生指标聚合（结论带配置快照） |
| `alert.go` | 基于连续时间窗的告警触发/恢复迟滞状态机 |
| `episode.go` | 事件管理：一次持续异常 = 一次事件，跨中断不拆分 |
| `silence.go` | 中间空洞与尾段静默的统一填充上限与截断结论 |
| `snapshot.go` | 配置快照：结论固化产生时的换算参数与规则版本 |
| `pipeline.go` | 串联全部阶段的并发安全编排器 |
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

## 4. 事件管理：一次持续异常 = 一次完整事件

`EpisodeRule`（按通道配置，可选）把一段持续异常归并为**一次事件**，用于值班与
事后复盘，而不是每越限一次就报一条：

- **开启**：首个越限槽开启事件，分配稳定编号（`通道#序号`，如 `HR#0001`）。
  编号只取决于输入内容，同一批数据在线处理与离线重放得到相同编号。
- **跨中断不拆分**：事件开启期间，设备摘下、伪迹遮挡或丢包造成的
  `QMissing`/`QArtifactUnusable` 段被标记为 `SegUnavailable`
  （**数据不可用、结论待定**）——既不是恢复正常，也不是事件结束；
  重连后若仍越限，**同一事件继续**，不会产生新事件。
- **结束条件**：只有**连续**正常满 `RecoverDuration` 事件才关闭
  （`End` = 连续恢复满时长被确认的时刻）。不可用段会打断“连续正常”计时，
  数据恢复后需重新连续计满——“没数据”不能冒充“已恢复”。
- **可串联复盘**：`Pipeline.Episodes()` 返回事件档案（含 `Segments`：
  异常 / 不可用待定 / 恢复观察 三段式时间线）；`Pipeline.EpisodeEvents()`
  返回生命周期迁移记录（开启 / 进入不可用 / 数据恢复 / 结束），
  每条都带 `EpisodeID`，可顺编号把一次事件从开始到结束串起来。
- **在线 = 重放**：事件完全由质量判定流推导，而判定流只取决于设备时间与
  序列号，因此在线逐包与历史重放产出的事件条数、编号、起止与片段逐条一致
  （`TestEpisodeOnlineEqualsReplay`）。

## 5. 长静默：中间断连空洞与采集尾段，同一个上限

设备**中途摘下/丢包很久后又连回**，与**一次采集彻底结束**，都会产生一段长时间
没有数据的静默。两类静默在本管线中受**完全相同**的处理与**同一个上限**约束，
区别只在于截断结论定位的是哪个通道、哪一段数据：

- **静默一律补缺失、但绝不无限补**：超过 `ExpectedInterval + GapTolerance` 的静默
  按 `SlotStep` 补 `QMissing`；缺失填充最多补到 `最后实际点 + SilenceCap`
  （截断点那一槽仍补一条“此处已无数据”），截断点之后的时间轴**不再补点**。
  因此即使断连几小时，也不会按原采样密度补出成千上万个点——耗时与内存只随
  上限线性增长，不随空洞跨度增长。
- **超过上限必有明确结论**：只要某段静默跨度超过生效上限，就记录一条
  `SilenceTruncation`（`Pipeline.Truncations()`），明确给出**通道、最后实际数据
  时刻 `LastDataTime`、截断发生位置 `TruncatedAt = LastDataTime + SilenceCap`**，
  以及人类可读原因与机器可读配置快照。同一段静默只记录一次，反复推进不会重复。
- **两类静默结论字段一致**：中途空洞（重连后得出）与尾段（`Advance(t)` 把时间推到
  最后数据点之后得出）产出的 `SilenceTruncation` 只描述数据本身（最后实际点与截断
  位置），不依赖“之后是否又连回来”。这保证了**在线逐包**（空洞期间先按尾段补到
  水位、重连后收尾）与**把同一批数据一次性重放**（直接在两点间补空洞）对同一段
  静默产出**逐字段相同**的一条结论。
- **尾段不再“看起来一直正常”**：最后一个实际点之后补的 `QMissing` 会让活跃告警在
  第一个静默槽被强制恢复、指标窗在最后实际点关闭、事件尾段标记为
  “数据不可用、结论待定”。

### 5.1 上限的确定、稳定与可复现

- **改大改小，截断位置确定性地随之移动**：`TruncatedAt` 严格等于
  `LastDataTime + 生效 SilenceCap`（`TestCapDeterministicAcrossValues` 验证单调性）。
- **相同上限重复跑，逐条一致**：事件、指标窗、截断结论（含快照）在重复运行、
  在线处理与批量重放之间逐条对得上（`TestMiddleGapOnlineEqualsReplay`、
  `TestUnconfiguredCapIsBoundedAndReproducible`）。
- **未配置也必须有明确、有限的边界**：`Config.SilenceCap` 为零或任意非正值时，
  生效值为内建常量 `DefaultSilenceCap`（2 分钟），**绝不退化为“不设边界、一路补
  下去还不给结论”**。生效值可通过 `Pipeline.EffectiveSilenceCap()` 查询，并被
  固化进每条结论的快照。
- 截断策略有自己的身份与版本：`SilenceRuleID = "builtin.silence-truncation"`、
  `SilenceRuleVersion = "v1"`，随截断结论快照输出，可按版本重放。

## 6. 配置快照与新旧兼容

每条可查询/导出的结论都固化**产生它时**的配置，以结构化字段输出
（`ConfigSnapshot`，程序可直接读取，非人类可读文字）：

| 结论 | 快照内容 |
| --- | --- |
| `AlertEvent.Snapshot` | 规则 ID/版本、高低限、TriggerN/RecoverN、质量参数、槽步长、静默上限、通道 `Calibration` |
| `Episode.Snapshot` / `EpisodeEvent.Snapshot` | 规则 ID/版本、高低限、RecoverDuration、质量参数、槽步长、静默上限、通道 `Calibration` |
| `MetricWindow.Snapshot` | 质量规则 ID/版本、期望间隔/容差、生理范围 `PhysMin/PhysMax`、槽步长、静默上限、通道 `Calibration`（旧字段 `MetricWindow.Cal` 保留） |
| `SilenceTruncation.Snapshot` | 截断策略 ID（`builtin.silence-truncation`）/版本、质量参数、槽步长、**当时生效的静默上限**、通道 `Calibration` |

即每条结论都能直接取到三类信息：**换算参数**（`Cal` 的 Scale/Offset/Unit）、
**判定用到的规则身份与版本**（`RuleID/RuleVersion`）、**当时的上限值**
（`SilenceCap`），另含槽步长与质量阈值。质量规则 `QualityRule` 新增可选的
`ID/Version` 字段，未配置时为空串，不影响判定。

- 换算参数或规则改版后，**历史结论不被改写**（各自携带当时的快照）；
  用旧版本配置（含当时的 `SilenceCap`）对同一批历史数据重放，可逐条还原相同
  结论（含快照字段本身，见 `TestConfigChangeDoesNotRewriteHistory`、
  `TestConfigSnapshotPinnedAndReplay`、`TestRuleVersionReproducibility`）。
- **兼容范围**：
  - `EpisodeRules` 不配置 → 无事件输出，其余行为与旧版一致；
  - `SilenceCap` 零值语义变更：**旧版是“不设上限、一路补下去”，新版一律回退到
    有限默认值 `DefaultSilenceCap`（2 分钟）并在超限时给出截断结论**。这是有意的
    语义收紧——长静默必须有明确、有限、可复现的边界，不允许无限填充；
  - 尾段静默填充本身（`Advance` 越过最后数据点会补 `QMissing`，使活跃告警强制
    恢复、指标窗在最后实际点关闭）相较最初版本即为新语义，继续保留；
  - `TailTruncation` 现是 `SilenceTruncation` 的**类型别名**（字段去掉了
    `Kind`，新增 `Snapshot`），按旧类型名与字段（Channel/LastDataTime/
    TruncatedAt/Reason）构造/读取的既有代码无需改动；
  - `AlertEvent.Snapshot`、`MetricWindow.Cal`、`QualityRule.ID/Version`、
    `MetricWindow.Snapshot` 均为纯新增字段，按字段名构造的既有代码不受影响。

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
| `TestEpisodeDisconnectReconnectSameEpisode` | 断连重连仍是同一事件；不可用段标“结论待定”；连续正常满时长才结束；生命周期事件可按编号串联 |
| `TestEpisodeUnavailableDoesNotCountAsRecovery` | 不可用段打断连续正常计时，恢复需重新计满 |
| `TestEpisodeOnlineEqualsReplay` | 事件档案与生命周期事件在线 vs 重放逐条一致 |
| `TestTailSilenceMarkedMissing` | 尾段静默被标成缺失：告警强制恢复、指标窗在最后实际点关闭、事件保持待定 |
| `TestSilenceCapTruncation` | 尾段静默超上限被截断：截断位置明确、再推进不重复不延伸 |
| `TestSilenceCapConfigurable` | 上限改大改小截断位置随之确定变化；未配置/负值回退有限默认上限；同配置重跑一致 |
| `TestMiddleGapCappedLikeTail` | **中途长时间断连空洞与尾段受同一上限**：补到最后点+cap 即止，悬空段无补点/指标窗，截断结论定位精确且带快照 |
| `TestMiddleGapOnlineEqualsReplay` | **中途长断连数据：在线逐包与批量重放的截断结论/告警事件/指标窗/事件逐条一致** |
| `TestTailAndMiddleBothCapped` | 一条流内中间空洞与采集尾段各自截断，位置明确不混淆 |
| `TestUnconfiguredCapIsBoundedAndReproducible` | **未配置上限不退化为无限补点**：回退 `DefaultSilenceCap`，重复运行逐条一致 |
| `TestCapDeterministicAcrossValues` | 截断位置随上限单调、确定性移动；同上限重放结论逐条一致 |
| `TestSilenceCapConcurrentRace` | 长空洞管线在 `-race` 并发下：计数守恒、无静默丢弃、截断结论结构与快照完整 |
| `TestAllConclusionsCarrySnapshot` | 告警事件/事件/生命周期迁移/指标窗/截断结论全部带换算参数+规则身份版本+上限值 |
| `TestConfigChangeDoesNotRewriteHistory` | **换算参数/规则/上限改版不改写历史；用当时版本（含当时 cap）重放逐条还原** |
| `TestConfigSnapshotPinnedAndReplay` | 结论携带机器可读配置快照；改版不改写历史；旧版本重放逐条还原 |

所有随机化均来自固定种子的确定性 LCG（仅用于制造固定的乱序与抖动），
不读取时间或环境，测试可重复运行并得到字节级一致的结论。

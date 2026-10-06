package pipeline

import (
	"sort"
	"sync"
	"time"
)

// Config 是管线总配置。
type Config struct {
	// Epoch 为时钟漂移模型基准时刻（取数据采集起点）。
	// 漂移是相对该时刻的一阶项，必须使用真实起点而非零时刻，否则漂移会被放大。
	Epoch      time.Time
	Channels   []ChannelSpec
	Clocks     map[DeviceID]ClockParams
	MaxDelay   time.Duration // 可接受的最大迟到
	SlotStep   time.Duration // 缺失填充/推进槽步长
	Quality    []QualityRule
	AlertRules []AlertRule
	// EpisodeRules 定义“持续异常算一次完整事件”的规则（可选）。
	EpisodeRules []EpisodeRule
	// SilenceCap 为所有静默（中间断连空洞 + 采集尾段）缺失填充的统一上限：
	//
	//   - 未配置（0）或任意非正值：采用 DefaultSilenceCap，绝不退化为
	//     “不设边界、一路补下去”——行为必须明确、有限、可复现；
	//   - >0：最多补到“最后实际点 + SilenceCap”。
	//
	// 只要静默跨度超过生效上限即截断，并记录一条 SilenceTruncation
	// 明确标出截断发生位置（见 Pipeline.Truncations）。
	// 生效值可通过 Pipeline.EffectiveSilenceCap() 查询。
	SilenceCap time.Duration
}

// Pipeline 串联时钟对齐、乱序缓冲、质量、指标、告警与事件管理。
//
// 处理顺序固定：Ingest（去重/对齐/校准/迟到拒绝）→
// Advance/Drain（按统一时间确定性排序）→ 质量分类（含缺失填充，中间空洞
// 与尾段静默共用同一 SilenceCap 上限）→
// 指标聚合（中断处关窗）→ 告警状态机（连续窗迟滞）→ 事件状态机（跨中断不拆分）。
// 同一输入序列无论在线逐包进入还是离线批量重放，产出完全一致。
type Pipeline struct {
	mu          sync.Mutex
	cfg         Config
	cap         time.Duration // 生效静默上限（已对非正值做默认值归一）
	clocks      *ClockRegistry
	buf         *ReorderBuffer
	classifier  *QualityClassifier
	metrics     *MetricAggregator
	engines     map[ChannelID][]*AlertEngine
	trackers    map[ChannelID][]*EpisodeTracker
	calByCh     map[ChannelID]Calibration
	qctx        map[ChannelID]qualityContext // 每通道构建快照所需上下文
	through     map[ChannelID]time.Time      // 各通道已评估到的统一时间
	lastPoint   map[ChannelID]AlignedPoint   // 各通道上一批最后实际点（跨批缺失检测）
	lastOK      map[ChannelID]bool
	events      []AlertEvent
	epEvents    []EpisodeEvent
	truncations []SilenceTruncation
	// truncSeen: 通道 -> 已记录截断结论的“最后实际点时刻”集合。
	// 同一静默段（中间空洞或尾段）只记录一次；新数据到达开启下一段后
	// 自然使用新的最后实际点时刻作为键，因此无需显式复位。
	truncSeen map[ChannelID]map[time.Time]bool
}

// NewPipeline 构建管线。
func NewPipeline(cfg Config) *Pipeline {
	// 漂移是相对基准时刻的一阶项：Epoch 取采集起点；未指定时退化为零基准。
	clocks := NewClockRegistry(cfg.Epoch)
	for d, p := range cfg.Clocks {
		clocks.SetClock(d, p)
	}
	cap := cfg.SilenceCap
	if cap <= 0 {
		cap = DefaultSilenceCap
	}
	p := &Pipeline{
		cfg:        cfg,
		cap:        cap,
		clocks:     clocks,
		buf:        NewReorderBuffer(clocks, cfg.Channels, cfg.MaxDelay),
		classifier: NewQualityClassifier(cfg.Quality),
		metrics:    NewMetricAggregator(),
		engines:    make(map[ChannelID][]*AlertEngine),
		trackers:   make(map[ChannelID][]*EpisodeTracker),
		calByCh:    make(map[ChannelID]Calibration),
		qctx:       make(map[ChannelID]qualityContext),
		through:    make(map[ChannelID]time.Time),
		lastPoint:  make(map[ChannelID]AlignedPoint),
		lastOK:     make(map[ChannelID]bool),
		truncSeen:  make(map[ChannelID]map[time.Time]bool),
	}
	qrByCh := make(map[ChannelID]QualityRule, len(cfg.Quality))
	for _, r := range cfg.Quality {
		qrByCh[r.Channel] = r
	}
	for _, s := range cfg.Channels {
		p.calByCh[s.Channel] = s.Cal
		p.qctx[s.Channel] = qualityContext{
			cal:        s.Cal,
			qr:         qrByCh[s.Channel],
			slotStep:   cfg.SlotStep,
			silenceCap: cap,
		}
	}
	for _, r := range cfg.AlertRules {
		e := NewAlertEngine(r)
		e.SetSnapshot(snapshotFromAlertRule(r, p.qctx[r.Channel]))
		p.engines[r.Channel] = append(p.engines[r.Channel], e)
	}
	for _, r := range cfg.EpisodeRules {
		tr := NewEpisodeTracker(r, snapshotFromEpisodeRule(r, p.qctx[r.Channel]))
		p.trackers[r.Channel] = append(p.trackers[r.Channel], tr)
	}
	return p
}

// EffectiveSilenceCap 返回实际生效的静默截断上限（配置非正值时为 DefaultSilenceCap）。
// 该值被固化进每条结论的配置快照；行为随该值确定性变化、可复现。
func (p *Pipeline) EffectiveSilenceCap() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cap
}

// Ingest 接收原始包，返回处置结论（拒绝原因可区分）。并发安全。
func (p *Pipeline) Ingest(s RawSample) IngestResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.buf.Ingest(s)
}

// Advance 排空所有统一时间 <= t 的点，逐通道分类、聚合指标、评估告警与事件，
// 返回该次推进关闭的指标窗与产生的告警事件。并发安全。
//
// 静默填充（中间空洞与尾段同一套语义、同一个 SilenceCap）：
//   - 中途断连再回来：两点之间的空洞按 SlotStep 补 QMissing，
//     最多补到“最后实际点 + SilenceCap”，超出即截断并给出结论；
//   - 采集尾段：对每个已有数据的通道，若 t 超过最后实际点 + 容差，
//     同样补 QMissing 到 cap 为止（尾段不会被误认为“一直正常”），
//     超过 cap 记录截断结论后不再向下补。
func (p *Pipeline) Advance(t time.Time) ([]MetricWindow, []AlertEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	points := p.buf.Drain(t)

	// 按通道分组（通道字典序），保证跨通道处理顺序确定。
	byCh := make(map[ChannelID][]AlignedPoint)
	for _, pt := range points {
		byCh[pt.Channel] = append(byCh[pt.Channel], pt)
	}
	channels := make([]ChannelID, 0, len(byCh))
	for ch := range byCh {
		channels = append(channels, ch)
	}
	sort.Slice(channels, func(i, j int) bool { return channels[i] < channels[j] })

	var windows []MetricWindow
	var events []AlertEvent
	var facts []gapTruncation

	for _, ch := range channels {
		pts := byCh[ch]
		verdicts, last, chTruncs := p.classifier.ClassifyStream(
			p.lastPoint[ch], p.lastOK[ch], pts, p.cfg.SlotStep, p.cap)
		if len(pts) > 0 {
			p.lastPoint[ch], p.lastOK[ch] = last, true
		}
		facts = append(facts, chTruncs...)
		windows, events = p.evalVerdicts(ch, verdicts, windows, events)
	}

	// 尾段静默填充：通道字典序，保证顺序确定。
	tailChs := make([]ChannelID, 0, len(p.lastPoint))
	for ch := range p.lastOK {
		if p.lastOK[ch] {
			tailChs = append(tailChs, ch)
		}
	}
	sort.Slice(tailChs, func(i, j int) bool { return tailChs[i] < tailChs[j] })
	for _, ch := range tailChs {
		last := p.lastPoint[ch]
		verdicts, fact := p.classifier.FillTail(last, t, p.cfg.SlotStep, p.cap)
		windows, events = p.evalVerdicts(ch, verdicts, windows, events)
		if fact != nil {
			facts = append(facts, *fact)
		}
	}

	// 截断结论统一落地：按通道字典序记录（同通道内事实已按时间序产生），
	// 并按 (LastDataTime, Channel) 全局排序，使在线逐包与批量重放的
	// 结论序列逐条一致（同一静默段在线先以尾段产生、重放以空洞产生，
	// 去重键为“通道+最后实际点时刻”，最终只保留一条）。
	sort.Slice(facts, func(i, j int) bool {
		if !facts[i].lastDataTime.Equal(facts[j].lastDataTime) {
			return facts[i].lastDataTime.Before(facts[j].lastDataTime)
		}
		return facts[i].channel < facts[j].channel
	})
	for _, f := range facts {
		p.recordTruncation(f)
	}
	return windows, events
}

// recordTruncation 对一个截断事实去重并落地带配置快照的完整结论。
func (p *Pipeline) recordTruncation(f gapTruncation) {
	seen := p.truncSeen[f.channel]
	if seen == nil {
		seen = make(map[time.Time]bool)
		p.truncSeen[f.channel] = seen
	}
	if seen[f.lastDataTime] {
		return
	}
	seen[f.lastDataTime] = true
	snap := snapshotFromQuality(p.qctx[f.channel])
	snap.RuleID = SilenceRuleID
	snap.RuleVersion = SilenceRuleVersion
	tr := SilenceTruncation{
		Channel:      f.channel,
		LastDataTime: f.lastDataTime,
		TruncatedAt:  f.truncatedAt,
		Reason: "silence exceeded effective cap " + p.cap.String() +
			" (applies to both middle disconnect gaps and end-of-stream tail); " +
			"missing-slot filling truncated at " +
			f.truncatedAt.Format(time.RFC3339Nano) +
			" (last real data at " + f.lastDataTime.Format(time.RFC3339Nano) + ")",
		Snapshot: snap,
	}
	p.truncations = append(p.truncations, tr)
}

// evalVerdicts 把一串质量判定依次送入指标、告警与事件状态机。
func (p *Pipeline) evalVerdicts(ch ChannelID, verdicts []QualityVerdict, windows []MetricWindow, events []AlertEvent) ([]MetricWindow, []AlertEvent) {
	for _, v := range verdicts {
		if !v.Time.After(p.through[ch]) {
			// 理论上不会发生（Drain 已严格排序），双保险防止重复评估。
			continue
		}
		p.through[ch] = v.Time
		if w := p.metrics.Append(v); w != nil {
			w.Cal = p.calByCh[ch]
			w.Snapshot = snapshotFromQuality(p.qctx[ch])
			windows = append(windows, *w)
		}
		for _, e := range p.engines[ch] {
			if ev := e.Evaluate(v); ev != nil {
				events = append(events, *ev)
				p.events = append(p.events, *ev)
			}
		}
		for _, tr := range p.trackers[ch] {
			p.epEvents = append(p.epEvents, tr.Observe(v)...)
		}
	}
	return windows, events
}

// Flush 关闭全部指标窗，返回剩余窗（不再产生新告警）。并发安全。
func (p *Pipeline) Flush() []MetricWindow {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.metrics.Flush("")
	for i := range out {
		out[i].Cal = p.calByCh[out[i].Channel]
		out[i].Snapshot = snapshotFromQuality(p.qctx[out[i].Channel])
	}
	return out
}

// Events 返回管线运行以来全部告警事件的快照（用于重放对账）。并发安全。
func (p *Pipeline) Events() []AlertEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]AlertEvent, len(p.events))
	copy(out, p.events)
	return out
}

// EpisodeEvents 返回全部事件生命周期迁移记录（按产生顺序，可按 EpisodeID 串联）。
// 并发安全。
func (p *Pipeline) EpisodeEvents() []EpisodeEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]EpisodeEvent, len(p.epEvents))
	copy(out, p.epEvents)
	return out
}

// Episodes 返回全部事件档案（含已结束与仍开放的），
// 按 (Start, ID) 确定性排序。并发安全。
func (p *Pipeline) Episodes() []Episode {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []Episode
	channels := make([]ChannelID, 0, len(p.trackers))
	for ch := range p.trackers {
		channels = append(channels, ch)
	}
	sort.Slice(channels, func(i, j int) bool { return channels[i] < channels[j] })
	for _, ch := range channels {
		for _, tr := range p.trackers[ch] {
			out = append(out, tr.Episodes()...)
			if ep, ok := tr.Open(); ok {
				out = append(out, ep)
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].Start.Equal(out[j].Start) {
			return out[i].Start.Before(out[j].Start)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// Truncations 返回全部“静默超上限被截断”的明确结论（中间空洞与尾段统一），
// 按 (LastDataTime, Channel) 确定性排序，每条带机器可读配置快照。并发安全。
func (p *Pipeline) Truncations() []SilenceTruncation {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]SilenceTruncation, len(p.truncations))
	copy(out, p.truncations)
	return out
}

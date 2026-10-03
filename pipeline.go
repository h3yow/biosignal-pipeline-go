package pipeline

import (
	"sort"
	"sync"
	"time"
)

// Config 是管线总配置。
type Config struct {
	// Version 为整套配置的版本标识，随每条结论固化进 ConfigSnapshot；
	// 配置改版后历史结论仍指向旧版本，可用旧版本配置重放复现。
	Version string
	// Epoch 为时钟漂移模型基准时刻（取数据采集起点）。
	// 漂移是相对该时刻的一阶项，必须使用真实起点而非零时刻，否则漂移会被放大。
	Epoch      time.Time
	Channels   []ChannelSpec
	Clocks     map[DeviceID]ClockParams
	MaxDelay   time.Duration // 可接受的最大迟到
	SlotStep   time.Duration // 缺失填充/推进槽步长
	Quality    []QualityRule
	AlertRules []AlertRule
	// MaxSilence 为静默缺失填充的上限。>0 时：
	// 任一静默跨度（点间空洞或最后数据之后的尾段）最多补 MaxSilence 时长的
	// 缺失槽，超出后在“静默起点+MaxSilence”处打 Truncated 截断标记，
	// 之后的静默不再展开（结论待定）。=0 关闭该语义（不补尾段、不限上限，
	// 与旧行为一致）。
	MaxSilence time.Duration
	// IncidentRecoverAfter 为关闭一次异常事件所需的连续正常时长；
	// <=0 表示首个正常槽即关闭。事件跟踪始终开启。
	IncidentRecoverAfter time.Duration
}

// AdvanceResult 是一次 AdvanceFull 的完整产出，全部机器可读。
type AdvanceResult struct {
	Windows   []MetricWindow       // 本次关闭的指标窗
	Events    []AlertEvent         // 本次产生的告警状态迁移
	Verdicts  []QualityVerdict     // 本次评估的全部质量判定（含缺失/截断槽，带配置快照）
	Incidents []IncidentTransition // 本次产生的事件阶段迁移
}

// Pipeline 串联时钟对齐、乱序缓冲、质量、指标与告警。
//
// 处理顺序固定：Ingest（去重/对齐/校准/迟到拒绝）→
// Advance/Drain（按统一时间确定性排序）→ 质量分类（含缺失填充）→
// 指标聚合（中断处关窗）→ 告警状态机（连续窗迟滞）。
// 同一输入序列无论在线逐包进入还是离线批量重放，产出完全一致。
type Pipeline struct {
	mu         sync.Mutex
	cfg        Config
	clocks     *ClockRegistry
	buf        *ReorderBuffer
	classifier *QualityClassifier
	metrics    *MetricAggregator
	engines    map[ChannelID][]*AlertEngine
	through    map[ChannelID]time.Time    // 各通道已评估到的统一时间
	lastPoint  map[ChannelID]AlignedPoint // 各通道上一批最后实际点（跨批缺失检测）
	lastOK     map[ChannelID]bool
	events     []AlertEvent
	snapshots  map[ChannelID]ConfigSnapshot // 各通道结论固化的配置快照
	trackers   map[ChannelID]*IncidentTracker
}

// NewPipeline 构建管线。
func NewPipeline(cfg Config) *Pipeline {
	// 漂移是相对基准时刻的一阶项：Epoch 取采集起点；未指定时退化为零基准。
	clocks := NewClockRegistry(cfg.Epoch)
	for d, p := range cfg.Clocks {
		clocks.SetClock(d, p)
	}
	p := &Pipeline{
		cfg:        cfg,
		clocks:     clocks,
		buf:        NewReorderBuffer(clocks, cfg.Channels, cfg.MaxDelay),
		classifier: NewQualityClassifier(cfg.Quality),
		metrics:    NewMetricAggregator(),
		engines:    make(map[ChannelID][]*AlertEngine),
		through:    make(map[ChannelID]time.Time),
		lastPoint:  make(map[ChannelID]AlignedPoint),
		lastOK:     make(map[ChannelID]bool),
		snapshots:  make(map[ChannelID]ConfigSnapshot),
		trackers:   make(map[ChannelID]*IncidentTracker),
	}
	qualityVer := make(map[ChannelID]string)
	for _, q := range cfg.Quality {
		qualityVer[q.Channel] = q.Version
	}
	for _, r := range cfg.AlertRules {
		p.engines[r.Channel] = append(p.engines[r.Channel], NewAlertEngine(r))
	}
	// 为每个已注册通道构建配置快照与事件跟踪器：快照在建管线时固化，
	// 之后改配置不影响已产出或将要产出的结论所携带的版本信息。
	for _, spec := range cfg.Channels {
		snap := ConfigSnapshot{
			ConfigVersion:      cfg.Version,
			Cal:                spec.Cal,
			QualityRuleVersion: qualityVer[spec.Channel],
			AlertRules:         make(map[string]string),
		}
		for _, r := range cfg.AlertRules {
			if r.Channel == spec.Channel {
				snap.AlertRules[r.ID] = r.Version
			}
		}
		p.snapshots[spec.Channel] = snap
		p.trackers[spec.Channel] = NewIncidentTracker(spec.Channel, cfg.IncidentRecoverAfter, snap)
	}
	return p
}

// Ingest 接收原始包，返回处置结论（拒绝原因可区分）。并发安全。
func (p *Pipeline) Ingest(s RawSample) IngestResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.buf.Ingest(s)
}

// Advance 排空所有统一时间 <= t 的点，逐通道分类、聚合指标、评估告警，
// 返回该次推进关闭的指标窗与产生的告警事件。并发安全。
//
// 需要质量判定明细与事件（Incident）迁移时请用 AdvanceFull。
func (p *Pipeline) Advance(t time.Time) ([]MetricWindow, []AlertEvent) {
	r := p.AdvanceFull(t)
	return r.Windows, r.Events
}

// AdvanceFull 同 Advance，但返回完整产出：指标窗、告警事件、
// 全部质量判定（含尾段静默与截断标记，均带配置快照）以及事件阶段迁移。并发安全。
//
// 尾段静默语义（MaxSilence>0 时）：对“最后实际点之后不再有数据”的通道，
// 按 SlotStep 补缺失槽，最多补到 最后实际点+MaxSilence；推进时间超过该上限时
// 在上限处打 Truncated 截断标记，之后不再展开。重复推进同一时间段不会产生
// 重复判定（按 through 水位去重），因此在线逐步推进与一次性重放结论一致。
func (p *Pipeline) AdvanceFull(t time.Time) AdvanceResult {
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

	var res AdvanceResult

	// process 统一处置一个质量判定：水位去重、固化快照、喂指标/告警/事件跟踪。
	process := func(v QualityVerdict) {
		if !v.Time.After(p.through[v.Channel]) {
			// 重复推进同一时间段（或迟到点）不重复评估。
			return
		}
		p.through[v.Channel] = v.Time
		v.Snapshot = p.snapshots[v.Channel]
		res.Verdicts = append(res.Verdicts, v)
		if w := p.metrics.Append(v); w != nil {
			w.Snapshot = p.snapshots[w.Channel]
			res.Windows = append(res.Windows, *w)
		}
		for _, e := range p.engines[v.Channel] {
			if ev := e.Evaluate(v); ev != nil {
				ev.Snapshot = p.snapshots[v.Channel]
				res.Events = append(res.Events, *ev)
				p.events = append(p.events, *ev)
			}
		}
		if tr := p.trackers[v.Channel]; tr != nil {
			res.Incidents = append(res.Incidents, tr.Observe(v)...)
		}
	}

	for _, ch := range channels {
		pts := byCh[ch]
		verdicts, last := p.classifier.ClassifyStreamLimit(
			p.lastPoint[ch], p.lastOK[ch], pts, p.cfg.SlotStep, p.cfg.MaxSilence)
		if len(pts) > 0 {
			p.lastPoint[ch], p.lastOK[ch] = last, true
		}
		for _, v := range verdicts {
			process(v)
		}
	}

	// 尾段静默：最后实际点之后到 t 之间没有数据的通道，显式标出缺失/截断，
	// 不让尾段看起来“一直正常”。仅 MaxSilence>0 时启用（有界，可配置）。
	if p.cfg.MaxSilence > 0 {
		tail := make([]ChannelID, 0, len(p.lastOK))
		for ch, ok := range p.lastOK {
			if ok {
				tail = append(tail, ch)
			}
		}
		sort.Slice(tail, func(i, j int) bool { return tail[i] < tail[j] })
		for _, ch := range tail {
			last := p.lastPoint[ch]
			for _, v := range p.classifier.FillSilence(
				last.Device, ch, last.Unit, last.Time, t, p.cfg.SlotStep, p.cfg.MaxSilence) {
				process(v)
			}
		}
	}
	return res
}

// Flush 关闭全部指标窗，返回剩余窗（不再产生新告警）。并发安全。
func (p *Pipeline) Flush() []MetricWindow {
	p.mu.Lock()
	defer p.mu.Unlock()
	ws := p.metrics.Flush("")
	for i := range ws {
		ws[i].Snapshot = p.snapshots[ws[i].Channel]
	}
	return ws
}

// Incidents 返回全部事件档案（已关闭的与进行中的），按通道与开始时间
// 确定性排序，供事后复盘按编号串联查询。并发安全。
func (p *Pipeline) Incidents() []Incident {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []Incident
	channels := make([]ChannelID, 0, len(p.trackers))
	for ch := range p.trackers {
		channels = append(channels, ch)
	}
	sort.Slice(channels, func(i, j int) bool { return channels[i] < channels[j] })
	for _, ch := range channels {
		out = append(out, p.trackers[ch].Incidents()...)
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

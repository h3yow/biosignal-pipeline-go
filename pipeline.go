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
	// SilenceCap 为尾段静默缺失填充的上限：
	// <=0 表示不设上限（推进到哪就补到哪）；
	// >0 时最多补到“最后实际点 + SilenceCap”，超出后记录一条
	// TailTruncation 明确结论（截断位置可见），之后不再向下补。
	SilenceCap time.Duration
}

// Pipeline 串联时钟对齐、乱序缓冲、质量、指标、告警与事件管理。
//
// 处理顺序固定：Ingest（去重/对齐/校准/迟到拒绝）→
// Advance/Drain（按统一时间确定性排序）→ 质量分类（含缺失填充与尾段静默填充）→
// 指标聚合（中断处关窗）→ 告警状态机（连续窗迟滞）→ 事件状态机（跨中断不拆分）。
// 同一输入序列无论在线逐包进入还是离线批量重放，产出完全一致。
type Pipeline struct {
	mu          sync.Mutex
	cfg         Config
	clocks      *ClockRegistry
	buf         *ReorderBuffer
	classifier  *QualityClassifier
	metrics     *MetricAggregator
	engines     map[ChannelID][]*AlertEngine
	trackers    map[ChannelID][]*EpisodeTracker
	calByCh     map[ChannelID]Calibration
	through     map[ChannelID]time.Time    // 各通道已评估到的统一时间
	lastPoint   map[ChannelID]AlignedPoint // 各通道上一批最后实际点（跨批缺失检测）
	lastOK      map[ChannelID]bool
	events      []AlertEvent
	epEvents    []EpisodeEvent
	truncations []TailTruncation
	truncated   map[ChannelID]bool // 当前静默段是否已记录截断（新数据到达后复位）
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
		trackers:   make(map[ChannelID][]*EpisodeTracker),
		calByCh:    make(map[ChannelID]Calibration),
		through:    make(map[ChannelID]time.Time),
		lastPoint:  make(map[ChannelID]AlignedPoint),
		lastOK:     make(map[ChannelID]bool),
		truncated:  make(map[ChannelID]bool),
	}
	for _, s := range cfg.Channels {
		p.calByCh[s.Channel] = s.Cal
	}
	for _, r := range cfg.AlertRules {
		e := NewAlertEngine(r)
		e.SetSnapshot(snapshotFromAlertRule(r, p.calByCh[r.Channel]))
		p.engines[r.Channel] = append(p.engines[r.Channel], e)
	}
	for _, r := range cfg.EpisodeRules {
		tr := NewEpisodeTracker(r, snapshotFromEpisodeRule(r, p.calByCh[r.Channel]))
		p.trackers[r.Channel] = append(p.trackers[r.Channel], tr)
	}
	return p
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
// 尾段静默：对每个已有数据的通道，若 t 超过最后实际点 + 容差，按 SlotStep
// 补 QMissing（最后一段不会被误认为“一直正常”）；补点上限为
// 最后实际点 + SilenceCap（SilenceCap>0 时），超过上限记录一次
// TailTruncation 后不再向下补。
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

	for _, ch := range channels {
		pts := byCh[ch]
		verdicts, last := p.classifier.ClassifyStream(
			p.lastPoint[ch], p.lastOK[ch], pts, p.cfg.SlotStep)
		if len(pts) > 0 {
			p.lastPoint[ch], p.lastOK[ch] = last, true
			p.truncated[ch] = false // 新数据到达：当前静默段结束，截断标记复位
		}
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
		until := t
		capped := false
		if p.cfg.SilenceCap > 0 {
			if capAt := last.Time.Add(p.cfg.SilenceCap); capAt.Before(t) {
				until = capAt
				capped = true
			}
		}
		verdicts := p.classifier.FillTail(last, until, p.cfg.SlotStep)
		windows, events = p.evalVerdicts(ch, verdicts, windows, events)
		if capped && !p.truncated[ch] {
			p.truncated[ch] = true
			p.truncations = append(p.truncations, TailTruncation{
				Channel:      ch,
				LastDataTime: last.Time,
				TruncatedAt:  last.Time.Add(p.cfg.SilenceCap),
				Reason: "silence exceeded configured cap " + p.cfg.SilenceCap.String() +
					"; tail filling truncated at " +
					last.Time.Add(p.cfg.SilenceCap).Format(time.RFC3339Nano),
			})
		}
	}
	return windows, events
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

// Truncations 返回全部“静默超上限被截断”的明确结论。并发安全。
func (p *Pipeline) Truncations() []TailTruncation {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]TailTruncation, len(p.truncations))
	copy(out, p.truncations)
	return out
}

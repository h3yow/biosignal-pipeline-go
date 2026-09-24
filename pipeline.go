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
	}
	for _, r := range cfg.AlertRules {
		p.engines[r.Channel] = append(p.engines[r.Channel], NewAlertEngine(r))
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
		}
		for _, v := range verdicts {
			if !v.Time.After(p.through[ch]) {
				// 理论上不会发生（Drain 已严格排序），双保险防止重复评估。
				continue
			}
			p.through[ch] = v.Time
			if w := p.metrics.Append(v); w != nil {
				windows = append(windows, *w)
			}
			for _, e := range p.engines[ch] {
				if ev := e.Evaluate(v); ev != nil {
					events = append(events, *ev)
					p.events = append(p.events, *ev)
				}
			}
		}
	}
	return windows, events
}

// Flush 关闭全部指标窗，返回剩余窗（不再产生新告警）。并发安全。
func (p *Pipeline) Flush() []MetricWindow {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.metrics.Flush("")
}

// Events 返回管线运行以来全部告警事件的快照（用于重放对账）。并发安全。
func (p *Pipeline) Events() []AlertEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]AlertEvent, len(p.events))
	copy(out, p.events)
	return out
}

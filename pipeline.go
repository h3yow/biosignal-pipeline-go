package pipeline

import (
	"sort"
	"time"
)

// Config 是管线总配置。
type Config struct {
	Aligner AlignerConfig
	Bucket  time.Duration // 质量评估网格宽度（0 = 1s）
	MaxGap  time.Duration // 中断判定阈值（0 = 2*Bucket）
	Metric  time.Duration // 指标翻滚窗宽（0 = 5s）
}

// Result 是管线一次输入/推进后产出的全部结论。
// 所有片段按其在主时间轴上被处理的确定顺序排列，可直接打印留痕。
type Result struct {
	Aligns  []AlignEvent  // 对齐结论：接受 / 重复 / 迟到
	Points  []JudgedPoint // 质量判定点（含合成的缺失点）
	Metrics []Metric      // 关闭窗口产出的派生指标
	Alarms  []AlarmEvent  // 告警触发/恢复事件
}

func (r *Result) append(o Result) {
	r.Aligns = append(r.Aligns, o.Aligns...)
	r.Points = append(r.Points, o.Points...)
	r.Metrics = append(r.Metrics, o.Metrics...)
	r.Alarms = append(r.Alarms, o.Alarms...)
}

// Pipeline 串联校准、对齐、质量、指标与告警。
//
// 全部公开方法以同一把互斥锁串行化内部状态，可被多设备多 goroutine
// 并发调用；由于下游处理本身只依赖“按接收时间排序的输入”，
// 并发结果与单线程确定性处理一致（测试以 -race 验证）。
type Pipeline struct {
	mu      *FakeMu
	cfg     Config
	cal     *CalibrationRegistry
	aligner *Aligner
	assess  *WindowAssessor
	acc     *SegmentAccumulator
	engine  *Engine

	// hintBySeq 保存每个已接受序号对应的设备原始提示，
	// 待该点从对齐缓冲有序释放时取回。
	hints    map[DeviceID]map[ChannelID]map[int64]RawHint
	channels map[DeviceID][]ChannelID
}

// New 创建管线。specs 给出通道质量规格，rules 给出告警规则。
func New(cfg Config, specs []ChannelSpec, rules []Rule) *Pipeline {
	cal := NewCalibrationRegistry()
	return &Pipeline{
		mu:       &FakeMu{},
		cfg:      cfg,
		cal:      cal,
		aligner:  NewAligner(cfg.Aligner, cal),
		assess:   NewWindowAssessor(NewClassifier(specs), AssessorConfig{BucketWidth: cfg.Bucket, MaxGap: cfg.MaxGap}),
		acc:      NewSegmentAccumulator(MetricsConfig{Window: cfg.Metric}),
		engine:   NewEngine(rules),
		hints:    make(map[DeviceID]map[ChannelID]map[int64]RawHint),
		channels: make(map[DeviceID][]ChannelID),
	}
}

// Calibration 暴露校准注册表，供启动期注册换算。
func (p *Pipeline) Calibration() *CalibrationRegistry { return p.cal }

func (p *Pipeline) rememberChannel(dev DeviceID, ch ChannelID) {
	chs, ok := p.channels[dev]
	if !ok {
		p.channels[dev] = []ChannelID{ch}
		return
	}
	for _, c := range chs {
		if c == ch {
			return
		}
	}
	p.channels[dev] = append(chs, ch)
}

func (p *Pipeline) storeHint(dev DeviceID, ch ChannelID, seq int64, h RawHint) {
	hm, ok := p.hints[dev]
	if !ok {
		hm = make(map[ChannelID]map[int64]RawHint)
		p.hints[dev] = hm
	}
	sm, ok := hm[ch]
	if !ok {
		sm = make(map[int64]RawHint)
		hm[ch] = sm
	}
	sm[seq] = h
}

func (p *Pipeline) takeHint(dev DeviceID, ch ChannelID, seq int64) RawHint {
	if hm, ok := p.hints[dev]; ok {
		if sm, ok := hm[ch]; ok {
			h := sm[seq]
			delete(sm, seq)
			return h
		}
	}
	return HintOK
}

// Push 接入一个原始采样包（可在多 goroutine 中并发调用）。
// 硬错误（无校准/未知量纲）通过 error 返回且与重复/迟到等正常
// 拒绝事件严格区分；后者出现在 Result.Aligns 中。
func (p *Pipeline) Push(s Sample) (Result, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	events, err := p.aligner.Push(s)
	if err != nil {
		return Result{}, err
	}
	var res Result
	accepted := false
	for _, ev := range events {
		if ev.Action == ActionAccept {
			accepted = true
		}
		res.Aligns = append(res.Aligns, ev)
	}
	if accepted {
		// 接受即登记通道与提示；提示要等点从重排缓冲有序释放时才取回，
		// 保证乱序点带着它自己的原始质量提示进入判定。
		p.rememberChannel(s.Device, s.Channel)
		p.storeHint(s.Device, s.Channel, s.Seq, s.Hint)
		for _, ev := range events {
			if ev.Action != ActionAccept {
				continue
			}
			hint := p.takeHint(ev.Sample.Device, ev.Sample.Channel, ev.Sample.Seq)
			res.append(p.feed(ev.Sample, hint))
		}
	}
	return res, nil
}

// feed 将一个已对齐的接受点送入质量、指标与告警阶段。
func (p *Pipeline) feed(s AlignedSample, hint RawHint) Result {
	var res Result
	for _, jp := range p.assess.Assess(s, hint) {
		res.Points = append(res.Points, jp)
		res.Metrics = append(res.Metrics, p.acc.Push(jp)...)
		res.Alarms = append(res.Alarms, p.engine.Evaluate(jp)...)
	}
	return res
}

// Advance 按主时间轴推进到 now，为所有已知设备/通道补齐到期但
// 没有采样的网格（MISSING）。在线模式由墙钟定时驱动；重放模式
// 传入数据覆盖的最大主时间戳，即可得到与在线一致的最终结论。
func (p *Pipeline) Advance(now time.Time) Result {
	p.mu.Lock()
	defer p.mu.Unlock()

	var res Result
	devs := make([]DeviceID, 0, len(p.channels))
	for d := range p.channels {
		devs = append(devs, d)
	}
	sort.Slice(devs, func(i, j int) bool { return devs[i] < devs[j] })
	for _, dev := range devs {
		chNames := append([]ChannelID(nil), p.channels[dev]...)
		sort.Slice(chNames, func(i, j int) bool { return chNames[i] < chNames[j] })
		for _, ch := range chNames {
			for _, jp := range p.assess.DrainMissing(dev, ch, now) {
				res.Points = append(res.Points, jp)
				res.Metrics = append(res.Metrics, p.acc.Push(jp)...)
				res.Alarms = append(res.Alarms, p.engine.Evaluate(jp)...)
			}
		}
	}
	return res
}

// Close 释放对齐缓冲并关闭未完成的指标窗口（断连/收尾）。
func (p *Pipeline) Close() Result {
	p.mu.Lock()
	defer p.mu.Unlock()

	var res Result
	for _, ev := range p.aligner.Flush() {
		res.Aligns = append(res.Aligns, ev)
		hint := p.takeHint(ev.Sample.Device, ev.Sample.Channel, ev.Sample.Seq)
		res.append(p.feed(ev.Sample, hint))
	}
	res.Metrics = append(res.Metrics, p.acc.Flush()...)
	return res
}

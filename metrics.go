package pipeline

import "time"

// MetricName 标识派生指标。
type MetricName string

// 内置派生指标名称。
const (
	MetricMean MetricName = "mean"
	MetricMin  MetricName = "min"
	MetricMax  MetricName = "max"
)

// Metric 是一个翻滚时间窗内的派生指标结论。
//
// Valid=false 表示该窗口结论不可信（数据缺失/信号不可用/数值异常/中断），
// 此时 Value 无意义，Reason 给出可区分、可断言的失效依据；
// 失效窗口仍然输出，是为了让“为什么没有指标”本身可解释、可复现，
// 而不是静默消失。
type Metric struct {
	Device  DeviceID
	Channel ChannelID
	Name    MetricName
	Window  [2]time.Time // 左闭右开 [Start, End)
	Value   float64
	Valid   bool
	Reason  ReasonCode
}

// MetricsConfig 配置指标聚合。
type MetricsConfig struct {
	Window time.Duration // 翻滚窗宽，按纪元整除对齐
}

type windowAgg struct {
	start   time.Time
	count   int
	good    int
	values  []float64
	sawGap  bool
	sawMiss bool
	sawUnav bool
	sawAbn  bool
}

type accState struct {
	cur *windowAgg
}

// SegmentAccumulator 按固定翻滚窗聚合指标。
//
// 不可用片段不会污染派生指标：UNAVAILABLE / MISSING / ABORMAL 点都不进入
// 数值聚合；GapAfter 中断边界一旦落在窗口内，窗口直接判失效，
// 因而任何指标都不可能跨越中断把两段采样拼出“连续”的虚假结果。
// 判定点流由 WindowAssessor 保证每个网格恰好一个点，因此窗口覆盖度
// 完全可校验，且只依赖时间戳整除，结果确定。
type SegmentAccumulator struct {
	cfg    MetricsConfig
	states map[DeviceID]map[ChannelID]*accState
}

// NewSegmentAccumulator 创建片段聚合器。
func NewSegmentAccumulator(cfg MetricsConfig) *SegmentAccumulator {
	if cfg.Window <= 0 {
		cfg.Window = 5 * time.Second
	}
	return &SegmentAccumulator{cfg: cfg, states: make(map[DeviceID]map[ChannelID]*accState)}
}

func (a *SegmentAccumulator) state(dev DeviceID, ch ChannelID, t time.Time) (*windowAgg, []Metric) {
	chs, ok := a.states[dev]
	if !ok {
		chs = make(map[ChannelID]*accState)
		a.states[dev] = chs
	}
	st, ok := chs[ch]
	if !ok {
		st = &accState{}
		chs[ch] = st
	}
	start := windowStart(t, a.cfg.Window)
	var closed []Metric
	if st.cur != nil && !st.cur.start.Equal(start) {
		closed = append(closed, a.finalize(dev, ch, st.cur)...)
		st.cur = nil
	}
	if st.cur == nil {
		st.cur = &windowAgg{start: start}
	}
	return st.cur, closed
}

// Push 接收一个判定点（时间有序），可能在窗口关闭时输出指标。
func (a *SegmentAccumulator) Push(p JudgedPoint) []Metric {
	cur, closed := a.state(p.Sample.Device, p.Sample.Channel, p.Sample.MasterTime)
	cur.count++
	switch p.Verdict.Class {
	case QualityGood:
		cur.good++
		cur.values = append(cur.values, p.Sample.Value)
	case QualityMissing:
		cur.sawMiss = true
	case QualityUnavailable:
		cur.sawUnav = true
	case QualityAbnormal:
		cur.sawAbn = true
	}
	if p.GapAfter {
		cur.sawGap = true
	}
	return closed
}

// Flush 关闭并输出所有通道当前窗口的结论（收尾/断连时使用）。
// 遍历顺序固定（设备、通道、指标均排序），保证输出可复现。
func (a *SegmentAccumulator) Flush() []Metric {
	var out []Metric
	devs := sortedKeys(a.states)
	for _, dev := range devs {
		chs := a.states[dev]
		names := make([]ChannelID, 0, len(chs))
		for ch := range chs {
			names = append(names, ch)
		}
		for _, ch := range names {
			st := chs[ch]
			if st.cur != nil {
				out = append(out, a.finalize(dev, ch, st.cur)...)
				st.cur = nil
			}
		}
	}
	return out
}

// finalize 汇总一个窗口。失效原因按固定优先级选取，保证可复现：
// 中断 > 缺失 > 不可用 > 数值异常。
func (a *SegmentAccumulator) finalize(dev DeviceID, ch ChannelID, w *windowAgg) []Metric {
	win := [2]time.Time{w.start, w.start.Add(a.cfg.Window)}
	reason := ReasonCode("")
	switch {
	case w.sawGap:
		reason = ReasonGapInterrupted
	case w.sawMiss:
		reason = ReasonMissingWindow
	case w.sawUnav:
		reason = ReasonUnavailableInputs
	case w.sawAbn:
		reason = ReasonValueOutOfRange
	}
	emit := func(name MetricName, v float64, valid bool) Metric {
		m := Metric{Device: dev, Channel: ch, Name: name, Window: win, Valid: valid, Reason: reason}
		if valid {
			m.Reason = ReasonAccepted
			m.Value = v
		}
		return m
	}
	if reason != "" || len(w.values) == 0 {
		if reason == "" {
			reason = ReasonMissingWindow
		}
		return []Metric{
			emit(MetricMean, 0, false),
			emit(MetricMin, 0, false),
			emit(MetricMax, 0, false),
		}
	}
	mn, mx := w.values[0], w.values[0]
	sum := 0.0
	for _, v := range w.values {
		sum += v
		if v < mn {
			mn = v
		}
		if v > mx {
			mx = v
		}
	}
	return []Metric{
		emit(MetricMean, sum/float64(len(w.values)), true),
		emit(MetricMin, mn, true),
		emit(MetricMax, mx, true),
	}
}

func windowStart(t time.Time, w time.Duration) time.Time {
	n := t.UnixNano() / int64(w)
	return time.Unix(0, n*int64(w))
}

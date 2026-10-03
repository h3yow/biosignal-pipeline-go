package pipeline

import (
	"sort"
	"time"
)

// MetricWindow 是一个连续、未跨越中断边界的有效数据窗。
type MetricWindow struct {
	Channel ChannelID
	Start   time.Time
	End     time.Time
	Count   int
	Mean    float64
	Min     float64
	Max     float64
	// AbnormalCount 为窗内“数值异常但信号可信”的点数；
	// 异常值参与统计并被显式计数，与缺失/不可用严格区分。
	AbnormalCount int
	// GapBefore 为该窗与上一个窗起点（或流起点）之间的中断间隔。
	GapBefore time.Duration
	// Snapshot 固化产出该窗时的换算参数与规则版本（机器可读）。
	Snapshot ConfigSnapshot
}

type metricAcc struct {
	channel       ChannelID
	start, end    time.Time
	count         int
	abnormal      int
	sum, min, max float64
	lastTime      time.Time
	lastEnd       time.Time // 上一窗结束时间，用于计算 GapBefore
	gapBefore     time.Duration
	open          bool
}

// MetricAggregator 在不跨越缺失/不可用中断的前提下聚合派生指标。
//
// 规则：
//   - QGood / QValueAbnormal 计入当前窗（异常值参与统计且单独计数）。
//   - QMissing / QArtifactUnusable 关闭当前窗：指标绝不跨越中断边界拼接，
//     中断之后的有效数据开启新窗，新窗记录 GapBefore。
//   - 各通道状态独立，输入须按各通道时间升序。
type MetricAggregator struct {
	acc map[ChannelID]*metricAcc
}

// NewMetricAggregator 创建指标聚合器。
func NewMetricAggregator() *MetricAggregator {
	return &MetricAggregator{acc: make(map[ChannelID]*metricAcc)}
}

// Append 输入一个质量判定；当该判定导致旧窗关闭时返回该窗，否则返回 nil。
func (a *MetricAggregator) Append(v QualityVerdict) *MetricWindow {
	m := a.acc[v.Channel]
	if m == nil {
		m = &metricAcc{}
		a.acc[v.Channel] = m
	}
	if v.Class == QMissing || v.Class == QArtifactUnusable {
		w := m.close()
		if w != nil {
			m.lastEnd = w.End
		}
		m.lastTime = v.Time
		return w
	}
	if !m.open {
		gap := time.Duration(0)
		if !m.lastEnd.IsZero() {
			gap = v.Time.Sub(m.lastEnd)
		}
		m.channel = v.Channel
		m.start = v.Time
		m.end = v.Time
		m.count = 1
		m.abnormal = 0
		m.sum = v.Value
		m.min, m.max = v.Value, v.Value
		if v.Class == QValueAbnormal {
			m.abnormal = 1
		}
		m.open = true
		m.gapBefore = gap
	} else {
		m.count++
		m.sum += v.Value
		m.end = v.Time
		if v.Value < m.min {
			m.min = v.Value
		}
		if v.Value > m.max {
			m.max = v.Value
		}
		if v.Class == QValueAbnormal {
			m.abnormal++
		}
	}
	m.lastTime = v.Time
	return nil
}

// Flush 关闭并返回指定通道（空字符串表示全部通道）尚未关闭的窗。
// 全量 Flush 按通道字典序输出，保证多次运行结果逐位一致。
func (a *MetricAggregator) Flush(channel ChannelID) []MetricWindow {
	var out []MetricWindow
	if channel != "" {
		if m := a.acc[channel]; m != nil {
			if w := m.close(); w != nil {
				out = append(out, *w)
			}
		}
		return out
	}
	channels := make([]ChannelID, 0, len(a.acc))
	for ch := range a.acc {
		channels = append(channels, ch)
	}
	sort.Slice(channels, func(i, j int) bool { return channels[i] < channels[j] })
	for _, ch := range channels {
		if w := a.acc[ch].close(); w != nil {
			out = append(out, *w)
		}
	}
	return out
}

func (m *metricAcc) close() *MetricWindow {
	if !m.open || m.count == 0 {
		m.open = false
		return nil
	}
	w := &MetricWindow{
		Channel:       m.channel,
		Start:         m.start,
		End:           m.end,
		Count:         m.count,
		Mean:          m.sum / float64(m.count),
		Min:           m.min,
		Max:           m.max,
		AbnormalCount: m.abnormal,
		GapBefore:     m.gapBefore,
	}
	m.open = false
	return w
}

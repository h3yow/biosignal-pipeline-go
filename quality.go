package pipeline

import "time"

// QualityRule 定义一个通道的质量判定规则。
type QualityRule struct {
	// ID/Version 为质量判定规则的身份与版本，随结论快照输出，
	// 支持按版本重放、还原历史结论。未配置时为空（仍可正常判定）。
	ID      string
	Version string
	// Channel 该规则适用的通道。
	Channel ChannelID
	// ExpectedInterval 为该通道标称采样周期；
	// 相邻点间隔超过 ExpectedInterval+GapTolerance 即判定中间存在缺失。
	ExpectedInterval time.Duration
	// GapTolerance 允许的抖动容差。
	GapTolerance time.Duration
	// PhysMin/PhysMax 为可信生理数值范围（闭区间）。
	// 超出判 QValueAbnormal：信号可信但数值异常；缺失与不可用优先于数值判定。
	PhysMin, PhysMax float64
}

// QualityClassifier 对对齐点流做质量分类。
type QualityClassifier struct {
	rules map[ChannelID]QualityRule
}

// NewQualityClassifier 创建质量分类器。
func NewQualityClassifier(rules []QualityRule) *QualityClassifier {
	c := &QualityClassifier{rules: make(map[ChannelID]QualityRule)}
	for _, r := range rules {
		c.rules[r.Channel] = r
	}
	return c
}

// Classify 对单个对齐点做基于标志与数值的分类（不做缺失判定）。
//
// 语义优先级：
//  1. 传感器脱落/饱和/导联脱落等硬件标志 → QArtifactUnusable（有数据但不可信）。
//  2. 校准值超出可信生理范围 → QValueAbnormal（信号可信、数值异常）。
//  3. 其余 → QGood。
func (c *QualityClassifier) Classify(p AlignedPoint) QualityVerdict {
	v := QualityVerdict{
		Device:  p.Device,
		Channel: p.Channel,
		Time:    p.Time,
		Present: true,
		Value:   p.Value,
		Unit:    p.Unit,
		Class:   QGood,
	}
	switch {
	case p.Flags&FlagSensorOff != 0:
		v.Class = QArtifactUnusable
		v.Reasons = append(v.Reasons, "sensor-off flag reported by device")
	case p.Flags&FlagSaturation != 0:
		v.Class = QArtifactUnusable
		v.Reasons = append(v.Reasons, "ADC saturation flag; waveform clipped")
	case p.Flags&FlagLeadsOff != 0:
		v.Class = QArtifactUnusable
		v.Reasons = append(v.Reasons, "leads-off flag; signal electrically floating")
	}
	if v.Class != QGood {
		return v
	}
	if r, ok := c.rules[p.Channel]; ok {
		if p.Value < r.PhysMin {
			v.Class = QValueAbnormal
			v.Reasons = append(v.Reasons, "value "+formatFloat(p.Value)+
				" below credible minimum "+formatFloat(r.PhysMin))
		} else if p.Value > r.PhysMax {
			v.Class = QValueAbnormal
			v.Reasons = append(v.Reasons, "value "+formatFloat(p.Value)+
				" above credible maximum "+formatFloat(r.PhysMax))
		}
	}
	if v.Class == QGood {
		v.Reasons = append(v.Reasons, "no artifact flags and value within credible range")
	}
	return v
}

// ClassifyStream 对一个通道按统一时间升序的对齐点序列分类，
// 并在超过容差的间隔内按 step 槽补 QMissing 判定。
//
// prev 为上一批次最后一个实际点（跨批次的缺失检测靠它衔接），
// 无前置点时传零值并用 ok=false 标识。
// 返回该批次结束后应记住的“最后实际点”（points 为空时保持 prev）。
//
// 缺失槽时间戳从“上一个实际点 + step”开始，严格早于下一个实际点，
// 且不超过“上一个实际点 + cap”（与尾段静默同一上限）：
// 中途长时间断连再回来留下的空洞同样在 cap 处截断，跨度再大也不会
// 按原采样密度无限补下去；cap 截断时返回一条截断事实，由编排层
// 附加配置快照与可读原因。缺失只填真正的时间空洞，不修改实际点，
// 保证后续指标计算不会跨越空洞拼接。
func (c *QualityClassifier) ClassifyStream(prev AlignedPoint, prevOK bool, points []AlignedPoint, step, cap time.Duration) ([]QualityVerdict, AlignedPoint, []gapTruncation) {
	out := make([]QualityVerdict, 0, len(points))
	var truncs []gapTruncation
	for _, p := range points {
		if prevOK {
			gap := p.Time.Sub(prev.Time)
			// 空洞内补点：上限 prev+cap（含截断点槽），且严格早于下一实际点。
			out = append(out, c.fillMissing(prev, prev.Time.Add(cap), p.Time, step, cap)...)
			if f := c.truncationFor(prev, gap, cap); f != nil {
				truncs = append(truncs, *f)
			}
		}
		out = append(out, c.Classify(p))
		prev, prevOK = p, true
	}
	return out, prev, truncs
}

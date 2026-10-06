package pipeline

import "time"

// QualityRule 定义一个通道的质量判定规则。
// ID/Version 标识规则身份与版本，随结论快照输出，支持配置改版后的历史复现。
type QualityRule struct {
	ID      string
	Version string
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

// Rule 返回通道注册的质量规则（供管线构建结论快照）。
func (c *QualityClassifier) Rule(ch ChannelID) (QualityRule, bool) {
	r, ok := c.rules[ch]
	return r, ok
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
// 返回该批次结束后应记住的“最后实际点”（points 为空时保持 prev），
// 以及本批次内被上限截断的空洞结论（未截断时为空）。
//
// 缺失槽时间戳从“上一个实际点 + step”开始，严格早于下一个实际点；
// 若 step<=0 则使用规则的 ExpectedInterval。缺失只填真正的时间空洞，
// 不修改实际点，保证后续指标计算不会跨越空洞拼接。
//
// 中间空洞与尾段静默适用同一个上限 cap（cap>0 时）：空洞跨度超过
// cap 时，填充在 prev.Time+cap 处截断，并返回一条 Kind=TruncationMidGap
// 的 SilenceTruncation（截断位置与空洞结束位置明确标出）；
// cap<=0 表示调用方未设上限，不做截断（管线层会解析为默认上限，
// 不会以未设边界的形态调用）。
func (c *QualityClassifier) ClassifyStream(prev AlignedPoint, prevOK bool, points []AlignedPoint, step, cap time.Duration) ([]QualityVerdict, AlignedPoint, []SilenceTruncation) {
	out := make([]QualityVerdict, 0, len(points))
	var truncs []SilenceTruncation
	for _, p := range points {
		if prevOK {
			fill, tr := c.fillGap(prev, p, step, cap)
			out = append(out, fill...)
			if tr != nil {
				truncs = append(truncs, *tr)
			}
		}
		out = append(out, c.Classify(p))
		prev, prevOK = p, true
	}
	return out, prev, truncs
}

// fillGap 填充 prev 与 next 之间的缺失槽；cap>0 时填充最多到
// prev.Time+cap，若该位置早于 next（空洞超长）则截断并返回截断结论。
func (c *QualityClassifier) fillGap(prev, next AlignedPoint, step, cap time.Duration) ([]QualityVerdict, *SilenceTruncation) {
	r, ok := c.rules[next.Channel]
	if !ok {
		return nil, nil
	}
	if step <= 0 {
		step = r.ExpectedInterval
	}
	gap := next.Time.Sub(prev.Time)
	if gap <= r.ExpectedInterval+r.GapTolerance {
		return nil, nil
	}
	// 截断位置：空洞跨度超过上限时，填充到 prev.Time+cap 为止
	// （含该槽位，与尾段 FillTail 的边界语义一致）。
	var trunc *SilenceTruncation
	limit := next.Time
	inclusive := false
	if cap > 0 {
		if capAt := prev.Time.Add(cap); capAt.Before(next.Time) {
			limit = capAt
			inclusive = true
			trunc = &SilenceTruncation{
				Channel:      next.Channel,
				Kind:         TruncationMidGap,
				LastDataTime: prev.Time,
				TruncatedAt:  capAt,
				GapEnd:       next.Time,
				Reason: "mid-stream gap " + gap.String() + " exceeded silence cap " +
					cap.String() + "; missing fill truncated at " +
					capAt.Format(time.RFC3339Nano) + ", gap ends at " +
					next.Time.Format(time.RFC3339Nano),
			}
		}
	}
	missing := make([]QualityVerdict, 0)
	for t := prev.Time.Add(step); ; t = t.Add(step) {
		if inclusive {
			if t.After(limit) {
				break
			}
		} else if !t.Before(limit) {
			break
		}
		missing = append(missing, QualityVerdict{
			Device:  next.Device,
			Channel: next.Channel,
			Time:    t,
			Class:   QMissing,
			Present: false,
			Unit:    next.Unit,
			Reasons: []string{"no sample in interval; gap " + gap.String() +
				" exceeds expected " + r.ExpectedInterval.String() +
				" + tolerance " + r.GapTolerance.String()},
		})
	}
	return missing, trunc
}

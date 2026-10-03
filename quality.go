package pipeline

import "time"

// QualityRule 定义一个通道的质量判定规则。
type QualityRule struct {
	Channel ChannelID
	// Version 为该规则的版本标识，随结论固化进 ConfigSnapshot，
	// 规则改版后历史结论仍指向旧版本，可用旧版本重放复现。
	Version string
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
// 缺失槽时间戳从“上一个实际点 + step”开始，严格早于下一个实际点；
// 若 step<=0 则使用规则的 ExpectedInterval。缺失只填真正的时间空洞，
// 不修改实际点，保证后续指标计算不会跨越空洞拼接。
func (c *QualityClassifier) ClassifyStream(prev AlignedPoint, prevOK bool, points []AlignedPoint, step time.Duration) ([]QualityVerdict, AlignedPoint) {
	return c.ClassifyStreamLimit(prev, prevOK, points, step, 0)
}

// ClassifyStreamLimit 同 ClassifyStream，但 maxSilence>0 时任一静默跨度
// （两个实际点之间的空洞）最多只补 maxSilence 时长的缺失槽；
// 空洞延伸超过该上限时，在“空洞起点+maxSilence”处追加一个 Truncated
// 标记槽，之后的静默不再逐槽展开（结论待定）。
func (c *QualityClassifier) ClassifyStreamLimit(prev AlignedPoint, prevOK bool, points []AlignedPoint, step, maxSilence time.Duration) ([]QualityVerdict, AlignedPoint) {
	out := make([]QualityVerdict, 0, len(points))
	for _, p := range points {
		if prevOK {
			out = append(out, c.fillGap(prev, p, step, maxSilence)...)
		}
		out = append(out, c.Classify(p))
		prev, prevOK = p, true
	}
	return out, prev
}

// FillSilence 为“最后实际点之后不再有数据”的尾段静默生成缺失判定：
// 槽从 from+step 到 to（含 to），仅当静默超过 ExpectedInterval+GapTolerance；
// maxSilence>0 时最多补到 from+maxSilence，静默延伸超过上限时在该处
// 追加 Truncated 标记槽。无规则通道返回 nil（与 fillGap 一致）。
func (c *QualityClassifier) FillSilence(dev DeviceID, ch ChannelID, unit Unit, from, to time.Time, step, maxSilence time.Duration) []QualityVerdict {
	r, ok := c.rules[ch]
	if !ok {
		return nil
	}
	if step <= 0 {
		step = r.ExpectedInterval
	}
	silence := to.Sub(from)
	if silence <= r.ExpectedInterval+r.GapTolerance {
		return nil
	}
	limit := to
	capped := false
	if maxSilence > 0 {
		if mf := from.Add(maxSilence); mf.Before(limit) {
			limit = mf
			capped = true
		}
	}
	var out []QualityVerdict
	if capped {
		for t := from.Add(step); t.Before(limit); t = t.Add(step) {
			out = append(out, c.missingSlot(dev, ch, unit, t, silence, r))
		}
		out = append(out, truncationSlot(dev, ch, unit, limit, maxSilence))
	} else {
		for t := from.Add(step); !t.After(limit); t = t.Add(step) {
			out = append(out, c.missingSlot(dev, ch, unit, t, silence, r))
		}
	}
	return out
}

func (c *QualityClassifier) fillGap(prev, next AlignedPoint, step, maxSilence time.Duration) []QualityVerdict {
	r, ok := c.rules[next.Channel]
	if !ok {
		return nil
	}
	if step <= 0 {
		step = r.ExpectedInterval
	}
	gap := next.Time.Sub(prev.Time)
	if gap <= r.ExpectedInterval+r.GapTolerance {
		return nil
	}
	limit := next.Time
	capped := false
	if maxSilence > 0 {
		if mf := prev.Time.Add(maxSilence); mf.Before(limit) {
			limit = mf
			capped = true
		}
	}
	missing := make([]QualityVerdict, 0)
	for t := prev.Time.Add(step); t.Before(limit); t = t.Add(step) {
		missing = append(missing, c.missingSlot(prev.Device, next.Channel, next.Unit, t, gap, r))
	}
	if capped {
		missing = append(missing, truncationSlot(prev.Device, next.Channel, next.Unit, limit, maxSilence))
	}
	return missing
}

func (c *QualityClassifier) missingSlot(dev DeviceID, ch ChannelID, unit Unit, t time.Time, gap time.Duration, r QualityRule) QualityVerdict {
	return QualityVerdict{
		Device:  dev,
		Channel: ch,
		Time:    t,
		Class:   QMissing,
		Present: false,
		Unit:    unit,
		Reasons: []string{"no sample in interval; gap " + gap.String() +
			" exceeds expected " + r.ExpectedInterval.String() +
			" + tolerance " + r.GapTolerance.String()},
	}
}

// truncationSlot 生成“静默超上限截断”标记槽：机器可读（Truncated=true），
// 位置即截断发生点，之后的静默不再展开，结论待定。
func truncationSlot(dev DeviceID, ch ChannelID, unit Unit, at time.Time, maxSilence time.Duration) QualityVerdict {
	return QualityVerdict{
		Device:    dev,
		Channel:   ch,
		Time:      at,
		Class:     QMissing,
		Present:   false,
		Unit:      unit,
		Truncated: true,
		Reasons: []string{"silence exceeded MaxSilence " + maxSilence.String() +
			"; filling truncated at " + at.Format(time.RFC3339Nano) +
			"; further silence not expanded, conclusion pending"},
	}
}

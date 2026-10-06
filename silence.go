package pipeline

import "time"

// TruncationKind 区分截断发生在哪一类静默段上。
type TruncationKind uint8

const (
	// TruncationTail 采集结束后的尾段静默（最后实际点之后再无数据）。
	TruncationTail TruncationKind = iota
	// TruncationMidGap 流中间的空洞（设备摘下/丢包一段时间后重连，
	// 空洞由前后两个实际点夹出）。
	TruncationMidGap
)

// SilenceTruncation 是“静默超过上限被截断”的明确结论，可查询、可复现。
//
// 所有长静默适用同一个上限：自 LastDataTime 起缺失填充最多补到
// TruncatedAt（= LastDataTime + 生效上限），之后不再补点。
//   - 尾段：Kind=TruncationTail，GapEnd 为零值（空洞尚未结束）；
//   - 中间空洞：Kind=TruncationMidGap，GapEnd 为空洞结束后的第一个实际点
//     时刻（即重连点），截断位置同样由此记录明确标出。
//
// Snapshot 固化产生该结论时的质量规则身份/版本、换算参数与实际生效的
// 上限值（机器可读）；配置改版后历史结论不被改写，用当时的版本与上限
// 重放同一批数据可逐条还原。
type SilenceTruncation struct {
	Channel      ChannelID
	Kind         TruncationKind
	LastDataTime time.Time
	TruncatedAt  time.Time
	// GapEnd 仅 MidGap 有效：空洞之后第一个实际点的时刻。
	GapEnd   time.Time
	Reason   string
	Snapshot ConfigSnapshot
}

// FillTail 为“最后一个实际点之后、直到 until”的尾段静默生成 QMissing 判定。
//
// 语义与 ClassifyStream 的内部缺失填充一致：
//   - 仅当 until 超过 prev.Time + ExpectedInterval + GapTolerance 才补；
//   - 槽位从 prev.Time + step 开始、不超过 until；
//   - 不修改实际点，只标记“该有数据但没有”。
//
// until 由调用方按 SilenceCap 截断（见 Pipeline.Advance），
// 因此本函数本身不需要知道上限配置。
func (c *QualityClassifier) FillTail(prev AlignedPoint, until time.Time, step time.Duration) []QualityVerdict {
	r, ok := c.rules[prev.Channel]
	if !ok {
		return nil
	}
	if step <= 0 {
		step = r.ExpectedInterval
	}
	if until.Sub(prev.Time) <= r.ExpectedInterval+r.GapTolerance {
		return nil
	}
	missing := make([]QualityVerdict, 0)
	for t := prev.Time.Add(step); !t.After(until); t = t.Add(step) {
		missing = append(missing, QualityVerdict{
			Device:  prev.Device,
			Channel: prev.Channel,
			Time:    t,
			Class:   QMissing,
			Present: false,
			Unit:    prev.Unit,
			Reasons: []string{"no sample since last point; tail silence " +
				until.Sub(prev.Time).String() + " exceeds expected " +
				r.ExpectedInterval.String() + " + tolerance " + r.GapTolerance.String()},
		})
	}
	return missing
}

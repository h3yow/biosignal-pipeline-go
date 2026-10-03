package pipeline

import "time"

// TailTruncation 是“静默超过上限被截断”的明确结论：
// 某通道自 LastDataTime 后再无数据，缺失填充最多补到 TruncatedAt
// （= LastDataTime + SilenceCap），之后的时间轴不再补点，
// 该截断位置由此记录明确标出，可查询、可复现。
type TailTruncation struct {
	Channel      ChannelID
	LastDataTime time.Time
	TruncatedAt  time.Time
	Reason       string
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

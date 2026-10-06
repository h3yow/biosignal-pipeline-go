package pipeline

import "time"

const (
	// DefaultSilenceCap 是使用方未配置（或配置为任意非正值）静默上限时的生效值。
	// 任何静默都必须有明确、有限、可复现的边界：未配置不得退化为“一路补下去”。
	DefaultSilenceCap = 2 * time.Minute
	// SilenceRuleID / SilenceRuleVersion 是内建静默截断策略的身份与版本，
	// 随每条截断结论的配置快照输出（机器可读，可用于按版本重放）。
	SilenceRuleID      = "builtin.silence-truncation"
	SilenceRuleVersion = "v1"
)

// SilenceTruncation 是“静默超过上限被截断”的明确结论。
//
// 同一种结论同时覆盖两类长静默，二者受同一个 SilenceCap 约束、字段完全一致：
//   - 设备中途摘下/丢包很久后又连回：两点之间的中间空洞；
//   - 采集彻底结束后的尾段静默。
//
// 某通道自 LastDataTime 后再无数据，缺失填充最多补到 TruncatedAt
// （= LastDataTime + 生效 SilenceCap），TruncatedAt 之后的时间轴不再补点；
// 截断位置（含是哪个通道、最后数据在何时）由本结论明确标出，
// 可查询、可导出、可复现。Snapshot 固化产生该结论时的换算参数、
// 截断策略身份/版本与生效上限值。
//
// 结论只描述数据本身（最后实际点与截断位置），不依赖“之后是否又连回来”，
// 因此在线逐包（空洞先按尾段补、重连后补齐）与批量重放（一次性补齐）
// 对同一静默段产出逐字段相同的一条结论。
type SilenceTruncation struct {
	Channel      ChannelID
	LastDataTime time.Time
	TruncatedAt  time.Time
	Reason       string
	// Snapshot 固化产生该截断结论时生效的配置（机器可读）。
	Snapshot ConfigSnapshot
}

// TailTruncation 是旧名称的别名，保持既有调用方源码兼容。
type TailTruncation = SilenceTruncation

// gapTruncation 是质量分类器在发现“静默跨度超过上限”时给出的截断事实：
// 只含定位信息，配置快照与可读原因由编排层统一附加，
// 保证中间空洞与尾段两条路径产出的结论逐字段一致。
type gapTruncation struct {
	channel      ChannelID
	lastDataTime time.Time
	truncatedAt  time.Time
}

// fillMissing 为静默段生成 QMissing 槽位，中间空洞与尾段共用同一实现：
//
//   - limit：含端点的外边界。尾段传 min(推进水位, prev+cap)；
//     中间空洞传 prev+cap（截断点本身仍补一条“此处已无数据”的槽）。
//   - nextReal：下一实际点时刻（零值表示尾段、无下一实际点）。
//     中间空洞时槽位严格早于该点，绝不占用真实点自己的槽。
//
// 因此当 cap 生效（prev+cap 早于下一实际点）时，中间空洞与“在线逐包时
// 先按尾段补到同一水位”补出的槽位集合完全相同（都含 prev+cap 截断槽），
// 保证在线与批量重放逐槽一致。
func (c *QualityClassifier) fillMissing(prev AlignedPoint, limit, nextReal time.Time, step, cap time.Duration) []QualityVerdict {
	r, ok := c.rules[prev.Channel]
	if !ok {
		return nil
	}
	if step <= 0 {
		step = r.ExpectedInterval
	}
	if capAt := prev.Time.Add(cap); capAt.Before(limit) {
		limit = capAt
	}
	end := limit
	if !nextReal.IsZero() && nextReal.Before(end) {
		end = nextReal
	}
	if end.Sub(prev.Time) <= r.ExpectedInterval+r.GapTolerance {
		return nil
	}
	missing := make([]QualityVerdict, 0)
	for t := prev.Time.Add(step); !t.After(limit); t = t.Add(step) {
		if !nextReal.IsZero() && !t.Before(nextReal) {
			break // 不占用下一实际点的槽
		}
		missing = append(missing, QualityVerdict{
			Device:  prev.Device,
			Channel: prev.Channel,
			Time:    t,
			Class:   QMissing,
			Present: false,
			Unit:    prev.Unit,
			Reasons: []string{"no sample in interval; no data since last real point at " +
				prev.Time.Format(time.RFC3339Nano) + "; silence exceeds expected " +
				r.ExpectedInterval.String() + " + tolerance " + r.GapTolerance.String()},
		})
	}
	return missing
}

// truncationFor 在静默跨度超过 cap 时给出截断事实，否则返回 nil。
// span 为“从最后实际点到下一实际点（中间空洞）或推进水位（尾段）”的跨度。
func (c *QualityClassifier) truncationFor(prev AlignedPoint, span, cap time.Duration) *gapTruncation {
	if _, ok := c.rules[prev.Channel]; !ok {
		return nil
	}
	if span <= cap {
		return nil
	}
	return &gapTruncation{
		channel:      prev.Channel,
		lastDataTime: prev.Time,
		truncatedAt:  prev.Time.Add(cap),
	}
}

// FillTail 为“最后一个实际点之后、直到 horizon”的尾段静默生成 QMissing 判定，
// 并在静默跨度超过 cap 时给出截断事实。
//
// 槽位从 prev.Time + step 开始，不超过 min(horizon, prev.Time+cap)；
// 与 ClassifyStream 的内部缺失填充共用同一套语义与上限，
// 不修改实际点，只标记“该有数据但没有”。
func (c *QualityClassifier) FillTail(prev AlignedPoint, horizon time.Time, step, cap time.Duration) ([]QualityVerdict, *gapTruncation) {
	verdicts := c.fillMissing(prev, horizon, time.Time{}, step, cap)
	fact := c.truncationFor(prev, horizon.Sub(prev.Time), cap)
	return verdicts, fact
}

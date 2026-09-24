package pipeline

import "time"

// 设备原始质量提示（RawHint）的取值约定：
//
//	0 = 设备声称信号正常；非 0 = 设备报告该点受伪迹/脱落影响。
const (
	HintOK       RawHint = 0
	HintArtifact RawHint = 1
)

// QualityClass 是四类可区分的质量结论。
type QualityClass int

const (
	// QualityGood：采样存在、信号可用、数值在合理范围内。
	QualityGood QualityClass = iota
	// QualityMissing：数据缺失（该网格时间段根本没有任何采样）。
	QualityMissing
	// QualityUnavailable：采样存在，但信号不可用（设备报告伪迹/脱落）。
	QualityUnavailable
	// QualityAbnormal：数值本身异常：信号可用，但数值超出通道合理范围。
	QualityAbnormal
)

func (q QualityClass) String() string {
	switch q {
	case QualityGood:
		return "GOOD"
	case QualityMissing:
		return "MISSING"
	case QualityUnavailable:
		return "UNAVAILABLE"
	case QualityAbnormal:
		return "ABNORMAL"
	default:
		return "UNKNOWN"
	}
}

// QualityVerdict 描述一个判定结论及其依据（可区分、可解释）。
type QualityVerdict struct {
	Class  QualityClass
	Reason ReasonCode
}

// ChannelSpec 描述通道的规范量纲与数值合理范围。
// 范围检查仅用于区分“数值本身异常”；Min/Max 为闭区间。
type ChannelSpec struct {
	Channel ChannelID
	Unit    Unit
	Min     float64
	Max     float64
}

// Classifier 依据通道规格与设备提示判定单点信号质量。
type Classifier struct {
	specs map[ChannelID]ChannelSpec
}

// NewClassifier 创建质量判定器。
func NewClassifier(specs []ChannelSpec) *Classifier {
	c := &Classifier{specs: make(map[ChannelID]ChannelSpec, len(specs))}
	for _, s := range specs {
		c.specs[s.Channel] = s
	}
	return c
}

// Classify 对单个对齐点给出点级结论。
// 注意：该方法只判“有采样”的点；缺失（没有采样）由 WindowAssessor 判定。
//
// 优先级固定：先判“信号是否可用”（伪迹提示），再判“数值是否异常”。
// 这样受伪迹影响的越限值归为 UNAVAILABLE 而非 ABORMAL，
// 避免一个伪迹尖峰同时触发质量与数值两类告警。
func (c *Classifier) Classify(s AlignedSample, hint RawHint) QualityVerdict {
	if hint != HintOK {
		return QualityVerdict{Class: QualityUnavailable, Reason: ReasonArtifactHint}
	}
	spec, ok := c.specs[s.Channel]
	if ok && (s.Value < spec.Min || s.Value > spec.Max) {
		return QualityVerdict{Class: QualityAbnormal, Reason: ReasonValueOutOfRange}
	}
	return QualityVerdict{Class: QualityGood, Reason: ReasonAccepted}
}

// JudgedPoint 是时间轴上的一个判定点（真实点或合成的缺失标记）。
//
// GapAfter=true 表示该点之后检测到超过容忍阈值的采样间隔（中断），
// 派生指标不得跨越该边界拼接。
type JudgedPoint struct {
	Sample    AlignedSample
	Verdict   QualityVerdict
	Hint      RawHint
	GapAfter  bool
	Synthetic bool // true = 由缺失网格合成，Sample 中无真实采样
}

// AssessorConfig 配置窗口评估器。
type AssessorConfig struct {
	// BucketWidth 评估网格宽度（主时间轴，建议与采样周期对齐）。
	BucketWidth time.Duration
	// MaxGap 允许的最大采样间隔；超过即认定一次中断。
	MaxGap time.Duration
}

// WindowAssessor 在统一时间网格上把有序的对齐点流转换为判定点流，
// 对没有任何采样的网格合成 MISSING 标记，对超时间隔标记 GapAfter。
//
// 输入要求：同一 (设备, 通道) 的点按 MasterTime 非递减到达
// （由 Aligner 的有序输出保证；迟到事件不得喂入本评估器）。
type WindowAssessor struct {
	classifier *Classifier
	cfg        AssessorConfig
	last       map[DeviceID]map[ChannelID]judgeState
}

type judgeState struct {
	lastTime time.Time
	have     bool
}

// NewWindowAssessor 创建窗口评估器。
func NewWindowAssessor(cf *Classifier, cfg AssessorConfig) *WindowAssessor {
	if cfg.BucketWidth <= 0 {
		cfg.BucketWidth = time.Second
	}
	if cfg.MaxGap <= 0 {
		cfg.MaxGap = 2 * cfg.BucketWidth
	}
	return &WindowAssessor{classifier: cf, cfg: cfg, last: make(map[DeviceID]map[ChannelID]judgeState)}
}

// Assess 接收一个已对齐点（仅 ActionAccept），返回零个或多个判定点。
// 若与上一点之间存在空网格，先按网格顺序输出合成 MISSING 点；
// 这些缺失点的时间位置完全由网格除法决定，结果确定可复现。
func (w *WindowAssessor) Assess(s AlignedSample, hint RawHint) []JudgedPoint {
	st := w.state(s.Device, s.Channel)
	out := make([]JudgedPoint, 0, 2)

	verdict := w.classifier.Classify(s, hint)

	if st.have {
		// 填充中间缺失网格：从 last 之后的第一个网格起点到当前点所在网格之前。
		start := bucketStart(st.lastTime, w.cfg.BucketWidth).Add(w.cfg.BucketWidth)
		end := bucketStart(s.MasterTime, w.cfg.BucketWidth)
		for t := start; t.Before(end); t = t.Add(w.cfg.BucketWidth) {
			out = append(out, JudgedPoint{
				Sample:    AlignedSample{Device: s.Device, Channel: s.Channel, MasterTime: t, Unit: s.Unit},
				Verdict:   QualityVerdict{Class: QualityMissing, Reason: ReasonMissingWindow},
				Synthetic: true,
			})
		}
	}

	jp := JudgedPoint{Sample: s, Verdict: verdict, Hint: hint}
	if st.have && s.MasterTime.Sub(st.lastTime) > w.cfg.MaxGap {
		// 将中断标记在缺失序列的最后一个标记上；若无缺失网格则标记当前点之前。
		if n := len(out); n > 0 {
			out[n-1].GapAfter = true
		} else {
			jp.GapAfter = true
		}
	}
	out = append(out, jp)

	st.lastTime = s.MasterTime
	st.have = true
	w.setState(s.Device, s.Channel, st)
	return out
}

// DrainMissing 在没有新点到达时（由管线按主时间轴时钟驱动）为
// [最后观测时间, now) 之间到期的空网格合成缺失标记。
// 这是“在线处理”与“重放处理”使用同一套逻辑的入口：
// 重放时以数据中的最大主时间戳作为 now，即可得到与在线一致的结论。
func (w *WindowAssessor) DrainMissing(dev DeviceID, ch ChannelID, now time.Time) []JudgedPoint {
	st, ok := w.get(dev, ch)
	if !ok || !st.have {
		return nil
	}
	var out []JudgedPoint
	start := bucketStart(st.lastTime, w.cfg.BucketWidth).Add(w.cfg.BucketWidth)
	last := start
	for t := start; !t.After(now); t = t.Add(w.cfg.BucketWidth) {
		out = append(out, JudgedPoint{
			Sample:    AlignedSample{Device: dev, Channel: ch, MasterTime: t},
			Verdict:   QualityVerdict{Class: QualityMissing, Reason: ReasonMissingWindow},
			Synthetic: true,
		})
		last = t.Add(w.cfg.BucketWidth)
	}
	if len(out) > 0 {
		st.lastTime = last.Add(-w.cfg.BucketWidth)
		w.setState(dev, ch, st)
	}
	return out
}

func bucketStart(t time.Time, width time.Duration) time.Time {
	n := t.UnixNano() / int64(width)
	return time.Unix(0, n*int64(width))
}

func (w *WindowAssessor) state(dev DeviceID, ch ChannelID) judgeState {
	if chs, ok := w.last[dev]; ok {
		if st, ok := chs[ch]; ok {
			return st
		}
	}
	return judgeState{}
}

func (w *WindowAssessor) get(dev DeviceID, ch ChannelID) (judgeState, bool) {
	if chs, ok := w.last[dev]; ok {
		st, ok := chs[ch]
		return st, ok
	}
	return judgeState{}, false
}

func (w *WindowAssessor) setState(dev DeviceID, ch ChannelID, st judgeState) {
	chs, ok := w.last[dev]
	if !ok {
		chs = make(map[ChannelID]judgeState)
		w.last[dev] = chs
	}
	chs[ch] = st
}

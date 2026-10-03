package pipeline

import (
	"fmt"
	"time"
)

// EpisodeRule 定义“一次持续异常算一次事件”的判定规则。
type EpisodeRule struct {
	ID      string
	Version string
	Channel ChannelID
	// 高/低限定义“异常”的数值边界（与告警规则相互独立）。
	HighEnabled   bool
	HighThreshold float64
	LowEnabled    bool
	LowThreshold  float64
	// RecoverDuration: 连续恢复正常满该时长，事件才结束；
	// 期间出现数据不可用（缺失/伪迹/断连）不结束事件，但会打断“连续正常”的计时。
	RecoverDuration time.Duration
}

// EpisodeSegmentKind 是事件内部片段的类型。
type EpisodeSegmentKind uint8

const (
	// SegAbnormal 数值越限，异常持续中。
	SegAbnormal EpisodeSegmentKind = iota
	// SegUnavailable 数据不可用（缺失/伪迹/断连），结论待定——不是恢复，也不是结束。
	SegUnavailable
	// SegRecovering 数值已回到范围内，但连续正常未满 RecoverDuration，处于观察期。
	SegRecovering
)

// EpisodeSegment 是事件内一段同质时间区间 [From, To]。
type EpisodeSegment struct {
	Kind EpisodeSegmentKind
	From time.Time
	To   time.Time
}

// EpisodeStatus 是事件生命周期状态。
type EpisodeStatus uint8

const (
	// EpisodeOpen 事件未结束（异常持续或数据不可用待定中）。
	EpisodeOpen EpisodeStatus = iota
	// EpisodeClosed 连续恢复正常满 RecoverDuration，事件已结束。
	EpisodeClosed
)

// Episode 是一次完整异常事件的档案，带稳定编号与配置快照。
type Episode struct {
	// ID 为稳定编号：同一输入序列（在线或重放）产出相同编号。
	ID      string
	Channel ChannelID
	Start   time.Time
	// End 仅在 Status=EpisodeClosed 时有效。
	End      time.Time
	Status   EpisodeStatus
	Segments []EpisodeSegment
	// Snapshot 固化产生该事件时使用的规则与换算参数。
	Snapshot ConfigSnapshot
}

// EpisodeEventType 是事件生命周期迁移类型。
type EpisodeEventType uint8

const (
	// EpisodeEvOpened 事件开启（首次越限）。
	EpisodeEvOpened EpisodeEventType = iota
	// EpisodeEvUnavailable 进入数据不可用段（结论待定）。
	EpisodeEvUnavailable
	// EpisodeEvDataBack 数据恢复（从不可用段回到可判定状态）。
	EpisodeEvDataBack
	// EpisodeEvClosed 连续恢复正常满时长，事件结束。
	EpisodeEvClosed
)

// EpisodeEvent 是事件生命周期的一条迁移记录，可按 EpisodeID 串联复盘。
type EpisodeEvent struct {
	EpisodeID string
	Channel   ChannelID
	Time      time.Time
	Type      EpisodeEventType
	Reason    string
	Snapshot  ConfigSnapshot
}

// EpisodeTracker 是单通道的事件状态机。
//
// 语义：
//   - 首次越限开启事件，编号按通道内序号单调分配（确定性，重放一致）。
//   - 事件开启期间，数据不可用（缺失/伪迹/断连）只标记为 SegUnavailable
//     “数据不可用、结论待定”，既不结束事件，也不算作恢复；
//     它会打断“连续正常”计时——恢复必须由连续可判定的正常数据确认。
//   - 只有连续正常满 RecoverDuration，事件才结束。
type EpisodeTracker struct {
	rule EpisodeRule
	snap ConfigSnapshot
	seq  int

	done        []Episode
	open        *Episode
	normalSince time.Time // 连续正常起点；hasNormal=false 表示当前不在连续正常中
	hasNormal   bool
}

// NewEpisodeTracker 创建事件跟踪器；snap 为固化的配置快照。
func NewEpisodeTracker(rule EpisodeRule, snap ConfigSnapshot) *EpisodeTracker {
	return &EpisodeTracker{rule: rule, snap: snap}
}

// Observe 输入一个质量判定，返回本次产生的生命周期事件（可能为零或多个）。
func (tr *EpisodeTracker) Observe(v QualityVerdict) []EpisodeEvent {
	if v.Channel != tr.rule.Channel {
		return nil
	}
	unavailable := v.Class == QMissing || v.Class == QArtifactUnusable
	breach := false
	if !unavailable && v.Present {
		if tr.rule.HighEnabled && v.Value >= tr.rule.HighThreshold {
			breach = true
		}
		if tr.rule.LowEnabled && v.Value <= tr.rule.LowThreshold {
			breach = true
		}
	}

	if tr.open == nil {
		if !breach {
			return nil
		}
		tr.seq++
		tr.open = &Episode{
			ID:       fmt.Sprintf("%s#%04d", tr.rule.Channel, tr.seq),
			Channel:  tr.rule.Channel,
			Start:    v.Time,
			Status:   EpisodeOpen,
			Snapshot: tr.snap,
		}
		tr.hasNormal = false
		tr.seg(SegAbnormal, v.Time)
		return []EpisodeEvent{tr.ev(EpisodeEvOpened, v.Time,
			"first breach slot opens episode (value "+formatFloat(v.Value)+")")}
	}

	var out []EpisodeEvent
	wasUnavailable := len(tr.open.Segments) > 0 &&
		tr.open.Segments[len(tr.open.Segments)-1].Kind == SegUnavailable

	switch {
	case unavailable:
		// 数据不可用：事件不结束、恢复计时被打断，明确标记“结论待定”。
		tr.hasNormal = false
		if !wasUnavailable {
			out = append(out, tr.ev(EpisodeEvUnavailable, v.Time,
				"data unavailable (missing/artifact/disconnected); conclusion pending, episode stays open"))
		}
		tr.seg(SegUnavailable, v.Time)
	case breach:
		tr.hasNormal = false
		if wasUnavailable {
			out = append(out, tr.ev(EpisodeEvDataBack, v.Time,
				"data available again; still breaching, episode continues"))
		}
		tr.seg(SegAbnormal, v.Time)
	default: // 正常
		if wasUnavailable {
			out = append(out, tr.ev(EpisodeEvDataBack, v.Time,
				"data available again; observing for sustained recovery"))
		}
		if !tr.hasNormal {
			tr.hasNormal = true
			tr.normalSince = v.Time
		}
		tr.seg(SegRecovering, v.Time)
		if v.Time.Sub(tr.normalSince) >= tr.rule.RecoverDuration {
			closed := *tr.open
			closed.Status = EpisodeClosed
			closed.End = v.Time
			tr.done = append(tr.done, closed)
			out = append(out, tr.ev(EpisodeEvClosed, v.Time, fmt.Sprintf(
				"normal continuously for %s (>= %s); episode closed",
				v.Time.Sub(tr.normalSince), tr.rule.RecoverDuration)))
			tr.open = nil
			tr.hasNormal = false
		}
	}
	return out
}

// seg 延续或开启一个片段：同类片段延伸 To，异类则新开。
func (tr *EpisodeTracker) seg(kind EpisodeSegmentKind, t time.Time) {
	segs := tr.open.Segments
	if n := len(segs); n > 0 && segs[n-1].Kind == kind {
		segs[n-1].To = t
		return
	}
	tr.open.Segments = append(segs, EpisodeSegment{Kind: kind, From: t, To: t})
}

func (tr *EpisodeTracker) ev(typ EpisodeEventType, t time.Time, reason string) EpisodeEvent {
	return EpisodeEvent{
		EpisodeID: tr.open.ID,
		Channel:   tr.rule.Channel,
		Time:      t,
		Type:      typ,
		Reason:    reason,
		Snapshot:  tr.snap,
	}
}

// Episodes 返回已结束事件的档案快照。
func (tr *EpisodeTracker) Episodes() []Episode {
	out := make([]Episode, len(tr.done))
	copy(out, tr.done)
	return out
}

// Open 返回当前未结束事件的档案快照（无则 ok=false）。
func (tr *EpisodeTracker) Open() (Episode, bool) {
	if tr.open == nil {
		return Episode{}, false
	}
	return *tr.open, true
}

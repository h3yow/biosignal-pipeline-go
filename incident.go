package pipeline

import "time"

// IncidentPhase 是一次异常事件（Incident）在其生命周期内的阶段。
type IncidentPhase uint8

const (
	// PhaseAbnormal 确认异常中：信号可信且数值越限。
	PhaseAbnormal IncidentPhase = iota
	// PhaseUnavailable 数据不可用、结论待定：设备摘除/伪迹/丢包导致
	// 无法判断异常是否仍在持续，但事件不因此结束。
	PhaseUnavailable
	// PhaseRecovering 观察到连续正常数据，恢复计时中（尚未达到关闭条件）。
	PhaseRecovering
	// PhaseClosed 事件已结束（连续正常满 RecoverAfter）。
	PhaseClosed
)

// IncidentTransition 是事件的一次阶段迁移，带时间与可解释原因。
type IncidentTransition struct {
	IncidentID string
	Channel    ChannelID
	Time       time.Time
	To         IncidentPhase
	Reason     string
}

// Incident 是一次持续异常的完整档案：从开始、期间状态变化到结束，
// 可由稳定编号串联查询。
type Incident struct {
	// ID 为稳定编号，形如 INC-<channel>-<序号>，同一次异常全程不变。
	ID      string
	Channel ChannelID
	Start   time.Time
	// End 仅在 Closed 后有效。
	End    time.Time
	Closed bool
	// Timeline 按时间顺序记录全部阶段迁移。
	Timeline []IncidentTransition
	// Snapshot 固化事件开启时生效的换算参数与规则版本（机器可读）。
	Snapshot ConfigSnapshot
}

// IncidentTracker 把单个通道的质量判定流折叠成“事件”：
// 同一段持续异常（中间夹杂断连/伪迹/丢包）只算一次事件。
type IncidentTracker struct {
	channel      ChannelID
	recoverAfter time.Duration
	snapshot     ConfigSnapshot

	seq          int
	open         *Incident
	closed       []*Incident
	phase        IncidentPhase
	recoverStart time.Time
}

// NewIncidentTracker 创建单通道事件跟踪器。
// recoverAfter 为关闭事件所需的连续正常时长；<=0 表示首个正常槽即关闭。
func NewIncidentTracker(ch ChannelID, recoverAfter time.Duration, snap ConfigSnapshot) *IncidentTracker {
	return &IncidentTracker{channel: ch, recoverAfter: recoverAfter, snapshot: snap}
}

// Observe 输入一个按时间升序的质量判定；
// 若引起阶段迁移，返回迁移记录（可能多条），否则返回 nil。
//
// 语义：
//   - QValueAbnormal：无事件 → 开启新事件（分配稳定编号）；事件进行中 →
//     回到 PhaseAbnormal（打断恢复计时，事件不新增、不结束）。
//   - QMissing / QArtifactUnusable（含静默截断标记）：事件进行中 →
//     PhaseUnavailable（数据不可用、结论待定），不关闭事件。
//   - QGood：事件进行中 → 进入/延续 PhaseRecovering；自恢复起点连续正常
//     满 recoverAfter → PhaseClosed，事件结束。
func (t *IncidentTracker) Observe(v QualityVerdict) []IncidentTransition {
	if v.Channel != t.channel {
		return nil
	}
	var out []IncidentTransition
	emit := func(to IncidentPhase, reason string) {
		tr := IncidentTransition{
			IncidentID: t.open.ID, Channel: t.channel,
			Time: v.Time, To: to, Reason: reason,
		}
		t.open.Timeline = append(t.open.Timeline, tr)
		t.phase = to
		out = append(out, tr)
	}
	switch v.Class {
	case QValueAbnormal:
		switch {
		case t.open == nil:
			t.seq++
			t.open = &Incident{
				ID:       "INC-" + string(t.channel) + "-" + formatSeq(t.seq),
				Channel:  t.channel,
				Start:    v.Time,
				Snapshot: t.snapshot,
			}
			emit(PhaseAbnormal, "trustworthy value out of credible range: "+
				formatFloat(v.Value)+"; incident opened")
		case t.phase != PhaseAbnormal:
			emit(PhaseAbnormal, "abnormal value "+formatFloat(v.Value)+
				" observed again; recovery/interruption did not end the incident")
		}
	case QMissing, QArtifactUnusable:
		if t.open != nil && t.phase != PhaseUnavailable {
			reason := "data unavailable (missing/unusable); conclusion pending, incident stays open"
			if v.Truncated {
				reason = "data unavailable and silence truncated at this slot; conclusion pending, incident stays open"
			}
			emit(PhaseUnavailable, reason)
		}
	case QGood:
		if t.open == nil {
			return nil
		}
		if t.phase != PhaseRecovering {
			t.recoverStart = v.Time
			emit(PhaseRecovering, "normal value observed; recovery timing started")
		}
		if !v.Time.Before(t.recoverStart) && v.Time.Sub(t.recoverStart) >= t.recoverAfter {
			t.open.End = v.Time
			t.open.Closed = true
			emit(PhaseClosed, "recovered: continuous normal for "+
				v.Time.Sub(t.recoverStart).String()+" (>= "+t.recoverAfter.String()+")")
			t.closed = append(t.closed, t.open)
			t.open = nil
		}
	}
	return out
}

// Incidents 返回全部事件档案：已关闭的按开启顺序在前，进行中的在最后。
func (t *IncidentTracker) Incidents() []Incident {
	out := make([]Incident, 0, len(t.closed)+1)
	for _, inc := range t.closed {
		out = append(out, *inc)
	}
	if t.open != nil {
		out = append(out, *t.open)
	}
	return out
}

// Open 返回当前未关闭的事件（无则 nil）。
func (t *IncidentTracker) Open() *Incident { return t.open }

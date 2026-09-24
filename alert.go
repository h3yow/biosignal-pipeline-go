package pipeline

import (
	"fmt"
	"time"
)

// AlertRule 是基于连续时间窗的告警规则，带版本号。
type AlertRule struct {
	ID      string
	Version string
	Channel ChannelID
	// HighEnabled/HighThreshold 配置高限（>= 触发）。
	HighEnabled   bool
	HighThreshold float64
	// LowEnabled/LowThreshold 配置低限（<= 触发），可与高限同时启用。
	LowEnabled   bool
	LowThreshold float64
	// TriggerN: 连续 TriggerN 个越限槽才触发；
	// RecoverN: Active 下连续 RecoverN 个非越限槽才恢复（迟滞，单点抖动不翻转）。
	TriggerN int
	RecoverN int
}

// AlertEngine 是单规则告警状态机。
//
// 状态迁移（每个输入槽评估一次，槽等宽 → 以槽计数实现连续时间窗）：
//
//	Inactive    -- 越限(计数达 TriggerN) --> Active（发出触发事件）
//	Active      -- 非越限(计数达 RecoverN) --> Inactive（发出恢复事件）
//	Active      -- 缺失/不可用 ------------> Inactive（强制恢复，断连不滞留告警）
//
// 计数在中途反向时立即清零，因此单个采样点抖动不会造成反复触发。
// 所有事件带 RuleVersion：规则或配置版本变化后，用旧版本重放即可复现历史结论。
type AlertEngine struct {
	rule        AlertRule
	state       AlertState
	breachRun   int
	recoverRun  int
	activeSince time.Time
	lastValue   float64
}

// NewAlertEngine 创建告警引擎。
func NewAlertEngine(rule AlertRule) *AlertEngine {
	if rule.TriggerN < 1 {
		rule.TriggerN = 1
	}
	if rule.RecoverN < 1 {
		rule.RecoverN = 1
	}
	return &AlertEngine{rule: rule}
}

// Evaluate 输入一个有效质量判定槽，返回状态迁移事件（无迁移时为 nil）。
//
// 只有信号可信的槽（QGood/QValueAbnormal）参与阈值比较；
// QMissing/QArtifactUnusable 走 RecoverAt 语义强制恢复。
func (e *AlertEngine) Evaluate(v QualityVerdict) *AlertEvent {
	if v.Channel != e.rule.Channel {
		return nil
	}
	e.lastValue = v.Value
	if v.Class == QMissing || v.Class == QArtifactUnusable {
		return e.RecoverAt(v.Time)
	}

	breach := false
	if e.rule.HighEnabled && v.Value >= e.rule.HighThreshold {
		breach = true
	}
	if e.rule.LowEnabled && v.Value <= e.rule.LowThreshold {
		breach = true
	}
	switch e.state {
	case AlertInactive, AlertRecovering:
		if breach {
			e.breachRun++
			if e.breachRun == 1 {
				e.activeSince = v.Time
			}
			if e.breachRun >= e.rule.TriggerN {
				return e.transition(AlertActive, v.Time, fmt.Sprintf(
					"%d consecutive breach slots (value %s at %s)",
					e.breachRun, formatFloat(v.Value), v.Time.Format(time.RFC3339Nano)))
			}
		} else {
			e.breachRun = 0
		}
	case AlertActive:
		if breach {
			e.recoverRun = 0
		} else {
			e.recoverRun++
			if e.recoverRun >= e.rule.RecoverN {
				return e.transition(AlertInactive, v.Time, fmt.Sprintf(
					"%d consecutive in-range slots (value %s at %s)",
					e.recoverRun, formatFloat(v.Value), v.Time.Format(time.RFC3339Nano)))
			}
		}
	}
	return nil
}

// RecoverAt 因中断/缺失/断连强制在 t 时刻恢复到 Inactive。
// 仅当当前处于 Active（或恢复中）时产生恢复事件；未告警时返回 nil。
func (e *AlertEngine) RecoverAt(t time.Time) *AlertEvent {
	if e.state == AlertActive {
		return e.transition(AlertInactive, t,
			"forced recovery: data missing or signal unusable / device disconnected")
	}
	e.state = AlertInactive
	e.breachRun = 0
	e.recoverRun = 0
	return nil
}

func (e *AlertEngine) transition(to AlertState, t time.Time, reason string) *AlertEvent {
	from := e.state
	ev := &AlertEvent{
		RuleID:      e.rule.ID,
		RuleVersion: e.rule.Version,
		Channel:     e.rule.Channel,
		Time:        t,
		From:        from,
		To:          to,
		Value:       e.lastValue,
		Reason:      reason,
	}
	e.state = to
	if to == AlertActive {
		e.recoverRun = 0
	} else {
		e.breachRun = 0
	}
	return ev
}

// State 返回当前状态。
func (e *AlertEngine) State() AlertState { return e.state }

// RuleVersion 返回当前规则版本。
func (e *AlertEngine) RuleVersion() string { return e.rule.Version }

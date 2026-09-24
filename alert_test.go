package pipeline

import (
	"testing"
	"time"
)

func alertVerdict(t time.Time, class QualityClass, v float64) QualityVerdict {
	return QualityVerdict{Channel: "c", Time: t, Class: class, Present: true, Value: v}
}

func runEngine(t *testing.T, values []float64) []AlertEvent {
	t.Helper()
	base := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	e := NewAlertEngine(AlertRule{
		ID: "r", Version: "v9", Channel: "c",
		HighEnabled: true, HighThreshold: 100, TriggerN: 3, RecoverN: 3,
	})
	var evs []AlertEvent
	for i, v := range values {
		if ev := e.Evaluate(alertVerdict(base.Add(time.Duration(i)*time.Second), QGood, v)); ev != nil {
			evs = append(evs, *ev)
		}
	}
	if e.RuleVersion() != "v9" {
		t.Fatalf("rule version must be carried, got %s", e.RuleVersion())
	}
	return evs
}

// TestAlertTriggerHysteresisJitter 连续 3 个越限才触发；连续 3 个正常才恢复；
// 单个抖动点既不触发也不让活跃告警抖动。
func TestAlertTriggerHysteresisJitter(t *testing.T) {
	// 单点越限抖动：不触发。
	if evs := runEngine(t, []float64{90, 150, 90}); len(evs) != 0 {
		t.Fatalf("single jitter point must not trigger, got %+v", evs)
	}
	// 连续 3 越限触发；随后 2 个正常（不足恢复窗）+1 越限，不得反复告警；
	// 再连续 3 正常才恢复一次。
	evs := runEngine(t, []float64{110, 110, 110, 90, 90, 110, 90, 90, 90})
	if len(evs) != 2 {
		t.Fatalf("want exactly 1 trigger + 1 recovery, got %d: %+v", len(evs), evs)
	}
	if evs[0].From != AlertInactive || evs[0].To != AlertActive || evs[0].RuleVersion != "v9" {
		t.Fatalf("trigger event wrong: %+v", evs[0])
	}
	if evs[1].From != AlertActive || evs[1].To != AlertInactive {
		t.Fatalf("recovery event wrong: %+v", evs[1])
	}
	// 触发必须发生在第 3 个连续越限槽（t=2s），恢复在最后一个槽（t=8s）。
	base := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	if !evs[0].Time.Equal(base.Add(2*time.Second)) || !evs[1].Time.Equal(base.Add(8*time.Second)) {
		t.Fatalf("event timing not window-based: %v / %v", evs[0].Time, evs[1].Time)
	}
	logf(t, "输入: 110×3,90×2,110,90×3; 依据: TriggerN=3/RecoverN=3 连续槽计数, 反向清零; 结论: 仅 1 次触发(t=2s)+1 次恢复(t=8s), 无抖动反复告警")
}

// TestAlertForcedRecoveryOnGap 活跃告警期间数据缺失/设备断连，必须强制恢复。
func TestAlertForcedRecoveryOnGap(t *testing.T) {
	base := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	e := NewAlertEngine(AlertRule{
		ID: "r", Version: "v1", Channel: "c",
		HighEnabled: true, HighThreshold: 100, TriggerN: 2, RecoverN: 2,
	})
	for i := 0; i < 2; i++ {
		e.Evaluate(alertVerdict(base.Add(time.Duration(i)*time.Second), QGood, 120))
	}
	if e.State() != AlertActive {
		t.Fatal("expected active")
	}
	ev := e.Evaluate(QualityVerdict{Channel: "c", Time: base.Add(2 * time.Second), Class: QMissing})
	if ev == nil || ev.To != AlertInactive {
		t.Fatalf("missing slot must force-recover, ev=%+v", ev)
	}
	// 未处于告警态时强制恢复不产生重复事件。
	if ev := e.RecoverAt(base.Add(3 * time.Second)); ev != nil {
		t.Fatalf("recover while inactive must emit no event, got %+v", ev)
	}
	logf(t, "输入: 连续越限进入 ACTIVE 后出现 QMissing; 依据: 缺失/不可用强制恢复, 非活跃态不产事件; 结论: ACTIVE->INACTIVE 一次, 无重复/滞留")
}

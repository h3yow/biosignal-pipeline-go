package pipeline

import (
	"testing"
	"time"
)

func alarmPoint(dev DeviceID, sec int64, v float64, class QualityClass, gap bool) JudgedPoint {
	return JudgedPoint{
		Sample:   alignedAt(dev, "HR", sec, alarmBase.Add(time.Duration(sec)*time.Second), v),
		Verdict:  QualityVerdict{Class: class, Reason: ReasonAccepted},
		GapAfter: gap,
	}
}

var alarmBase = time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)

func newTestEngine() *Engine {
	return NewEngine([]Rule{{
		ID: "hr-high", Version: "v1", Channel: "HR",
		High: 100, Low: 40, EnterWindow: 2 * time.Second, ExitWindow: 2 * time.Second,
	}})
}

// TestAlarmSingleSpikeNoTrigger：单点越界抖动不得触发告警。
func TestAlarmSingleSpikeNoTrigger(t *testing.T) {
	e := newTestEngine()
	e.Evaluate(alarmPoint("d", 0, 70, QualityGood, false))
	if evs := e.Evaluate(alarmPoint("d", 1, 120, QualityGood, false)); len(evs) != 0 {
		t.Fatalf("single spike produced alarm: %+v", evs)
	}
	// 立即回到正常，也不应有任何事件。
	if evs := e.Evaluate(alarmPoint("d", 2, 70, QualityGood, false)); len(evs) != 0 {
		t.Fatalf("recovery from single spike produced event: %+v", evs)
	}
	t.Log("basis: EnterWindow=2s; only 1 of 3 points breached -> IDLE, no event (jitter filtered)")
}

// TestAlarmSustainedTriggersAndRecovers：持续越界触发一次，
// 持续安全恢复一次，长时间持续也不重复告警。
func TestAlarmSustainedTriggersAndRecovers(t *testing.T) {
	e := newTestEngine()
	feed := func(sec int64, v float64) []AlarmEvent {
		return e.Evaluate(alarmPoint("d", sec, v, QualityGood, false))
	}
	feed(0, 70)
	if evs := feed(1, 120); len(evs) != 0 { // 越界跨度 0s，未满窗
		t.Fatalf("breach below EnterWindow triggered: %+v", evs)
	}
	if evs := feed(2, 120); len(evs) != 0 { // 跨度 1s，仍未满窗
		t.Fatalf("breach below EnterWindow triggered: %+v", evs)
	}
	evs := feed(3, 120) // 三个越界点 t=1,2,3，跨度 2s >= EnterWindow
	if len(evs) != 1 || evs[0].Status != AlarmActive || evs[0].RuleVersion != "v1" {
		t.Fatalf("expected one ACTIVE at sustained breach, got %+v", evs)
	}
	t.Logf("basis: breach held across 3 points spanning 2s >= EnterWindow -> %s at %s", evs[0].Status, evs[0].At.Format("05"))

	// 持续越界多秒，不得重复告警。
	for s := int64(4); s < 8; s++ {
		if evs := feed(s, 120); len(evs) != 0 {
			t.Fatalf("duplicate alarm while staying breached: %+v", evs)
		}
	}

	// 回到安全：3 个连续安全点 t=8,9,10 覆盖 2s 才恢复；前两个不足。
	if evs := feed(8, 70); len(evs) != 0 {
		t.Fatalf("recovery below ExitWindow produced event: %+v", evs)
	}
	if evs := feed(9, 70); len(evs) != 0 {
		t.Fatalf("recovery below ExitWindow produced event: %+v", evs)
	}
	evs = feed(10, 70) // 安全点 t=8,9,10 覆盖 2s >= ExitWindow
	if len(evs) != 1 || evs[0].Status != AlarmIdle {
		t.Fatalf("expected one IDLE recovery, got %+v", evs)
	}
	t.Logf("basis: safe held 2s >= ExitWindow -> %s at %s", evs[0].Status, evs[0].At.Format("05"))

	if e.State("hr-high", "d") != AlarmIdle {
		t.Fatal("engine state not idle after recovery")
	}
}

// TestAlarmMissingDoesNotRecover：告警态遇到缺失/伪迹不得自动恢复，
// 恢复只能由足够长的连续安全 GOOD 点达成。
func TestAlarmMissingDoesNotRecover(t *testing.T) {
	e := newTestEngine()
	feed := func(sec int64, v float64, c QualityClass) {
		t.Helper()
		if evs := e.Evaluate(alarmPoint("d", sec, v, c, false)); evs != nil {
			t.Fatalf("unexpected event during degraded signal: %+v", evs)
		}
	}
	feed(0, 70, QualityGood)
	if evs := e.Evaluate(alarmPoint("d", 1, 120, QualityGood, false)); len(evs) != 0 {
		t.Fatalf("premier breach point must not trigger: %+v", evs)
	}
	if evs := e.Evaluate(alarmPoint("d", 2, 120, QualityGood, false)); len(evs) != 0 {
		t.Fatalf("second breach point must not trigger yet: %+v", evs)
	}
	evs := e.Evaluate(alarmPoint("d", 3, 120, QualityGood, false))
	if len(evs) != 1 {
		t.Fatalf("expected trigger on 3rd sustained breach point, got %+v", evs)
	}
	// 连续缺失/不可用点：状态必须保持 ACTIVE，不恢复、也不重复触发。
	for s := int64(4); s < 14; s++ {
		c := QualityMissing
		if s%2 == 0 {
			c = QualityUnavailable
		}
		feed(s, 0, c)
	}
	if e.State("hr-high", "d") != AlarmActive {
		t.Fatal("alarm must remain ACTIVE across missing/unavailable stretch")
	}
	t.Log("basis: degraded points reset streaks but hold ACTIVE state -> no recover, no re-alarm")

	// 恢复连接后，连续 2 个安全点才恢复。
	e.Evaluate(alarmPoint("d", 14, 70, QualityGood, false))
	e.Evaluate(alarmPoint("d", 15, 70, QualityGood, false))
	if e.State("hr-high", "d") != AlarmActive {
		t.Fatal("two safe points after reconnect must not recover until coverage>=ExitWindow")
	}
	evs = e.Evaluate(alarmPoint("d", 16, 70, QualityGood, false)) // 14,15,16 覆盖 2s
	if len(evs) != 1 || evs[0].Status != AlarmIdle {
		t.Fatalf("expected recovery after 2 sustained safe points, got %+v", evs)
	}
}

// TestAlarmReplayDeterministic：同一判定点序列两次喂入，事件流必须逐字一致。
func TestAlarmReplayDeterministic(t *testing.T) {
	run := func() []AlarmEvent {
		e := newTestEngine()
		var out []AlarmEvent
		values := []float64{70, 120, 120, 70, 70, 130, 130, 130, 60, 60}
		for s, v := range values {
			out = append(out, e.Evaluate(alarmPoint("d", int64(s), v, QualityGood, false))...)
		}
		return out
	}
	a := run()
	b := run()
	if len(a) != len(b) {
		t.Fatalf("replay event count differs: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("replay event %d differs: %+v vs %+v", i, a[i], b[i])
		}
	}
	t.Logf("basis: identical input sequence -> identical events=%v", a)
}

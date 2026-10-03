package pipeline

import (
	"testing"
	"time"
)

// snapshotConfig 生成带版本标识的配置；scale/阈值/规则版本可按版本变化。
func snapshotConfig(version string, scale float64, qVer, rVer string, highTh float64) Config {
	cfg := incidentConfig()
	cfg.Version = version
	cfg.Channels[0].Cal = Calibration{Scale: scale, Unit: "bpm"}
	cfg.Quality[0].Version = qVer
	cfg.AlertRules = []AlertRule{
		{ID: "hr-high", Version: rVer, Channel: "HR",
			HighEnabled: true, HighThreshold: highTh, TriggerN: 2, RecoverN: 2},
	}
	return cfg
}

// snapshotPackets t=5..9 原始值 130（scale=1 时越限 120），其余 75。
func snapshotPackets(epoch time.Time) []RawSample {
	var pk []RawSample
	for i := 0; i <= 15; i++ {
		v := 75.0
		if i >= 5 && i <= 9 {
			v = 130
		}
		pk = append(pk, hrPkt(int64(i), epoch.Add(time.Duration(i)*time.Second), v))
	}
	return pk
}

func assertSnapshots(t *testing.T, run fullRun, cfgVer string, scale float64, qVer, rVer string) {
	t.Helper()
	check := func(where string, s ConfigSnapshot) {
		if s.ConfigVersion != cfgVer || s.Cal.Scale != scale || s.QualityRuleVersion != qVer ||
			s.AlertRules["hr-high"] != rVer || s.Cal.Unit != "bpm" {
			t.Fatalf("%s carries wrong snapshot: %+v (want cfg=%s scale=%v q=%s r=%s)",
				where, s, cfgVer, scale, qVer, rVer)
		}
	}
	if len(run.verdicts) == 0 || len(run.windows) == 0 || len(run.incidents) == 0 {
		t.Fatalf("run must produce verdicts/windows/incidents, got %d/%d/%d",
			len(run.verdicts), len(run.windows), len(run.incidents))
	}
	for _, v := range run.verdicts {
		check("verdict", v.Snapshot)
	}
	for _, w := range run.windows {
		check("metric window", w.Snapshot)
	}
	for _, e := range run.events {
		check("alert event", e.Snapshot)
		if e.RuleVersion != rVer {
			t.Fatalf("alert event RuleVersion want %s got %s", rVer, e.RuleVersion)
		}
	}
	for _, inc := range run.incidents {
		check("incident", inc.Snapshot)
	}
}

// TestConfigSnapshotPinnedAndReplay 每条可导出结论都固化产出时的换算参数与
// 规则版本（机器可读）；配置改版不影响历史结论；用旧版本配置重放可逐条还原。
func TestConfigSnapshotPinnedAndReplay(t *testing.T) {
	epoch := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	packets := snapshotPackets(epoch)

	cfgV1 := snapshotConfig("cfg-A", 1, "q1", "r1", 120)
	cfgV2 := snapshotConfig("cfg-B", 2, "q2", "r2", 300) // 换算与规则都改版

	// 1) 旧版本配置产出历史结论。
	hist := runFull(t, cfgV1, packets, false)
	assertSnapshots(t, hist, "cfg-A", 1, "q1", "r1")
	if len(hist.events) != 2 { // 触发 + 恢复
		t.Fatalf("cfg-A must fire+recover, got %s", fmtEvents(hist.events))
	}

	// 2) 配置改版后重放同一批原始数据：结论独立变化，快照随之更新。
	//    scale=2 后 130*2=260 < 300，新规则不触发。
	nu := runFull(t, cfgV2, packets, false)
	assertSnapshots(t, nu, "cfg-B", 2, "q2", "r2")
	if len(nu.events) != 0 {
		t.Fatalf("cfg-B (threshold 300) must not fire on same raw data, got %s", fmtEvents(nu.events))
	}
	for _, v := range nu.verdicts {
		if v.Present && v.Value == 130 {
			t.Fatalf("cfg-B must calibrate with scale=2 (value 260), got raw-scale value %v", v.Value)
		}
	}

	// 3) 历史结论不被改写：改版重放之后，此前产出的结论仍带 cfg-A 快照。
	assertSnapshots(t, hist, "cfg-A", 1, "q1", "r1")

	// 4) 用旧版本配置再跑一遍，逐条还原出与历史完全一致的结论。
	replay := runFull(t, cfgV1, packets, false)
	if !verdictsEqual(hist.verdicts, replay.verdicts) {
		t.Fatalf("replay with cfg-A must reproduce historical verdicts")
	}
	if !eventsEqual(hist.events, replay.events) {
		t.Fatalf("replay with cfg-A must reproduce historical events:\n hist=%s\n replay=%s",
			fmtEvents(hist.events), fmtEvents(replay.events))
	}
	if !windowsEqual(hist.windows, replay.windows) {
		t.Fatalf("replay with cfg-A must reproduce historical windows")
	}
	if len(hist.incidents) != len(replay.incidents) {
		t.Fatalf("incident count diverges: %d vs %d", len(hist.incidents), len(replay.incidents))
	}
	for i := range hist.incidents {
		h, r := hist.incidents[i], replay.incidents[i]
		if h.ID != r.ID || !h.Start.Equal(r.Start) || !h.End.Equal(r.End) || h.Closed != r.Closed ||
			len(h.Timeline) != len(r.Timeline) {
			t.Fatalf("incident %d not reproduced:\n hist=%+v\n replay=%+v", i, h, r)
		}
	}
	logf(t, "输入: 同一批原始包(t=5..9 raw=130)分别用 cfg-A(scale=1,阈值120,r1/q1) 与 cfg-B(scale=2,阈值300,r2/q2) 重放; "+
		"依据: 每条结论固化 ConfigSnapshot(版本/换算/规则版本); "+
		"结论: cfg-A 触发+恢复且全部结论带 cfg-A 快照, cfg-B 不触发且值按 scale=2 换算, "+
		"历史结论不被改写, 用 cfg-A 重放逐条还原(判定/事件/指标窗/事件档案全一致)")
	for _, e := range hist.events {
		t.Logf("    EVENT %s %s %d->%d v=%.0f rule=%s snap.cfg=%s snap.scale=%.0f | %s",
			e.Time.Format("15:04:05"), e.RuleID, e.From, e.To, e.Value, e.RuleVersion,
			e.Snapshot.ConfigVersion, e.Snapshot.Cal.Scale, e.Reason)
	}
}

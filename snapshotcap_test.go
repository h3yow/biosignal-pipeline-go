package pipeline

import (
	"testing"
	"time"
)

// TestAllConclusionsCarrySnapshot 每条可查询/导出的结论都必须带程序可直接读取的
// 结构化配置快照：换算参数、判定规则身份/版本、当时的静默上限值。
// 覆盖：告警事件、事件档案、事件生命周期迁移、指标窗、静默截断结论。
func TestAllConclusionsCarrySnapshot(t *testing.T) {
	cfg := gapConfig(10 * time.Second)
	// t0..t3 越限 130（触发告警/事件），随后 28s 长静默（在 t13 截断），
	// t31 起重连正常（连续正常关闭事件）。
	packets := []RawSample{}
	ch := cfg.Channels[0]
	mk := func(seq int64, at time.Duration, v float64) RawSample {
		return RawSample{Device: ch.Device, Channel: ch.Channel, Seq: seq,
			DeviceTime: cfg.Epoch.Add(at), RawValue: v, Flags: FlagOK}
	}
	for i := int64(0); i < 4; i++ {
		packets = append(packets, mk(i, time.Duration(i)*time.Second, 130))
	}
	for i := int64(0); i < 5; i++ {
		packets = append(packets, mk(4+i, 31*time.Second+time.Duration(i)*time.Second, 75))
	}

	r := runFull(t, cfg, packets, false)

	if len(r.events) == 0 {
		t.Fatal("scenario must produce alert events")
	}
	for _, e := range r.events {
		s := e.Snapshot
		if s.RuleID != "hr-high" || s.RuleVersion != "cfg-1" {
			t.Fatalf("alert event must pin rule id/version, got %+v", s)
		}
		if s.Cal.Scale != 1 || s.Cal.Unit != "bpm" {
			t.Fatalf("alert event must pin calibration, got %+v", s.Cal)
		}
		if s.SilenceCap != 10*time.Second {
			t.Fatalf("alert event must pin effective silence cap, got %s", s.SilenceCap)
		}
	}
	if len(r.episodes) == 0 || len(r.epEvents) == 0 {
		t.Fatal("scenario must produce episodes and lifecycle events")
	}
	for _, ep := range r.episodes {
		s := ep.Snapshot
		if s.RuleID != "hr-episode" || s.RuleVersion != "ep-1" ||
			s.Cal.Unit != "bpm" || s.SilenceCap != 10*time.Second {
			t.Fatalf("episode must pin rule/cal/cap snapshot, got %+v", s)
		}
	}
	for _, e := range r.epEvents {
		if e.Snapshot.RuleID != "hr-episode" || e.Snapshot.SilenceCap != 10*time.Second {
			t.Fatalf("episode lifecycle event must pin snapshot, got %+v", e.Snapshot)
		}
	}
	if len(r.windows) == 0 {
		t.Fatal("scenario must produce metric windows")
	}
	for _, w := range r.windows {
		if w.Snapshot.RuleID != "hr-quality" || w.Snapshot.RuleVersion != "q-1" {
			t.Fatalf("metric window must pin quality rule id/version, got %+v", w.Snapshot)
		}
		if w.Snapshot.Cal.Scale != 1 || w.Snapshot.Cal.Unit != "bpm" ||
			w.Snapshot.ExpectedInterval != time.Second || w.Snapshot.GapTolerance != 200*time.Millisecond ||
			w.Snapshot.SlotStep != time.Second || w.Snapshot.SilenceCap != 10*time.Second {
			t.Fatalf("metric window must pin full quality/cal/cap snapshot, got %+v", w.Snapshot)
		}
	}
	if len(r.truncations) != 1 {
		t.Fatalf("long silence must yield 1 truncation, got %+v", r.truncations)
	}
	ts := r.truncations[0].Snapshot
	if ts.RuleID != SilenceRuleID || ts.RuleVersion != SilenceRuleVersion ||
		ts.SilenceCap != 10*time.Second || ts.Cal.Unit != "bpm" {
		t.Fatalf("truncation must pin silence rule id/version, cap and cal, got %+v", ts)
	}
	logf(t, "输入: t0-3 越限130/静默28s(截断t13)/t31起重连正常, cap=10s; 依据: 所有结论构建时固化结构化快照; "+
		"结论: 告警事件(hr-high/cfg-1)、事件(hr-episode/ep-1)、指标窗(hr-quality/q-1)、截断(%s/%s) 均带 Cal+规则身份版本+cap=10s",
		SilenceRuleID, SilenceRuleVersion)
}

// TestConfigChangeDoesNotRewriteHistory 换算参数、规则版本、静默上限改版后：
// 新结论带新快照，历史结论保持原快照不被改写；用“当时的版本（含当时 cap）”
// 重放同一批数据，事件/指标窗/截断结论逐条还原（含快照字段本身）。
func TestConfigChangeDoesNotRewriteHistory(t *testing.T) {
	v1 := gapConfig(10 * time.Second) // Scale=1, 版本 cfg-1/ep-1/q-1, cap=10s
	span := 28 * time.Second
	packets := gapPackets(v1, span) // 静默跨度 28s，超过下面两个 cap

	run1 := runFull(t, v1, packets, false)
	if len(run1.truncations) != 1 || run1.truncations[0].TruncatedAt != v1.Epoch.Add(13*time.Second) {
		t.Fatalf("v1 must truncate at t13, got %+v", run1.truncations)
	}

	// 改版：Scale=2、规则版本升级、cap 改小到 5s、生理范围收紧。
	v2 := gapConfig(5 * time.Second)
	v2.Channels[0].Cal.Scale = 2
	v2.AlertRules[0].Version = "cfg-2"
	v2.EpisodeRules[0].Version = "ep-2"
	v2.Quality[0].Version = "q-2"
	v2.Quality[0].PhysMax = 200
	run2 := runFull(t, v2, packets, false)

	// 新结论全部带新快照、截断位置随新 cap 移动到 t8。
	if len(run2.truncations) != 1 || !run2.truncations[0].TruncatedAt.Equal(v2.Epoch.Add(8*time.Second)) {
		t.Fatalf("v2 must truncate at t3+5s=t8, got %+v", run2.truncations)
	}
	if run2.truncations[0].Snapshot.SilenceCap != 5*time.Second ||
		run2.truncations[0].Snapshot.Cal.Scale != 2 {
		t.Fatalf("v2 truncation must pin new cap/cal, got %+v", run2.truncations[0].Snapshot)
	}
	for _, w := range run2.windows {
		if w.Snapshot.RuleVersion != "q-2" || w.Snapshot.Cal.Scale != 2 || w.Snapshot.SilenceCap != 5*time.Second {
			t.Fatalf("v2 windows must pin new snapshot, got %+v", w.Snapshot)
		}
	}

	// 历史结论（run1）不被改写：仍是旧 cap=10s、旧 Scale=1、旧版本。
	if run1.truncations[0].Snapshot.SilenceCap != 10*time.Second ||
		run1.truncations[0].TruncatedAt != v1.Epoch.Add(13*time.Second) {
		t.Fatalf("historical v1 truncation must not be rewritten, got %+v", run1.truncations[0])
	}
	for _, w := range run1.windows {
		if w.Snapshot.RuleVersion != "q-1" || w.Snapshot.Cal.Scale != 1 {
			t.Fatalf("historical window must keep q-1/scale=1, got %+v", w.Snapshot)
		}
	}

	// 用当时版本 v1 重放：事件、指标窗、截断结论（含快照）逐条还原。
	replay := runFull(t, v1, packets, false)
	if !truncationsEqual(replay.truncations, run1.truncations) {
		t.Fatalf("v1 replay must reproduce truncations incl snapshot:\n %+v\n %+v",
			run1.truncations, replay.truncations)
	}
	if !eventsEqual(replay.events, run1.events) {
		t.Fatalf("v1 replay must reproduce alert events: %s vs %s",
			fmtEvents(run1.events), fmtEvents(replay.events))
	}
	if !windowsEqualSorted(append([]MetricWindow(nil), replay.windows...),
		append([]MetricWindow(nil), run1.windows...)) {
		t.Fatalf("v1 replay must reproduce metric windows:\n %+v\n %+v", run1.windows, replay.windows)
	}
	if !episodesEqual(replay.episodes, run1.episodes) {
		t.Fatalf("v1 replay must reproduce episodes")
	}
	for i := range replay.events {
		if replay.events[i].Snapshot != run1.events[i].Snapshot {
			t.Fatalf("replayed event snapshot must be identical: %+v vs %+v",
				run1.events[i].Snapshot, replay.events[i].Snapshot)
		}
	}
	logf(t, "输入: 同批 t0-3 后静默28s数据, 分别用 v1(cap=10s,Scale=1,cfg-1/q-1) 与 v2(cap=5s,Scale=2,cfg-2/q-2), 再用 v1 重放; "+
		"依据: 每条结论固化产生时快照; 结论: v1截断t13/v2截断t8 各带各快照, 历史 v1 不被改写, v1 重放事件/指标窗/截断逐条还原(含快照)")
}

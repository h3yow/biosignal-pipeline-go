package pipeline

import (
	"testing"
	"time"
)

// snapshotConfig 生成指定换算参数与规则版本的配置。
func snapshotConfig(scale float64, ruleVer, epVer string, triggerN int) Config {
	start := time.Date(2026, 9, 24, 13, 0, 0, 0, time.UTC)
	const hr ChannelID = "HR"
	const d1 DeviceID = "dev1"
	return Config{
		Epoch:    start,
		SlotStep: time.Second,
		MaxDelay: 3 * time.Second,
		Channels: []ChannelSpec{
			{Device: d1, Channel: hr, Cal: Calibration{Scale: scale, Unit: "bpm"}, NominalInterval: time.Second},
		},
		Clocks: map[DeviceID]ClockParams{d1: {}},
		Quality: []QualityRule{
			{Channel: hr, ExpectedInterval: time.Second, GapTolerance: 200 * time.Millisecond, PhysMin: 30, PhysMax: 220},
		},
		AlertRules: []AlertRule{
			{ID: "hr-high", Version: ruleVer, Channel: hr, HighEnabled: true, HighThreshold: 120, TriggerN: triggerN, RecoverN: 3},
		},
		EpisodeRules: []EpisodeRule{
			{ID: "hr-episode", Version: epVer, Channel: hr, HighEnabled: true, HighThreshold: 120, RecoverDuration: 5 * time.Second},
		},
	}
}

// snapshotPackets 固定数据：seq 4..8 越限（raw=130），其余正常（raw=75）。
func snapshotPackets(cfg Config) []RawSample {
	gen := func(ch ChannelID, seq int64, t time.Time) (float64, RawFlag) {
		if seq >= 4 && seq <= 8 {
			return 130, FlagOK
		}
		return 75, FlagOK
	}
	return NewSimulator(SimConfig{
		Device: cfg.Channels[0].Device, Channels: cfg.Channels,
		StartTime: cfg.Epoch, Duration: 20 * time.Second,
	}, gen).Generate()
}

type snapshotRun struct {
	windows  []MetricWindow
	events   []AlertEvent
	episodes []Episode
}

func runSnapshot(cfg Config, packets []RawSample) snapshotRun {
	p := NewPipeline(cfg)
	for _, pk := range packets {
		p.Ingest(pk)
	}
	w, e := p.Advance(cfg.Epoch.Add(2 * time.Minute))
	return snapshotRun{
		windows:  append(w, p.Flush()...),
		events:   e,
		episodes: p.Episodes(),
	}
}

// TestConfigSnapshotPinnedAndReplay 每条导出结论都带机器可读的配置快照；
// 换算参数/规则改版后历史结论不被改写；用旧版本重放可逐条还原。
func TestConfigSnapshotPinnedAndReplay(t *testing.T) {
	cfgV1 := snapshotConfig(1, "cfg-1", "ep-1", 3)
	packets := snapshotPackets(cfgV1)

	v1 := runSnapshot(cfgV1, packets)
	if len(v1.events) == 0 || len(v1.episodes) == 0 || len(v1.windows) == 0 {
		t.Fatalf("scenario must produce events/episodes/windows, got %+v", v1)
	}
	// 1) 每条告警事件都固化当时的规则版本、规则参数与换算参数。
	for _, e := range v1.events {
		s := e.Snapshot
		if s.RuleID != "hr-high" || s.RuleVersion != "cfg-1" ||
			!s.HighEnabled || s.HighThreshold != 120 || s.TriggerN != 3 || s.RecoverN != 3 ||
			s.Cal.Scale != 1 || s.Cal.Unit != "bpm" {
			t.Fatalf("alert event must pin full config snapshot, got %+v", s)
		}
	}
	// 2) 每个事件档案固化当时的事件规则版本与换算参数。
	for _, ep := range v1.episodes {
		s := ep.Snapshot
		if s.RuleID != "hr-episode" || s.RuleVersion != "ep-1" ||
			s.RecoverDuration != 5*time.Second || s.Cal.Scale != 1 {
			t.Fatalf("episode must pin full config snapshot, got %+v", s)
		}
	}
	// 3) 每个指标窗固化当时的换算参数。
	for _, w := range v1.windows {
		if w.Cal.Scale != 1 || w.Cal.Unit != "bpm" {
			t.Fatalf("metric window must pin calibration snapshot, got %+v", w.Cal)
		}
	}

	// 4) 配置改版（换算 Scale=2、规则版本 cfg-2/ep-2）：新结论带新快照，
	//    历史结论（v1 运行结果）保持原快照不被改写。
	cfgV2 := snapshotConfig(2, "cfg-2", "ep-2", 3)
	v2 := runSnapshot(cfgV2, packets)
	for _, e := range v2.events {
		if e.Snapshot.RuleVersion != "cfg-2" || e.Snapshot.Cal.Scale != 2 {
			t.Fatalf("v2 events must carry v2 snapshot, got %+v", e.Snapshot)
		}
	}
	for _, e := range v1.events {
		if e.Snapshot.RuleVersion != "cfg-1" || e.Snapshot.Cal.Scale != 1 {
			t.Fatalf("historical conclusions must not be rewritten, got %+v", e.Snapshot)
		}
	}

	// 5) 用旧版本配置重放同一批历史数据：逐条还原相同结论（含快照）。
	replay := runSnapshot(cfgV1, packets)
	if !eventsEqual(replay.events, v1.events) {
		t.Fatalf("replay with old config must reproduce alert events:\n v1=%+v\n replay=%+v", v1.events, replay.events)
	}
	for i := range v1.events {
		if replay.events[i].Snapshot != v1.events[i].Snapshot {
			t.Fatalf("snapshot must reproduce exactly: %+v vs %+v", replay.events[i].Snapshot, v1.events[i].Snapshot)
		}
	}
	if !episodesEqual(replay.episodes, v1.episodes) {
		t.Fatalf("replay with old config must reproduce episodes:\n v1=%+v\n replay=%+v", v1.episodes, replay.episodes)
	}
	logf(t, "输入: 同一批数据(raw越限130) 分别用 cfg-1(Scale=1) 与 cfg-2(Scale=2) 处理, 再用 cfg-1 重放; "+
		"依据: 每条结论固化 RuleVersion+规则参数+Cal 的机器可读快照; "+
		"结论: v1 事件/事件档案/指标窗全部带 cfg-1 快照, 改版不改写历史, 旧版本重放逐条还原(含快照)")
}

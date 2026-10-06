package pipeline

import (
	"testing"
	"time"
)

func epRule() EpisodeRule {
	return EpisodeRule{
		ID: "hr-episode", Version: "ep-1", Channel: "HR",
		HighEnabled: true, HighThreshold: 120,
		RecoverDuration: 5 * time.Second,
	}
}

func epVerdict(ch ChannelID, t time.Time, class QualityClass, v float64) QualityVerdict {
	return QualityVerdict{Channel: ch, Time: t, Class: class, Present: class != QMissing, Value: v}
}

// TestEpisodeDisconnectReconnectSameEpisode 异常中途断连（缺失）再重连仍异常：
// 必须是同一次事件，编号不变，不可重报；断连段标记“数据不可用、结论待定”；
// 只有连续正常满 RecoverDuration 才结束。
func TestEpisodeDisconnectReconnectSameEpisode(t *testing.T) {
	base := time.Date(2026, 9, 24, 9, 0, 0, 0, time.UTC)
	tr := NewEpisodeTracker(epRule(), snapshotFromEpisodeRule(epRule(), qualityContext{
		cal: Calibration{Scale: 1, Unit: "bpm"}, slotStep: time.Second, silenceCap: DefaultSilenceCap,
	}))

	// 时间线（1s 槽）：
	//   t0..t2   越限 130            -> 事件开启
	//   t3..t5   缺失（设备摘下/丢包）-> 同一事件，标“数据不可用、结论待定”
	//   t6..t7   重连，仍越限 130     -> 同一事件继续，不产生新事件
	//   t8..t13  正常 75             -> 连续正常，t13 满 5s 事件结束
	seq := []QualityVerdict{}
	add := func(i int, class QualityClass, v float64) {
		seq = append(seq, epVerdict("HR", base.Add(time.Duration(i)*time.Second), class, v))
	}
	for i := 0; i <= 2; i++ {
		add(i, QGood, 130)
	}
	for i := 3; i <= 5; i++ {
		add(i, QMissing, 0)
	}
	for i := 6; i <= 7; i++ {
		add(i, QGood, 130)
	}
	for i := 8; i <= 13; i++ {
		add(i, QGood, 75)
	}

	var evs []EpisodeEvent
	for _, v := range seq {
		evs = append(evs, tr.Observe(v)...)
	}

	closed := tr.Episodes()
	if len(closed) != 1 {
		t.Fatalf("disconnect/reconnect must be ONE episode, got %d: %+v", len(closed), closed)
	}
	ep := closed[0]
	if ep.ID != "HR#0001" {
		t.Fatalf("stable episode id expected HR#0001, got %s", ep.ID)
	}
	if ep.Status != EpisodeClosed || !ep.End.Equal(base.Add(13*time.Second)) {
		t.Fatalf("episode must close at t13 (5s continuous normal), got status=%d end=%v", ep.Status, ep.End)
	}
	if !ep.Start.Equal(base) {
		t.Fatalf("episode start must be first breach t0, got %v", ep.Start)
	}
	// 片段序列：异常 -> 不可用(待定) -> 异常 -> 恢复观察。
	wantKinds := []EpisodeSegmentKind{SegAbnormal, SegUnavailable, SegAbnormal, SegRecovering}
	if len(ep.Segments) != len(wantKinds) {
		t.Fatalf("want %d segments, got %+v", len(wantKinds), ep.Segments)
	}
	for i, k := range wantKinds {
		if ep.Segments[i].Kind != k {
			t.Fatalf("segment %d kind want %d got %+v", i, k, ep.Segments[i])
		}
	}
	unavail := ep.Segments[1]
	if !unavail.From.Equal(base.Add(3*time.Second)) || !unavail.To.Equal(base.Add(5*time.Second)) {
		t.Fatalf("unavailable segment must cover t3..t5, got %+v", unavail)
	}
	// 生命周期事件可按编号串联：开启 -> 不可用 -> 数据恢复 -> 结束。
	var types []EpisodeEventType
	for _, e := range evs {
		if e.EpisodeID != ep.ID {
			t.Fatalf("all lifecycle events must carry the same episode id, got %+v", e)
		}
		types = append(types, e.Type)
	}
	wantTypes := []EpisodeEventType{EpisodeEvOpened, EpisodeEvUnavailable, EpisodeEvDataBack, EpisodeEvClosed}
	if len(types) != len(wantTypes) {
		t.Fatalf("lifecycle events mismatch: %+v", evs)
	}
	for i := range wantTypes {
		if types[i] != wantTypes[i] {
			t.Fatalf("lifecycle event %d want %d got %d (%+v)", i, wantTypes[i], types[i], evs)
		}
	}
	logf(t, "输入: 越限t0-2/缺失t3-5/重连仍越限t6-7/正常t8-13; 依据: 不可用不结束事件且打断连续正常计时, 连续正常满5s才结束; "+
		"结论: 同一事件 %s, 片段 异常->不可用(待定)->异常->恢复, t13 关闭, 生命周期事件 %d 条均可按编号串联",
		ep.ID, len(evs))
	for _, e := range evs {
		t.Logf("    EP-EVENT %s %s type=%d | %s", e.Time.Format("15:04:05"), e.EpisodeID, e.Type, e.Reason)
	}
}

// TestEpisodeUnavailableDoesNotCountAsRecovery 不可用段既不算恢复也不算结束：
// 正常 2s 后被伪迹打断，再正常时需重新计满 RecoverDuration。
func TestEpisodeUnavailableDoesNotCountAsRecovery(t *testing.T) {
	base := time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)
	tr := NewEpisodeTracker(epRule(), ConfigSnapshot{})

	seq := []QualityVerdict{
		epVerdict("HR", base, QGood, 130),                              // t0 越限，开事件
		epVerdict("HR", base.Add(1*time.Second), QGood, 75),            // t1 正常（计时开始）
		epVerdict("HR", base.Add(2*time.Second), QGood, 75),            // t2 正常 2s
		epVerdict("HR", base.Add(3*time.Second), QArtifactUnusable, 0), // t3 伪迹：待定，打断计时
		epVerdict("HR", base.Add(4*time.Second), QGood, 75),            // t4 正常（重新计时）
		epVerdict("HR", base.Add(5*time.Second), QGood, 75),            // t5
		epVerdict("HR", base.Add(6*time.Second), QGood, 75),            // t6
		epVerdict("HR", base.Add(7*time.Second), QGood, 75),            // t7
		epVerdict("HR", base.Add(8*time.Second), QGood, 75),            // t8
		epVerdict("HR", base.Add(9*time.Second), QGood, 75),            // t9 满5s -> 结束
	}
	for _, v := range seq {
		tr.Observe(v)
	}
	closed := tr.Episodes()
	if len(closed) != 1 {
		t.Fatalf("want exactly 1 closed episode, got %d", len(closed))
	}
	if !closed[0].End.Equal(base.Add(9 * time.Second)) {
		t.Fatalf("recovery clock must restart after artifact; close at t9, got %v", closed[0].End)
	}
	logf(t, "输入: 越限t0/正常t1-2/伪迹t3/正常t4-9; 依据: 不可用段打断连续正常计时(2s+伪迹+需重新满5s); 结论: 事件于 t9(而非 t6)结束")
}

// episodeScenario 构造一次“异常中断连重连”的完整管线场景。
func episodeScenario() (Config, []RawSample) {
	start := time.Date(2026, 9, 24, 11, 0, 0, 0, time.UTC)
	const hr ChannelID = "HR"
	const d1 DeviceID = "dev1"
	cfg := Config{
		Epoch:    start,
		SlotStep: time.Second,
		MaxDelay: 3 * time.Second,
		Channels: []ChannelSpec{
			{Device: d1, Channel: hr, Cal: Calibration{Scale: 1, Unit: "bpm"}, NominalInterval: time.Second},
		},
		Clocks: map[DeviceID]ClockParams{d1: {}},
		Quality: []QualityRule{
			{Channel: hr, ExpectedInterval: time.Second, GapTolerance: 200 * time.Millisecond, PhysMin: 30, PhysMax: 220},
		},
		EpisodeRules: []EpisodeRule{epRule()},
	}
	// seq 5..8 越限；seq 9..13 丢失（断连）；seq 14..16 重连仍越限；其后恢复正常。
	gen := func(ch ChannelID, seq int64, t time.Time) (float64, RawFlag) {
		if seq >= 5 && seq <= 8 || seq >= 14 && seq <= 16 {
			return 130, FlagOK
		}
		return 75, FlagOK
	}
	sim := NewSimulator(SimConfig{
		Device: d1, Channels: cfg.Channels,
		StartTime: start, Duration: 30 * time.Second,
		DropSeq: map[int64]bool{9: true, 10: true, 11: true, 12: true, 13: true},
	}, gen)
	return cfg, sim.Generate()
}

// runEpisodes 以批式或在线方式跑管线，返回事件档案与生命周期事件。
func runEpisodes(t *testing.T, cfg Config, packets []RawSample, online bool) ([]Episode, []EpisodeEvent) {
	t.Helper()
	p := NewPipeline(cfg)
	if !online {
		for _, pk := range packets {
			p.Ingest(pk)
		}
		p.Advance(cfg.Epoch.Add(2 * time.Minute))
		return p.Episodes(), p.EpisodeEvents()
	}
	for _, pk := range packets {
		if r := p.Ingest(pk); r.Point != nil {
			p.Advance(r.Point.Time.Add(-cfg.MaxDelay))
		}
	}
	p.Advance(cfg.Epoch.Add(2 * time.Minute))
	return p.Episodes(), p.EpisodeEvents()
}

// TestEpisodeOnlineEqualsReplay 同一批“断连重连”历史数据，在线逐包与离线重放
// 产出的事件档案（条数、编号、起止、片段）与生命周期事件必须逐条一致。
func TestEpisodeOnlineEqualsReplay(t *testing.T) {
	cfg, packets := episodeScenario()

	batchEp, batchEv := runEpisodes(t, cfg, packets, false)
	onlineEp, onlineEv := runEpisodes(t, cfg, packets, true)

	if !episodesEqual(batchEp, onlineEp) {
		t.Fatalf("episodes diverge:\n batch=%+v\n online=%+v", batchEp, onlineEp)
	}
	if !episodeEventsEqual(batchEv, onlineEv) {
		t.Fatalf("episode events diverge:\n batch=%+v\n online=%+v", batchEv, onlineEv)
	}
	// 结论性断言：恰好一次事件；断连段标不可用；连续正常满 5s 后结束。
	if len(onlineEp) != 1 {
		t.Fatalf("want exactly 1 episode across disconnect, got %+v", onlineEp)
	}
	ep := onlineEp[0]
	if ep.Status != EpisodeClosed {
		t.Fatalf("episode must be closed after sustained recovery, got %+v", ep)
	}
	var hasUnavailable bool
	for _, s := range ep.Segments {
		if s.Kind == SegUnavailable {
			hasUnavailable = true
		}
	}
	if !hasUnavailable {
		t.Fatalf("disconnect span must be marked unavailable/pending, got %+v", ep.Segments)
	}
	// 恢复从 seq17 开始，满 5s 于 seq22 结束。
	wantEnd := cfg.Epoch.Add(22 * time.Second)
	if !ep.End.Equal(wantEnd) {
		t.Fatalf("episode must close at %v (5s continuous normal), got %v", wantEnd, ep.End)
	}
	logf(t, "输入: seq5-8越限/seq9-13断连丢失/seq14-16重连仍越限/seq17起正常; 依据: 同一事件跨中断不拆分, 不可用段待定, 连续正常满5s结束; "+
		"结论: 在线与重放各产出 1 次事件 %s 且逐条一致, 结束于 %v", ep.ID, ep.End.Format("15:04:05"))
	for _, s := range ep.Segments {
		t.Logf("    SEGMENT kind=%d [%s, %s]", s.Kind, s.From.Format("15:04:05"), s.To.Format("15:04:05"))
	}
}

func episodesEqual(a, b []Episode) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.ID != y.ID || x.Channel != y.Channel || x.Status != y.Status ||
			!x.Start.Equal(y.Start) || !x.End.Equal(y.End) ||
			len(x.Segments) != len(y.Segments) ||
			x.Snapshot.RuleVersion != y.Snapshot.RuleVersion {
			return false
		}
		for j := range x.Segments {
			if x.Segments[j] != y.Segments[j] {
				return false
			}
		}
	}
	return true
}

func episodeEventsEqual(a, b []EpisodeEvent) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.EpisodeID != y.EpisodeID || x.Channel != y.Channel || x.Type != y.Type ||
			!x.Time.Equal(y.Time) || x.Reason != y.Reason {
			return false
		}
	}
	return true
}

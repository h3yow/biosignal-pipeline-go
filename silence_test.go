package pipeline

import (
	"testing"
	"time"
)

// silenceConfig 单通道管线：t0 起 1s 采样，可选告警/事件规则与静默上限。
func silenceConfig(cap time.Duration) Config {
	start := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	const hr ChannelID = "HR"
	const d1 DeviceID = "dev1"
	return Config{
		Epoch:      start,
		SlotStep:   time.Second,
		MaxDelay:   3 * time.Second,
		SilenceCap: cap,
		Channels: []ChannelSpec{
			{Device: d1, Channel: hr, Cal: Calibration{Scale: 1, Unit: "bpm"}, NominalInterval: time.Second},
		},
		Clocks: map[DeviceID]ClockParams{d1: {}},
		Quality: []QualityRule{
			{Channel: hr, ExpectedInterval: time.Second, GapTolerance: 200 * time.Millisecond, PhysMin: 30, PhysMax: 220},
		},
		AlertRules: []AlertRule{
			{ID: "hr-high", Version: "cfg-1", Channel: hr, HighEnabled: true, HighThreshold: 120, TriggerN: 2, RecoverN: 2},
		},
		EpisodeRules: []EpisodeRule{
			{ID: "hr-episode", Version: "ep-1", Channel: hr, HighEnabled: true, HighThreshold: 120, RecoverDuration: 5 * time.Second},
		},
	}
}

// silencePackets 4 个越限包（t0..t3, 130）之后彻底静默（设备摘下/采集结束）。
func silencePackets(cfg Config) []RawSample {
	gen := func(ch ChannelID, seq int64, t time.Time) (float64, RawFlag) { return 130, FlagOK }
	return NewSimulator(SimConfig{
		Device: cfg.Channels[0].Device, Channels: cfg.Channels,
		StartTime: cfg.Epoch, Duration: 3 * time.Second,
	}, gen).Generate()
}

// TestTailSilenceMarkedMissing 设备摘下不再上报后，把时间往后推：
// 尾段必须被标成缺失（告警被强制恢复、指标窗在最后实际点关闭），
// 而不是看起来“一直正常”。
func TestTailSilenceMarkedMissing(t *testing.T) {
	cfg := silenceConfig(0) // 未配置上限：按 DefaultSilenceCap(5min) 生效，推进 60s 仍在界内
	p := NewPipeline(cfg)
	for _, pk := range silencePackets(cfg) {
		p.Ingest(pk)
	}
	// 先把 t0..t3 评估掉：告警应在 t1 触发（连续2槽越限）。
	_, evs := p.Advance(cfg.Epoch.Add(3 * time.Second))
	if len(evs) != 1 || evs[0].To != AlertActive {
		t.Fatalf("alert must fire on breach, got %+v", evs)
	}
	// 之后设备摘下，推进到 +60s：尾段静默应被补成缺失。
	windows, evs2 := p.Advance(cfg.Epoch.Add(60 * time.Second))
	windows = append(windows, p.Flush()...)

	var forced *AlertEvent
	for i := range evs2 {
		if evs2[i].From == AlertActive && evs2[i].To == AlertInactive {
			forced = &evs2[i]
		}
	}
	if forced == nil {
		t.Fatalf("tail silence must force-recover active alert, got %+v", evs2)
	}
	// 强制恢复发生在第一个静默槽 t4，证明最后一段被标成“没有数据”。
	wantAt := cfg.Epoch.Add(4 * time.Second)
	if !forced.Time.Equal(wantAt) {
		t.Fatalf("forced recovery must happen at first silent slot %v, got %v", wantAt, forced.Time)
	}
	// 指标窗在最后实际点 t3 关闭，不会被静默段“续命”。
	if len(windows) != 1 || !windows[0].End.Equal(cfg.Epoch.Add(3*time.Second)) {
		t.Fatalf("metric window must close at last real point t3, got %+v", windows)
	}
	// 事件不结束：静默段是“数据不可用、结论待定”，不是恢复。
	ep := p.Episodes()
	if len(ep) != 1 || ep[0].Status != EpisodeOpen {
		t.Fatalf("episode must stay open (pending) through silence, got %+v", ep)
	}
	last := ep[0].Segments[len(ep[0].Segments)-1]
	if last.Kind != SegUnavailable || !last.To.Equal(cfg.Epoch.Add(60*time.Second)) {
		t.Fatalf("tail must be marked unavailable up to advance horizon, got %+v", last)
	}
	logf(t, "输入: t0-3 越限130 后设备摘下, 推进到 t+60s; 依据: 尾段静默按槽补缺失, 缺失强制恢复告警并关指标窗; "+
		"结论: 告警于 t4 强制恢复, 指标窗关闭于 t3, 事件保持开放且 t4-60 标记为数据不可用(待定)")
}

// TestSilenceCapTruncation 静默超过配置上限后：补点截断、给出明确截断结论，
// 且反复推进不会重复记录或无限向下补。
func TestSilenceCapTruncation(t *testing.T) {
	cfg := silenceConfig(10 * time.Second)
	p := NewPipeline(cfg)
	for _, pk := range silencePackets(cfg) {
		p.Ingest(pk)
	}
	p.Advance(cfg.Epoch.Add(3 * time.Second))
	p.Advance(cfg.Epoch.Add(1 * time.Hour)) // 远超上限

	truncs := p.Truncations()
	if len(truncs) != 1 {
		t.Fatalf("want exactly 1 truncation conclusion, got %+v", truncs)
	}
	tr := truncs[0]
	// 最后数据 t3，上限 10s -> 截断位置 t13。
	wantAt := cfg.Epoch.Add(13 * time.Second)
	if tr.Kind != TruncationTail {
		t.Fatalf("tail silence must yield TruncationTail, got kind=%d", tr.Kind)
	}
	if !tr.LastDataTime.Equal(cfg.Epoch.Add(3*time.Second)) || !tr.TruncatedAt.Equal(wantAt) {
		t.Fatalf("truncation position wrong: %+v (want last=t3 at=%v)", tr, wantAt)
	}
	// 截断结论带机器可读快照：当时生效的上限值必须能直接取到。
	if tr.Snapshot.SilenceCap != 10*time.Second {
		t.Fatalf("truncation snapshot must pin effective cap 10s, got %+v", tr.Snapshot)
	}
	// 不可用段补到截断位置为止，不再向下。
	ep := p.Episodes()
	if len(ep) != 1 {
		t.Fatalf("want 1 open episode, got %+v", ep)
	}
	last := ep[0].Segments[len(ep[0].Segments)-1]
	if last.Kind != SegUnavailable || !last.To.Equal(wantAt) {
		t.Fatalf("unavailable tail must stop at truncation point %v, got %+v", wantAt, last)
	}
	// 再次推进到 +2h：不重复记录、不再向下补（结论稳定）。
	p.Advance(cfg.Epoch.Add(2 * time.Hour))
	if got := len(p.Truncations()); got != 1 {
		t.Fatalf("re-advancing must not duplicate truncation, got %d", got)
	}
	ep2 := p.Episodes()
	last2 := ep2[0].Segments[len(ep2[0].Segments)-1]
	if !last2.To.Equal(wantAt) {
		t.Fatalf("filling must stay truncated at %v after re-advance, got %v", wantAt, last2.To)
	}
	logf(t, "输入: t0-3 越限后静默, SilenceCap=10s, 推进到 +1h 再 +2h; 依据: 缺失填充上限=最后实际点+10s; "+
		"结论: 截断结论 1 条(最后数据 t3, 截断于 t13), 不可用段止于 t13, 再推进不重复不延伸")
}

// TestSilenceCapConfigurable 上限由使用方配置：改大改小截断位置随之确定性地变化；
// 未配置（<=0）时不退化为无边界填充，而是按 DefaultSilenceCap 截断并给出结论；
// 相同配置重复运行结论逐条一致（可复现）。
func TestSilenceCapConfigurable(t *testing.T) {
	run := func(cap time.Duration) []SilenceTruncation {
		cfg := silenceConfig(cap)
		p := NewPipeline(cfg)
		for _, pk := range silencePackets(cfg) {
			p.Ingest(pk)
		}
		p.Advance(cfg.Epoch.Add(3 * time.Second))
		p.Advance(cfg.Epoch.Add(1 * time.Hour))
		return p.Truncations()
	}
	epoch := silenceConfig(0).Epoch

	small := run(5 * time.Second)
	large := run(20 * time.Second)
	if len(small) != 1 || len(large) != 1 {
		t.Fatalf("both caps must yield exactly 1 truncation, got %+v / %+v", small, large)
	}
	if !small[0].TruncatedAt.Equal(epoch.Add(8 * time.Second)) {
		t.Fatalf("cap=5s must truncate at t3+5s=t8, got %v", small[0].TruncatedAt)
	}
	if !large[0].TruncatedAt.Equal(epoch.Add(23 * time.Second)) {
		t.Fatalf("cap=20s must truncate at t3+20s=t23, got %v", large[0].TruncatedAt)
	}
	// 未配置上限：不退化为无边界——按 DefaultSilenceCap 截断并给出明确结论，
	// 快照中记录的是解析后的实际生效值。
	unset := run(0)
	if len(unset) != 1 {
		t.Fatalf("unset cap must fall back to default cap and still conclude, got %+v", unset)
	}
	if want := epoch.Add(3*time.Second + DefaultSilenceCap); !unset[0].TruncatedAt.Equal(want) {
		t.Fatalf("unset cap must truncate at t3+DefaultSilenceCap=%v, got %v", want, unset[0].TruncatedAt)
	}
	if unset[0].Snapshot.SilenceCap != DefaultSilenceCap {
		t.Fatalf("snapshot must record effective default cap %v, got %+v",
			DefaultSilenceCap, unset[0].Snapshot)
	}
	// 可复现：相同配置再跑一遍，结论逐条一致。
	again := run(5 * time.Second)
	if len(again) != 1 || !again[0].TruncatedAt.Equal(small[0].TruncatedAt) ||
		!again[0].LastDataTime.Equal(small[0].LastDataTime) || again[0].Kind != small[0].Kind ||
		again[0].Snapshot != small[0].Snapshot {
		t.Fatalf("same cap must reproduce identical truncation, got %+v vs %+v", again, small)
	}
	logf(t, "输入: 同一批 t0-3 后静默的数据, SilenceCap=5s/20s/未配置; 依据: 截断位置=最后实际点+生效上限(未配置取默认 %v); "+
		"结论: 5s->t8, 20s->t23, 未配置->t3+%v 且快照记录生效上限; 同配置重跑结论逐条一致",
		DefaultSilenceCap, DefaultSilenceCap)
}

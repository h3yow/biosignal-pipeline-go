package pipeline

import (
	"testing"
	"time"
)

// gapConfig 单通道管线：1s 采样，质量规则带身份/版本，可配静默上限。
// 用于“中途长时间断连再回来”的场景。
func gapConfig(cap time.Duration, qVer string) Config {
	start := time.Date(2026, 9, 24, 15, 0, 0, 0, time.UTC)
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
			{ID: "hr-quality", Version: qVer, Channel: hr,
				ExpectedInterval: time.Second, GapTolerance: 200 * time.Millisecond,
				PhysMin: 30, PhysMax: 220},
		},
		AlertRules: []AlertRule{
			{ID: "hr-high", Version: "cfg-1", Channel: hr, HighEnabled: true, HighThreshold: 120, TriggerN: 2, RecoverN: 2},
		},
		EpisodeRules: []EpisodeRule{
			{ID: "hr-episode", Version: "ep-1", Channel: hr, HighEnabled: true, HighThreshold: 120, RecoverDuration: 5 * time.Second},
		},
	}
}

// gapPackets t0..t3 越限(130) → 设备摘下 28 秒 → t31..t35 重连且恢复正常(75)。
func gapPackets(cfg Config) []RawSample {
	gen := func(ch ChannelID, seq int64, t time.Time) (float64, RawFlag) {
		if seq <= 3 {
			return 130, FlagOK
		}
		return 75, FlagOK
	}
	return NewSimulator(SimConfig{
		Device: cfg.Channels[0].Device, Channels: cfg.Channels,
		StartTime: cfg.Epoch, Duration: 8 * time.Second,
		GapAfterSeq: 3, Gap: 27 * time.Second, // seq4 起整体后移：t3 之后下一点是 t31
	}, gen).Generate()
}

// gapRun 批量重放：全部灌入后推进到 +40s（尾段静默 5s，小于所有用例的上限，
// 不会再产生尾段截断），返回全部结论。
type gapResult struct {
	windows []MetricWindow
	events  []AlertEvent
	truncs  []SilenceTruncation
	eps     []Episode
}

func gapRunBatch(cfg Config, packets []RawSample) gapResult {
	p := NewPipeline(cfg)
	for _, pk := range packets {
		p.Ingest(pk)
	}
	var r gapResult
	w1, e1 := p.Advance(cfg.Epoch.Add(3 * time.Second))
	w2, e2 := p.Advance(cfg.Epoch.Add(40 * time.Second))
	r.windows = append(append(w1, w2...), p.Flush()...)
	r.events = append(e1, e2...)
	r.truncs = p.Truncations()
	r.eps = p.Episodes()
	return r
}

// gapRunOnline 在线逐包：每收到一个包就把水位推进到“该包统一时间 - MaxDelay”。
func gapRunOnline(cfg Config, packets []RawSample) gapResult {
	p := NewPipeline(cfg)
	var r gapResult
	for _, pk := range packets {
		res := p.Ingest(pk)
		if res.Point != nil {
			w, e := p.Advance(res.Point.Time.Add(-cfg.MaxDelay))
			r.windows = append(r.windows, w...)
			r.events = append(r.events, e...)
		}
	}
	w, e := p.Advance(cfg.Epoch.Add(40 * time.Second))
	r.windows = append(append(r.windows, w...), p.Flush()...)
	r.events = append(r.events, e...)
	r.truncs = p.Truncations()
	r.eps = p.Episodes()
	return r
}

func truncsEqual(a, b []SilenceTruncation) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.Channel != y.Channel || x.Kind != y.Kind ||
			!x.LastDataTime.Equal(y.LastDataTime) || !x.TruncatedAt.Equal(y.TruncatedAt) ||
			!x.GapEnd.Equal(y.GapEnd) || x.Snapshot != y.Snapshot {
			return false
		}
	}
	return true
}

// TestMidGapDisconnectReconnectTruncation 中途长时间断连再回来：
// 中间空洞与尾段受同一个上限约束——空洞跨度超过上限时，缺失填充在
// “断连前最后点 + 上限”处截断，给出 Kind=MidGap 的明确结论（截断位置、
// 空洞结束位置、当时生效的配置快照都可直接读取），空洞其余部分不再补点。
func TestMidGapDisconnectReconnectTruncation(t *testing.T) {
	cfg := gapConfig(10*time.Second, "q-1")
	r := gapRunBatch(cfg, gapPackets(cfg))
	epoch := cfg.Epoch
	at := func(s int) time.Time { return epoch.Add(time.Duration(s) * time.Second) }

	// 恰好 1 条截断结论：中间空洞在 t13(=t3+10s) 截断，空洞结束于重连点 t31。
	if len(r.truncs) != 1 {
		t.Fatalf("want exactly 1 truncation for the mid-stream gap, got %+v", r.truncs)
	}
	tr := r.truncs[0]
	if tr.Kind != TruncationMidGap {
		t.Fatalf("mid-stream hole must yield TruncationMidGap, got kind=%d", tr.Kind)
	}
	if !tr.LastDataTime.Equal(at(3)) || !tr.TruncatedAt.Equal(at(13)) || !tr.GapEnd.Equal(at(31)) {
		t.Fatalf("truncation position wrong: last=%v at=%v gapEnd=%v (want t3/t13/t31)",
			tr.LastDataTime, tr.TruncatedAt, tr.GapEnd)
	}
	// 结论带机器可读快照：规则身份/版本、换算参数、当时生效的上限值。
	s := tr.Snapshot
	if s.RuleID != "hr-quality" || s.RuleVersion != "q-1" ||
		s.Cal.Scale != 1 || s.Cal.Unit != "bpm" || s.SilenceCap != 10*time.Second {
		t.Fatalf("truncation must pin full config snapshot, got %+v", s)
	}

	// 空洞只补到截断位置：事件的“数据不可用”段止于 t13，而不是一路补到重连点 t31。
	if len(r.eps) != 1 {
		t.Fatalf("want 1 episode, got %+v", r.eps)
	}
	segs := r.eps[0].Segments
	if len(segs) < 2 || segs[1].Kind != SegUnavailable ||
		!segs[1].From.Equal(at(4)) || !segs[1].To.Equal(at(13)) {
		t.Fatalf("unavailable segment must stop at truncation point t13, got %+v", segs)
	}

	// 告警：t1 触发（连续2槽越限），t4 被第一个缺失槽强制恢复，不多不少。
	if len(r.events) != 2 || r.events[0].To != AlertActive ||
		r.events[1].From != AlertActive || r.events[1].To != AlertInactive ||
		!r.events[1].Time.Equal(at(4)) {
		t.Fatalf("alert must fire at t1 and force-recover at t4, got %s", fmtEvents(r.events))
	}

	// 指标窗：断连前 t0..t3 一窗，重连后 t31..t35 新窗且 GapBefore=28s；
	// 窗结论同样携带规则版本与生效上限的快照。
	if len(r.windows) != 2 {
		t.Fatalf("want 2 metric windows, got %+v", r.windows)
	}
	w1, w2 := r.windows[0], r.windows[1]
	if !w1.Start.Equal(at(0)) || !w1.End.Equal(at(3)) || w1.Count != 4 {
		t.Fatalf("window before disconnect must be t0..t3 count=4, got %+v", w1)
	}
	if !w2.Start.Equal(at(31)) || !w2.End.Equal(at(35)) || w2.GapBefore != 28*time.Second {
		t.Fatalf("window after reconnect must start at t31 with GapBefore=28s, got %+v", w2)
	}
	if w2.Snapshot.RuleVersion != "q-1" || w2.Snapshot.SilenceCap != 10*time.Second ||
		w2.Snapshot.Cal.Scale != 1 {
		t.Fatalf("metric window must pin quality rule version and effective cap, got %+v", w2.Snapshot)
	}
	logf(t, "输入: t0-3 越限130 后断连28s, t31-35 重连恢复75, SilenceCap=10s; "+
		"依据: 中间空洞与尾段同上限, 填充到 最后点+上限 截断并记 MidGap 结论; "+
		"结论: 截断于 t13(空洞终于 t31), 不可用段 t4-13, 告警 t1 触发/t4 强制恢复, "+
		"指标窗 t0-3 与 t31-35(GapBefore=28s), 结论快照含规则 q-1 与生效上限 10s")
}

// TestMidGapOnlineEqualsReplay 同一批“断连再回来”的历史数据：
// 在线逐包处理（先按尾段截断、重连后订正为 MidGap）与一次性重放，
// 截断结论、告警事件、指标窗、事件档案必须逐条一致。
func TestMidGapOnlineEqualsReplay(t *testing.T) {
	cfg := gapConfig(10*time.Second, "q-1")
	packets := gapPackets(cfg)

	batch := gapRunBatch(cfg, packets)
	online := gapRunOnline(cfg, packets)

	if len(online.truncs) != 1 || online.truncs[0].Kind != TruncationMidGap {
		t.Fatalf("online must converge to a single MidGap truncation, got %+v", online.truncs)
	}
	if !truncsEqual(batch.truncs, online.truncs) {
		t.Fatalf("truncations diverge:\n batch=%+v\n online=%+v", batch.truncs, online.truncs)
	}
	if !eventsEqual(batch.events, online.events) {
		t.Fatalf("alert events diverge:\n batch=%s\n online=%s",
			fmtEvents(batch.events), fmtEvents(online.events))
	}
	if !windowsEqual(batch.windows, online.windows) {
		t.Fatalf("metric windows diverge:\n batch=%+v\n online=%+v", batch.windows, online.windows)
	}
	if !episodesEqual(batch.eps, online.eps) {
		t.Fatalf("episodes diverge:\n batch=%+v\n online=%+v", batch.eps, online.eps)
	}
	logf(t, "输入: 同一批 t0-3 越限后断连28s再重连的数据, 分别在线逐包与一次性重放; "+
		"依据: 截断结论按静默段去重, 重连闭合空洞时尾段结论订正为 MidGap(位置不变); "+
		"结论: 两条路径的截断结论(含快照)/告警事件/指标窗/事件档案逐条一致")
}

// TestMidGapCapConfigurable 上限调大调小，中间空洞的截断位置确定性地跟着变；
// 上限大于空洞跨度则不截断；相同上限重复跑结论逐条一致。
func TestMidGapCapConfigurable(t *testing.T) {
	epoch := gapConfig(0, "q-1").Epoch
	at := func(s int) time.Time { return epoch.Add(time.Duration(s) * time.Second) }
	run := func(cap time.Duration) []SilenceTruncation {
		cfg := gapConfig(cap, "q-1")
		return gapRunBatch(cfg, gapPackets(cfg)).truncs
	}

	small, large := run(5*time.Second), run(20*time.Second)
	if len(small) != 1 || !small[0].TruncatedAt.Equal(at(8)) {
		t.Fatalf("cap=5s must truncate mid-gap at t3+5s=t8, got %+v", small)
	}
	if len(large) != 1 || !large[0].TruncatedAt.Equal(at(23)) {
		t.Fatalf("cap=20s must truncate mid-gap at t3+20s=t23, got %+v", large)
	}
	// 上限 40s 大于 28s 空洞：不截断，空洞被完整填充。
	if uncapped := run(40 * time.Second); len(uncapped) != 0 {
		t.Fatalf("cap larger than the 28s gap must not truncate, got %+v", uncapped)
	}
	// 可复现：相同上限重跑，结论逐条一致。
	if again := run(5 * time.Second); !truncsEqual(again, small) {
		t.Fatalf("same cap must reproduce identical truncation, got %+v vs %+v", again, small)
	}
	logf(t, "输入: 同一批 t3 后断连28s再重连的数据, SilenceCap=5s/20s/40s; "+
		"依据: 截断位置=断连前最后点+生效上限; 结论: 5s->t8, 20s->t23, 40s>空洞不截断; 同配置重跑一致")
}

// TestQualityRuleVersionReplay 质量规则/上限改版后：历史结论（带着当时的
// 规则版本与上限快照）不被改写；用当时的版本与上限重放同一批数据，
// 逐条还原出一样的结论。
func TestQualityRuleVersionReplay(t *testing.T) {
	cfgV1 := gapConfig(10*time.Second, "q-1")
	packets := gapPackets(cfgV1)
	epoch := cfgV1.Epoch
	at := func(s int) time.Time { return epoch.Add(time.Duration(s) * time.Second) }

	v1 := gapRunBatch(cfgV1, packets)
	if len(v1.truncs) != 1 || !v1.truncs[0].TruncatedAt.Equal(at(13)) {
		t.Fatalf("v1(cap=10s) must truncate at t13, got %+v", v1.truncs)
	}
	if s := v1.truncs[0].Snapshot; s.RuleVersion != "q-1" || s.SilenceCap != 10*time.Second {
		t.Fatalf("v1 truncation must pin q-1 and cap=10s, got %+v", s)
	}

	// 改版：规则版本 q-2、上限 20s——同一批数据得出新位置与新快照。
	cfgV2 := gapConfig(20*time.Second, "q-2")
	v2 := gapRunBatch(cfgV2, packets)
	if len(v2.truncs) != 1 || !v2.truncs[0].TruncatedAt.Equal(at(23)) {
		t.Fatalf("v2(cap=20s) must truncate at t23, got %+v", v2.truncs)
	}
	if s := v2.truncs[0].Snapshot; s.RuleVersion != "q-2" || s.SilenceCap != 20*time.Second {
		t.Fatalf("v2 truncation must pin q-2 and cap=20s, got %+v", s)
	}
	// 历史结论不被改写：v1 的运行结果仍钉在 q-1/t13。
	if !v1.truncs[0].TruncatedAt.Equal(at(13)) || v1.truncs[0].Snapshot.RuleVersion != "q-1" {
		t.Fatalf("historical v1 conclusion must not be rewritten, got %+v", v1.truncs[0])
	}

	// 用当时的版本与上限重放：截断结论与指标窗快照逐条还原。
	replay := gapRunBatch(cfgV1, packets)
	if !truncsEqual(replay.truncs, v1.truncs) {
		t.Fatalf("replay with q-1/cap=10s must reproduce truncations:\n v1=%+v\n replay=%+v",
			v1.truncs, replay.truncs)
	}
	if !windowsEqual(replay.windows, v1.windows) {
		t.Fatalf("replay must reproduce metric windows:\n v1=%+v\n replay=%+v",
			v1.windows, replay.windows)
	}
	for i := range v1.windows {
		if replay.windows[i].Snapshot != v1.windows[i].Snapshot {
			t.Fatalf("window snapshot must reproduce exactly: %+v vs %+v",
				replay.windows[i].Snapshot, v1.windows[i].Snapshot)
		}
	}
	logf(t, "输入: 同一批断连重连数据, 分别用 q-1/cap=10s 与 q-2/cap=20s 处理, 再用 q-1 重放; "+
		"依据: 结论快照钉住规则版本与生效上限; 结论: v1->t13(q-1), v2->t23(q-2), "+
		"历史不改写, q-1 重放逐条还原(含快照)")
}

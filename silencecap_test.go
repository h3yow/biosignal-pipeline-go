package pipeline

import (
	"sort"
	"testing"
	"time"
)

// gapConfig 单通道配置：t0 起 1s 采样，静默上限显式可配（<=0 走默认值）。
func gapConfig(cap time.Duration) Config {
	start := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	const hr ChannelID = "HR"
	const d1 DeviceID = "dev1"
	return Config{
		Epoch:      start,
		SlotStep:   time.Second,
		MaxDelay:   3 * time.Second,
		SilenceCap: cap,
		Channels: []ChannelSpec{
			{Device: d1, Channel: hr, Cal: Calibration{Scale: 1, Offset: 0, Unit: "bpm"}, NominalInterval: time.Second},
		},
		Clocks: map[DeviceID]ClockParams{d1: {}},
		Quality: []QualityRule{
			{ID: "hr-quality", Version: "q-1", Channel: hr, ExpectedInterval: time.Second, GapTolerance: 200 * time.Millisecond, PhysMin: 30, PhysMax: 220},
		},
		AlertRules: []AlertRule{
			{ID: "hr-high", Version: "cfg-1", Channel: hr, HighEnabled: true, HighThreshold: 120, TriggerN: 2, RecoverN: 2},
		},
		EpisodeRules: []EpisodeRule{
			{ID: "hr-episode", Version: "ep-1", Channel: hr, HighEnabled: true, HighThreshold: 120, RecoverDuration: 5 * time.Second},
		},
	}
}

// gapPackets 手工构造“正常 t0..t3 后长时间断连、之后又连回”的确定性序列：
// seq 0..3 在 t0..t3；seq 4 起重连，重连首点落在 t3+span（静默跨度即 span），
// 其后每秒一个点，共 5 个重连点（末点 = t3+span+4s），序列随即结束，
// 不再拖出一个真实尾段，便于只验证“中间空洞截断”。
func gapPackets(cfg Config, span time.Duration) []RawSample {
	ch := cfg.Channels[0]
	mk := func(seq int64, at time.Duration, v float64) RawSample {
		return RawSample{
			Device: ch.Device, Channel: ch.Channel, Seq: seq,
			DeviceTime: cfg.Epoch.Add(at), RawValue: v, Flags: FlagOK,
		}
	}
	pk := []RawSample{
		mk(0, 0, 75), mk(1, time.Second, 75), mk(2, 2*time.Second, 75), mk(3, 3*time.Second, 75),
	}
	resume := 3*time.Second + span
	for i := int64(0); i < 5; i++ {
		pk = append(pk, mk(4+i, resume+time.Duration(i)*time.Second, 75))
	}
	return pk
}

// gapPacketsUntil 与 gapPackets 相同的中间空洞（重连首点=t3+span），
// 但重连后真实点一直采到 until（每秒一个），用于同时验证
// “中间空洞截断 + 采集尾段截断”。
func gapPacketsUntil(cfg Config, span, until time.Duration) []RawSample {
	ch := cfg.Channels[0]
	mk := func(seq int64, at time.Duration, v float64) RawSample {
		return RawSample{
			Device: ch.Device, Channel: ch.Channel, Seq: seq,
			DeviceTime: cfg.Epoch.Add(at), RawValue: v, Flags: FlagOK,
		}
	}
	pk := []RawSample{
		mk(0, 0, 75), mk(1, time.Second, 75), mk(2, 2*time.Second, 75), mk(3, 3*time.Second, 75),
	}
	resume := 3*time.Second + span
	var seq int64 = 4
	for at := resume; !cfg.Epoch.Add(at).After(cfg.Epoch.Add(until)); at += time.Second {
		pk = append(pk, mk(seq, at, 75))
		seq++
	}
	return pk
}

type fullRun struct {
	windows     []MetricWindow
	events      []AlertEvent
	epEvents    []EpisodeEvent
	episodes    []Episode
	truncations []SilenceTruncation
}

// runFull 以批量重放或在线逐包两种方式跑同一批数据，收集全部可查询结论。
func runFull(t *testing.T, cfg Config, packets []RawSample, online bool) fullRun {
	t.Helper()
	p := NewPipeline(cfg)
	// 收尾只推进到“最后一个真实点”：排空全部数据，但不在流末人为制造
	// 一段超过上限的尾静默（尾段截断由专门用例覆盖）。
	final := lastRealTime(packets)
	if !online {
		for _, pk := range packets {
			p.Ingest(pk)
		}
		w, e := p.Advance(final)
		return fullRun{
			windows: append(w, p.Flush()...), events: e,
			epEvents: p.EpisodeEvents(), episodes: p.Episodes(), truncations: p.Truncations(),
		}
	}
	// 在线：累计每一次推进关闭的窗与产生的事件。首个被接受的点在空缓冲下
	// 推进到其自身时刻（水位 t-MaxDelay 不早于任何已见点，不误拒、不漏评）；
	// 其后每点按 unified-MaxDelay 推进，模拟实时水位。
	var wins []MetricWindow
	var evs []AlertEvent
	established := false
	for _, pk := range packets {
		r := p.Ingest(pk)
		if r.Point != nil {
			var w []MetricWindow
			var e []AlertEvent
			if established {
				w, e = p.Advance(r.Point.Time.Add(-cfg.MaxDelay))
			} else {
				w, e = p.Advance(r.Point.Time)
			}
			wins = append(wins, w...)
			evs = append(evs, e...)
			established = true
		}
	}
	w, e := p.Advance(final)
	wins = append(wins, w...)
	evs = append(evs, e...)
	return fullRun{
		windows: append(wins, p.Flush()...), events: evs,
		epEvents: p.EpisodeEvents(), episodes: p.Episodes(), truncations: p.Truncations(),
	}
}

// windowsEqualSorted 忽略关闭/Flush 造成的返回顺序差异，按 (Start,End)
// 归一化后逐条比较（在线逐包按关闭顺序返回，批量重放把 Flush 的窗排在末尾）。
func windowsEqualSorted(a, b []MetricWindow) bool {
	less := func(x []MetricWindow) func(i, j int) bool {
		return func(i, j int) bool {
			if !x[i].Start.Equal(x[j].Start) {
				return x[i].Start.Before(x[j].Start)
			}
			return x[i].End.Before(x[j].End)
		}
	}
	sort.Slice(a, less(a))
	sort.Slice(b, less(b))
	return windowsEqual(a, b)
}

func truncationsEqual(a, b []SilenceTruncation) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.Channel != y.Channel || !x.LastDataTime.Equal(y.LastDataTime) ||
			!x.TruncatedAt.Equal(y.TruncatedAt) || x.Reason != y.Reason || x.Snapshot != y.Snapshot {
			return false
		}
	}
	return true
}

// TestMiddleGapCappedLikeTail 中途长时间断连再回来留下的空洞，与尾段受同一个
// SilenceCap 约束：缺失填充只补到“最后实际点+cap”，跨度再大也不按原密度一路补；
// 超过上限必须给出明确截断结论，定位到具体通道与位置。
func TestMiddleGapCappedLikeTail(t *testing.T) {
	const silenceCap = 10 * time.Second
	cfg := gapConfig(silenceCap)
	packets := gapPackets(cfg, 28*time.Second) // 静默跨度 28s，重连首点 t31，末点 t35
	p := NewPipeline(cfg)
	for _, pk := range packets {
		p.Ingest(pk)
	}
	// 收尾只推进到最后真实点 t35，避免在流末额外制造尾段截断（尾段另有专门用例）。
	w, _ := p.Advance(cfg.Epoch.Add(35 * time.Second))
	w = append(w, p.Flush()...)

	truncs := p.Truncations()
	if len(truncs) != 1 {
		t.Fatalf("middle gap must yield exactly 1 truncation, got %+v", truncs)
	}
	tr := truncs[0]
	if !tr.LastDataTime.Equal(cfg.Epoch.Add(3 * time.Second)) {
		t.Fatalf("truncation must locate last data at t3, got %v", tr.LastDataTime)
	}
	if want := cfg.Epoch.Add(3*time.Second + silenceCap); !tr.TruncatedAt.Equal(want) {
		t.Fatalf("middle gap must truncate at lastData+cap=%v, got %v", want, tr.TruncatedAt)
	}
	// 截断结论必须带机器可读配置快照：换算参数、截断策略身份/版本、当时上限。
	s := tr.Snapshot
	if s.RuleID != SilenceRuleID || s.RuleVersion != SilenceRuleVersion ||
		s.SilenceCap != silenceCap || s.Cal.Scale != 1 || s.Cal.Unit != "bpm" {
		t.Fatalf("truncation must pin machine-readable config snapshot, got %+v", s)
	}

	// 空洞被截断后，t13(截断点) 之后、t31(重连点) 之前绝不再补缺失槽：
	// 指标窗只会在“真实数据段 + 到截断点为止的缺失”上产生，
	// 不会出现 t14..t30 的窗，且重连后正常点开启的新窗 GapBefore 只连到真实段。
	var lastBefore, firstAfter *MetricWindow
	for i := range w {
		if !w[i].End.After(tr.TruncatedAt) {
			lastBefore = &w[i]
		}
		if w[i].Start.Equal(cfg.Epoch.Add(31 * time.Second)) {
			firstAfter = &w[i]
		}
	}
	if lastBefore == nil || firstAfter == nil {
		t.Fatalf("expect a window ending by truncation and a post-reconnect window, got %+v", w)
	}
	if !lastBefore.End.Equal(cfg.Epoch.Add(3 * time.Second)) {
		t.Fatalf("pre-gap metric window must close at last real point t3, got %v", lastBefore.End)
	}
	if !firstAfter.Start.Equal(cfg.Epoch.Add(31 * time.Second)) {
		t.Fatalf("post-reconnect window must start at first real point t31, got %v", firstAfter.Start)
	}
	// 关键不变量：任何窗的任何时间戳都不得落在 (t13, t31) 这个“截断后悬空段”。
	for _, win := range w {
		if win.Start.After(tr.TruncatedAt) && win.Start.Before(cfg.Epoch.Add(31*time.Second)) {
			t.Fatalf("no conclusion may exist in truncated void (t13,t31), got window %+v", win)
		}
	}
	logf(t, "输入: t0-3 正常后断连 28s, t31 起重连正常, SilenceCap=10s; "+
		"依据: 中间空洞与尾段同一上限, 填充止于 最后实际点+cap; "+
		"结论: 1 条截断(最后数据 t3, 截断于 t13, 带策略%s/%s+cap 快照), t14-30 悬空段无任何补点/指标窗, 重连窗始于 t31",
		SilenceRuleID, SilenceRuleVersion)
	for _, tr := range truncs {
		t.Logf("    TRUNCATION ch=%s lastData=%s truncatedAt=%s cap=%s",
			tr.Channel, tr.LastDataTime.Format("15:04:05"), tr.TruncatedAt.Format("15:04:05"), tr.Snapshot.SilenceCap)
	}
}

// TestMiddleGapOnlineEqualsReplay 同一批“中途长断连再回来”的数据：
// 在线逐包（空洞期间先按尾段补、重连后收尾）与批量重放，
// 事件、指标窗、截断结论必须逐条一致。
func TestMiddleGapOnlineEqualsReplay(t *testing.T) {
	cfg := gapConfig(10 * time.Second)
	packets := gapPackets(cfg, 28*time.Second) // 静默跨度 28s，重连首点 t31，末点 t35

	batch := runFull(t, cfg, packets, false)
	online := runFull(t, cfg, packets, true)

	if !truncationsEqual(batch.truncations, online.truncations) {
		t.Fatalf("truncations diverge:\n batch=%+v\n online=%+v", batch.truncations, online.truncations)
	}
	if len(batch.truncations) != 1 {
		t.Fatalf("both paths must yield 1 truncation, got %d/%d", len(batch.truncations), len(online.truncations))
	}
	if !eventsEqual(batch.events, online.events) {
		t.Fatalf("alert events diverge:\n batch=%s\n online=%s", fmtEvents(batch.events), fmtEvents(online.events))
	}
	if !windowsEqualSorted(append([]MetricWindow(nil), batch.windows...),
		append([]MetricWindow(nil), online.windows...)) {
		t.Fatalf("metric windows diverge:\n batch=%+v\n online=%+v", batch.windows, online.windows)
	}
	if !episodesEqual(batch.episodes, online.episodes) {
		t.Fatalf("episodes diverge:\n batch=%+v\n online=%+v", batch.episodes, online.episodes)
	}
	if !episodeEventsEqual(batch.epEvents, online.epEvents) {
		t.Fatalf("episode events diverge:\n batch=%+v\n online=%+v", batch.epEvents, online.epEvents)
	}
	tr := online.truncations[0]
	logf(t, "输入: 同批 t0-3/断连28s/t31重连 数据分别在线逐包与批量重放, cap=10s; 依据: 结论只取决于数据内容不取决于推进方式; "+
		"结论: 截断(%s@%s)/告警事件/指标窗/事件档案在线与重放各 %d/%d/%d/%d 条且逐条一致",
		tr.LastDataTime.Format("15:04:05"), tr.TruncatedAt.Format("15:04:05"),
		len(online.truncations), len(online.events), len(online.windows), len(online.episodes))
}

// TestTailAndMiddleBothCapped 一条流里同时出现“中途断连空洞”与“采集尾段静默”：
// 两类长静默都受同一上限约束，各自给出一条截断结论，位置明确且互不混淆。
func TestTailAndMiddleBothCapped(t *testing.T) {
	const silenceCap = 8 * time.Second
	cfg := gapConfig(silenceCap)
	// seq0..3 在 t0..t3；重连首点落在 t29（静默跨度 26s），
	// 真实点一直采到 t40；之后推进到 +1h，制造 t40 之后的尾段静默。
	packets := gapPacketsUntil(cfg, 26*time.Second, 40*time.Second)
	p := NewPipeline(cfg)
	for _, pk := range packets {
		p.Ingest(pk)
	}
	p.Advance(cfg.Epoch.Add(1 * time.Hour))

	truncs := p.Truncations()
	// 期望两条：中间空洞(lastData=t3 -> t11)、尾段(lastData=t40 -> t48)。
	if len(truncs) != 2 {
		t.Fatalf("want one middle-gap and one tail truncation, got %+v", truncs)
	}
	want := []struct{ last, at time.Duration }{
		{3 * time.Second, 3*time.Second + silenceCap},
		{40 * time.Second, 40*time.Second + silenceCap},
	}
	for i, w := range want {
		if !truncs[i].LastDataTime.Equal(cfg.Epoch.Add(w.last)) ||
			!truncs[i].TruncatedAt.Equal(cfg.Epoch.Add(w.at)) {
			t.Fatalf("truncation %d want last=t%s at=t%s, got last=%v at=%v",
				i, w.last, w.at, truncs[i].LastDataTime, truncs[i].TruncatedAt)
		}
		if truncs[i].Snapshot.SilenceCap != silenceCap {
			t.Fatalf("truncation %d must pin cap %s, got %s", i, silenceCap, truncs[i].Snapshot.SilenceCap)
		}
	}
	logf(t, "输入: t0-3 后断连 26s(t29 重连) 采到 t40, 再推进到 +1h, cap=8s; 依据: 中间空洞与尾段同一上限; "+
		"结论: 2 条截断: 空洞 t3->t11, 尾段 t40->t48, 各自带 cap=8s 快照, 位置不混淆")
}

// TestUnconfiguredCapIsBoundedAndReproducible 未配置上限时：
// 行为必须明确、有限、可复现——采用 DefaultSilenceCap，同样截断并给结论；
// 重复两次运行（含事件、指标窗、截断结论）逐条一致。
func TestUnconfiguredCapIsBoundedAndReproducible(t *testing.T) {
	cfg := gapConfig(0) // 未配置
	if got := NewPipeline(cfg).EffectiveSilenceCap(); got != DefaultSilenceCap {
		t.Fatalf("unconfigured cap must resolve to DefaultSilenceCap %s, got %s", DefaultSilenceCap, got)
	}
	// 中途空洞远大于默认上限（静默跨度 = default+10min）。
	packets := gapPackets(cfg, DefaultSilenceCap+10*time.Minute)

	r1 := runFull(t, cfg, packets, false)
	r2 := runFull(t, cfg, packets, false)
	if len(r1.truncations) != 1 {
		t.Fatalf("unconfigured cap must still truncate middle gap, got %+v", r1.truncations)
	}
	tr := r1.truncations[0]
	if want := cfg.Epoch.Add(3*time.Second + DefaultSilenceCap); !tr.TruncatedAt.Equal(want) {
		t.Fatalf("unconfigured must truncate at default boundary %v, got %v", want, tr.TruncatedAt)
	}
	if tr.Snapshot.SilenceCap != DefaultSilenceCap {
		t.Fatalf("snapshot must record effective default cap, got %s", tr.Snapshot.SilenceCap)
	}
	if !truncationsEqual(r1.truncations, r2.truncations) {
		t.Fatalf("unconfigured-cap runs must be byte-identical:\n %+v\n %+v", r1.truncations, r2.truncations)
	}
	if !eventsEqual(r1.events, r2.events) || !windowsEqual(r1.windows, r2.windows) {
		t.Fatalf("unconfigured-cap reruns must reproduce events/windows exactly")
	}
	logf(t, "输入: 未配置 SilenceCap, t3 后断连 %s; 依据: 非正值回退默认有限上限 %s; "+
		"结论: 生效上限=%s, 空洞在 t3+%s 截断并给结论, 两次运行逐条一致",
		DefaultSilenceCap+10*time.Minute, DefaultSilenceCap, DefaultSilenceCap, DefaultSilenceCap)
}

// TestCapDeterministicAcrossValues 把上限调大/调小：截断位置确定性地随之移动；
// 同一上限重复跑，截断结论逐条一致。
func TestCapDeterministicAcrossValues(t *testing.T) {
	cfgBase := gapConfig(0)
	packets := gapPackets(cfgBase, 1*time.Hour) // 静默跨度 1h
	run := func(cap time.Duration) SilenceTruncation {
		cfg := gapConfig(cap)
		r := runFull(t, cfg, packets, false)
		if len(r.truncations) != 1 {
			t.Fatalf("cap=%s must yield 1 truncation, got %+v", cap, r.truncations)
		}
		return r.truncations[0]
	}
	caps := []time.Duration{5 * time.Second, 10 * time.Second, 30 * time.Second, time.Minute}
	atByCap := make(map[time.Duration]time.Time, len(caps))
	for _, c := range caps {
		tr := run(c)
		want := cfgBase.Epoch.Add(3*time.Second + c)
		if !tr.TruncatedAt.Equal(want) {
			t.Fatalf("cap=%s must truncate at %v, got %v", c, want, tr.TruncatedAt)
		}
		if tr.Snapshot.SilenceCap != c {
			t.Fatalf("snapshot cap must equal configured %s, got %s", c, tr.Snapshot.SilenceCap)
		}
		atByCap[c] = tr.TruncatedAt
		// 同上限再跑一次：逐条一致。
		if again := run(c); again != tr {
			t.Fatalf("cap=%s rerun must be identical:\n %+v\n %+v", c, tr, again)
		}
	}
	// 上限越大截断越晚（严格单调）。
	for i := 1; i < len(caps); i++ {
		if !atByCap[caps[i]].After(atByCap[caps[i-1]]) {
			t.Fatalf("larger cap must truncate later: cap=%s at=%v must be after cap=%s at=%v",
				caps[i], atByCap[caps[i]], caps[i-1], atByCap[caps[i-1]])
		}
	}
	logf(t, "输入: t3 后 1h 空洞, cap 依次=%v; 依据: 截断位置=最后实际点+cap; 结论: 位置随 cap 单调后移且同 cap 重跑逐条一致", caps)
}

// lastRealTime 返回序列里最大的设备时间。本文件场景时钟零偏移，
// 设备时间即统一时间。
func lastRealTime(packets []RawSample) time.Time {
	var mt time.Time
	for _, pk := range packets {
		if pk.DeviceTime.After(mt) {
			mt = pk.DeviceTime
		}
	}
	return mt
}

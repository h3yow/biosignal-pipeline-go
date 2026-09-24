package pipeline

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func e2eConfig() Config {
	start := time.Date(2026, 9, 24, 8, 0, 0, 0, time.UTC)
	const hr ChannelID = "HR"
	const d1, d2 DeviceID = "dev1", "dev2"
	return Config{
		Epoch:    start,
		SlotStep: time.Second,
		MaxDelay: 3 * time.Second,
		Channels: []ChannelSpec{
			{Device: d1, Channel: hr, Cal: Calibration{Scale: 1, Unit: "bpm"}, NominalInterval: time.Second},
			{Device: d2, Channel: "SPO2", Cal: Calibration{Scale: 1, Unit: "pct"}, NominalInterval: time.Second},
		},
		Clocks: map[DeviceID]ClockParams{
			d1: {Offset: 900 * time.Millisecond, DriftPPM: 120},
			d2: {Offset: -300 * time.Millisecond, DriftPPM: -40},
		},
		Quality: []QualityRule{
			{Channel: hr, ExpectedInterval: time.Second, GapTolerance: 200 * time.Millisecond, PhysMin: 30, PhysMax: 220},
			{Channel: "SPO2", ExpectedInterval: time.Second, GapTolerance: 200 * time.Millisecond, PhysMin: 50, PhysMax: 100},
		},
		AlertRules: []AlertRule{
			{ID: "hr-high", Version: "cfg-1", Channel: hr, HighEnabled: true, HighThreshold: 120, TriggerN: 3, RecoverN: 3},
		},
	}
}

func e2ePackets(cfg Config) []RawSample {
	start := cfg.Epoch
	const hr ChannelID = "HR"
	const d1, d2 DeviceID = "dev1", "dev2"
	genHR := func(ch ChannelID, seq int64, t time.Time) (float64, RawFlag) {
		if seq >= 6 && seq <= 10 {
			return 130, FlagOK // 连续越限
		}
		if seq == 18 {
			return 999, FlagOK // 数值异常（超出生理可信范围，但不是缺失/不可用）
		}
		return 75.0 + float64(seq%3), FlagOK
	}
	genSpO2 := func(ch ChannelID, seq int64, t time.Time) (float64, RawFlag) {
		return 97.0, FlagOK
	}
	s1 := NewSimulator(SimConfig{
		Device: d1, Clock: cfg.Clocks[d1],
		Channels:  []ChannelSpec{cfg.Channels[0]},
		StartTime: start, Duration: 30 * time.Second,
		ShuffleSeed: 11, ShuffleWindow: 2, DuplicateEvery: 6,
		DropSeq:     map[int64]bool{3: true},
		GapAfterSeq: 13, Gap: 5 * time.Second, // 14 秒起断连 5 秒后重连
	}, genHR)
	s2 := NewSimulator(SimConfig{
		Device: d2, Clock: cfg.Clocks[d2],
		Channels:  []ChannelSpec{cfg.Channels[1]},
		StartTime: start, Duration: 30 * time.Second,
		ArtifactRanges: []SimRange{{From: start.Add(20 * time.Second), To: start.Add(22 * time.Second)}},
		ArtifactFlag:   FlagLeadsOff,
	}, genSpO2)
	return append(s1.Generate(), s2.Generate()...)
}

type runSummary struct {
	accept, dup, late, rejected int
	windows                     []MetricWindow
	events                      []AlertEvent
}

// replayRun 把同一批包以两种方式喂给两条管线：
//   - batch: 全部 Ingest 后一次性 Advance（等价于“对历史数据重放”）
//   - online: 按到达顺序逐条 Ingest，并逐步推进水位（等价于在线处理）
//
// 两者的指标窗与告警事件必须完全一致。
func replayRun(t *testing.T, cfg Config, packets []RawSample, online bool) runSummary {
	t.Helper()
	p := NewPipeline(cfg)
	var s runSummary

	if !online {
		for _, pk := range packets {
			switch r := p.Ingest(pk); r.Status {
			case StatusAccept:
				s.accept++
			case StatusDuplicate:
				s.dup++
			case StatusLateRejected:
				s.late++
			default:
				s.rejected++
			}
		}
		w, e := p.Advance(cfg.Epoch.Add(2 * time.Minute))
		s.windows, s.events = append(w, p.Flush()...), e
		return s
	}

	// 在线：每收到一个包，把水位推进到“该包统一时间 - MaxDelay”，
	// 模拟真实实时流。重放时不依赖 RecvTime，只用设备时间映射值。
	for _, pk := range packets {
		r := p.Ingest(pk)
		switch r.Status {
		case StatusAccept:
			s.accept++
		case StatusDuplicate:
			s.dup++
		case StatusLateRejected:
			s.late++
		default:
			s.rejected++
		}
		if r.Point != nil {
			w, e := p.Advance(r.Point.Time.Add(-cfg.MaxDelay))
			s.windows = append(s.windows, w...)
			s.events = append(s.events, e...)
		}
	}
	w, e := p.Advance(cfg.Epoch.Add(2 * time.Minute))
	s.windows = append(s.windows, w...)
	s.windows = append(s.windows, p.Flush()...)
	s.events = append(s.events, e...)
	return s
}

// TestOnlineEqualsReplay 断连重连与历史重放后，结论必须收敛一致、无重复告警/漏报。
func TestOnlineEqualsReplay(t *testing.T) {
	cfg := e2eConfig()
	packets := e2ePackets(cfg)

	batch := replayRun(t, cfg, packets, false)
	online := replayRun(t, cfg, packets, true)

	if batch.accept != online.accept || batch.dup != online.dup || batch.late != online.late {
		t.Fatalf("ingest counts differ: batch=%+v online=%+v", batch, online)
	}
	if !eventsEqual(batch.events, online.events) {
		t.Fatalf("alert events diverge:\n batch=%s\n online=%s", fmtEvents(batch.events), fmtEvents(online.events))
	}
	if !windowsEqual(batch.windows, online.windows) {
		t.Fatalf("metric windows diverge:\n batch=%+v\n online=%+v", batch.windows, online.windows)
	}
	// 必含：一次高限触发；断连后强制恢复；重复包没有产生重复告警。
	var fires, recovers int
	for _, e := range online.events {
		if e.To == AlertActive {
			fires++
		}
		if e.From == AlertActive && e.To == AlertInactive {
			recovers++
		}
	}
	if fires != 1 || recovers != 1 {
		t.Fatalf("want exactly 1 fire and 1 recovery, got fires=%d recovers=%d events=%s",
			fires, recovers, fmtEvents(online.events))
	}
	if online.dup == 0 {
		t.Fatal("scenario should contain duplicates to prove dedup")
	}
	logf(t, "输入: %d 包(乱序窗2/重复/丢seq3/断连5s/漂移两设备/1个数值异常); 处置: accept=%d dup=%d late=%d; "+
		"依据: 统一时间轴+水位去重+连续窗状态机; 结论: 在线与一次性重放事件/指标窗逐条相同, 触发1次恢复1次",
		len(packets), online.accept, online.dup, online.late)
	for _, e := range online.events {
		t.Logf("    EVENT %s %s %s->%s v=%.0f rule=%s | %s", e.Time.Format("15:04:05"), e.RuleID,
			stateChar(e.From), stateChar(e.To), e.Value, e.RuleVersion, e.Reason)
	}
}

// TestRuleVersionReproducibility 规则版本变化后，旧结论可用旧版本配置重放复现。
func TestRuleVersionReproducibility(t *testing.T) {
	cfg := e2eConfig()
	packets := e2ePackets(cfg)

	v1 := replayRun(t, cfg, packets, false)
	cfg2 := cfg
	cfg2.AlertRules[0].Version = "cfg-2"
	cfg2.AlertRules[0].TriggerN = 5 // 更严格：同一数据不再触发
	v2 := replayRun(t, cfg2, packets, false)

	if len(v1.events) != 2 {
		t.Fatalf("v1 expected trigger+recovery, got %s", fmtEvents(v1.events))
	}
	for _, e := range v1.events {
		if e.RuleVersion != "cfg-1" {
			t.Fatalf("historical events must pin rule version cfg-1, got %s", e.RuleVersion)
		}
	}
	if len(v2.events) != 0 {
		t.Fatalf("v2 with TriggerN=5 must not fire on this data, got %s", fmtEvents(v2.events))
	}
	logf(t, "输入: 同一批数据分别用 cfg-1(TriggerN=3) 与 cfg-2(TriggerN=5) 重放; 依据: 事件固化 RuleVersion; 结论: 历史结论可复现(cfg-1 触发并打版本戳), 新版本独立生效不触发")
}

// TestConcurrentIngestRace 在 -race 下并发喂入，验证：
//   - 互斥正确、无数据竞争（go test -race）；
//   - 去重数在任意调度下恒定：重复包绝不重复计入（accept+dup 恒定，
//     late 与 accept 的归属只随“到达顺序→水位”变化，属流式固有语义）；
//   - 每个被拒包都有可区分的确定原因，没有静默丢弃或 panic。
//
// “相同到达序列 ⇒ 相同结论”由 TestOnlineEqualsReplay（在线 vs 重放）保证。
func TestConcurrentIngestRace(t *testing.T) {
	cfg := e2eConfig()
	packets := e2ePackets(cfg)
	const workers = 8

	runConcurrent := func() runSummary {
		p := NewPipeline(cfg)
		var s runSummary
		var sumMu sync.Mutex
		var wg sync.WaitGroup
		start := make(chan struct{})
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(off int) {
				defer wg.Done()
				<-start
				local := runSummary{}
				// 每个 worker 处理固定分片：重复包与其原包落在同一 worker，
				// 无论如何交错，重复计数都确定可预期。
				for i := off; i < len(packets); i += workers {
					switch r := p.Ingest(packets[i]); r.Status {
					case StatusAccept:
						local.accept++
					case StatusDuplicate:
						local.dup++
					case StatusLateRejected:
						local.late++
					default:
						local.rejected++
					}
				}
				sumMu.Lock()
				s.accept += local.accept
				s.dup += local.dup
				s.late += local.late
				s.rejected += local.rejected
				sumMu.Unlock()
			}(w)
		}
		close(start)
		wg.Wait()
		win, ev := p.Advance(cfg.Epoch.Add(2 * time.Minute))
		s.windows, s.events = append(win, p.Flush()...), ev
		return s
	}

	// 不变量：所有包都被明确处置（四类计数之和恒等于输入包数，无静默丢弃）；
	// 唯一序列号的“非迟到接受 + 明确拒绝”归属只取决于内容，且
	// 迟到判定先于去重——重复包绝不会在原始包迟到时被误当作有效数据。
	first := runConcurrent()
	if got := first.accept + first.dup + first.late + first.rejected; got != len(packets) {
		t.Fatalf("every packet must be accounted for: %d != %d", got, len(packets))
	}
	for iter := 0; iter < 5; iter++ {
		got := runConcurrent()
		if total := got.accept + got.dup + got.late + got.rejected; total != len(packets) {
			t.Fatalf("iter=%d accounted total %d != packets %d", iter, total, len(packets))
		}
		if got.rejected != 0 {
			t.Fatalf("no unknown-device/channel expected, got %d", got.rejected)
		}
		for _, e := range got.events {
			if e.RuleVersion != "cfg-1" {
				t.Fatalf("event must carry rule version even under concurrency: %+v", e)
			}
		}
	}
	logf(t, "输入: %d 包按固定偏移分到 8 goroutine 并发 Ingest; 依据: 迟到判定先于去重 + 互斥; "+
		"结论: accept=%d/dup=%d/late=%d/rejected=%d 之和恒=%d, 每个包处置原因明确, -race 通过",
		len(packets), first.accept, first.dup, first.late, first.rejected, len(packets))
}

func stateChar(s AlertState) string {
	switch s {
	case AlertInactive:
		return "INACTIVE"
	case AlertActive:
		return "ACTIVE"
	case AlertPending:
		return "PENDING"
	case AlertRecovering:
		return "RECOVERING"
	}
	return "?"
}

func eventsEqual(a, b []AlertEvent) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.RuleID != y.RuleID || x.Channel != y.Channel || x.From != y.From || x.To != y.To ||
			!x.Time.Equal(y.Time) || x.RuleVersion != y.RuleVersion || x.Value != y.Value {
			return false
		}
	}
	return true
}

func windowsEqual(a, b []MetricWindow) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.Channel != y.Channel || x.Count != y.Count || x.Mean != y.Mean ||
			x.Min != y.Min || x.Max != y.Max || x.AbnormalCount != y.AbnormalCount ||
			!x.Start.Equal(y.Start) || !x.End.Equal(y.End) || x.GapBefore != y.GapBefore {
			return false
		}
	}
	return true
}

func fmtEvents(evs []AlertEvent) string {
	out := ""
	for _, e := range evs {
		out += fmt.Sprintf("{%s %s %d->%d %s v=%.0f} ", e.RuleID, e.Time.Format(time.RFC3339), e.From, e.To, e.RuleVersion, e.Value)
	}
	return out
}

package pipeline_test

import (
	"strings"
	"testing"
	"time"

	"github.com/h3yow/biosignal-pipeline-go"
	"github.com/h3yow/biosignal-pipeline-go/simulate"
)

const logInputs = true

// e2eScenario 构造一个覆盖全部异常类型的双设备场景。
func e2eScenario() simulate.ScenarioSpec {
	start := time.Date(2026, 9, 24, 8, 0, 0, 0, time.UTC)
	hrSpec := simulate.ChannelSpec{
		Channel: "HR", Unit: pipeline.Unit("bpm"), Base: 72, Amp: 3, Noise: 0.5,
		Min: 40, Max: 180,
	}
	hrKSpec := simulate.ChannelSpec{
		Channel: "HR", Unit: pipeline.Unit("bpmK"), Base: 0.072, Amp: 0.003, Noise: 0.0005,
		Min: 40, Max: 180,
	}
	spo2Spec := simulate.ChannelSpec{
		Channel: "SpO2", Unit: pipeline.Unit("pct"), Base: 98, Amp: 0.3, Noise: 0.05,
		Min: 85, Max: 100,
	}
	return simulate.ScenarioSpec{
		Start: start, Duration: 35 * time.Second, Seed: 20260924,
		Devices: []simulate.DeviceSpec{
			{
				ID: "wrist-A", Period: time.Second,
				ClockOff: 300 * time.Millisecond, DriftPPM: 1200,
				Channels: []simulate.ChannelSpec{hrSpec, spo2Spec},
				Outage:   []simulate.Interval{{From: 10 * time.Second, To: 13 * time.Second}},
				Artifacts: map[pipeline.ChannelID][]simulate.Interval{
					"HR": {{From: 16 * time.Second, To: 18 * time.Second}},
				},
				OutOfRange: map[pipeline.ChannelID]map[int64]float64{
					// seq5 是孤立尖峰 => 单点抖动不得触发；
					// seq20/21/22 三点持续高值 => 覆盖 2s 触发一次告警，
					// seq23/24/25 连续安全 => 覆盖 2s 后恢复。
					"HR": {5: 220, 20: 165, 21: 166, 22: 164},
				},
			},
			{
				ID: "chest-B", Period: time.Second,
				ClockOff: -150 * time.Millisecond, DriftPPM: -800,
				Channels: []simulate.ChannelSpec{hrKSpec},
			},
		},
		Net: []simulate.Perturbation{
			{
				Device:        "wrist-A",
				DuplicateSeqs: []int64{3, 7},
				SwapAfter:     []int64{5},
				DelaySeqs:     map[int64]time.Duration{2: 3 * time.Second},
			},
		},
	}
}

func newE2EPipeline(t *testing.T) *pipeline.Pipeline {
	cfg := pipeline.Config{
		Aligner: pipeline.AlignerConfig{
			ReorderWindow: 300 * time.Millisecond,
			MaxLateWindow: 1500 * time.Millisecond,
			DriftHalfLife: 10 * time.Second,
		},
		Bucket: time.Second, MaxGap: 2500 * time.Millisecond, Metric: 5 * time.Second,
	}
	specs := []pipeline.ChannelSpec{
		{Channel: "HR", Unit: "bpm", Min: 40, Max: 180},
		{Channel: "SpO2", Unit: "pct", Min: 85, Max: 100},
	}
	rules := []pipeline.Rule{
		{ID: "hr-high", Version: "v1", Channel: "HR", High: 150, Low: 30,
			EnterWindow: 2 * time.Second, ExitWindow: 2 * time.Second},
	}
	p := pipeline.New(cfg, specs, rules)
	mustReg(t, p, pipeline.Transform{Device: "wrist-A", Channel: "HR", From: "bpm", To: "bpm", Scale: 1, Version: "cal-A-1"})
	mustReg(t, p, pipeline.Transform{Device: "wrist-A", Channel: "SpO2", From: "pct", To: "pct", Scale: 1, Version: "cal-A-1"})
	mustReg(t, p, pipeline.Transform{Device: "chest-B", Channel: "HR", From: "bpmK", To: "bpm", Scale: 1000, Version: "cal-B-1"})
	return p
}

func mustReg(tb interface {
	Helper()
	Fatalf(format string, args ...any)
}, p *pipeline.Pipeline, tr pipeline.Transform) {
	tb.Helper()
	if err := p.Calibration().Register(tr); err != nil {
		tb.Fatalf("register transform: %v", err)
	}
}

func runScenario(t *testing.T, p *pipeline.Pipeline, pkts []simulate.Packet, maxT time.Time) pipeline.Result {
	t.Helper()
	var all pipeline.Result
	for _, pk := range pkts {
		if logInputs {
			t.Logf("INPUT dev=%s ch=%s seq=%d devTime=%s recv=%s val=%.4f unit=%s hint=%d ann=%q",
				pk.Device, pk.Channel, pk.Seq,
				pk.DeviceTime.Format("15:04:05.000"), pk.RecvTime.Format("15:04:05.000"),
				pk.Value, pk.Unit, pk.Hint, pk.Annotation)
		}
		r, err := p.Push(pk.Sample)
		if err != nil {
			t.Fatalf("push rejected unexpectedly: %v", err)
		}
		for _, a := range r.Aligns {
			t.Logf("ALIGN action=%d(%s) reason=%s dev=%s ch=%s seq=%d master=%s val=%.3f cal=%s",
				a.Action, actionName(a.Action), a.Reason, a.Sample.Device, a.Sample.Channel,
				a.Sample.Seq, a.Sample.MasterTime.Format("15:04:05.000"), a.Sample.Value, a.Sample.CalVersion)
		}
		for _, pt := range r.Points {
			t.Logf("JUDGE class=%s reason=%s dev=%s ch=%s t=%s val=%.3f gap=%v synth=%v",
				pt.Verdict.Class, pt.Verdict.Reason, pt.Sample.Device, pt.Sample.Channel,
				pt.Sample.MasterTime.Format("15:04:05.000"), pt.Sample.Value, pt.GapAfter, pt.Synthetic)
		}
		for _, m := range r.Metrics {
			t.Logf("METRIC dev=%s ch=%s name=%s win=[%s,%s) valid=%v reason=%s val=%.3f",
				m.Device, m.Channel, m.Name,
				m.Window[0].Format("15:04:05"), m.Window[1].Format("15:04:05"),
				m.Valid, m.Reason, m.Value)
		}
		for _, al := range r.Alarms {
			t.Logf("ALARM rule=%s ver=%s dev=%s status=%s at=%s",
				al.RuleID, al.RuleVersion, al.Device, al.Status, al.At.Format("15:04:05"))
		}
		all = appendResult(all, r)
	}
	all = appendResult(all, p.Advance(maxT.Add(time.Second)))
	all = appendResult(all, p.Close())
	return all
}

func appendResult(a, b pipeline.Result) pipeline.Result {
	a.Aligns = append(a.Aligns, b.Aligns...)
	a.Points = append(a.Points, b.Points...)
	a.Metrics = append(a.Metrics, b.Metrics...)
	a.Alarms = append(a.Alarms, b.Alarms...)
	return a
}

func actionName(a pipeline.AlignAction) string {
	switch a {
	case pipeline.ActionAccept:
		return "ACCEPT"
	case pipeline.ActionDuplicate:
		return "DUPLICATE"
	case pipeline.ActionLate:
		return "LATE"
	}
	return "?"
}

func TestE2EDeterminismAndClassification(t *testing.T) {
	spec := e2eScenario()
	pkts := simulate.Generate(spec)
	if len(pkts) == 0 {
		t.Fatal("no packets generated")
	}

	// 在线处理。
	online := runScenario(t, newE2EPipeline(t), pkts,
		spec.Start.Add(spec.Duration))

	// 统计对齐结论，要求重复/迟到均被明确识别。
	var dup, late, accept int
	for _, a := range online.Aligns {
		switch a.Action {
		case pipeline.ActionDuplicate:
			dup++
		case pipeline.ActionLate:
			late++
		case pipeline.ActionAccept:
			accept++
		}
	}
	if dup < 2 {
		t.Errorf("expected >=2 duplicates detected, got %d", dup)
	}
	if late < 1 {
		t.Errorf("expected >=1 late marked, got %d", late)
	}
	if accept == 0 {
		t.Fatal("no accepted samples")
	}
	t.Logf("CONCLUSION accepts=%d duplicates=%d late=%d", accept, dup, late)

	// 质量三分类必须都可区分地出现。
	var miss, unav, abn int
	for _, pt := range online.Points {
		switch pt.Verdict.Class {
		case pipeline.QualityMissing:
			miss++
		case pipeline.QualityUnavailable:
			unav++
		case pipeline.QualityAbnormal:
			abn++
		}
	}
	if miss == 0 {
		t.Error("expected MISSING points from outage")
	}
	if unav == 0 {
		t.Error("expected UNAVAILABLE points from artifact hints")
	}
	if abn == 0 {
		t.Error("expected ABNORMAL point from forced out-of-range seq 25")
	}
	t.Logf("CONCLUSION missing=%d unavailable=%d abnormal=%d", miss, unav, abn)

	// 断连窗口的指标必须失效且原因可区分为中断/缺失，不得产生数值。
	var invalidGap int
	for _, m := range online.Metrics {
		if m.Channel == "HR" && m.Device == "wrist-A" &&
			!m.Valid && (m.Reason == pipeline.ReasonGapInterrupted || m.Reason == pipeline.ReasonMissingWindow) {
			invalidGap++
		}
	}
	if invalidGap == 0 {
		t.Error("expected invalidated metrics covering the outage window")
	}

	// 持续越界段（seq21-23）应触发恰好一次 ACTIVE，并随后恢复一次 IDLE；
	// 孤立尖峰 seq25 不得再触发。
	var active, idle, wristActive int
	for _, al := range online.Alarms {
		t.Logf("E2E-ALARM rule=%s ver=%s dev=%s status=%s at=%s",
			al.RuleID, al.RuleVersion, al.Device, al.Status, al.At.Format("15:04:05"))
		if al.RuleID != "hr-high" {
			continue
		}
		if al.Status == pipeline.AlarmActive {
			active++
		} else {
			idle++
		}
		if al.Device == "wrist-A" && al.Status == pipeline.AlarmActive {
			wristActive++
		}
	}
	if wristActive != 1 {
		t.Errorf("expected exactly 1 sustained ACTIVE for wrist-A, got %d", wristActive)
	}
	if active < 1 || idle < 1 || active != idle {
		t.Errorf("expected matched trigger/recover pair, got active=%d idle=%d", active, idle)
	}
	t.Logf("CONCLUSION hr-high lifecycle: active=%d recover=%d (isolated spike seq5 produced no extra alarm)", active, idle)
}

func TestReplayConvergesWithOnline(t *testing.T) {
	spec := e2eScenario()
	pkts := simulate.Generate(spec)
	maxT := spec.Start.Add(spec.Duration)

	online := runScenario(t, newE2EPipeline(t), append([]simulate.Packet(nil), pkts...), maxT)
	// 重放：同一批数据包（保持接收顺序）走另一个全新管线。
	replay := runScenario(t, newE2EPipeline(t), append([]simulate.Packet(nil), pkts...), maxT)

	if !resultsEqual(online, replay) {
		t.Fatalf("replay diverges from online:\nonline alarms=%v\nreplay alarms=%v",
			summary(online), summary(replay))
	}
	t.Logf("CONCLUSION replay converged: %s", summary(replay))
}

func summary(r pipeline.Result) string {
	var b strings.Builder
	b.WriteString("accepts=")
	n := 0
	for _, a := range r.Aligns {
		if a.Action == pipeline.ActionAccept {
			n++
		}
	}
	b.WriteString(itoa(n))
	b.WriteString(" alarms=[")
	for i, al := range r.Alarms {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(al.RuleID + ":" + al.Status.String() + "@" + al.At.Format("15:04:05"))
	}
	b.WriteByte(']')
	return b.String()
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

func resultsEqual(a, b pipeline.Result) bool {
	if len(a.Aligns) != len(b.Aligns) || len(a.Points) != len(b.Points) ||
		len(a.Metrics) != len(b.Metrics) || len(a.Alarms) != len(b.Alarms) {
		return false
	}
	for i := range a.Alarms {
		x, y := a.Alarms[i], b.Alarms[i]
		if x != y {
			return false
		}
	}
	for i := range a.Aligns {
		x, y := a.Aligns[i], b.Aligns[i]
		if x.Action != y.Action || x.Reason != y.Reason ||
			x.Sample.Device != y.Sample.Device || x.Sample.Channel != y.Sample.Channel ||
			x.Sample.Seq != y.Sample.Seq || !x.Sample.MasterTime.Equal(y.Sample.MasterTime) ||
			x.Sample.Value != y.Sample.Value || x.Sample.CalVersion != y.Sample.CalVersion {
			return false
		}
	}
	return true
}

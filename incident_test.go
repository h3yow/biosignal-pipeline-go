package pipeline

import (
	"sync"
	"testing"
	"time"
)

// fullRun 是一次 AdvanceFull 级别的完整运行结果，供事件/会话/快照测试对账。
type fullRun struct {
	verdicts    []QualityVerdict
	windows     []MetricWindow
	events      []AlertEvent
	transitions []IncidentTransition
	incidents   []Incident
}

// runFull 以两种方式跑同一批包：online=true 模拟在线逐包+逐步推进水位，
// online=false 模拟历史数据一次性重放。两者产出必须逐条一致。
func runFull(t *testing.T, cfg Config, packets []RawSample, online bool) fullRun {
	t.Helper()
	p := NewPipeline(cfg)
	var s fullRun
	advance := func(ts time.Time) {
		r := p.AdvanceFull(ts)
		s.verdicts = append(s.verdicts, r.Verdicts...)
		s.windows = append(s.windows, r.Windows...)
		s.events = append(s.events, r.Events...)
		s.transitions = append(s.transitions, r.Incidents...)
	}
	if !online {
		for _, pk := range packets {
			p.Ingest(pk)
		}
		advance(cfg.Epoch.Add(2 * time.Minute))
	} else {
		for _, pk := range packets {
			if r := p.Ingest(pk); r.Point != nil {
				advance(r.Point.Time.Add(-cfg.MaxDelay))
			}
		}
		advance(cfg.Epoch.Add(2 * time.Minute))
	}
	s.windows = append(s.windows, p.Flush()...)
	s.incidents = p.Incidents()
	return s
}

// incidentConfig 单设备单通道，零时钟偏移，便于直接按秒构造场景。
func incidentConfig() Config {
	start := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	return Config{
		Version:  "inc-cfg-1",
		Epoch:    start,
		SlotStep: time.Second,
		MaxDelay: 3 * time.Second,
		Channels: []ChannelSpec{
			{Device: "dev1", Channel: "HR", Cal: Calibration{Scale: 1, Unit: "bpm"}, NominalInterval: time.Second},
		},
		Clocks: map[DeviceID]ClockParams{"dev1": {}},
		Quality: []QualityRule{
			{Channel: "HR", Version: "q1", ExpectedInterval: time.Second,
				GapTolerance: 200 * time.Millisecond, PhysMin: 30, PhysMax: 100},
		},
		IncidentRecoverAfter: 3 * time.Second,
	}
}

func hrPkt(seq int64, at time.Time, v float64) RawSample {
	return RawSample{Device: "dev1", Channel: "HR", Seq: seq, DeviceTime: at, RawValue: v}
}

// reconnectPackets 构造“异常→断连→重连仍异常→恢复”场景：
//
//	t=0..4   正常 75
//	t=5..9   异常 150（超 PhysMax=100，信号可信）
//	t=10..19 无数据（设备摘下/丢包）
//	t=20..24 重连，仍异常 150
//	t=25..30 恢复 75（连续正常满 3s 后事件应关闭于 t=28）
func reconnectPackets(epoch time.Time) []RawSample {
	var pk []RawSample
	mk := func(seq int64, sec int, v float64) {
		pk = append(pk, hrPkt(seq, epoch.Add(time.Duration(sec)*time.Second), v))
	}
	for i := 0; i <= 4; i++ {
		mk(int64(i), i, 75)
	}
	for i := 5; i <= 9; i++ {
		mk(int64(i), i, 150)
	}
	for i := 20; i <= 24; i++ {
		mk(int64(i), i, 150)
	}
	for i := 25; i <= 30; i++ {
		mk(int64(i), i, 75)
	}
	return pk
}

func phaseNames(phases []IncidentPhase) []IncidentPhase { return phases }

func incidentPhases(inc Incident) []IncidentPhase {
	out := make([]IncidentPhase, 0, len(inc.Timeline))
	for _, tr := range inc.Timeline {
		out = append(out, tr.To)
	}
	return out
}

func incidentTimes(inc Incident) []int {
	out := make([]int, 0, len(inc.Timeline))
	for _, tr := range inc.Timeline {
		out = append(out, tr.Time.Second())
	}
	return out
}

// TestIncidentDisconnectReconnect 同一段持续异常中间断连 10s，
// 必须仍是同一次事件：编号不变、不新增、不提前结束；
// 断连段明确标为“数据不可用、结论待定”；连续正常满 3s 才关闭。
// 在线逐包与历史重放两条路径产出的事件逐条一致。
func TestIncidentDisconnectReconnect(t *testing.T) {
	cfg := incidentConfig()
	packets := reconnectPackets(cfg.Epoch)

	batch := runFull(t, cfg, packets, false)
	online := runFull(t, cfg, packets, true)

	for name, run := range map[string]fullRun{"batch": batch, "online": online} {
		if len(run.incidents) != 1 {
			t.Fatalf("[%s] want exactly 1 incident, got %d: %+v", name, len(run.incidents), run.incidents)
		}
		inc := run.incidents[0]
		if inc.ID != "INC-HR-0001" {
			t.Fatalf("[%s] stable incident ID want INC-HR-0001, got %s", name, inc.ID)
		}
		wantPhases := []IncidentPhase{PhaseAbnormal, PhaseUnavailable, PhaseAbnormal, PhaseRecovering, PhaseClosed}
		gotPhases := incidentPhases(inc)
		if len(gotPhases) != len(wantPhases) {
			t.Fatalf("[%s] want %d transitions, got %v", name, len(wantPhases), gotPhases)
		}
		for i := range wantPhases {
			if gotPhases[i] != wantPhases[i] {
				t.Fatalf("[%s] transition %d want %d got %d (all=%v)", name, i, wantPhases[i], gotPhases[i], gotPhases)
			}
		}
		wantSecs := []int{5, 10, 20, 25, 28}
		gotSecs := incidentTimes(inc)
		for i := range wantSecs {
			if gotSecs[i] != wantSecs[i] {
				t.Fatalf("[%s] transition %d at t=%ds, want t=%ds (all=%v)", name, i, gotSecs[i], wantSecs[i], gotSecs)
			}
		}
		if !inc.Closed || !inc.End.Equal(cfg.Epoch.Add(28*time.Second)) {
			t.Fatalf("[%s] incident must close at t=28s, got closed=%v end=%v", name, inc.Closed, inc.End)
		}
		if inc.Snapshot.ConfigVersion != "inc-cfg-1" || inc.Snapshot.QualityRuleVersion != "q1" ||
			inc.Snapshot.Cal.Scale != 1 || inc.Snapshot.Cal.Unit != "bpm" {
			t.Fatalf("[%s] incident must pin config snapshot, got %+v", name, inc.Snapshot)
		}
	}

	// 在线 vs 重放：事件条数、编号、时间线逐条一致。
	if len(batch.transitions) != len(online.transitions) {
		t.Fatalf("transition count diverges: batch=%d online=%d", len(batch.transitions), len(online.transitions))
	}
	for i := range batch.transitions {
		b, o := batch.transitions[i], online.transitions[i]
		if b.IncidentID != o.IncidentID || b.To != o.To || !b.Time.Equal(o.Time) {
			t.Fatalf("transition %d diverges:\n batch=%+v\n online=%+v", i, b, o)
		}
	}
	logf(t, "输入: t=5..9 异常150 → t=10..19 断连无数据 → t=20..24 重连仍异常 → t=25..30 正常; "+
		"依据: 异常开事件/断连标不可用不结束/连续正常满3s才关闭; "+
		"结论: 全程 1 次事件 %s, 阶段 %v@t=%v, 在线与重放逐条一致", "INC-HR-0001",
		phaseNames(incidentPhases(batch.incidents[0])), incidentTimes(batch.incidents[0]))
	for _, tr := range batch.transitions {
		t.Logf("    TRANSITION %s %s -> phase=%d | %s", tr.Time.Format("15:04:05"), tr.IncidentID, tr.To, tr.Reason)
	}
}

// TestConcurrentNewPathsRace 在 -race 下并发压新路径：
// 1 个 goroutine 按标准在线模式驱动（按序 Ingest + 推进到 包时间-MaxDelay，
// 不越过安全水位），其余 goroutine 并发执行“已过期水位”的 AdvanceFull
// （恒为幂等空操作）与 Incidents/Events 查询。验证：
//   - 事件跟踪器、尾段静默、快照读取全部在同一把互斥锁下，-race 无竞争；
//   - 并发空转不改变结论：事件档案与串行在线运行逐条一致。
func TestConcurrentNewPathsRace(t *testing.T) {
	cfg := incidentConfig()
	cfg.MaxSilence = 5 * time.Second
	packets := reconnectPackets(cfg.Epoch)

	p := NewPipeline(cfg)
	var wg sync.WaitGroup
	start := make(chan struct{})

	// 在线驱动 goroutine：到达序列与推进水位都和串行在线运行一致。
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for _, pk := range packets {
			if r := p.Ingest(pk); r.Point != nil {
				_ = p.AdvanceFull(r.Point.Time.Add(-cfg.MaxDelay))
			}
		}
		_ = p.AdvanceFull(cfg.Epoch.Add(2 * time.Minute))
	}()
	// 2 个“过期水位”推进 goroutine：cutoff 恒在过去，是幂等空操作，
	// 但会与写入方并发竞争同一把锁，供 -race 校验。
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 50; i++ {
				_ = p.AdvanceFull(cfg.Epoch.Add(-time.Second))
			}
		}()
	}
	// 2 个查询 goroutine：并发读事件档案与告警事件。
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 50; i++ {
				_ = p.Incidents()
				_ = p.Events()
			}
		}()
	}
	close(start)
	wg.Wait()

	// 并发空转不得改变结论：与串行在线运行逐条对账。
	ref := runFull(t, cfg, packets, true)
	got := p.Incidents()
	if len(got) != len(ref.incidents) {
		t.Fatalf("incident count diverges after concurrent run: got %d want %d", len(got), len(ref.incidents))
	}
	for i := range got {
		if got[i].ID != ref.incidents[i].ID || got[i].Closed != ref.incidents[i].Closed ||
			len(got[i].Timeline) != len(ref.incidents[i].Timeline) {
			t.Fatalf("incident %d diverges after concurrent run:\n got=%+v\n want=%+v",
				i, got[i], ref.incidents[i])
		}
		for j := range got[i].Timeline {
			g, r := got[i].Timeline[j], ref.incidents[i].Timeline[j]
			if g.To != r.To || !g.Time.Equal(r.Time) {
				t.Fatalf("incident %d transition %d diverges: got %+v want %+v", i, j, g, r)
			}
		}
	}
	logf(t, "输入: %d 包单 goroutine 在线驱动 + 2 goroutine 过期水位空转 + 2 goroutine 并发查询; "+
		"依据: 共享状态同一把互斥锁, 过期推进幂等为空操作; "+
		"结论: -race 无数据竞争, 事件档案与串行在线运行逐条一致(%d 起事件)",
		len(packets), len(got))
}

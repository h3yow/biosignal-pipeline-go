package pipeline

import (
	"testing"
	"time"
)

func testAligner(t *testing.T, cfg AlignerConfig) *Aligner {
	t.Helper()
	cal := NewCalibrationRegistry()
	if err := cal.Register(Transform{Device: "d1", Channel: "HR", From: "bpm", To: "bpm", Scale: 1, Version: "v9"}); err != nil {
		t.Fatal(err)
	}
	return NewAligner(cfg, cal)
}

func mkSample(seq int64, devT, recvT time.Time, val float64) Sample {
	return Sample{Device: "d1", Channel: "HR", Seq: seq, DeviceTime: devT, RecvTime: recvT, Value: val, Unit: "bpm"}
}

// TestAlignerDuplicate：相同序号即使重新送达，也必须只计入一次。
func TestAlignerDuplicate(t *testing.T) {
	base := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	a := testAligner(t, AlignerConfig{ReorderWindow: 0, MaxLateWindow: time.Second, DriftHalfLife: time.Minute})

	if _, err := a.Push(mkSample(1, base, base, 70)); err != nil {
		t.Fatal(err)
	}
	// 完全相同的包重发（连接收时间都相同）。
	evs2, err := a.Push(mkSample(1, base, base, 70))
	if err != nil {
		t.Fatal(err)
	}
	var dup int
	for _, e := range evs2 {
		t.Logf("dup-packet verdict action=%d reason=%s", e.Action, e.Reason)
		if e.Action == ActionDuplicate {
			dup++
		}
		if e.Action == ActionAccept {
			t.Fatal("duplicate must never be accepted")
		}
	}
	if dup != 1 {
		t.Fatalf("expected exactly 1 duplicate event, got %d", dup)
	}
	// 首个包在 Flush 时只释放一次。
	flushed := a.Flush()
	var accepts int
	for _, e := range flushed {
		if e.Action == ActionAccept && e.Sample.Seq == 1 {
			accepts++
		}
	}
	if accepts != 1 {
		t.Fatalf("expected first packet released exactly once, got %d", accepts)
	}
}

// TestAlignerReorder：窗口内乱序必须在释放时按主时间戳恢复有序。
func TestAlignerReorder(t *testing.T) {
	base := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	a := testAligner(t, AlignerConfig{ReorderWindow: 2 * time.Second, MaxLateWindow: 5 * time.Second, DriftHalfLife: time.Minute})

	// 主时间戳 seq0=0s, seq1=0.8s, seq2=1.0s。到达顺序 0,2,1：
	// seq2 先到（前沿推进到 1s），seq1 在 1.0s 才收到，其主时间戳 0.8s
	// 仍在容忍窗口内，必须作为正常点重排，而不是被误判迟到或丢弃。
	pushes := []Sample{
		mkSample(0, base, base, 70),
		mkSample(2, base.Add(time.Second), base.Add(time.Second), 72),
		mkSample(1, base.Add(800*time.Millisecond), base.Add(time.Second), 71),
	}
	for _, sm := range pushes {
		if _, err := a.Push(sm); err != nil {
			t.Fatal(err)
		}
	}
	var seqOrder []int64
	for _, e := range a.Flush() {
		if e.Action == ActionAccept {
			seqOrder = append(seqOrder, e.Sample.Seq)
		}
	}
	t.Logf("reorder released sequence order=%v (input arrival was 0,2,1)", seqOrder)
	wantSeq := []int64{0, 1, 2}
	if len(seqOrder) != len(wantSeq) {
		t.Fatalf("expected %d released points, got %d (%v)", len(wantSeq), len(seqOrder), seqOrder)
	}
	for i := range wantSeq {
		if seqOrder[i] != wantSeq[i] {
			t.Fatalf("released order not sorted by master time: %v", seqOrder)
		}
	}
}

// TestAlignerLate：超出 MaxLateWindow 的迟到包必须被明确标记，
// 既不静默丢弃，也不回插到已释放序列。
func TestAlignerLate(t *testing.T) {
	base := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	a := testAligner(t, AlignerConfig{ReorderWindow: 0, MaxLateWindow: 2 * time.Second, DriftHalfLife: time.Minute})

	push := func(seq int64, ts time.Time) {
		t.Helper()
		if _, err := a.Push(mkSample(seq, ts, ts.Add(20*time.Millisecond), 70)); err != nil {
			t.Fatal(err)
		}
	}
	// 先建立前沿到 t=10s，早期点已全部释放。
	for s := int64(0); s <= 10; s++ {
		push(s, base.Add(time.Duration(s)*time.Second))
	}
	// t=5s 的包在 t=10.5s 才收到：主时间戳 5s 早于前沿 10-2=8s => 迟到。
	evs, err := a.Push(mkSample(99, base.Add(5*time.Second), base.Add(10500*time.Millisecond), 69))
	if err != nil {
		t.Fatal(err)
	}
	var late, accept int
	for _, e := range evs {
		t.Logf("late-packet verdict action=%d reason=%s master=%s", e.Action, e.Reason, e.Sample.MasterTime.Format("05.000"))
		switch e.Action {
		case ActionLate:
			late++
			if e.Reason != ReasonLateBeyondWindow {
				t.Fatalf("late reason = %s, want LATE_BEYOND_WINDOW", e.Reason)
			}
		case ActionAccept:
			accept++
		}
	}
	if late != 1 || accept != 0 {
		t.Fatalf("expected 1 late / 0 accept, got %d late %d accept", late, accept)
	}

	// 迟到点之后 Flush：它绝不能出现在正常有序流里。
	for _, e := range a.Flush() {
		if e.Sample.Seq == 99 && e.Action == ActionAccept {
			t.Fatal("late point must never be inserted into ordered stream")
		}
	}
}

// TestAlignerClockOffsetAndDrift：验证偏移与漂移被时钟模型跟踪，
// 主时间戳收敛到接近主时间轴真值。
func TestAlignerClockOffsetAndDrift(t *testing.T) {
	base := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	a := testAligner(t, AlignerConfig{ReorderWindow: 0, MaxLateWindow: time.Minute, DriftHalfLife: 30 * time.Second})

	// 设备时间 = 主时间 + 1s 固定偏移；接收时间即主时间（本模拟以接收
	// 主机时钟为主时间轴）。
	var lastMaster time.Time
	for s := int64(0); s < 40; s++ {
		master := base.Add(time.Duration(s) * time.Second)
		evs, err := a.Push(mkSample(s, master.Add(time.Second), master, 70))
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range evs {
			if e.Action == ActionAccept && e.Sample.Seq == 39 {
				lastMaster = e.Sample.MasterTime
			}
		}
	}
	if lastMaster.IsZero() {
		for _, e := range a.Flush() {
			if e.Action == ActionAccept && e.Sample.Seq == 39 {
				lastMaster = e.Sample.MasterTime
			}
		}
	}
	// 设备钟 +1s 偏移、接收 +20ms 延迟：观测模型只能把设备时间映射到
	// “接收时间轴”，即主时间 +20ms（20ms 延迟与钟偏在数学上不可分）。
	want := base.Add(39 * time.Second)
	err := lastMaster.Sub(want)
	if d := absDur(err); d > 300*time.Millisecond {
		t.Fatalf("aligned time error %v exceeds tolerance; clock model failed to track offset", err)
	}
	t.Logf("offset tracking: final master=%s expected≈%s error=%v",
		lastMaster.Format("05.000"), want.Format("05.000"), err)
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

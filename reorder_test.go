package pipeline

import (
	"testing"
	"time"
)

func newTestBuffer(t *testing.T) *ReorderBuffer {
	epoch := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	clocks := NewClockRegistry(epoch)
	clocks.SetClock("d1", ClockParams{Offset: 0})
	specs := []ChannelSpec{{
		Device: "d1", Channel: "c1",
		Cal:             Calibration{Scale: 2, Offset: 1, Unit: "U"},
		NominalInterval: time.Second,
	}}
	return NewReorderBuffer(clocks, specs, 3*time.Second)
}

func pkt(seq int64, sec float64) RawSample {
	return RawSample{
		Device: "d1", Channel: "c1", Seq: seq,
		DeviceTime: time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC).Add(durSec(sec)),
		RawValue:   10,
	}
}

func durSec(f float64) time.Duration {
	return time.Duration(f * float64(time.Second))
}

// TestReorderDedupLateCalibration 覆盖乱序重排、重复去重、迟到拒绝、校准换算。
func TestReorderDedupLateCalibration(t *testing.T) {
	b := newTestBuffer(t)

	// 乱序到达：t=2 先于 t=1。
	r2 := b.Ingest(pkt(2, 2))
	r1 := b.Ingest(pkt(1, 1))
	if r2.Status != StatusAccept || r1.Status != StatusAccept {
		t.Fatalf("in-window reorder must accept, got %d %d", r2.Status, r1.Status)
	}
	// 重复序列号：不得重复计入。
	dup := b.Ingest(pkt(1, 1))
	if dup.Status != StatusDuplicate {
		t.Fatalf("duplicate seq must be StatusDuplicate, got %d (%s)", dup.Status, dup.Reason)
	}
	// 水位此时为 max(2)-3 = -1（秒）；送 t=10 推进水位到 7。
	if r := b.Ingest(pkt(10, 10)); r.Status != StatusAccept {
		t.Fatalf("t=10 should accept, got %d (%s)", r.Status, r.Reason)
	}
	// t=3 早于水位 7 → 超可接受延迟，明确拒绝。
	late := b.Ingest(pkt(3, 3))
	if late.Status != StatusLateRejected {
		t.Fatalf("out-of-delay packet must be StatusLateRejected, got %d (%s)", late.Status, late.Reason)
	}

	drained := b.Drain(time.Date(2026, 9, 24, 0, 0, 5, 0, time.UTC))
	if len(drained) != 2 {
		t.Fatalf("expected 2 drained points (t=1,t=2), got %d", len(drained))
	}
	if !drained[0].Time.Before(drained[1].Time) {
		t.Fatal("drain must return ascending unified time regardless of arrival order")
	}
	// 校准 physical = raw*2+1 = 21，换算可追溯。
	if drained[0].Value != 21 || drained[0].Unit != "U" {
		t.Fatalf("calibration wrong: value=%v unit=%v", drained[0].Value, drained[0].Unit)
	}
	// t=10 仍在缓冲中，不能插入到错误的早批次。
	if b.PendingCount() != 1 {
		t.Fatalf("late-epoch point must stay pending, pending=%d", b.PendingCount())
	}

	logf(t, "输入顺序: t=2,t=1,t=1(dup),t=10,t=3(late); 依据: 去重键(device,channel,seq), 水位=max-3s, 校准 raw*2+1; "+
		"结论: accept×3(1,2,10), duplicate×1, lateRejected×1, 首批输出升序 t=1,t=2 value=21")
}

// TestUnknownDeviceChannel 验证失败原因可区分。
func TestUnknownDeviceChannel(t *testing.T) {
	epoch := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	clocks := NewClockRegistry(epoch)
	clocks.SetClock("d1", ClockParams{})
	b := NewReorderBuffer(clocks, nil, time.Second)

	r := b.Ingest(RawSample{Device: "ghost", Channel: "c", Seq: 1, DeviceTime: epoch, RawValue: 1})
	if r.Status != StatusUnknownDevice {
		t.Fatalf("want StatusUnknownDevice, got %d", r.Status)
	}
	r = b.Ingest(RawSample{Device: "d1", Channel: "c", Seq: 1, DeviceTime: epoch, RawValue: 1})
	if r.Status != StatusUnknownChannel {
		t.Fatalf("want StatusUnknownChannel, got %d (%s)", r.Status, r.Reason)
	}
	logf(t, "输入: 未注册设备 + 已注册设备但无校准通道; 结论: 分别拒绝为 UnknownDevice / UnknownChannel, 原因=%q", r.Reason)
}

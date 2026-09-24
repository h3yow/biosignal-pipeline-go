package pipeline

import (
	"testing"
	"time"
)

// TestClockMapping 验证固定偏移 + 线性漂移映射的可追溯换算。
func TestClockMapping(t *testing.T) {
	epoch := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	r := NewClockRegistry(epoch)
	const d DeviceID = "d1"
	r.SetClock(d, ClockParams{Offset: 1 * time.Second, DriftPPM: 1000}) // +1s 相位、+1000ppm(=0.1%)

	// 1000 秒设备时间：漂移 0.1% = 1 秒，再加 1 秒固定偏移 → +2 秒。
	got, ok := r.ToUnified(d, epoch.Add(1000*time.Second))
	if !ok {
		t.Fatal("registered device must resolve")
	}
	want := epoch.Add(1002 * time.Second)
	if !got.Equal(want) {
		t.Fatalf("clock mapping = %v, want %v", got, want)
	}
	if _, ok := r.ToUnified("unknown", epoch); ok {
		t.Fatal("unregistered device must not be silently mapped")
	}
	logf(t, "输入: deviceTime=epoch+1000s, offset=1s, drift=1000ppm; 依据: unified=epoch+(dt)*(1+1e-3)+offset; 结论: %v (期望 %v)", got, want)
}

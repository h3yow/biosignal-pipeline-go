package pipeline

import (
	"testing"
	"time"
)

// TestMetricsNoCrossGap：中断窗口指标必须失效，且不把中断两侧的好值
// 拼接计算出虚假均值；正常窗口才产出数值。
func TestMetricsNoCrossGap(t *testing.T) {
	base := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	acc := NewSegmentAccumulator(MetricsConfig{Window: 5 * time.Second})

	good := func(sec int64, v float64) JudgedPoint {
		return JudgedPoint{
			Sample:  alignedAt("d", "HR", sec, base.Add(time.Duration(sec)*time.Second), v),
			Verdict: QualityVerdict{Class: QualityGood, Reason: ReasonAccepted},
		}
	}

	// 窗口 [0,5)：t=0 好值，然后 t=1 的点带 GapAfter（中断边界落在窗内）。
	acc.Push(good(0, 70))
	gapped := good(1, 72)
	gapped.GapAfter = true
	var closed []Metric
	closed = append(closed, acc.Push(gapped)...)

	// t=6 触发 [0,5) 关闭。
	closed = append(closed, acc.Push(good(6, 74))...)
	var w0 []Metric
	for _, m := range closed {
		if m.Window[0].Equal(base) {
			w0 = append(w0, m)
		}
	}
	if len(w0) == 0 {
		t.Fatal("window [0,5) was not closed/emitted")
	}
	for _, m := range w0 {
		t.Logf("gapped window metric=%s valid=%v reason=%s", m.Name, m.Valid, m.Reason)
		if m.Valid {
			t.Fatalf("metric %s must be invalid across gap boundary", m.Name)
		}
		if m.Reason != ReasonGapInterrupted {
			t.Fatalf("metric %s reason=%s want GAP_INTERRUPTED", m.Name, m.Reason)
		}
	}

	// 全好窗口 [5,10) 经 Flush 应有效，且均值仅由 74 等好值构成（无拼接）。
	acc.Push(good(7, 76))
	acc.Push(good(8, 78))
	final := acc.Flush()
	var foundValid bool
	for _, m := range final {
		t.Logf("final window metric=%s valid=%v reason=%s val=%.2f", m.Name, m.Valid, m.Reason, m.Value)
		if m.Name == MetricMean && m.Valid {
			foundValid = true
			if m.Value <= 74 || m.Value >= 78 {
				t.Fatalf("mean %.2f seems to stitch across the gap", m.Value)
			}
		}
	}
	if !foundValid {
		t.Fatal("expected at least one valid mean after recovery window")
	}
}

// TestMetricsUnavailableExcluded：含 UNAVAILABLE 的窗口指标失效且原因可区分。
func TestMetricsUnavailableExcluded(t *testing.T) {
	base := time.Date(2026, 9, 24, 0, 0, 10, 0, time.UTC) // 落在另一窗
	acc := NewSegmentAccumulator(MetricsConfig{Window: 5 * time.Second})
	acc.Push(JudgedPoint{Sample: alignedAt("d", "HR", 0, base, 70),
		Verdict: QualityVerdict{Class: QualityGood, Reason: ReasonAccepted}})
	acc.Push(JudgedPoint{Sample: alignedAt("d", "HR", 1, base.Add(time.Second), 70),
		Verdict: QualityVerdict{Class: QualityUnavailable, Reason: ReasonArtifactHint}})
	ms := acc.Flush()
	for _, m := range ms {
		t.Logf("metric=%s valid=%v reason=%s", m.Name, m.Valid, m.Reason)
		if m.Valid || m.Reason != ReasonUnavailableInputs {
			t.Fatalf("expected invalid/UNAVAILABLE_INPUTS, got valid=%v reason=%s", m.Valid, m.Reason)
		}
	}
}

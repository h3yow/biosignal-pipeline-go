package pipeline

import (
	"testing"
	"time"
)

func verdict(ch ChannelID, t time.Time, class QualityClass, v float64) QualityVerdict {
	return QualityVerdict{Channel: ch, Time: t, Class: class, Value: v, Present: class != QMissing}
}

// TestMetricsWindowSplitting 验证缺失与不可用都会切断指标窗，
// 数值异常参与统计并单独计数，绝不跨中断拼接。
func TestMetricsWindowSplitting(t *testing.T) {
	base := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	sec := time.Second
	a := NewMetricAggregator()

	// 窗1：t=0 正常、t=1 数值异常（仍计入）。
	if w := a.Append(verdict("c", base, QGood, 10)); w != nil {
		t.Fatal("unexpected window close")
	}
	if w := a.Append(verdict("c", base.Add(sec), QValueAbnormal, 100)); w != nil {
		t.Fatal("abnormal value must not close the window")
	}
	// t=2 缺失 → 关闭窗1。
	w := a.Append(verdict("c", base.Add(2*sec), QMissing, 0))
	if w == nil {
		t.Fatal("missing verdict must close the window")
	}
	if w.Count != 2 || w.Mean != 55 || w.Min != 10 || w.Max != 100 || w.AbnormalCount != 1 {
		t.Fatalf("window1 stats wrong: %+v", w)
	}
	// t=4 不可用 → 不产生新窗；t=5 恢复正常 → 开窗2。
	if w := a.Append(verdict("c", base.Add(4*sec), QArtifactUnusable, 0)); w != nil {
		t.Fatal("unusable slot opens no window and closes none")
	}
	a.Append(verdict("c", base.Add(5*sec), QGood, 30))
	w2 := a.Flush("c")
	if len(w2) != 1 {
		t.Fatalf("want 1 flushed window, got %d", len(w2))
	}
	if w2[0].Count != 1 || w2[0].Mean != 30 {
		t.Fatalf("window2 must not contain pre-gap data: %+v", w2[0])
	}
	if w2[0].GapBefore != 4*sec {
		t.Fatalf("GapBefore must record interruption length, got %s", w2[0].GapBefore)
	}
	logf(t, "输入: good(10), abnormal(100), missing, unusable, good(30); 依据: 缺失/不可用关窗且不跨窗, 异常值计入并计数; "+
		"结论: 窗1{n=2 mean=55 abnormal=1}, 窗2{n=1 mean=30 gapBefore=4s}, 两段未被拼接")
}

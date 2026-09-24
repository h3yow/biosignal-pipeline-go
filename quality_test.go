package pipeline

import (
	"testing"
	"time"
)

func qPoint(seq int64, t time.Time, v float64, f RawFlag) AlignedPoint {
	return AlignedPoint{Device: "d", Channel: "c", Seq: seq, Time: t, Value: v, Unit: "U", Flags: f, RawValue: v}
}

// TestQualityClasses 直接验证三类判定互斥且可区分。
func TestQualityClasses(t *testing.T) {
	base := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	c := NewQualityClassifier([]QualityRule{{
		Channel: "c", ExpectedInterval: time.Second, GapTolerance: 100 * time.Millisecond,
		PhysMin: 50, PhysMax: 100,
	}})

	good := c.Classify(qPoint(0, base, 80, FlagOK))
	if good.Class != QGood {
		t.Fatalf("want QGood, got %d", good.Class)
	}
	abn := c.Classify(qPoint(1, base, 250, FlagOK))
	if abn.Class != QValueAbnormal {
		t.Fatalf("want QValueAbnormal, got %d reasons=%v", abn.Class, abn.Reasons)
	}
	art := c.Classify(qPoint(2, base, 250, FlagSaturation))
	if art.Class != QArtifactUnusable {
		t.Fatalf("artifact flag must dominate value check, got %d", art.Class)
	}
	logf(t, "输入: 80(正常)/250(超范围)/250+饱和标志; 依据: 硬件标志优先, 其次生理范围[50,100]; 结论: Good / ValueAbnormal / ArtifactUnusable")
}

// TestMissingFillAndNoGapCrossing 验证缺失槽填充数量与边界语义。
func TestMissingFillAndNoGapCrossing(t *testing.T) {
	base := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	c := NewQualityClassifier([]QualityRule{{
		Channel: "c", ExpectedInterval: time.Second, GapTolerance: 0, PhysMin: 0, PhysMax: 1000,
	}})
	points := []AlignedPoint{
		qPoint(0, base, 70, FlagOK),
		qPoint(4, base.Add(4*time.Second), 70, FlagOK), // 中间缺 t=1,2,3
	}
	vs, _ := c.ClassifyStream(AlignedPoint{}, false, points, time.Second)
	if len(vs) != 5 {
		t.Fatalf("want 5 verdicts (2 real + 3 missing), got %d", len(vs))
	}
	for i := 1; i <= 3; i++ {
		if vs[i].Class != QMissing || vs[i].Present {
			t.Fatalf("slot %d must be missing/absent, got class=%d present=%v (%v)", i, vs[i].Class, vs[i].Present, vs[i].Reasons)
		}
		if !vs[i].Time.Equal(base.Add(time.Duration(i) * time.Second)) {
			t.Fatalf("missing slot %d timestamp wrong: %v", i, vs[i].Time)
		}
	}
	// 正常抖动（<1s 间隔）不得误报缺失。
	closePoints := []AlignedPoint{
		qPoint(0, base, 70, FlagOK),
		qPoint(1, base.Add(time.Second-50*time.Millisecond), 70, FlagOK),
	}
	if got, _ := c.ClassifyStream(AlignedPoint{}, false, closePoints, time.Second); len(got) != 2 {
		t.Fatalf("jitter within tolerance must not create missing slots, got %d", len(got))
	}
	logf(t, "输入: t=0 与 t=4 两个有效点(间隔4s>1s); 依据: 按 1s 槽在严格空洞处补缺失; 结论: 3 个 QMissing(t=1,2,3), 实际点不变")
}

package pipeline

import (
	"testing"
	"time"
)

func alignedAt(dev DeviceID, ch ChannelID, seq int64, t time.Time, v float64) AlignedSample {
	return AlignedSample{Device: dev, Channel: ch, Seq: seq, MasterTime: t, Value: v, Unit: "bpm", CalVersion: "c1"}
}

// TestClassifierThreeClasses：三类结论必须可区分，且“伪迹优先于越界”。
func TestClassifierThreeClasses(t *testing.T) {
	c := NewClassifier([]ChannelSpec{{Channel: "HR", Unit: "bpm", Min: 40, Max: 180}})
	base := time.Now()
	cases := []struct {
		name string
		s    AlignedSample
		hint RawHint
		want QualityClass
		rs   ReasonCode
	}{
		{"good", alignedAt("d", "HR", 1, base, 72), HintOK, QualityGood, ReasonAccepted},
		{"artifact", alignedAt("d", "HR", 2, base, 72), HintArtifact, QualityUnavailable, ReasonArtifactHint},
		{"artifact-spike-still-unavailable", alignedAt("d", "HR", 3, base, 999), HintArtifact, QualityUnavailable, ReasonArtifactHint},
		{"abnormal-high", alignedAt("d", "HR", 4, base, 200), HintOK, QualityAbnormal, ReasonValueOutOfRange},
		{"abnormal-low", alignedAt("d", "HR", 5, base, 10), HintOK, QualityAbnormal, ReasonValueOutOfRange},
	}
	for _, tc := range cases {
		got := c.Classify(tc.s, tc.hint)
		t.Logf("case=%-30s -> class=%s reason=%s (want %s)", tc.name, got.Class, got.Reason, tc.want)
		if got.Class != tc.want || got.Reason != tc.rs {
			t.Fatalf("case %s: got (%s,%s) want (%s,%s)", tc.name, got.Class, got.Reason, tc.want, tc.rs)
		}
	}
}

// TestAssessorMissingAndGap：缺失网格必须合成 MISSING；超长间隔必须产生
// GapAfter 中断标记，且时间位置由网格整除决定，确定可复现。
func TestAssessorMissingAndGap(t *testing.T) {
	cf := NewClassifier([]ChannelSpec{{Channel: "HR", Unit: "bpm", Min: 40, Max: 180}})
	w := NewWindowAssessor(cf, AssessorConfig{BucketWidth: time.Second, MaxGap: 2 * time.Second})
	base := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)

	// t=0 一点，下一点 t=5（缺口 5s > MaxGap）。
	p1 := w.Assess(alignedAt("d", "HR", 0, base, 70), HintOK)
	if len(p1) != 1 || p1[0].Verdict.Class != QualityGood {
		t.Fatalf("first point unexpected: %+v", p1)
	}
	p2 := w.Assess(alignedAt("d", "HR", 5, base.Add(5*time.Second), 71), HintOK)

	// 期望：t=1,2,3,4 四个合成 MISSING（t=5 是真实点），其中一个带 GapAfter。
	var miss []int64
	var gaps int
	for _, p := range p2 {
		t.Logf("t=%s class=%s synth=%v gap=%v reason=%s",
			p.Sample.MasterTime.Format("05"), p.Verdict.Class, p.Synthetic, p.GapAfter, p.Verdict.Reason)
		if p.Verdict.Class == QualityMissing {
			miss = append(miss, int64(p.Sample.MasterTime.Sub(base)/time.Second))
		}
		if p.GapAfter {
			gaps++
		}
	}
	wantMiss := []int64{1, 2, 3, 4}
	if len(miss) != len(wantMiss) {
		t.Fatalf("missing buckets=%v want %v", miss, wantMiss)
	}
	for i := range wantMiss {
		if miss[i] != wantMiss[i] {
			t.Fatalf("missing bucket order=%v want %v", miss, wantMiss)
		}
	}
	if gaps != 1 {
		t.Fatalf("expected exactly 1 GapAfter marker, got %d", gaps)
	}
}

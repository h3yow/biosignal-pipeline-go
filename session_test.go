package pipeline

import (
	"testing"
	"time"
)

// sessionConfig 在 incidentConfig 基础上打开静默上限语义。
func sessionConfig(maxSilence time.Duration) Config {
	cfg := incidentConfig()
	cfg.Version = "sess-cfg-1"
	cfg.MaxSilence = maxSilence
	return cfg
}

// tailPackets 全部正常的数据到 t=9 为止，之后设备摘下/采集结束，再无上报。
func tailPackets(epoch time.Time) []RawSample {
	var pk []RawSample
	for i := 0; i <= 9; i++ {
		pk = append(pk, hrPkt(int64(i), epoch.Add(time.Duration(i)*time.Second), 75))
	}
	return pk
}

func countTruncated(vs []QualityVerdict) (n int, at []time.Time) {
	for _, v := range vs {
		if v.Truncated {
			n++
			at = append(at, v.Time)
		}
	}
	return n, at
}

func countMissing(vs []QualityVerdict) int {
	n := 0
	for _, v := range vs {
		if v.Class == QMissing && !v.Truncated {
			n++
		}
	}
	return n
}

// TestTailSilenceVisibleAndTruncated 设备摘下后时间继续推进：
// 尾段必须显式标为缺失（不能看起来一直正常），且最多补到 MaxSilence，
// 超出后在“最后数据+MaxSilence”处给出截断标记，之后不再展开。
// 重复推进同一时间段是幂等的（不产生重复判定）。
func TestTailSilenceVisibleAndTruncated(t *testing.T) {
	cfg := sessionConfig(10 * time.Second)
	packets := tailPackets(cfg.Epoch)

	p := NewPipeline(cfg)
	for _, pk := range packets {
		p.Ingest(pk)
	}
	r1 := p.AdvanceFull(cfg.Epoch.Add(30 * time.Second))
	r2 := p.AdvanceFull(cfg.Epoch.Add(90 * time.Second)) // 再次推进：必须幂等

	if got := countMissing(r1.Verdicts); got != 9 {
		t.Fatalf("want 9 plain missing slots (t=10..18), got %d", got)
	}
	nTrunc, at := countTruncated(r1.Verdicts)
	if nTrunc != 1 || !at[0].Equal(cfg.Epoch.Add(19*time.Second)) {
		t.Fatalf("want exactly 1 truncation marker at t=19s, got n=%d at=%v", nTrunc, at)
	}
	for _, v := range r1.Verdicts {
		if v.Time.After(cfg.Epoch.Add(19 * time.Second)) {
			t.Fatalf("no verdict may be emitted beyond truncation point, got %+v", v)
		}
	}
	// 尾段不是“看起来正常”：最后数据之后必须有缺失判定，且指标窗在首个缺失槽关闭。
	lastMissing := r1.Verdicts[len(r1.Verdicts)-1]
	if lastMissing.Class != QMissing {
		t.Fatalf("tail must end marked as missing/truncated, got %+v", lastMissing)
	}
	if len(r1.Windows) != 1 || !r1.Windows[0].End.Equal(cfg.Epoch.Add(9*time.Second)) {
		t.Fatalf("open metric window must close at last real point t=9s, got %+v", r1.Windows)
	}
	if len(r2.Verdicts) != 0 || len(r2.Windows) != 0 {
		t.Fatalf("re-advancing over truncated silence must be idempotent, got verdicts=%d windows=%d",
			len(r2.Verdicts), len(r2.Windows))
	}
	logf(t, "输入: t=0..9 正常后设备摘下, MaxSilence=10s, 推进到 t=30s 再推进到 t=90s; "+
		"依据: 尾段按槽补缺失, 上限 10s, 超出截断且幂等; "+
		"结论: 缺失槽 t=10..18 共9个, 截断标记 t=19s 恰好1个, t=19 之后无任何判定, 二次推进零产出")
	for _, v := range r1.Verdicts {
		if v.Class == QMissing {
			t.Logf("    VERDICT %s class=Missing truncated=%v | %s", v.Time.Format("15:04:05"), v.Truncated, v.Reasons[0])
		}
	}
}

// TestSilenceCapConfigurable 上限由使用方配置：不同 MaxSilence 给出不同但
// 各自稳定、可复现的截断位置；MaxSilence=0 为旧行为（不补尾段）。
func TestSilenceCapConfigurable(t *testing.T) {
	type want struct {
		missing    int
		truncAtS   int // 截断位置（秒），-1 表示无截断标记
		maxVerdict int // 最后判定时间（秒），-1 表示无任何尾段判定
	}
	cases := []struct {
		name       string
		maxSilence time.Duration
		want       want
	}{
		{"cap=5s", 5 * time.Second, want{missing: 4, truncAtS: 14, maxVerdict: 14}},
		{"cap=15s", 15 * time.Second, want{missing: 14, truncAtS: 24, maxVerdict: 24}},
		{"cap=0(legacy)", 0, want{missing: 0, truncAtS: -1, maxVerdict: 9}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := sessionConfig(tc.maxSilence)
			packets := tailPackets(cfg.Epoch)
			// 同一配置跑两遍，结论必须逐位一致（可复现）。
			var first []QualityVerdict
			for iter := 0; iter < 2; iter++ {
				run := runFull(t, cfg, packets, false)
				if iter == 0 {
					first = run.verdicts
				} else if !verdictsEqual(first, run.verdicts) {
					t.Fatalf("same config must reproduce identical verdicts")
				}
			}
			run := runFull(t, cfg, packets, false)
			if got := countMissing(run.verdicts); got != tc.want.missing {
				t.Fatalf("want %d plain missing slots, got %d", tc.want.missing, got)
			}
			nTrunc, at := countTruncated(run.verdicts)
			if tc.want.truncAtS < 0 {
				if nTrunc != 0 {
					t.Fatalf("legacy mode must not truncate, got %d markers", nTrunc)
				}
			} else {
				wantAt := cfg.Epoch.Add(time.Duration(tc.want.truncAtS) * time.Second)
				if nTrunc != 1 || !at[0].Equal(wantAt) {
					t.Fatalf("want truncation at t=%ds, got n=%d at=%v", tc.want.truncAtS, nTrunc, at)
				}
			}
			last := run.verdicts[len(run.verdicts)-1]
			if last.Time.Second() != tc.want.maxVerdict {
				t.Fatalf("last verdict at t=%ds, want t=%ds", last.Time.Second(), tc.want.maxVerdict)
			}
			logf(t, "输入: t=0..9 正常后静默, MaxSilence=%s; 依据: 缺失填充上限可配置; "+
				"结论: 缺失槽 %d 个, 截断位置 t=%ds, 最后判定 t=%ds, 两遍运行逐位一致",
				tc.maxSilence, countMissing(run.verdicts), tc.want.truncAtS, tc.want.maxVerdict)
		})
	}
}

// TestIncidentPendingWhenDeviceRemoved 异常进行中设备被摘下且再无上报：
// 事件不关闭、不算恢复，标为“数据不可用、结论待定”，并能看到截断位置。
func TestIncidentPendingWhenDeviceRemoved(t *testing.T) {
	cfg := sessionConfig(10 * time.Second)
	var pk []RawSample
	for i := 0; i <= 4; i++ {
		pk = append(pk, hrPkt(int64(i), cfg.Epoch.Add(time.Duration(i)*time.Second), 75))
	}
	for i := 5; i <= 9; i++ {
		pk = append(pk, hrPkt(int64(i), cfg.Epoch.Add(time.Duration(i)*time.Second), 150))
	}
	// t=10 起设备摘下，再无任何数据。

	run := runFull(t, cfg, pk, false)
	if len(run.incidents) != 1 {
		t.Fatalf("want 1 incident, got %d", len(run.incidents))
	}
	inc := run.incidents[0]
	if inc.Closed {
		t.Fatalf("incident must stay open (pending) when data unavailable, got closed at %v", inc.End)
	}
	phases := incidentPhases(inc)
	last := phases[len(phases)-1]
	if last != PhaseUnavailable {
		t.Fatalf("last phase must be Unavailable(pending), got %v", phases)
	}
	nTrunc, at := countTruncated(run.verdicts)
	if nTrunc != 1 || !at[0].Equal(cfg.Epoch.Add(19*time.Second)) {
		t.Fatalf("want truncation marker at t=19s, got n=%d at=%v", nTrunc, at)
	}
	logf(t, "输入: t=5..9 异常150 后设备摘下, MaxSilence=10s, 推进到 t=120s; "+
		"依据: 不可用不结束事件/静默超上限截断; "+
		"结论: 事件 %s 未关闭, 最终阶段=Unavailable(结论待定), 截断标记 t=19s", inc.ID)
	for _, tr := range inc.Timeline {
		t.Logf("    TRANSITION %s %s -> phase=%d | %s", tr.Time.Format("15:04:05"), tr.IncidentID, tr.To, tr.Reason)
	}
}

func verdictsEqual(a, b []QualityVerdict) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.Channel != y.Channel || x.Class != y.Class || x.Truncated != y.Truncated ||
			!x.Time.Equal(y.Time) || x.Value != y.Value || x.Present != y.Present {
			return false
		}
	}
	return true
}

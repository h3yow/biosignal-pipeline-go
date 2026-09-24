// 本地确定性 demo：两台模拟设备（心率胸带 / 指夹血氧），
// 注入时钟偏移+漂移、乱序、重复、丢包与中断重连，
// 打印每个包的处置依据、指标窗与告警触发/恢复结论。
// 运行：go run ./cmd/demo
package main

import (
	"fmt"
	"time"

	pipeline "github.com/h3yow/biosignal-pipeline-go"
)

func main() {
	start := time.Date(2026, 9, 24, 8, 0, 0, 0, time.UTC)
	sec := time.Second

	const hr, spo2 pipeline.ChannelID = "HR_CHEST", "SPO2_FINGER"
	const d1, d2 pipeline.DeviceID = "chest-a1", "finger-b2"

	specs := []pipeline.ChannelSpec{
		{Device: d1, Channel: hr, Cal: pipeline.Calibration{Scale: 1, Unit: "bpm"}, NominalInterval: sec},
		{Device: d2, Channel: spo2, Cal: pipeline.Calibration{Scale: 1, Unit: "pct"}, NominalInterval: sec},
	}

	cfg := pipeline.Config{
		Epoch:    start,
		Channels: specs,
		Clocks: map[pipeline.DeviceID]pipeline.ClockParams{
			// 胸带慢 1.2 秒相位、每天漂 200ppm（演示用放大值）。
			d1: {Offset: 1200 * time.Millisecond, DriftPPM: 200},
			// 指夹快 400ms。
			d2: {Offset: -400 * time.Millisecond, DriftPPM: -50},
		},
		MaxDelay: 3 * sec,
		SlotStep: sec,
		Quality: []pipeline.QualityRule{
			{Channel: hr, ExpectedInterval: sec, GapTolerance: 300 * time.Millisecond, PhysMin: 30, PhysMax: 220},
			{Channel: spo2, ExpectedInterval: sec, GapTolerance: 300 * time.Millisecond, PhysMin: 50, PhysMax: 100},
		},
		AlertRules: []pipeline.AlertRule{
			{ID: "hr-high", Version: "2026-09-v1", Channel: hr, HighEnabled: true, HighThreshold: 120, TriggerN: 3, RecoverN: 3},
			{ID: "spo2-low", Version: "2026-09-v1", Channel: spo2, LowEnabled: true, LowThreshold: 92, TriggerN: 3, RecoverN: 2},
		},
	}
	p := pipeline.NewPipeline(cfg)

	genHR := func(ch pipeline.ChannelID, seq int64, t time.Time) (float64, pipeline.RawFlag) {
		// 第 8~12 秒心率飙到 130（连续越限触发告警），其余约 75；第 16 秒一个抖动尖峰。
		v := 75.0
		if seq >= 8 && seq <= 12 {
			v = 130
		}
		if seq == 16 {
			v = 150
		}
		return v, pipeline.FlagOK
	}
	genSpO2 := func(ch pipeline.ChannelID, seq int64, t time.Time) (float64, pipeline.RawFlag) {
		// 第 5~8 秒掉到 88（连续低限触发），第 10 秒一个抖动点（不触发）。
		if seq >= 5 && seq <= 8 {
			return 88.0, pipeline.FlagOK
		}
		if seq == 10 {
			return 80.0, pipeline.FlagOK
		}
		return 98.0, pipeline.FlagOK
	}

	sim1 := pipeline.NewSimulator(pipeline.SimConfig{
		Device: d1, Clock: cfg.Clocks[d1], Channels: []pipeline.ChannelSpec{specs[0]},
		StartTime: start, Duration: 24 * sec,
		ShuffleSeed: 42, ShuffleWindow: 2,
		DuplicateEvery: 5,
		DropSeq:        map[int64]bool{4: true},
		GapAfterSeq:    14, Gap: 6 * sec, // 15 秒起断连 6 秒后重连
	}, genHR)
	sim2 := pipeline.NewSimulator(pipeline.SimConfig{
		Device: d2, Clock: cfg.Clocks[d2], Channels: []pipeline.ChannelSpec{specs[1]},
		StartTime: start, Duration: 24 * sec, ShuffleSeed: 7, ShuffleWindow: 2, DuplicateEvery: 7,
		// 第 14~17 秒指夹松动：硬件上报传感器脱落，信号判为不可用。
		ArtifactRanges: []pipeline.SimRange{
			{From: start.Add(14 * sec), To: start.Add(18 * sec)},
		},
		ArtifactFlag: pipeline.FlagSensorOff,
	}, genSpO2)

	packets := append(sim1.Generate(), sim2.Generate()...)
	fmt.Printf("ingest %d packets (shuffled)\n", len(packets))
	var accepted, dup, late, rejected int
	for _, pk := range packets {
		r := p.Ingest(pk)
		switch r.Status {
		case pipeline.StatusAccept:
			accepted++
		case pipeline.StatusDuplicate:
			dup++
		case pipeline.StatusLateRejected:
			late++
		default:
			rejected++
		}
	}
	fmt.Printf("accepted=%d duplicate=%d lateRejected=%d otherRejected=%d\n", accepted, dup, late, rejected)

	end := start.Add(40 * sec)
	windows, events := p.Advance(end)
	windows = append(windows, p.Flush()...)
	fmt.Printf("\nmetric windows (gaps split windows): %d\n", len(windows))
	for _, w := range windows {
		fmt.Printf("  ch=%-11s [%s .. %s] n=%d mean=%.1f min=%.0f max=%.0f abnormal=%d gapBefore=%s\n",
			w.Channel, w.Start.Format("15:04:05"), w.End.Format("15:04:05"),
			w.Count, w.Mean, w.Min, w.Max, w.AbnormalCount, w.GapBefore)
	}
	fmt.Printf("\nalert events: %d\n", len(events))
	for _, e := range events {
		fmt.Printf("  %s@%s %s->%s value=%.0f rule=%s (%s)\n",
			e.RuleID, e.Time.Format("15:04:05"), stateName(e.From), stateName(e.To), e.Value, e.RuleVersion, e.Reason)
	}
}

func stateName(s pipeline.AlertState) string {
	switch s {
	case pipeline.AlertInactive:
		return "INACTIVE"
	case pipeline.AlertPending:
		return "PENDING"
	case pipeline.AlertActive:
		return "ACTIVE"
	case pipeline.AlertRecovering:
		return "RECOVERING"
	}
	return "?"
}

// Command demo 在本地用确定性数据跑一条多设备生命体征管线，
// 打印时间对齐、质量判定、派生指标与告警结论。
// 不连接任何外部服务、不需要真实凭据。
//
// 运行：go run ./cmd/demo
package main

import (
	"fmt"
	"time"

	"github.com/h3yow/biosignal-pipeline-go"
	"github.com/h3yow/biosignal-pipeline-go/simulate"
)

func main() {
	start := time.Date(2026, 9, 24, 8, 0, 0, 0, time.UTC)
	spec := simulate.ScenarioSpec{
		Start: start, Duration: 35 * time.Second, Seed: 20260924,
		Devices: []simulate.DeviceSpec{
			{
				ID: "wrist-A", Period: time.Second,
				ClockOff: 300 * time.Millisecond, DriftPPM: 1200,
				Channels: []simulate.ChannelSpec{
					{Channel: "HR", Unit: "bpm", Base: 72, Amp: 3, Noise: 0.5, Min: 40, Max: 180},
					{Channel: "SpO2", Unit: "pct", Base: 98, Amp: 0.3, Noise: 0.05, Min: 85, Max: 100},
				},
				Outage:    []simulate.Interval{{From: 10 * time.Second, To: 13 * time.Second}},
				Artifacts: map[pipeline.ChannelID][]simulate.Interval{"HR": {{From: 16 * time.Second, To: 18 * time.Second}}},
				OutOfRange: map[pipeline.ChannelID]map[int64]float64{
					"HR": {5: 220, 20: 165, 21: 166, 22: 164},
				},
			},
			{
				ID: "chest-B", Period: time.Second,
				ClockOff: -150 * time.Millisecond, DriftPPM: -800,
				Channels: []simulate.ChannelSpec{
					{Channel: "HR", Unit: "bpmK", Base: 0.072, Amp: 0.003, Noise: 0.0005, Min: 40, Max: 180},
				},
			},
		},
		Net: []simulate.Perturbation{{
			Device:        "wrist-A",
			DuplicateSeqs: []int64{3, 7},
			SwapAfter:     []int64{5},
			DelaySeqs:     map[int64]time.Duration{2: 3 * time.Second},
		}},
	}

	cfg := pipeline.Config{
		Aligner: pipeline.AlignerConfig{
			ReorderWindow: 300 * time.Millisecond,
			MaxLateWindow: 1500 * time.Millisecond,
			DriftHalfLife: 10 * time.Second,
		},
		Bucket: time.Second, MaxGap: 2500 * time.Millisecond, Metric: 5 * time.Second,
	}
	p := pipeline.New(cfg,
		[]pipeline.ChannelSpec{
			{Channel: "HR", Unit: "bpm", Min: 40, Max: 180},
			{Channel: "SpO2", Unit: "pct", Min: 85, Max: 100},
		},
		[]pipeline.Rule{{
			ID: "hr-high", Version: "v1", Channel: "HR",
			High: 150, Low: 30, EnterWindow: 2 * time.Second, ExitWindow: 2 * time.Second,
		}})
	must(p.Calibration().Register(pipeline.Transform{Device: "wrist-A", Channel: "HR", From: "bpm", To: "bpm", Scale: 1, Version: "cal-A-1"}))
	must(p.Calibration().Register(pipeline.Transform{Device: "wrist-A", Channel: "SpO2", From: "pct", To: "pct", Scale: 1, Version: "cal-A-1"}))
	must(p.Calibration().Register(pipeline.Transform{Device: "chest-B", Channel: "HR", From: "bpmK", To: "bpm", Scale: 1000, Version: "cal-B-1"}))

	var accepts, dups, lates int
	var miss, unav, abn int
	var alarms int

	ingest := func(r pipeline.Result, err error) {
		must(err)
		for _, a := range r.Aligns {
			switch a.Action {
			case pipeline.ActionAccept:
				accepts++
			case pipeline.ActionDuplicate:
				dups++
			case pipeline.ActionLate:
				lates++
			}
		}
		for _, pt := range r.Points {
			switch pt.Verdict.Class {
			case pipeline.QualityMissing:
				miss++
			case pipeline.QualityUnavailable:
				unav++
			case pipeline.QualityAbnormal:
				abn++
			}
		}
		alarms += len(r.Alarms)
		for _, al := range r.Alarms {
			fmt.Printf("ALARM %-8s rule=%s(%s) dev=%s at=%s\n",
				al.Status, al.RuleID, al.RuleVersion, al.Device, al.At.Format("15:04:05"))
		}
	}

	for _, pk := range simulate.Generate(spec) {
		ingest(p.Push(pk.Sample))
	}
	ingest(p.Advance(spec.Start.Add(spec.Duration).Add(time.Second)), nil)
	ingest(p.Close(), nil)

	fmt.Println("---- deterministic summary ----")
	fmt.Printf("alignment: accept=%d duplicate=%d late=%d\n", accepts, dups, lates)
	fmt.Printf("quality  : missing=%d unavailable=%d abnormal=%d\n", miss, unav, abn)
	fmt.Printf("alarms   : %d transition events\n", alarms)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

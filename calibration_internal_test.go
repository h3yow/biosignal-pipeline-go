package pipeline

import (
	"errors"
	"testing"
)

func TestCalibrationConvertAndTrace(t *testing.T) {
	r := NewCalibrationRegistry()
	tr := Transform{Device: "chest-B", Channel: "HR", From: "bpmK", To: "bpm", Scale: 1000, Offset: 0, Version: "cal-B-1"}
	if err := r.Register(tr); err != nil {
		t.Fatal(err)
	}
	v, used, err := r.Convert("chest-B", "HR", 0.0725, "bpmK")
	if err != nil {
		t.Fatal(err)
	}
	if !(v > 72.4 && v < 72.6) {
		t.Fatalf("convert = %v, want ~72.5", v)
	}
	t.Logf("basis: 0.0725 bpmK * %.0f => %.3f bpm using calibration version=%s (traceable)", used.Scale, v, used.Version)
	if used.Version != "cal-B-1" {
		t.Fatalf("calibration version not carried: %s", used.Version)
	}

	// 已是规范量纲：恒等换算。
	if v, _, err := r.Convert("chest-B", "HR", 72, "bpm"); err != nil || v != 72 {
		t.Fatalf("identity convert got %v err=%v", v, err)
	}
}

func TestCalibrationErrorDistinction(t *testing.T) {
	r := NewCalibrationRegistry()
	if err := r.Register(Transform{Device: "d", Channel: "HR", From: "bpm", To: "bpm", Scale: 1, Version: "v1"}); err != nil {
		t.Fatal(err)
	}
	// 未知设备/通道 => ErrNoCalibration（必须可与未知量纲区分）。
	if _, _, err := r.Convert("x", "HR", 1, "bpm"); !errors.Is(err, ErrNoCalibration) {
		t.Fatalf("want ErrNoCalibration, got %v", err)
	}
	// 已知通道但来源量纲未注册 => ErrUnknownUnit。
	if _, _, err := r.Convert("d", "HR", 1, "kelvin"); !errors.Is(err, ErrUnknownUnit) {
		t.Fatalf("want ErrUnknownUnit, got %v", err)
	}
	t.Log("basis: unknown device -> NO_CALIBRATION; known channel+unknown unit -> UNKNOWN_UNIT (distinguishable)")

	// 同来源量纲重复注册但版本不同 => 配置冲突错误。
	err := r.Register(Transform{Device: "d", Channel: "HR", From: "bpm", To: "bpm", Scale: 1, Version: "v2"})
	if err == nil {
		t.Fatal("re-registering same transform with different version must fail")
	}
	t.Logf("basis: conflicting calibration version rejected: %v", err)
}

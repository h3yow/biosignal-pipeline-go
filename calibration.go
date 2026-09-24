package pipeline

import (
	"errors"
	"fmt"
	"sync"
)

// Transform 描述一次线性量纲换算：To = Value*Scale + Offset。
//
// 线性换算覆盖了本地模拟场景所需的校准形式（单位线性换算 + 零点偏移），
// 每条换算带有不可为空的版本号 Version，使任何一个对齐后采样点的数值
// 都可以追溯到具体的校准配置版本。
type Transform struct {
	Device  DeviceID
	Channel ChannelID
	From    Unit // 设备上报量纲
	To      Unit // 通道规范量纲
	Scale   float64
	Offset  float64
	Version string
}

type calKey struct {
	device  DeviceID
	channel ChannelID
	from    Unit
}

// CalibrationRegistry 管理各设备/通道的校准换算。
// 同一 (设备, 通道) 的所有换算必须指向同一个规范量纲 To。
type CalibrationRegistry struct {
	mu        sync.RWMutex
	entries   map[calKey]Transform
	canonical map[calKey]Unit // 以 (device, channel, "") 形式记录规范量纲
}

// NewCalibrationRegistry 创建空的校准注册表。
func NewCalibrationRegistry() *CalibrationRegistry {
	return &CalibrationRegistry{
		entries:   make(map[calKey]Transform),
		canonical: make(map[calKey]Unit),
	}
}

var calKeyErr = errors.New("invalid transform")

// Register 注册一条校准换算。重复注册同一来源量纲必须使用相同版本，
// 否则视为配置冲突；换算的 Scale 不得为 0。
func (r *CalibrationRegistry) Register(t Transform) error {
	if t.Device == "" || t.Channel == "" || t.From == "" || t.To == "" || t.Version == "" {
		return fmt.Errorf("%w: device/channel/from/to/version must be non-empty", calKeyErr)
	}
	if t.Scale == 0 {
		return fmt.Errorf("%w: scale must not be zero", calKeyErr)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	ck := calKey{device: t.Device, channel: t.Channel}
	if got, ok := r.canonical[ck]; ok && got != t.To {
		return fmt.Errorf("%w: channel %s/%s already uses canonical unit %s, got %s",
			calKeyErr, t.Device, t.Channel, got, t.To)
	}
	k := calKey{device: t.Device, channel: t.Channel, from: t.From}
	if old, ok := r.entries[k]; ok && old.Version != t.Version {
		return fmt.Errorf("%w: transform %s/%s/%s already registered with version %s",
			calKeyErr, t.Device, t.Channel, t.From, old.Version)
	}
	r.entries[k] = t
	r.canonical[ck] = t.To
	return nil
}

// Convert 将指定设备/通道的值从 from 量纲换算到该通道的规范量纲。
// 返回值同时给出实际使用的 Transform，便于调用方记录校准版本。
//
// 错误区分：
//   - ErrNoCalibration：该设备/通道从未注册任何校准；
//   - ErrUnknownUnit：通道已知，但上报量纲不在已注册的来源量纲中。
func (r *CalibrationRegistry) Convert(device DeviceID, channel ChannelID, v float64, from Unit) (float64, Transform, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ck := calKey{device: device, channel: channel}
	canon, channelKnown := r.canonical[ck]
	if !channelKnown {
		return 0, Transform{}, fmt.Errorf("%w: device=%s channel=%s", ErrNoCalibration, device, channel)
	}
	// 上报量纲已经是规范量纲：恒等换算，版本记为 "identity"。
	if from == canon {
		return v, Transform{Device: device, Channel: channel, From: from, To: canon, Scale: 1, Version: "identity"}, nil
	}
	t, ok := r.entries[calKey{device: device, channel: channel, from: from}]
	if !ok {
		return 0, Transform{}, fmt.Errorf("%w: device=%s channel=%s unit=%s", ErrUnknownUnit, device, channel, from)
	}
	return v*t.Scale + t.Offset, t, nil
}

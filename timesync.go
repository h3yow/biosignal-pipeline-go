package pipeline

import (
	"math"
	"time"
)

// ClockRegistry 管理各设备的时钟模型。
//
// 映射规则（可追溯、确定性）：
//
//	unified = epoch + (deviceTime-epoch)*(1+driftPPM*1e-6) + offset
//
// 其中 epoch 为所有设备共享的漂移基准时刻（取模拟开始时刻），
// offset 为固定相位偏移，driftPPM 为线性漂移率。
type ClockRegistry struct {
	epoch  time.Time
	params map[DeviceID]ClockParams
}

// NewClockRegistry 创建时钟注册表；epoch 为漂移模型的基准时刻。
func NewClockRegistry(epoch time.Time) *ClockRegistry {
	return &ClockRegistry{
		epoch:  epoch,
		params: make(map[DeviceID]ClockParams),
	}
}

// SetClock 注册/更新某设备的时钟参数。
func (r *ClockRegistry) SetClock(d DeviceID, p ClockParams) {
	r.params[d] = p
}

// ToUnified 把设备本地时刻映射到统一时间轴。
// 未注册的设备返回 ok=false，调用方必须拒绝该数据（不得静默假设零偏移）。
func (r *ClockRegistry) ToUnified(d DeviceID, deviceTime time.Time) (time.Time, bool) {
	p, ok := r.params[d]
	if !ok {
		return time.Time{}, false
	}
	elapsed := deviceTime.Sub(r.epoch)
	// 舍入到纳秒，消除浮点漂移项的亚纳秒噪声，保证映射确定可复现。
	corrected := time.Duration(math.Round(float64(elapsed) * (1 + p.DriftPPM*1e-6)))
	return r.epoch.Add(corrected + p.Offset), true
}

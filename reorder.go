package pipeline

import (
	"sort"
	"time"
)

// ReorderBuffer 负责去重、乱序重排与迟到拒绝。
//
// 固定策略（确定性、可解释）：
//   - 去重：同一 (device,channel) 下 Seq 已见过的包判 StatusDuplicate，不重复计入。
//   - 校准：physical = raw*Scale + Offset，换算信息来自 ChannelSpec，可追溯。
//   - 水位：watermark = maxUnifiedSeen - maxDelay。统一时间早于水位的包判
//     StatusLateRejected，绝不插入到已 Drain 出去的错误位置，也不静默丢弃。
//   - 未注册时钟/校准的包分别判 StatusUnknownDevice/StatusUnknownChannel。
//   - Drain(cutoff) 输出统一时间 <= cutoff 的点，严格升序；同槽位多包按
//     (time, seq) 稳定排序，结果只取决于输入内容，与到达顺序无关。
type ReorderBuffer struct {
	clocks   *ClockRegistry
	specs    map[DeviceID]map[ChannelID]ChannelSpec
	maxDelay time.Duration
	pending  []AlignedPoint
	seen     map[struct {
		d DeviceID
		c ChannelID
		n int64
	}]struct{}
	maxSeen map[DeviceID]time.Time
}

// NewReorderBuffer 创建乱序缓冲。
func NewReorderBuffer(clocks *ClockRegistry, specs []ChannelSpec, maxDelay time.Duration) *ReorderBuffer {
	b := &ReorderBuffer{
		clocks:   clocks,
		specs:    make(map[DeviceID]map[ChannelID]ChannelSpec),
		maxDelay: maxDelay,
		seen: make(map[struct {
			d DeviceID
			c ChannelID
			n int64
		}]struct{}),
		maxSeen: make(map[DeviceID]time.Time),
	}
	for _, s := range specs {
		m := b.specs[s.Device]
		if m == nil {
			m = make(map[ChannelID]ChannelSpec)
			b.specs[s.Device] = m
		}
		m[s.Channel] = s
	}
	return b
}

// Ingest 接收一个原始包并返回处置结论；接受时返回 Point。
func (b *ReorderBuffer) Ingest(s RawSample) IngestResult {
	key := struct {
		d DeviceID
		c ChannelID
		n int64
	}{s.Device, s.Channel, s.Seq}
	if _, dup := b.seen[key]; dup {
		return IngestResult{
			Status: StatusDuplicate,
			Reason: "seq already accepted for device/channel; duplicate packet not counted",
		}
	}

	ut, ok := b.clocks.ToUnified(s.Device, s.DeviceTime)
	if !ok {
		return IngestResult{
			Status: StatusUnknownDevice,
			Reason: "device has no registered clock model; cannot map to unified timeline",
		}
	}

	// 迟到判定以“该设备已见到的最大统一时间”为水位基准，避免不同设备
	// 时钟差异互相影响。
	if max, seen := b.maxSeen[s.Device]; seen && ut.Before(max.Add(-b.maxDelay)) {
		return IngestResult{
			Status: StatusLateRejected,
			Reason: "unified time " + ut.Format(time.RFC3339Nano) +
				" is before watermark " + max.Add(-b.maxDelay).Format(time.RFC3339Nano) +
				"; exceeds acceptable delay, rejected by fixed policy",
		}
	}

	cm, ok := b.specs[s.Device]
	if !ok {
		return IngestResult{Status: StatusUnknownChannel, Reason: "device has no registered channels"}
	}
	spec, ok := cm[s.Channel]
	if !ok {
		return IngestResult{Status: StatusUnknownChannel, Reason: "channel has no registered calibration"}
	}

	ap := AlignedPoint{
		Device:   s.Device,
		Channel:  s.Channel,
		Seq:      s.Seq,
		Time:     ut,
		RawValue: s.RawValue,
		Value:    s.RawValue*spec.Cal.Scale + spec.Cal.Offset,
		Unit:     spec.Cal.Unit,
		Flags:    s.Flags,
	}
	b.seen[key] = struct{}{}
	b.pending = append(b.pending, ap)
	if max, seen := b.maxSeen[s.Device]; !seen || ut.After(max) {
		b.maxSeen[s.Device] = ut
	}
	return IngestResult{
		Status: StatusAccept,
		Reason: "mapped to unified time and calibrated: " +
			"physical=raw*" + formatFloat(spec.Cal.Scale) + "+" + formatFloat(spec.Cal.Offset),
		Point: &ap,
	}
}

// Drain 返回并移除所有时间 <= cutoff 的已接受点，按 (channel,time,seq)
// 确定性排序；跨通道按 ChannelID 字典序，保证输出可复现。
func (b *ReorderBuffer) Drain(cutoff time.Time) []AlignedPoint {
	ready := make([]AlignedPoint, 0)
	rest := make([]AlignedPoint, 0, len(b.pending))
	for _, p := range b.pending {
		if !p.Time.After(cutoff) {
			ready = append(ready, p)
		} else {
			rest = append(rest, p)
		}
	}
	sort.SliceStable(ready, func(i, j int) bool {
		if ready[i].Channel != ready[j].Channel {
			return ready[i].Channel < ready[j].Channel
		}
		if !ready[i].Time.Equal(ready[j].Time) {
			return ready[i].Time.Before(ready[j].Time)
		}
		return ready[i].Seq < ready[j].Seq
	})
	b.pending = rest
	return ready
}

// PendingCount 返回尚未 Drain 的缓冲点数，供测试与诊断使用。
func (b *ReorderBuffer) PendingCount() int { return len(b.pending) }

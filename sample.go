package pipeline

import "time"

// DeviceID 标识一台采集设备。
type DeviceID string

// ChannelID 标识一个生命体征通道（如 HR、SpO2）。
type ChannelID string

// Unit 标识采样值的量纲。
type Unit string

// RawHint 是设备随数据包上报的原始质量提示（仅作参考输入）。
type RawHint uint8

// Sample 表示从设备接收到的一个原始采样点（尚未对齐、未校准）。
type Sample struct {
	Device     DeviceID
	Channel    ChannelID
	Seq        int64 // 设备侧单调递增序号，用于去重与乱序判定
	DeviceTime time.Time
	RecvTime   time.Time // 管线所在主机接收到该包的时间
	Value      float64
	Unit       Unit
	Hint       RawHint
}

// AlignedSample 是经过时钟对齐、去重判定与量纲换算后的规范采样点。
type AlignedSample struct {
	Device     DeviceID
	Channel    ChannelID
	Seq        int64
	MasterTime time.Time // 归一到主时间轴后的时间
	Value      float64   // 已换算到规范量纲
	Unit       Unit      // 规范量纲
	CalVersion string    // 本次换算使用的校准版本，保证可追溯
}

// AlignAction 描述一个数据包在对齐阶段的处置结论。
type AlignAction int

const (
	ActionAccept AlignAction = iota
	ActionDuplicate
	ActionLate
)

// AlignEvent 是对齐阶段输出的一条可解释记录。
type AlignEvent struct {
	Sample AlignedSample
	Action AlignAction
	Reason ReasonCode
}

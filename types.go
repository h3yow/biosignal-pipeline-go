package pipeline

import "time"

// DeviceID / ChannelID 标识一个物理设备与其数据通道。
type DeviceID string

type ChannelID string

// Unit 是校准后的物理量纲。
type Unit string

// RawFlag 是设备自身上报的采集状态位。
type RawFlag uint8

const (
	FlagOK         RawFlag = 0
	FlagSensorOff  RawFlag = 1 << iota // 传感器脱落/未佩戴
	FlagSaturation                     // ADC 饱和削顶
	FlagLeadsOff                       // 导联脱落
)

// RawSample 是从设备接收到的原始采样包。
// DeviceTime 使用设备本地时钟；RecvTime 是管线接收到该包的时刻。
type RawSample struct {
	Device     DeviceID
	Channel    ChannelID
	Seq        int64 // 设备内单调递增的序列号，用于去重与丢包检测
	DeviceTime time.Time
	RecvTime   time.Time
	RawValue   float64
	Flags      RawFlag
}

// Calibration 描述物理值换算: physical = raw*Scale + Offset。
type Calibration struct {
	Scale  float64
	Offset float64
	Unit   Unit
}

// ChannelSpec 描述一个通道的静态属性。
type ChannelSpec struct {
	Device          DeviceID
	Channel         ChannelID
	Cal             Calibration
	NominalInterval time.Duration // 标称采样周期
}

// ClockParams 描述设备时钟相对统一时间轴的模型:
// unified = (deviceTime - epoch) * (1 + DriftPPM*1e-6) + epoch + Offset。
type ClockParams struct {
	Offset   time.Duration // 固定相位偏移（统一时间 - 设备时间）
	DriftPPM float64       // 线性漂移，百万分率
}

// PointStatus 描述一个原始包进入对齐缓冲后的处置结论。
type PointStatus uint8

const (
	// StatusAccept 表示数据被接受并进入统一时间轴。
	StatusAccept PointStatus = iota
	// StatusDuplicate 表示序列号重复，被去重忽略。
	StatusDuplicate
	// StatusLateRejected 表示超过可接受延迟、按固定策略拒绝。
	StatusLateRejected
	// StatusUnknownDevice 表示设备未注册时钟模型，无法对齐，拒绝。
	StatusUnknownDevice
	// StatusUnknownChannel 表示通道未注册校准信息，无法换算，拒绝。
	StatusUnknownChannel
)

// AlignedPoint 是归一到统一时间轴、完成量纲换算后的采样点。
type AlignedPoint struct {
	Device  DeviceID
	Channel ChannelID
	Seq     int64
	// Time 是映射到统一时间轴后的时间。
	Time time.Time
	// Value 是校准后的物理值。
	Value    float64
	RawValue float64
	Unit     Unit
	Flags    RawFlag
}

// IngestResult 是单次 Ingest 的可解释处置结果。
type IngestResult struct {
	Status PointStatus
	Reason string
	Point  *AlignedPoint
}

// QualityClass 是信号质量三分类（外加正常）。
type QualityClass uint8

const (
	// QGood 信号正常、数值可信。
	QGood QualityClass = iota
	// QMissing 数据缺失（该有数据但没有）。
	QMissing
	// QArtifactUnusable 信号不可用/受伪迹影响（有数据但不可信）。
	QArtifactUnusable
	// QValueAbnormal 信号本身可信，但数值超出正常生理范围。
	QValueAbnormal
)

// QualityVerdict 是单个通道在某个时间槽上的质量判定。
type QualityVerdict struct {
	Device  DeviceID
	Channel ChannelID
	Time    time.Time
	Class   QualityClass
	// Reasons 记录判定依据，保证结论可解释。
	Reasons []string
	// Value 为校准后的物理值；缺失误判时为 0 且 Present=false。
	Present bool
	Value   float64
	Unit    Unit
}

// AlertState 是告警状态机的四态。
type AlertState uint8

const (
	// AlertInactive 未告警。
	AlertInactive AlertState = iota
	// AlertPending 连续越限窗计数中，尚未达到触发条件。
	AlertPending
	// AlertActive 告警已触发。
	AlertActive
	// AlertRecovering 越限消失，连续恢复窗计数中（迟滞，防止抖动反复）。
	AlertRecovering
)

// AlertEvent 是告警状态迁移事件，带规则版本与配置快照以支持历史复现。
type AlertEvent struct {
	RuleID      string
	RuleVersion string
	Channel     ChannelID
	Time        time.Time
	From        AlertState
	To          AlertState
	Value       float64
	Reason      string
	// Snapshot 固化产生该事件时生效的换算参数与规则参数（机器可读）。
	Snapshot ConfigSnapshot
}

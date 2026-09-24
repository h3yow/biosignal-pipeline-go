package pipeline

import "errors"

// ReasonCode 以稳定字符串编码“拒绝/降级原因”，保证失败原因可区分、可断言。
type ReasonCode string

const (
	ReasonAccepted          ReasonCode = "ACCEPTED"
	ReasonDupSeq            ReasonCode = "DUP_SEQ"
	ReasonLateBeyondWindow  ReasonCode = "LATE_BEYOND_WINDOW"
	ReasonMissingWindow     ReasonCode = "MISSING_WINDOW"
	ReasonArtifactHint      ReasonCode = "ARTIFACT_HINT"
	ReasonValueOutOfRange   ReasonCode = "VALUE_OUT_OF_RANGE"
	ReasonGapInterrupted    ReasonCode = "GAP_INTERRUPTED"
	ReasonUnavailableInputs ReasonCode = "UNAVAILABLE_INPUTS"
)

// 硬错误（接入失败，区别于下面的正常拒绝/降级事件）。
var (
	// ErrNoCalibration：设备/通道从未注册校准，接入被拒。
	ErrNoCalibration = errors.New("pipeline: no calibration registered")
	// ErrUnknownUnit：通道已知，但上报量纲没有对应换算。
	ErrUnknownUnit = errors.New("pipeline: unknown unit")
)

// 下列哨兵用于语义化文档与供外部包 errors.Is 统一引用。重复与迟到在本管线中
// 属于“正常但可解释的拒绝事件”（通过 AlignEvent + ReasonCode 返回），而不是
// Go error；保留同名哨兵仅为暴露其稳定语义。
var (
	ErrDuplicate        = errors.New("pipeline: duplicate packet")
	ErrLateBeyondWindow = errors.New("pipeline: packet later than acceptable window")
	ErrNoData           = errors.New("pipeline: no data in window")
)

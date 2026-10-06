package pipeline

import "time"

// ConfigSnapshot 固化“产生某条结论时”生效的换算参数与规则版本，
// 以结构化字段随结论一起输出（程序可直接读取，而非仅人类可读文字）。
//
// 用途：配置（换算参数、告警/事件规则）后来改版后，历史结论不被改写；
// 用快照中的版本与参数对同一批历史数据重放，可逐条还原相同结论。
type ConfigSnapshot struct {
	// 规则身份与版本。
	RuleID      string
	RuleVersion string
	// 质量判定规则参数（缺失容差、生理可信范围）。
	ExpectedInterval time.Duration
	GapTolerance     time.Duration
	PhysMin          float64
	PhysMax          float64
	// 数值边界（告警规则与事件规则共用同一组字段）。
	HighEnabled   bool
	HighThreshold float64
	LowEnabled    bool
	LowThreshold  float64
	// 告警规则参数（事件规则下为零值）。
	TriggerN int
	RecoverN int
	// 事件规则参数：连续正常满该时长事件才结束（告警规则下为零值）。
	RecoverDuration time.Duration
	// 管线级确定性参数：缺失填充槽步长与静默截断上限（生效值）。
	SlotStep   time.Duration
	SilenceCap time.Duration
	// 产生结论时通道使用的换算参数与量纲。
	Cal Calibration
}

// qualityContext 是构建某条通道配置快照所需的全部上下文：
// 通道换算参数、质量规则、管线级槽步长与生效静默上限。
type qualityContext struct {
	cal        Calibration
	qr         QualityRule
	slotStep   time.Duration
	silenceCap time.Duration
}

// baseSnapshot 填充与具体告警/事件规则无关的公共字段。
func (qc qualityContext) baseSnapshot() ConfigSnapshot {
	return ConfigSnapshot{
		RuleID:           qc.qr.ID,
		RuleVersion:      qc.qr.Version,
		ExpectedInterval: qc.qr.ExpectedInterval,
		GapTolerance:     qc.qr.GapTolerance,
		PhysMin:          qc.qr.PhysMin,
		PhysMax:          qc.qr.PhysMax,
		SlotStep:         qc.slotStep,
		SilenceCap:       qc.silenceCap,
		Cal:              qc.cal,
	}
}

// snapshotFromQuality 为质量判定/指标窗/静默截断等“非告警事件”结论构建快照。
func snapshotFromQuality(qc qualityContext) ConfigSnapshot {
	return qc.baseSnapshot()
}

// snapshotFromAlertRule 由告警规则与通道上下文构建快照。
func snapshotFromAlertRule(r AlertRule, qc qualityContext) ConfigSnapshot {
	s := qc.baseSnapshot()
	s.RuleID = r.ID
	s.RuleVersion = r.Version
	s.HighEnabled = r.HighEnabled
	s.HighThreshold = r.HighThreshold
	s.LowEnabled = r.LowEnabled
	s.LowThreshold = r.LowThreshold
	s.TriggerN = r.TriggerN
	s.RecoverN = r.RecoverN
	return s
}

// snapshotFromEpisodeRule 由事件规则与通道上下文构建快照。
func snapshotFromEpisodeRule(r EpisodeRule, qc qualityContext) ConfigSnapshot {
	s := qc.baseSnapshot()
	s.RuleID = r.ID
	s.RuleVersion = r.Version
	s.HighEnabled = r.HighEnabled
	s.HighThreshold = r.HighThreshold
	s.LowEnabled = r.LowEnabled
	s.LowThreshold = r.LowThreshold
	s.RecoverDuration = r.RecoverDuration
	return s
}

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
	// 产生结论时通道使用的换算参数与量纲。
	Cal Calibration
	// SilenceCap 为产生结论时实际生效的静默填充上限
	// （未配置时取默认上限 DefaultSilenceCap，此处记录解析后的实际值）。
	SilenceCap time.Duration
}

// snapshotFromAlertRule 由告警规则与通道校准构建快照。
func snapshotFromAlertRule(r AlertRule, cal Calibration) ConfigSnapshot {
	return ConfigSnapshot{
		RuleID: r.ID, RuleVersion: r.Version,
		HighEnabled: r.HighEnabled, HighThreshold: r.HighThreshold,
		LowEnabled: r.LowEnabled, LowThreshold: r.LowThreshold,
		TriggerN: r.TriggerN, RecoverN: r.RecoverN,
		Cal: cal,
	}
}

// snapshotFromEpisodeRule 由事件规则与通道校准构建快照。
func snapshotFromEpisodeRule(r EpisodeRule, cal Calibration) ConfigSnapshot {
	return ConfigSnapshot{
		RuleID: r.ID, RuleVersion: r.Version,
		HighEnabled: r.HighEnabled, HighThreshold: r.HighThreshold,
		LowEnabled: r.LowEnabled, LowThreshold: r.LowThreshold,
		RecoverDuration: r.RecoverDuration,
		Cal:             cal,
	}
}

// snapshotFromQualityRule 由质量规则、通道校准与实际生效的静默上限构建快照，
// 用于指标窗与静默截断这类“由质量判定流直接产生”的结论。
func snapshotFromQualityRule(r QualityRule, cal Calibration, cap time.Duration) ConfigSnapshot {
	return ConfigSnapshot{
		RuleID: r.ID, RuleVersion: r.Version,
		Cal:        cal,
		SilenceCap: cap,
	}
}

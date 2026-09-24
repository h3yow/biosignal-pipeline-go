package pipeline

import "time"

// AlarmStatus 是告警机状态。
type AlarmStatus int

const (
	// AlarmIdle：正常态。
	AlarmIdle AlarmStatus = iota
	// AlarmActive：告警态（已触发、尚未恢复）。
	AlarmActive
)

func (s AlarmStatus) String() string {
	if s == AlarmActive {
		return "ACTIVE"
	}
	return "IDLE"
}

// Rule 描述一条基于连续时间窗的告警规则及其配置版本。
//
// Device 为空表示匹配该通道的任意设备；阈值越界定义为
// value > High 或 value < Low（闭区间 [Low, High] 视为安全）。
// 每个事件记录 RuleVersion，规则版本变化后历史结论仍可按版本复现。
type Rule struct {
	ID          string
	Version     string
	Device      DeviceID
	Channel     ChannelID
	High        float64
	Low         float64
	EnterWindow time.Duration // 连续越界覆盖此时长才触发
	ExitWindow  time.Duration // 连续安全覆盖此时长才恢复
}

func (r Rule) matches(dev DeviceID, ch ChannelID) bool {
	return ch == r.Channel && (r.Device == "" || r.Device == dev)
}

// AlarmEvent 是一次告警触发或恢复事件（仅在状态跳变时产生，天然去重）。
type AlarmEvent struct {
	RuleID      string
	RuleVersion string
	Device      DeviceID
	Channel     ChannelID
	Status      AlarmStatus
	At          time.Time
}

type alarmState struct {
	status      AlarmStatus
	breachFirst time.Time // 当前连续越界段的首个 GOOD 点时间
	breachLast  time.Time
	breachCount int
	safeFirst   time.Time // 当前连续安全段的首个 GOOD 点时间
	safeLast    time.Time
	safeCount   int
}

// Engine 执行带迟滞的连续窗告警判定。
//
// 判定原则：
//   - 只有 QualityGood 的点可以推进“越界连续时长”和“安全连续时长”；
//     MISSING / UNAVAILABLE / ABNORMAL 以及中断边界会清空两个连续段，
//     但保持当前告警状态不变——信号不好时既不触发、也不恢复，
//     因而伪迹抖动不会造成告警反复触发/消失；
//   - 事件仅在 IDLE→ACTIVE、ACTIVE→IDLE 跳变时输出一次，
//     同一越界无论持续多久或被重放多少次都不会重复告警；
//   - 引擎是“有序判定点流 → 事件流”的纯函数，同输入必同输出，
//     在线处理与历史重放收敛到完全一致的结论。
type Engine struct {
	rules []Rule
	state map[string]*alarmState // ruleID + "\x00" + device
}

// NewEngine 创建告警引擎。
func NewEngine(rules []Rule) *Engine {
	return &Engine{rules: rules, state: make(map[string]*alarmState)}
}

func alarmKey(ruleID string, dev DeviceID) string { return ruleID + "\x00" + string(dev) }

// Evaluate 接收一个时间有序的判定点，返回产生的告警事件（0 或 1 个/规则）。
func (e *Engine) Evaluate(p JudgedPoint) []AlarmEvent {
	var out []AlarmEvent
	t := p.Sample.MasterTime
	good := p.Verdict.Class == QualityGood
	for i := range e.rules {
		r := &e.rules[i]
		if !r.matches(p.Sample.Device, p.Sample.Channel) {
			continue
		}
		st := e.state[alarmKey(r.ID, p.Sample.Device)]
		if st == nil {
			st = &alarmState{}
			e.state[alarmKey(r.ID, p.Sample.Device)] = st
		}

		// 非 GOOD 点或中断边界：连续段作废，状态保持（信号差时既不
		// 触发也不恢复）。
		if !good || p.GapAfter {
			st.breachCount, st.safeCount = 0, 0
			continue
		}

		breach := p.Sample.Value > r.High || p.Sample.Value < r.Low
		if breach {
			st.safeCount = 0
			if st.breachCount == 0 {
				st.breachFirst, st.breachLast = t, t
			} else {
				st.breachLast = t
			}
			st.breachCount++
			// 连续跨度（末点-首点）达到 EnterWindow 才触发：
			// 需要至少 3 个周期点覆盖整个窗口，单帧/两帧抖动不触发。
			if st.status == AlarmIdle && st.breachCount >= 2 &&
				time.Duration(durationNS(st.breachLast, st.breachFirst)) >= r.EnterWindow {
				st.status = AlarmActive
				out = append(out, AlarmEvent{
					RuleID: r.ID, RuleVersion: r.Version,
					Device: p.Sample.Device, Channel: p.Sample.Channel,
					Status: AlarmActive, At: t,
				})
			}
			continue
		}

		// 安全点。
		st.breachCount = 0
		if st.safeCount == 0 {
			st.safeFirst, st.safeLast = t, t
		} else {
			st.safeLast = t
		}
		st.safeCount++
		if st.status == AlarmActive && st.safeCount >= 2 &&
			time.Duration(durationNS(st.safeLast, st.safeFirst)) >= r.ExitWindow {
			st.status = AlarmIdle
			out = append(out, AlarmEvent{
				RuleID: r.ID, RuleVersion: r.Version,
				Device: p.Sample.Device, Channel: p.Sample.Channel,
				Status: AlarmIdle, At: t,
			})
		}
	}
	return out
}

// State 返回某规则在某设备上的当前状态（用于测试与可观测）。
func (e *Engine) State(ruleID string, dev DeviceID) AlarmStatus {
	if st := e.state[alarmKey(ruleID, dev)]; st != nil {
		return st.status
	}
	return AlarmIdle
}

// durationNS 返回两个时刻之间的纳秒跨度，基于 UnixNano 差值计算，
// 因而不依赖 time.Time 的 Location（构造 loc=nil 的时间也确定有效）。
func durationNS(later, earlier time.Time) int64 {
	return later.UnixNano() - earlier.UnixNano()
}

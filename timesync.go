package pipeline

import (
	"sort"
	"time"
)

// AlignerConfig 配置时间对齐器。
type AlignerConfig struct {
	// ReorderWindow 乱序容忍窗口：主时间轴时间戳不晚于
	// (该通道前沿 - ReorderWindow) 的缓冲点可以被有序释放。
	ReorderWindow time.Duration
	// MaxLateWindow 可接受延迟上界：一个新到点的主时间戳早于
	// (该通道前沿 - MaxLateWindow) 时判定为迟到，立即以迟到事件
	// 形式输出，绝不回插到已释放的有序序列中。
	MaxLateWindow time.Duration
	// DriftHalfLife 设备时钟漂移估计的半衰期（按设备时间跨度加权）。
	DriftHalfLife time.Duration
}

// clockModel 用带指数衰减权重的最小二乘为单台设备估计
//
//	masterTime = slope*deviceTime + intercept
//
// 权重只依赖观测点的设备时间跨度与 DriftHalfLife，因此同一输入序列
// 永远得到同一估计结果（确定性、可复现）。
type clockModel struct {
	obs       []clockObs
	halfLife  time.Duration
	lastRef   time.Time // 已纳入模型的最新主机接收时刻
	have      bool
	x0        time.Time // 首个观测的设备时间（中心化原点）
	y0        time.Time // 首个观测的主机接收时间
	slope     float64   // d(master)/d(device)，相对原点拟合
	intercept float64   // 相对原点的截距（秒）
}

type clockObs struct {
	x float64 // 相对 x0 的设备秒数
	y float64 // 相对 y0 的主机秒数
}

const modelMaxObs = 128

func newClockModel(halfLife time.Duration) *clockModel {
	if halfLife <= 0 {
		halfLife = time.Minute
	}
	return &clockModel{halfLife: halfLife, slope: 1}
}

// observe 纳入一对 (设备时间, 主机接收时间) 观测。
// 接收时间倒退的观测不纳入模型（避免迟到/乱序包污染漂移估计）。
func (m *clockModel) observe(deviceTime, recvTime time.Time) {
	if m.have && recvTime.Before(m.lastRef) {
		return
	}
	if !m.have {
		m.x0, m.y0 = deviceTime, recvTime
	}
	m.lastRef = recvTime
	m.obs = append(m.obs, clockObs{
		x: deviceTime.Sub(m.x0).Seconds(),
		y: recvTime.Sub(m.y0).Seconds(),
	})
	if n := len(m.obs); n > modelMaxObs {
		m.obs = m.obs[n-modelMaxObs:]
	}
	m.fit()
	m.have = true
}

// fit 用衰减权重最小二乘拟合 slope/intercept（全部基于中心化后的相对秒，
// 避免在 ~1.79e9 的绝对 Unix 秒上做平方导致灾难性浮点抵消）。
// 权重 w_i = 2^(-(x_last - x_i)/halfLife)，最近的观测权重最高，
// 只依赖观测内容，因此估计结果确定可复现。
func (m *clockModel) fit() {
	if len(m.obs) == 0 {
		return
	}
	if len(m.obs) == 1 {
		m.slope, m.intercept = 1, m.obs[0].y-m.obs[0].x
		return
	}
	var sw, swx, swy, swxx, swxy float64
	xLast := m.obs[len(m.obs)-1].x
	hl := m.halfLife.Seconds()
	for _, o := range m.obs {
		w := 1.0
		if d := xLast - o.x; d > 0 && hl > 0 {
			w = pow2(-d / hl)
		}
		sw += w
		swx += w * o.x
		swy += w * o.y
		swxx += w * o.x * o.x
		swxy += w * o.x * o.y
	}
	den := sw*swxx - swx*swx
	if den == 0 {
		m.slope = 1
		m.intercept = swy/sw - swx/sw
		return
	}
	m.slope = (sw*swxy - swx*swy) / den
	m.intercept = (swy - m.slope*swx) / sw
}

func pow2(z float64) float64 {
	// 2^z = exp(z*ln2)
	const ln2 = 0.6931471805599453
	return expFast(z * ln2)
}

// masterOf 将设备时间戳换算到主时间轴。
func (m *clockModel) masterOf(t time.Time) time.Time {
	x := t.Sub(m.x0).Seconds()
	return m.y0.Add(time.Duration((m.slope*x + m.intercept) * float64(time.Second)))
}

type channelState struct {
	pending  []AlignedSample // 尚未释放的点，按主时间戳排序
	frontier time.Time       // 已接受点中的最大主时间戳
	seen     map[int64]bool
}

// Aligner 负责设备时钟建模、乱序重排、去重与迟到标记。
// 输出对每个 (设备, 通道) 保证按主时间戳有序（迟到事件除外，
// 它们被单独标记并即时输出，不会回插）。
type Aligner struct {
	cfg    AlignerConfig
	cal    *CalibrationRegistry
	clocks map[DeviceID]*clockModel
	state  map[DeviceID]map[ChannelID]*channelState
}

// NewAligner 创建对齐器，校准注册表用于量纲归一。
func NewAligner(cfg AlignerConfig, cal *CalibrationRegistry) *Aligner {
	if cfg.ReorderWindow <= 0 {
		cfg.ReorderWindow = 100 * time.Millisecond
	}
	if cfg.MaxLateWindow < cfg.ReorderWindow {
		cfg.MaxLateWindow = cfg.ReorderWindow
	}
	return &Aligner{
		cfg:    cfg,
		cal:    cal,
		clocks: make(map[DeviceID]*clockModel),
		state:  make(map[DeviceID]map[ChannelID]*channelState),
	}
}

// Push 接收一个原始采样点。
//
// 返回事件切片（可能因缓冲释放产生零个或多个有序事件）与硬错误。
// 错误与拒绝的区分：
//   - error 非空表示无法接入（ErrNoCalibration / ErrUnknownUnit），
//     该点不进入任何后续处理；
//   - ActionDuplicate / ActionLate 是正常输出的事件，携带可区分的
//     ReasonCode，调用方据此解释结论。
func (a *Aligner) Push(s Sample) ([]AlignEvent, error) {
	value, tr, err := a.cal.Convert(s.Device, s.Channel, s.Value, s.Unit)
	if err != nil {
		return nil, err
	}

	st := a.channel(s.Device, s.Channel)

	// 1) 去重优先：相同 (设备, 通道, 序号) 一律拒绝且不计入任何状态。
	if st.seen[s.Seq] {
		return []AlignEvent{{Action: ActionDuplicate, Reason: ReasonDupSeq,
			Sample: AlignedSample{Device: s.Device, Channel: s.Channel, Seq: s.Seq, Unit: tr.To}}}, nil
	}

	// 2) 时钟对齐：
	//    - 设备的首个接受点先纳入模型完成初始化，再换算主时间戳；
	//    - 后续点先用当前模型换算做迟到判定，只有被接受后才更新模型，
	//      保证重复点、迟到点都不会污染时钟偏移/漂移估计。
	cm := a.clocks[s.Device]
	first := !cm.have
	if first {
		cm.observe(s.DeviceTime, s.RecvTime)
	}
	master := cm.masterOf(s.DeviceTime)

	aligned := AlignedSample{
		Device:     s.Device,
		Channel:    s.Channel,
		Seq:        s.Seq,
		MasterTime: master,
		Value:      value,
		Unit:       tr.To,
		CalVersion: tr.Version,
	}

	// 3) 迟到判定：主时间戳满足 master + MaxLateWindow <= frontier
	//    （即延迟达到或超过上界）。固定策略 = 明确标记并即时输出
	//    （ActionLate），不回插有序流，不静默丢弃。边界采用闭区间，
	//    等间隔采样点恰好落在边界时仍属“可接受延迟”。
	if st.haveFrontier() && !master.Add(a.cfg.MaxLateWindow).After(st.frontier) {
		st.seen[s.Seq] = true
		return []AlignEvent{{Sample: aligned, Action: ActionLate, Reason: ReasonLateBeyondWindow}}, nil
	}

	// 非首点通过检查后，纳入时钟模型并用更新后的模型给出最终对齐时间。
	if !first {
		cm.observe(s.DeviceTime, s.RecvTime)
		master = cm.masterOf(s.DeviceTime)
		aligned.MasterTime = master
	}

	// 4) 正常接受：登记、放入重排缓冲。
	st.seen[s.Seq] = true
	st.pending = append(st.pending, aligned)

	// 只有当该点的主时间戳真正推进了前沿（即它是“按序到达的最新点”）
	// 时才释放到期缓冲；乱序补到的较早点只入缓冲、不触发释放，
	// 这样它仍能与缓冲内相邻点一起按主时间戳重排，而不会插到
	// 已经释放到下游的序列后面。
	if !st.haveFrontier() || master.After(st.frontier) {
		st.frontier = master
		return a.release(s.Device, s.Channel), nil
	}
	return nil, nil
}

// Flush 释放所有通道缓冲中剩余的点（断连/收尾时调用），
// 每个通道内部仍按主时间戳有序。
func (a *Aligner) Flush() []AlignEvent {
	var out []AlignEvent
	// 设备与通道均按固定（字典序）顺序释放，保证输出可复现。
	devs := make([]DeviceID, 0, len(a.state))
	for d := range a.state {
		devs = append(devs, d)
	}
	sort.Slice(devs, func(i, j int) bool { return devs[i] < devs[j] })
	for _, dev := range devs {
		chs := a.state[dev]
		names := make([]ChannelID, 0, len(chs))
		for ch := range chs {
			names = append(names, ch)
		}
		sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })
		for _, ch := range names {
			st := chs[ch]
			out = append(out, drain(st, time.Time{}, 0)...)
		}
	}
	return out
}

func (a *Aligner) channel(dev DeviceID, ch ChannelID) *channelState {
	cm, ok := a.clocks[dev]
	if !ok {
		cm = newClockModel(a.cfg.DriftHalfLife)
		a.clocks[dev] = cm
	}
	chs, ok := a.state[dev]
	if !ok {
		chs = make(map[ChannelID]*channelState)
		a.state[dev] = chs
	}
	st, ok := chs[ch]
	if !ok {
		st = &channelState{seen: make(map[int64]bool)}
		chs[ch] = st
	}
	return st
}

func (s *channelState) haveFrontier() bool { return !s.frontier.IsZero() }

// release 释放已经在缓冲中待满整个 ReorderWindow 的点：
// 条件为 master + ReorderWindow <= frontier（闭区间）。
// 用“点自身时间戳到前沿的跨度”衡量等待时长，保证乱序补到的点在
// 容忍窗口内仍有机会与相邻点一起重排，而不会被提前释放。
func (a *Aligner) release(dev DeviceID, ch ChannelID) []AlignEvent {
	st := a.state[dev][ch]
	return drain(st, st.frontier, a.cfg.ReorderWindow)
}

// drain 移除并输出 pending 中满足 t+window <= frontier 的点
// （window 为零值表示 Flush，全部释放）。
func drain(st *channelState, frontier time.Time, window time.Duration) []AlignEvent {
	sort.SliceStable(st.pending, func(i, j int) bool {
		if st.pending[i].MasterTime.Equal(st.pending[j].MasterTime) {
			return st.pending[i].Seq < st.pending[j].Seq
		}
		return st.pending[i].MasterTime.Before(st.pending[j].MasterTime)
	})
	var out []AlignEvent
	keep := make([]AlignedSample, 0, len(st.pending))
	for _, p := range st.pending {
		// window<=0 表示 Flush：全部释放。
		if window > 0 && p.MasterTime.Add(window).After(frontier) {
			keep = append(keep, p)
			continue
		}
		out = append(out, AlignEvent{Sample: p, Action: ActionAccept, Reason: ReasonAccepted})
	}
	st.pending = keep
	return out
}

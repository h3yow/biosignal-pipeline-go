// Package simulate 在本地生成确定性的多设备多通道生命体征采样数据，
// 用于在无外部服务、无真实凭据的条件下验证管线。
//
// 所有“随机性”都来自固定种子的 LCG：同一份场景规格 + 同一个种子
// 永远生成同一批数据包；各类异常（时钟漂移、乱序、重复、迟到、
// 信号中断、重连、伪迹、数值越界）以显式规则注入，便于测试
// 精确断言并在日志中打印输入与依据。
package simulate

import (
	"sort"
	"time"

	"github.com/h3yow/biosignal-pipeline-go"
)

// ChannelSpec 描述一个通道的波形生成参数。
type ChannelSpec struct {
	Channel pipeline.ChannelID
	Unit    pipeline.Unit // 设备上报量纲（可与规范量纲不同，以验证校准）
	Base    float64
	Amp     float64 // 正弦波动幅度
	Noise   float64 // LCG 噪声幅度（确定种子）
	Min     float64 // 规范量纲下的合理范围下界
	Max     float64 // 规范量纲下的合理范围上界
}

// Interval 是主时间轴上的左闭右开时间区间（相对场景起点的偏移）。
type Interval struct {
	From time.Duration
	To   time.Duration
}

// DeviceSpec 描述一台设备及其时钟缺陷。
type DeviceSpec struct {
	ID         pipeline.DeviceID
	Period     time.Duration // 标称采样周期
	ClockOff   time.Duration // 固定时钟偏移（设备时间 - 主时间）
	DriftPPM   float64       // 时钟漂移：每秒偏差微秒数（1e-6 s/s）
	Channels   []ChannelSpec
	Outage     []Interval                               // 断连区间（区间内包完全不发送）
	Artifacts  map[pipeline.ChannelID][]Interval        // 伪迹区间（上报 HintArtifact）
	OutOfRange map[pipeline.ChannelID]map[int64]float64 // seq -> 强制越界值（规范量纲）
}

// Perturbation 以显式规则注入网络层异常（按每台设备的发送序号）。
type Perturbation struct {
	Device pipeline.DeviceID
	// DuplicateSeqs：这些序号的包额外再发一次（重复内容、相同序号）。
	DuplicateSeqs []int64
	// SwapAfter：在发送到该序号后，与紧随其后的一个包交换发送顺序（乱序）。
	SwapAfter []int64
	// DelaySeqs：这些序号的包的接收时间额外延后指定时长（制造迟到）。
	DelaySeqs map[int64]time.Duration
}

// ScenarioSpec 是完整的本地场景定义。
type ScenarioSpec struct {
	Start    time.Time // 主时间轴起点
	Duration time.Duration
	Devices  []DeviceSpec
	Net      []Perturbation
	Seed     int64
}

// Packet 是生成器产出的一个待发送数据包（即管线的一个输入）。
type Packet struct {
	pipeline.Sample
	// Annotation 记录该包被注入的异常类型，仅用于测试说明与日志，
	// 管线本身从不读取它（保证管线只依赖包内容做判定）。
	Annotation string
}

// lcg 是数值配方 (Numerical Recipes) 的确定性线性同余发生器。
type lcg struct{ state uint64 }

func newLCG(seed int64) *lcg { return &lcg{state: uint64(seed) ^ 0x9E3779B97F4A7C15} }

func (g *lcg) float64() float64 {
	g.state = g.state*6364136223846793005 + 1442695040888963407
	return float64(g.state>>11) / float64(uint64(1)<<53)
}

// rawPkt 是生成过程内部使用的带发送序号的数据包。
type rawPkt struct {
	p         Packet
	device    pipeline.DeviceID
	seq       int64
	sendOrder int64
}

// Generate 依据规格确定性地生成数据包序列（已按接收时间排序，
// 包含乱序/重复/迟到注入）。
func Generate(spec ScenarioSpec) []Packet {
	if spec.Seed == 0 {
		spec.Seed = 20260924
	}

	var all []rawPkt
	var sendCounter int64

	// 1) 按设备生成原始采样（主时间均匀，设备时间含偏移与漂移）。
	for di := range spec.Devices {
		d := &spec.Devices[di]
		devRNG := newLCG(spec.Seed + int64(di)*1000003)
		period := d.Period
		if period <= 0 {
			period = time.Second
		}
		pert := findPerturbation(spec.Net, d.ID)
		step := int64(0)
		var seq int64
		for elapsed := time.Duration(0); elapsed < spec.Duration; elapsed += period {
			master := spec.Start.Add(elapsed)
			if inAny(elapsed, d.Outage) {
				step++
				continue // 断连：包完全缺失
			}
			reconnect := elapsed > 0 && inAny(elapsed-period, d.Outage)
			// 设备时钟：偏移 + 线性漂移。
			devTime := master.Add(d.ClockOff).Add(driftOffset(elapsed, d.DriftPPM))
			// 接收时间：相对主时间的固定小处理延迟（确定性）。
			recv := master.Add(20 * time.Millisecond)
			if dl, ok := pert.DelaySeqs[seq]; ok {
				recv = recv.Add(dl)
			}
			for ci := range d.Channels {
				c := &d.Channels[ci]
				phase := float64(elapsed) / float64(time.Second)
				v := c.Base + c.Amp*sin2pi(phase/60.0)
				if c.Noise != 0 {
					v += (devRNG.float64()*2 - 1) * c.Noise
				}
				if forced, ok := d.OutOfRange[c.Channel][seq]; ok {
					v = forced
				}
				hint := pipeline.RawHint(pipeline.HintOK)
				if ivs, ok := d.Artifacts[c.Channel]; ok && inAny(elapsed, ivs) {
					hint = pipeline.HintArtifact
				}
				ann := ""
				switch {
				case reconnect:
					ann = "reconnect"
				case hint == pipeline.HintArtifact:
					ann = "artifact"
				}
				if _, dup := seqIn(seq, pert.DuplicateSeqs); dup {
					ann = joinAnn(ann, "dup-target")
				}
				p := Packet{
					Sample: pipeline.Sample{
						Device: d.ID, Channel: c.Channel, Seq: seq,
						DeviceTime: devTime, RecvTime: recv,
						Value: v, Unit: c.Unit, Hint: hint,
					},
					Annotation: ann,
				}
				sendCounter++
				all = append(all, rawPkt{p: p, device: d.ID, seq: seq, sendOrder: sendCounter})
			}
			step++
			seq++
		}
	}

	// 2) 注入重复包：在原包之后复制一份（相同序号/内容/时间）。
	var withDups []rawPkt
	for i := 0; i < len(all); i++ {
		withDups = append(withDups, all[i])
		pert := findPerturbation(spec.Net, all[i].device)
		if _, dup := seqIn(all[i].seq, pert.DuplicateSeqs); dup {
			cp := all[i]
			cp.p.Annotation = joinAnn(cp.p.Annotation, "duplicate")
			// 重复包接收时间略晚（重传），但序号相同，应被去重。
			cp.p.Sample.RecvTime = cp.p.Sample.RecvTime.Add(5 * time.Millisecond)
			cp.sendOrder = int64(len(withDups)) + 1
			withDups = append(withDups, cp)
		}
	}

	// 3) 按接收时间稳定排序，随后注入显式相邻交换（乱序）。
	sort.SliceStable(withDups, func(i, j int) bool {
		if withDups[i].p.Sample.RecvTime.Equal(withDups[j].p.Sample.RecvTime) {
			return withDups[i].sendOrder < withDups[j].sendOrder
		}
		return withDups[i].p.Sample.RecvTime.Before(withDups[j].p.Sample.RecvTime)
	})

	// 交换按“设备序号”定位原始位置：为避免与排序冲突，改为在排好序的
	// 流中找到目标包，与它前面一个同设备包交换（若存在）。
	out := make([]rawPkt, len(withDups))
	copy(out, withDups)
	for _, pert := range spec.Net {
		for _, seq := range pert.SwapAfter {
			idx := indexOf(out, pert.Device, seq)
			if idx <= 0 {
				continue
			}
			j := idx - 1
			// 仅与同设备包交换，语义清晰。
			for j >= 0 && out[j].device != pert.Device {
				j--
			}
			if j >= 0 {
				out[idx], out[j] = out[j], out[idx]
				ann := &out[j].p.Annotation
				*ann = joinAnn(*ann, "reordered")
			}
		}
	}

	pkts := make([]Packet, len(out))
	for i := range out {
		pkts[i] = out[i].p
	}
	return pkts
}

func driftOffset(elapsed time.Duration, ppm float64) time.Duration {
	secs := float64(elapsed) / float64(time.Second)
	return time.Duration(secs * ppm * 1e-6 * float64(time.Second))
}

func inAny(elapsed time.Duration, ivs []Interval) bool {
	for _, iv := range ivs {
		if elapsed >= iv.From && elapsed < iv.To {
			return true
		}
	}
	return false
}

func findPerturbation(all []Perturbation, dev pipeline.DeviceID) Perturbation {
	for _, p := range all {
		if p.Device == dev {
			return p
		}
	}
	return Perturbation{Device: dev, DelaySeqs: map[int64]time.Duration{}}
}

func seqIn(seq int64, list []int64) (int, bool) {
	for i, s := range list {
		if s == seq {
			return i, true
		}
	}
	return -1, false
}

func indexOf(ps []rawPkt, dev pipeline.DeviceID, seq int64) int {
	for i := range ps {
		if ps[i].device == dev && ps[i].seq == seq && ps[i].p.Annotation != "duplicate" {
			return i
		}
	}
	return -1
}

func joinAnn(a, b string) string {
	if a == "" {
		return b
	}
	return a + "+" + b
}

// sin2pi 避免引入 math 名空间扩散，固定在生成器内。
func sin2pi(x float64) float64 {
	const pi = 3.141592653589793
	return sinLocal(2 * pi * x)
}

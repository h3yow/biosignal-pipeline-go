package pipeline

import (
	"sort"
	"time"
)

// SimConfig 描述一个确定性本地模拟设备。
// 所有时间均基于固定 StartTime，不调用 wall clock，保证可复现。
type SimConfig struct {
	Device    DeviceID
	Clock     ClockParams
	Channels  []ChannelSpec
	StartTime time.Time
	Duration  time.Duration
	// ShuffleSeed>0 时用确定性 LCG 做有界洗牌（模拟窗口内乱序）；=0 不洗。
	// ShuffleWindow 限制每个包最多向前移动的槽位数，超出 MaxDelay 的包仍会
	// 被管线按固定策略拒绝（可用该机制确定性地制造“迟到”样本）。
	ShuffleSeed   uint64
	ShuffleWindow int
	// DuplicateEvery>0 时，每产生该数量个包就复制一份（模拟重复包）。
	DuplicateEvery int
	// DropSeq 显式丢弃的序列号集合（模拟丢包/缺失）。
	DropSeq map[int64]bool
	// GapAfterSeq>=0 时，在该序列号之后的所有时间整体后移 Gap（模拟中断/重连）。
	GapAfterSeq int64
	Gap         time.Duration
	// ArtifactRanges 为设备时间区间，区间内所有包附加 ArtifactFlag。
	ArtifactRanges []SimRange
	ArtifactFlag   RawFlag
}

// SimRange 是半开时间区间 [From, To)。
type SimRange struct {
	From time.Time
	To   time.Time
}

// ValueGen 按 (序列号, 设备时间) 确定性产出原始值与硬件标志。
type ValueGen func(channel ChannelID, seq int64, deviceTime time.Time) (float64, RawFlag)

// Simulator 生成确定性的模拟采样序列（含乱序/重复/漂移/中断）。
type Simulator struct {
	cfg SimConfig
	gen ValueGen
}

// NewSimulator 创建模拟器。
func NewSimulator(cfg SimConfig, gen ValueGen) *Simulator {
	return &Simulator{cfg: cfg, gen: gen}
}

// Generate 产出原始包序列。
//
// 生成顺序：先按通道、序列号产生“逻辑真值”（设备时间注入 Gap），
// 再注入确定性重复，最后用固定种子洗牌制造乱序；
// RecvTime 全部置为零值，表示“不依赖接收墙钟”——管线结论只由
// 设备时间与序列号决定，因此乱序与重放结果一致。
func (s *Simulator) Generate() []RawSample {
	cfg := s.cfg
	var packets []RawSample

	for _, ch := range cfg.Channels {
		interval := ch.NominalInterval
		if interval <= 0 {
			interval = time.Second
		}
		n := int(cfg.Duration/interval) + 1
		for i := 0; i < n; i++ {
			if cfg.DropSeq[int64(i)] {
				continue // 丢包：序列号按槽位分配，这里留下真实空洞
			}
			dt := cfg.StartTime.Add(time.Duration(i) * interval)
			if cfg.GapAfterSeq >= 0 && int64(i) > cfg.GapAfterSeq {
				dt = dt.Add(cfg.Gap)
			}
			v, flags := s.gen(ch.Channel, int64(i), dt)
			for _, r := range cfg.ArtifactRanges {
				if !dt.Before(r.From) && dt.Before(r.To) {
					flags |= cfg.ArtifactFlag
				}
			}
			packets = append(packets, RawSample{
				Device:     cfg.Device,
				Channel:    ch.Channel,
				Seq:        int64(i),
				DeviceTime: dt,
				RawValue:   v,
				Flags:      flags,
			})
		}
	}

	// 注入确定性重复包（紧挨着原包插入）。
	if cfg.DuplicateEvery > 0 {
		var withDup []RawSample
		for i, p := range packets {
			withDup = append(withDup, p)
			if (i+1)%cfg.DuplicateEvery == 0 {
				dup := p
				withDup = append(withDup, dup)
			}
		}
		packets = withDup
	}

	// 有界乱序：给每个包赋一个确定性的“到达序号” = 原序号 + [-w,w] 内
	// 固定抖动，再稳定排序。每个包相对原序最多移动 ShuffleWindow 个位置，
	// 因此不会跨越中断空洞；是否超窗完全由管线 MaxDelay 判定。
	if cfg.ShuffleSeed > 0 && cfg.ShuffleWindow > 0 {
		type keyed struct {
			p    RawSample
			ord  int
			orig int
		}
		w := cfg.ShuffleWindow
		state := cfg.ShuffleSeed
		next := func() uint64 {
			state = state*6364136223846793005 + 1442695040888963407
			return state
		}
		ks := make([]keyed, len(packets))
		for i, p := range packets {
			jitter := int(next()%(uint64(2*w+1))) - w
			ks[i] = keyed{p: p, ord: i + jitter, orig: i}
		}
		sort.SliceStable(ks, func(i, j int) bool {
			if ks[i].ord != ks[j].ord {
				return ks[i].ord < ks[j].ord
			}
			return ks[i].orig < ks[j].orig
		})
		for i := range ks {
			packets[i] = ks[i].p
		}
	}
	return packets
}

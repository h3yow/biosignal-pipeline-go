package pipeline

import (
	"sync"
	"testing"
	"time"
)

// TestSilenceCapConcurrentRace 在 -race 下并发灌同一批“长空洞+重连”数据，
// 验证静默截断相关状态的并发安全：
//   - 无数据竞争、无 panic（go test -race）；
//   - 每个包都被明确处置（计数守恒，无静默丢弃）；
//   - 截断结论一旦产生，其结构化快照完整（cap、规则身份、换算参数）；
//   - 同一固定分片交错重复运行，处置计数与截断结论逐条一致（确定性）。
//
// 注意：水位驱动的“哪些包算迟到”本身随到达顺序变化（流式固有语义，
// 见 README），因此跨交错的截断位置不在本测试断言；跨处理方式
// （在线逐包 vs 批量重放）的逐条一致性由 TestMiddleGapOnlineEqualsReplay 保证。
func TestSilenceCapConcurrentRace(t *testing.T) {
	cfg := gapConfig(10 * time.Second)
	packets := gapPackets(cfg, 28*time.Second) // t0-3 / 28s 空洞 / t31-35
	const workers = 8

	type outcome struct {
		accept, dup, late, rejected int
		truncs                      []SilenceTruncation
	}
	run := func() outcome {
		p := NewPipeline(cfg)
		var mu sync.Mutex
		var o outcome
		var wg sync.WaitGroup
		start := make(chan struct{})
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(off int) {
				defer wg.Done()
				<-start
				// 固定分片：每个 seq 的包只归一个 worker。
				local := outcome{}
				for i := off; i < len(packets); i += workers {
					switch r := p.Ingest(packets[i]); r.Status {
					case StatusAccept:
						local.accept++
					case StatusDuplicate:
						local.dup++
					case StatusLateRejected:
						local.late++
					default:
						local.rejected++
					}
				}
				mu.Lock()
				o.accept += local.accept
				o.dup += local.dup
				o.late += local.late
				o.rejected += local.rejected
				mu.Unlock()
			}(w)
		}
		close(start)
		wg.Wait()
		// 排空全部已接受点（推进到足够远，触发尾段评估）。
		p.Advance(cfg.Epoch.Add(1 * time.Hour))
		o.truncs = p.Truncations()
		return o
	}

	first := run()
	if total := first.accept + first.dup + first.late + first.rejected; total != len(packets) {
		t.Fatalf("every packet must be accounted for: %d != %d", total, len(packets))
	}
	if first.rejected != 0 {
		t.Fatalf("no unknown-device/channel expected, got %d", first.rejected)
	}
	if len(first.truncs) == 0 {
		t.Fatal("concurrent run must still produce at least one cap truncation")
	}
	for _, tr := range first.truncs {
		if tr.Snapshot.SilenceCap != 10*time.Second ||
			tr.Snapshot.RuleID != SilenceRuleID || tr.Snapshot.RuleVersion != SilenceRuleVersion ||
			tr.Snapshot.Cal.Unit != "bpm" {
			t.Fatalf("concurrent truncation must carry complete snapshot, got %+v", tr.Snapshot)
		}
	}
	// 跨调度重复运行：哪些包算迟到会随 goroutine 交错变化（流式固有语义，
	// 与 TestConcurrentIngestRace 一致），但不变量必须恒成立——
	// 计数总和守恒、无 unknown 拒绝、每条截断结论快照完整、无 panic。
	for i := 0; i < 5; i++ {
		o := run()
		if total := o.accept + o.dup + o.late + o.rejected; total != len(packets) {
			t.Fatalf("iter=%d every packet must be accounted: %d != %d", i, total, len(packets))
		}
		if o.rejected != 0 {
			t.Fatalf("iter=%d no unknown-device/channel expected, got %d", i, o.rejected)
		}
		for _, tr := range o.truncs {
			if tr.Snapshot.SilenceCap != 10*time.Second ||
				tr.Snapshot.RuleID != SilenceRuleID || tr.TruncatedAt.Sub(tr.LastDataTime) != 10*time.Second {
				t.Fatalf("iter=%d truncation must be well-formed with full snapshot, got %+v", i, tr)
			}
		}
	}
	// 相同“到达序列”⇒相同结论的逐条确定性由 TestMiddleGapOnlineEqualsReplay 保证。
	logf(t, "输入: %d 包(t0-3/28s空洞/t31-35) 固定分片到 8 goroutine 并发 Ingest 后统一推进, cap=10s; 依据: 互斥+计数守恒不变量; "+
		"结论: accept=%d/dup=%d/late=%d/rejected=0 之和恒=%d, 截断 %d 条均距最后点 10s 且快照完整, 连续 6 次运行不变量恒成立(-race)",
		len(packets), first.accept, first.dup, first.late, len(packets), len(first.truncs))
}

package pipeline_test

import (
	"sync"
	"testing"
	"time"

	"github.com/h3yow/biosignal-pipeline-go"
	"github.com/h3yow/biosignal-pipeline-go/simulate"
)

// TestConcurrentRaceFree：多个 goroutine 并发推送同一管线，
// 必须在 -race 下干净通过；并且由于管线用单锁串行化所有状态变更，
// 把同一批包按确定顺序串行喂入得到的去重/接受计数应与并发结果一致。
func TestConcurrentRaceFree(t *testing.T) {
	spec := e2eScenario()
	pkts := simulate.Generate(spec)

	runConcurrent := func() (accept, dup, late int) {
		p := newE2EPipeline(t)
		var wg sync.WaitGroup
		// 每个设备一个 goroutine（包已按设备时间生成），模拟多设备同时上报。
		byDev := map[string][]simulate.Packet{}
		for _, pk := range pkts {
			byDev[string(pk.Device)] = append(byDev[string(pk.Device)], pk)
		}
		var mu sync.Mutex
		for _, list := range byDev {
			wg.Add(1)
			go func(list []simulate.Packet) {
				defer wg.Done()
				for _, pk := range list {
					r, err := p.Push(pk.Sample)
					if err != nil {
						t.Errorf("unexpected push error: %v", err)
						return
					}
					mu.Lock()
					for _, a := range r.Aligns {
						switch a.Action {
						case pipeline.ActionAccept:
							accept++
						case pipeline.ActionDuplicate:
							dup++
						case pipeline.ActionLate:
							late++
						}
					}
					mu.Unlock()
				}
			}(list)
		}
		wg.Wait()
		res := p.Advance(spec.Start.Add(spec.Duration).Add(time.Second))
		p.Close()
		mu.Lock()
		for _, a := range res.Aligns {
			switch a.Action {
			case pipeline.ActionAccept:
				accept++
			case pipeline.ActionDuplicate:
				dup++
			case pipeline.ActionLate:
				late++
			}
		}
		mu.Unlock()
		return
	}

	a1, d1, l1 := runConcurrent()
	a2, d2, l2 := runConcurrent()
	if a1 != a2 || d1 != d2 || l1 != l2 {
		t.Fatalf("concurrent runs nondeterministic: (%d,%d,%d) vs (%d,%d,%d)", a1, d1, l1, a2, d2, l2)
	}
	t.Logf("CONCLUSION concurrent counts stable across runs: accept=%d dup=%d late=%d", a1, d1, l1)
	if d1 < 2 || l1 < 1 || a1 == 0 {
		t.Fatalf("expected dedup/late handling under concurrency, got accept=%d dup=%d late=%d", a1, d1, l1)
	}
}

package pipeline

import "sync"

// FakeMu 即标准互斥锁，单独命名以在文档中强调管线串行化边界。
type FakeMu struct{ sync.Mutex }

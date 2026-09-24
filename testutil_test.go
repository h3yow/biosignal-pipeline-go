package pipeline

import (
	"fmt"
	"testing"
)

// logf 在单测日志中打印“输入 / 判定依据 / 最终结论”，失败与成功时都可用 -v 查看。
func logf(t *testing.T, format string, args ...any) {
	t.Helper()
	t.Logf("CASE %s :: %s", t.Name(), fmt.Sprintf(format, args...))
}

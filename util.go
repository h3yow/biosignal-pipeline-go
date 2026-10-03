package pipeline

import (
	"fmt"
	"strconv"
)

func formatFloat(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }

// formatSeq 生成事件编号用的定长序号（0001, 0002, ...），保证字典序即时间序。
func formatSeq(n int) string { return fmt.Sprintf("%04d", n) }

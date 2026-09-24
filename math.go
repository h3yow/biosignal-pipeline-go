package pipeline

import "math"

// expFast 直接使用标准库指数函数，单独成文件以便说明权重计算位置。
func expFast(x float64) float64 { return math.Exp(x) }

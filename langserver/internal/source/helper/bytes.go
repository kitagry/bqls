package helper

import (
	"fmt"
	"math"
	"strings"
)

// BytesConvert formats a byte count with binary units, e.g. "1.50 KiB".
func BytesConvert(bytes int64) string {
	if bytes == 0 {
		return "0 bytes"
	}

	// Floats keep math.MinInt64 from overflowing on negation, and the sign
	// is split off because the logarithm of a negative count is NaN.
	sign, abs := "", float64(bytes)
	if abs < 0 {
		sign, abs = "-", -abs
	}
	base := math.Floor(math.Log(abs) / math.Log(1024))
	units := []string{"bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB"}

	stringVal := fmt.Sprintf("%.2f", abs/math.Pow(1024, base))
	stringVal = strings.TrimSuffix(stringVal, ".00")
	return fmt.Sprintf("%s%s %v",
		sign,
		stringVal,
		units[int(base)],
	)
}

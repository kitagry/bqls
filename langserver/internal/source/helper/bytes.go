package helper

import (
	"fmt"
	"strings"
)

var byteUnits = []string{"bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}

// BytesConvert formats a byte count with binary units, e.g. "1.50 KiB".
func BytesConvert(bytes int64) string {
	if bytes == 0 {
		return "0 bytes"
	}

	// Floats keep math.MinInt64 from overflowing on negation.
	sign, v := "", float64(bytes)
	if v < 0 {
		sign, v = "-", -v
	}
	unit := 0
	for v >= 1024 && unit < len(byteUnits)-1 {
		v /= 1024
		unit++
	}
	s := fmt.Sprintf("%.2f", v)
	// Values just below a boundary, such as 1048575 bytes, round to "1024.00";
	// they read better as 1 of the next unit.
	if s == "1024.00" && unit < len(byteUnits)-1 {
		v /= 1024
		unit++
		s = fmt.Sprintf("%.2f", v)
	}
	return fmt.Sprintf("%s%s %s", sign, strings.TrimSuffix(s, ".00"), byteUnits[unit])
}

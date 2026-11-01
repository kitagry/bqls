package helper

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strings"
)

// ByteSize is a byte count. In JSON it is either an integer or a string with
// a unit such as "100GiB", so that thresholds in .bqls.yaml do not have to be
// spelled out as raw byte counts.
type ByteSize int64

// parseUnits only has full unit names: in a BigQuery context "100G" is likely
// meant as GiB, while it conventionally means 10^9, so it is rejected instead
// of guessed.
var parseUnits = map[string]int64{
	"":    1,
	"b":   1,
	"kb":  1e3,
	"mb":  1e6,
	"gb":  1e9,
	"tb":  1e12,
	"pb":  1e15,
	"eb":  1e18,
	"kib": 1 << 10,
	"mib": 1 << 20,
	"gib": 1 << 30,
	"tib": 1 << 40,
	"pib": 1 << 50,
	"eib": 1 << 60,
}

var byteSizePattern = regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([A-Za-z]*)$`)

func (b *ByteSize) UnmarshalJSON(data []byte) error {
	if !bytes.HasPrefix(data, []byte(`"`)) {
		var n int64
		if err := json.Unmarshal(data, &n); err != nil {
			return err
		}
		*b = ByteSize(n)
		return nil
	}

	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	n, err := parseByteSize(s)
	if err != nil {
		return err
	}
	*b = n
	return nil
}

func parseByteSize(s string) (ByteSize, error) {
	m := byteSizePattern.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return 0, fmt.Errorf("invalid byte size %q, want e.g. \"100GiB\"", s)
	}
	unit, ok := parseUnits[strings.ToLower(m[2])]
	if !ok {
		return 0, fmt.Errorf("unknown unit %q in byte size %q, want one of B, KiB, MiB, GiB, TiB, PiB, EiB, KB, MB, GB, TB, PB, EB", m[2], s)
	}
	// big.Rat keeps fractions such as "1.5KiB" exact; float64 would lose
	// precision for values above 2^53.
	r, ok := new(big.Rat).SetString(m[1])
	if !ok {
		return 0, fmt.Errorf("invalid number in byte size %q", s)
	}
	r.Mul(r, new(big.Rat).SetInt64(unit))
	n := new(big.Int).Quo(r.Num(), r.Denom())
	if n.Cmp(big.NewInt(math.MaxInt64)) > 0 {
		return 0, fmt.Errorf("byte size %q is too large", s)
	}
	return ByteSize(n.Int64()), nil
}

var formatUnits = []string{"bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}

// String formats b with binary units, e.g. "1.50 KiB".
func (b ByteSize) String() string {
	if b == 0 {
		return "0 bytes"
	}

	// Floats keep math.MinInt64 from overflowing on negation.
	sign, v := "", float64(b)
	if v < 0 {
		sign, v = "-", -v
	}
	unit := 0
	for v >= 1024 && unit < len(formatUnits)-1 {
		v /= 1024
		unit++
	}
	s := fmt.Sprintf("%.2f", v)
	// Values just below a boundary, such as 1048575 bytes, round to "1024.00";
	// they read better as 1 of the next unit.
	if s == "1024.00" && unit < len(formatUnits)-1 {
		v /= 1024
		unit++
		s = fmt.Sprintf("%.2f", v)
	}
	return fmt.Sprintf("%s%s %s", sign, strings.TrimSuffix(s, ".00"), formatUnits[unit])
}

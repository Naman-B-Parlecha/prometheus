package summary

type Summary struct {
	Count           float64
	Sum             float64
	QuantileValues  []float64
	QuantileTargets []float64
}

func MatchQuantileTargets(s1 []float64, s2 []float64) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i, t := range s2 {
		if s1[i] != t {
			return false
		}
	}
	return true
}

func Equal(s1, s2 *Summary) bool {
	if s1 == nil || s2 == nil {
		return s1 == s2
	}
	if s1.Count != s2.Count || s1.Sum != s2.Sum {
		return false
	}
	if !MatchQuantileTargets(s1.QuantileTargets, s2.QuantileTargets) {
		return false
	}
	if len(s1.QuantileValues) != len(s2.QuantileValues) {
		return false
	}
	for i, v := range s1.QuantileValues {
		if v != s2.QuantileValues[i] {
			return false
		}
	}
	return true
}

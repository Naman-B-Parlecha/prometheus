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

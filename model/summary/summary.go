package summary

import "errors"

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

func (s *Summary) Validate() error {
	if s.Count < 0 {
		return errors.New("count must be non-negative")
	}
	if len(s.QuantileValues) != len(s.QuantileTargets) {
		return errors.New("quantile values and targets must have the same length")
	}
	for i := range len(s.QuantileTargets) - 1 {
		if s.QuantileTargets[i] > s.QuantileTargets[i+1] {
			return errors.New("quantile targets must be in ascending order")
		}
	}

	return nil
}

func (s *Summary) Equals(s2 *Summary) bool {
	if s == nil || s2 == nil {
		return s == s2
	}
	if s.Count != s2.Count || s.Sum != s2.Sum {
		return false
	}
	if !MatchQuantileTargets(s.QuantileTargets, s2.QuantileTargets) {
		return false
	}
	if len(s.QuantileValues) != len(s2.QuantileValues) {
		return false
	}
	for i, v := range s.QuantileValues {
		if v != s2.QuantileValues[i] {
			return false
		}
	}
	return true
}

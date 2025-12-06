package summary

import (
	"errors"
	"slices"
)

var (
	// ErrInvalidSummary is returned when a summary is invalid.
	ErrInvalidSummary = errors.New("invalid summary structure")
)

type Summary struct {
	// Total number of observations.
	Count uint64
	// Sum of observations.
	Sum float64
	// Holds the calculated quantile values for configured quantile targets in sorted order.
	QuantileValues []float64
	// Holds user configured quantile targets in sorted order.
	QuantileTargets []float64
}

func (s *Summary) Validate() error {
	if len(s.QuantileValues) != len(s.QuantileTargets) {
		return ErrInvalidSummary
	}
	for i := 1; i < len(s.QuantileTargets); i++ {
		if s.QuantileTargets[i] < s.QuantileTargets[i-1] {
			return ErrInvalidSummary
		}
		if s.QuantileValues[i] < s.QuantileValues[i-1] {
			return ErrInvalidSummary
		}
	}
	return nil
}

func (s *Summary) Equals(s2 *Summary) bool {
	if s2 == nil {
		return false
	}
	if s.Count != s2.Count || s.Sum != s2.Sum {
		return false
	}
	if !slices.Equal(s.QuantileValues, s2.QuantileValues) || !slices.Equal(s.QuantileTargets, s2.QuantileTargets) {
		return false
	}
	return true
}

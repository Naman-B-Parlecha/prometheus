// Copyright 2025 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package summary

import (
	"errors"
	"slices"
)

// ErrInvalidSummary is returned when a summary is invalid.
var ErrInvalidSummary = errors.New("invalid summary structure")

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

func (s *Summary) Copy() *Summary {
	c := Summary{
		Sum:   s.Sum,
		Count: s.Count,
	}
	if len(s.QuantileTargets) != 0 {
		c.QuantileTargets = make([]float64, len(s.QuantileTargets))
		copy(c.QuantileTargets, s.QuantileTargets)
	}
	if len(s.QuantileValues) != 0 {
		c.QuantileValues = make([]float64, len(s.QuantileValues))
		copy(c.QuantileValues, s.QuantileValues)
	}
	return &c
}

package convertns

import (
	"fmt"
	"sort"

	"github.com/prometheus/prometheus/model/summary"
)

type TempSummary struct {
	count     float64
	sum       float64
	quantiles []tempQuantile
	err       error
}

type tempQuantile struct {
	target float64
	value  float64
}

func NewTempSummary() TempSummary {
	return TempSummary{quantiles: make([]tempQuantile, 0, 5)}
}

func (s *TempSummary) Err() error {
	return s.err
}
func (s *TempSummary) Reset() {
	s.count = 0
	s.sum = 0
	s.quantiles = s.quantiles[:0]
	s.err = nil
}

func (s *TempSummary) SetQuantile(quantile, value float64) error {
	if s.err != nil {
		return s.err
	}
	if quantile < 0 {
		s.err = fmt.Errorf("quantile targets must be non-negative: quantile=%q", quantile)
	}

	switch {
	case len(s.quantiles) == 0:
		s.quantiles = append(s.quantiles, tempQuantile{value: value, target: quantile})
	case s.quantiles[len(s.quantiles)-1].target < quantile:
		// summaries quantile values are always in increasing so useful adding a check for that as well
		if value < s.quantiles[len(s.quantiles)-1].value {
			s.err = fmt.Errorf("quantile values should be in incremenatl valeus: %g < %g", value, s.quantiles[len(s.quantiles)-1].value)
			return s.err
		}
		s.quantiles = append(s.quantiles, tempQuantile{value: value, target: quantile})
	case s.quantiles[len(s.quantiles)-1].target == quantile:
		// Ignore this, as it is a duplicate sample.
	default:
		// Out of order - find correct position to insert.
		i := sort.Search(len(s.quantiles), func(i int) bool {
			return s.quantiles[i].target >= quantile
		})
		if s.quantiles[i].target == quantile {
			return nil
		}
		// Insert at correct position.
		s.quantiles = append(s.quantiles, tempQuantile{})
		copy(s.quantiles[i+1:], s.quantiles[i:])
		s.quantiles[i] = tempQuantile{target: quantile, value: value}
	}
	return nil
}
func (s *TempSummary) SetCount(count float64) error {
	if s.err != nil {
		return s.err
	}
	if count < 0 {
		s.err = fmt.Errorf("summaries cant have negative count: count=%g", count)
		return s.err
	}
	s.count = count
	return nil
}

func (s *TempSummary) SetSum(sum float64) error {
	if s.err != nil {
		return s.err
	}
	s.sum = sum
	return nil
}

func (s TempSummary) Convert() (*summary.Summary, error) {
	if s.err != nil {
		return nil, s.err
	}

	// Build the native summary
	ns := &summary.Summary{
		Count: s.count,
		Sum:   s.sum,
	}

	if len(s.quantiles) > 0 {
		ns.QuantileTargets = make([]float64, len(s.quantiles))
		ns.QuantileValues = make([]float64, len(s.quantiles))

		for i, q := range s.quantiles {
			ns.QuantileTargets[i] = q.target
			ns.QuantileValues[i] = q.value
		}
	}

	return ns, nil
}

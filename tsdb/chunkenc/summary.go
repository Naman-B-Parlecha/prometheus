package chunkenc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/summary"
	"github.com/prometheus/prometheus/model/value"
)

type summaryRet struct {
	t int64
	s *summary.Summary
}

type SummaryChunk struct {
	b bstream
}

func NewSummaryChunk() *SummaryChunk {
	b := make([]byte, chunkHeaderSize, chunkAllocationSize)
	return &SummaryChunk{b: bstream{stream: b, count: 0}}
}

func (c *SummaryChunk) Appender() (Appender, error) {
	if len(c.b.stream) == chunkHeaderSize { // Avoid allocating an Iterator when chunk is empty.
		return &SummaryAppender{b: &c.b, t: math.MinInt64, sum: xorValue{leading: 0xff}, cnt: xorValue{leading: 0xff}, quantileValues: []xorValue{}}, nil
	}
	it := c.iterator(nil)
	for it.Next() != ValSummary {
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	quantileValues := make([]xorValue, len(it.quantileValues))
	for i := range len(quantileValues) {
		quantileValues[i] = xorValue{
			value:    it.quantileValues[i],
			leading:  it.quantileValuesLeading[i],
			trailing: it.quantileValuesTrailing[i],
		}
	}

	a := &SummaryAppender{
		b: &c.b,

		t:               it.t,
		tDelta:          it.tDelta,
		cnt:             it.cnt,
		quantileTargets: it.quantileTarget,
		quantileValues:  quantileValues,
		sum:             it.sum,
	}
	return a, nil
}

type SummaryAppender struct {
	b               *bstream
	quantileTargets []float64

	t, tDelta      int64
	sum, cnt       xorValue
	quantileValues []xorValue
}

func (a *SummaryAppender) Append(int64, float64) {
	panic("appended a float sample to a summary chunk")
}

func (a *SummaryAppender) AppendHistogram(prev *HistogramAppender, t int64, h *histogram.Histogram, appendOnly bool) (c Chunk, isRecoded bool, app Appender, err error) {
	panic("appended a float sample to a summary chunk")
}

func (a *SummaryAppender) AppendFloatHistogram(prev *FloatHistogramAppender, t int64, h *histogram.FloatHistogram, appendOnly bool) (c Chunk, isRecoded bool, app Appender, err error) {
	panic("appended a float sample to a summary chunk")
}

// complete the change in quantile targets length and add new appender
// shift to appendSumary
func (a *SummaryAppender) AppendSummary(t int64, s *summary.Summary, appendOnly bool) (c Chunk, app Appender, err error) {
	if a.NumSamples() == 0 {
		// first sample in chunk
		a.appendSummary(t, s)
		return nil, a, nil
	}

	if ok := a.appendableSummary(s); !ok {
		// not sure what is appendOnly mode T-T
		if appendOnly {
			// we need to return error
			// since histograms is not appendable in appendOnly mode
			return nil, a, errors.New("summary sample not appendable in appendOnly mode")
		}
		newChunk := NewSummaryChunk()
		app, err = newChunk.Appender()
		if err != nil {
			panic(err) // TODO: check why this can never happen
		}
		sapp := app.(*SummaryAppender)
		sapp.appendSummary(t, s)
		return newChunk, sapp, nil
	}
	a.appendSummary(t, s)
	return nil, a, nil
}

func (a *SummaryAppender) appendSummary(t int64, s *summary.Summary) {
	var tDelta int64
	num := binary.BigEndian.Uint16(a.b.bytes())

	if value.IsStaleNaN(s.Sum) {
		// Emptying out other fields to write no quantile, and an empty
		// layout in case of first summary in the chunk.
		s = &summary.Summary{Sum: s.Sum}
	}

	if num == 0 {
		// first we write the summary layout
		// add all the values to the appender
		// in chunk add the values
		writeSummaryLayout(a.b, s.QuantileTargets)
		if len(s.QuantileTargets) > 0 {
			a.quantileTargets = make([]float64, len(s.QuantileTargets))
			copy(a.quantileTargets, s.QuantileTargets)
		} else {
			a.quantileTargets = nil
		}
		if len(s.QuantileValues) > 0 {
			a.quantileValues = make([]xorValue, len(s.QuantileValues))
			for i, qv := range s.QuantileValues {
				a.quantileValues[i] = xorValue{
					value:   qv,
					leading: 0xff,
				}
			}
		} else {
			a.quantileValues = nil
		}
		// write first sample values
		putVarbitInt(a.b, t)
		a.b.writeBits(math.Float64bits(s.Count), 64)
		a.b.writeBits(math.Float64bits(s.Sum), 64)
		a.cnt.value = s.Count
		a.sum.value = s.Sum
		for _, v := range s.QuantileValues {
			a.b.writeBits(math.Float64bits(v), 64)
		}
	} else {
		// 2nd sample will have delta values
		tDelta = t - a.t
		tDod := tDelta - a.tDelta
		putVarbitInt(a.b, tDod)

		a.writeXorValue(&a.cnt, s.Count)
		a.writeXorValue(&a.sum, s.Sum)

		for i, v := range s.QuantileValues {
			a.writeXorValue(&a.quantileValues[i], v)
		}
	}
	fmt.Printf("SummaryAppender: %v\n", a.b.bytes())
	binary.BigEndian.PutUint16(a.b.bytes(), num+1)
	a.t = t
	a.tDelta = tDelta
}

func (a *SummaryAppender) NumSamples() int {
	return int(binary.BigEndian.Uint16(a.b.bytes()))
}

func (a *SummaryAppender) appendableSummary(s *summary.Summary) (okToAppend bool) {
	if value.IsStaleNaN(s.Sum) {
		// This is a stale sample so quantiles doesn't get affected and we can append.
		okToAppend = true
		return okToAppend
	}
	// current sample is not stale if we are here
	// if prev was stale we cant append need to make
	// new chunk
	if value.IsStaleNaN(a.sum.value) {
		// If the last sample was stale, then we can only accept stale
		// samples in this chunk.
		return okToAppend
	}
	if s.Count < a.cnt.value {
		// this means counter reset happened idk what to do
		// i assume append not possible and new chunk needed
		// need to check on appendOnly mode (not able to understand its working)
		return okToAppend
	}
	if !summary.MatchQuantileTargets(s.QuantileTargets, a.quantileTargets) {
		return okToAppend
	}
	okToAppend = true
	return okToAppend
}

func (a *SummaryAppender) writeXorValue(old *xorValue, v float64) {
	xorWrite(a.b, v, old.value, &old.leading, &old.trailing)
	old.value = v
}

func writeSummaryLayout(b *bstream, quantileTarget []float64) {
	// quantileTarget nothing else since that the only constant part of the summary layout
	// first we add the number of quantiles
	// then we add each quantile target as a float64
	putVarbitUint(b, uint64(len(quantileTarget)))
	for _, qt := range quantileTarget {
		putQuantileTarget(b, qt)
	}
}

func putQuantileTarget(b *bstream, quantileTargetValue float64) {
	// we will follow the same optimization as custom values where we convert
	// float values to int by multiplying by 1000
	// the range of values should range from 0 to 2^25 = 33554431

	tf := quantileTargetValue * 1000
	if tf < 0 || tf > 33554431 || !isWholeWhenMultiplied(quantileTargetValue) {
		b.writeBit(zero) // this will work as a flag that the value is stored as float64
		b.writeBits(math.Float64bits(quantileTargetValue), 64)
		return
	}
	putVarbitUint(b, uint64(math.Round(tf))+1)
}

// Bytes returns the underlying byte slice of the chunk.
func (c *SummaryChunk) Bytes() []byte {
	return c.b.bytes()
}

func (c *SummaryChunk) Encoding() Encoding {
	return EncSummary
}

func (c *SummaryChunk) Compact() {
	if l := len(c.b.stream); cap(c.b.stream) > l+chunkCompactCapacityThreshold {
		buf := make([]byte, l)
		copy(buf, c.b.stream)
		c.b.stream = buf
	}
}

func (c *SummaryChunk) Reset(stream []byte) {
	c.b.Reset(stream)
}

func (c *SummaryChunk) NumSamples() int {
	return int(binary.BigEndian.Uint16(c.Bytes()))
}

func (c *SummaryChunk) iterator(it Iterator) *summaryIterator {
	summaryIter, ok := it.(*summaryIterator)
	if !ok {
		summaryIter = &summaryIterator{}
	}
	//
	summaryIter.Reset(c.b.bytes())
	return summaryIter
}
func (c *SummaryChunk) Iterator(it Iterator) Iterator {
	return c.iterator(it)
}

type summaryIterator struct {
	br       bstreamReader
	numTotal uint16
	numRead  uint16

	t      int64
	tDelta int64

	sum, cnt       xorValue
	quantileTarget []float64

	quantileValues         []float64
	quantileValuesLeading  []uint8
	quantileValuesTrailing []uint8

	err             error
	atSummaryCalled bool
}

func (it *summaryIterator) Next() ValueType {
	return ValNone
}

// todo fix
func (it *summaryIterator) Seek(t int64) ValueType {
	if it.err != nil {
		return ValNone
	}
	for t > it.t || it.numRead == 0 {
		if it.Next() == ValNone {
			return ValNone
		}
	}
	return ValSummary
}

func (it *summaryIterator) AtT() int64 {
	return it.t
}

// Err returns the current error. It should be used only after the
// iterator is exhausted, i.e. `Next` or `Seek` have returned ValNone.
func (it *summaryIterator) Err() error {
	return it.err
}

func (it *summaryIterator) At() (int64, float64) {
	panic("cannot call summaryIterator.At")
}

func (*summaryIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	panic("cannot call summaryIterator.AtHistogram")
}

func (*summaryIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	panic("cannot call summaryIterator.AtFloatHistogram")
}

func (it *summaryIterator) Reset(b []byte) {
	panic("unimplemented")
}

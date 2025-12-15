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

package chunkenc

import (
	"encoding/binary"
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/summary"
	"github.com/prometheus/prometheus/model/value"
)

type SummaryChunk struct {
	b bstream
}

func NewSummaryChunk() *SummaryChunk {
	b := make([]byte, chunkHeaderSize, chunkAllocationSize)
	return &SummaryChunk{b: bstream{stream: b, count: 0}}
}

func (c *SummaryChunk) Reset(stream []byte) {
	c.b.Reset(stream)
}

func (x *SummaryChunk) Encoding() Encoding {
	return EncSummary
}

func (c *SummaryChunk) Bytes() []byte {
	return c.b.bytes()
}

func (c *SummaryChunk) NumSamples() int {
	return int(binary.BigEndian.Uint16(c.Bytes()))
}

func (c *SummaryChunk) Compact() {
	if l := len(c.b.stream); cap(c.b.stream) > l+chunkCompactCapacityThreshold {
		buf := make([]byte, l)
		copy(buf, c.b.stream)
		c.b.stream = buf
	}
}

func (c *SummaryChunk) iterator(it Iterator) *summaryIterator {
	// This comment is copied from XORChunk.iterator:
	//   Should iterators guarantee to act on a copy of the data so it doesn't lock append?
	//   When using striped locks to guard access to chunks, probably yes.
	//   Could only copy data if the chunk is not completed yet.
	if summaryIter, ok := it.(*summaryIterator); ok {
		summaryIter.Reset(c.b.bytes())
		return summaryIter
	}
	return newSummaryIterator(c.b.bytes())
}

func (c *SummaryChunk) Iterator(it Iterator) Iterator {
	return c.iterator(it)
}

func newSummaryIterator(b []byte) *summaryIterator {
	return &summaryIterator{
		br:       newBReader(b[histogramHeaderSize:]),
		numTotal: binary.BigEndian.Uint16(b),
		t:        math.MinInt64,
	}
}

func (c *SummaryChunk) Appender() (Appender, error) {
	// basically this means chunk is empty
	if len(c.b.stream) == chunkHeaderSize {
		return &SummaryAppender{b: &c.b, t: math.MinInt64, sum: xorValue{leading: 0xff}}, nil
	}

	it := c.iterator(nil)
	// To get an appender, we must know the state it would have if we had
	// appended all existing data from scratch. We iterate through the end
	// and populate via the iterator's state.
	for it.Next() == ValSummary {
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	quantileValues := make([]xorValue, len(it.quantileValues))
	for i := 0; i < len(it.quantileValues); i++ {
		quantileValues[i] = xorValue{
			value:    it.quantileValues[i],
			trailing: it.quantileValuesTrailing[i],
			leading:  it.quantileValuesLeading[i],
		}
	}

	a := &SummaryAppender{
		b:              &c.b,
		t:              it.t,
		tDelta:         it.tDelta,
		cnt:            it.cnt,
		sum:            it.sum,
		quantileValues: quantileValues,
	}
	return a, nil
}

type SummaryAppender struct {
	b *bstream

	// Layout:
	t, tDelta      int64
	cnt            uint64
	sum            xorValue
	quantileValues []xorValue
}

func (a *SummaryAppender) NumSamples() int {
	return int(binary.BigEndian.Uint16(a.b.bytes()))
}

func (a *SummaryAppender) appendable(s *summary.Summary) (
	okToAppend bool,
) {
	if a.NumSamples() > 0 {
		return okToAppend
	}
	if value.IsStaleNaN(s.Sum) {
		// This is a stale sample whose buckets and spans don't matter.
		okToAppend = true
		return okToAppend
	}
	if value.IsStaleNaN(a.sum.value) {
		// If the last sample was stale, then we can only accept stale
		// samples in this chunk.
		return okToAppend
	}

	if s.Count < a.cnt {
		return okToAppend
	}

	return true
}
func (*SummaryAppender) Append(int64, float64) {
	panic("appended a float sample to a summary chunk")
}

func (*SummaryAppender) AppendFloatHistogram(*FloatHistogramAppender, int64, *histogram.FloatHistogram, bool) (Chunk, bool, Appender, error) {
	panic("appended a float histogram sample to a summary chunk")

}

func (*SummaryAppender) AppendHistogram(*HistogramAppender, int64, *histogram.Histogram, bool) (Chunk, bool, Appender, error) {
	panic("appended a histogram sample to a summary chunk")
}

func (*SummaryAppender) AppendSummary(*SummaryAppender, int64, *summary.Summary, bool) (Chunk, Appender, error) {
	panic("appended a histogram sample to a summary chunk")
}

type summaryIterator struct {
	br       bstreamReader
	numTotal uint16
	numRead  uint16

	// since quantileTargets will be static it will be present as float
	quantileTargets []float64

	t      int64
	tDelta int64
	cnt    uint64
	sum    xorValue

	// since quantileValues changes frequently we need to apply gorilla xor encoding
	quantileValues         []float64
	quantileValuesLeading  []uint8
	quantileValuesTrailing []uint8

	err error
}

func (*summaryIterator) Reset(b []byte) {
	panic("cannot call summaryIterator.At")
}

func (*summaryIterator) At() (int64, float64) {
	panic("cannot call summaryIterator.At")
}

func (*summaryIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	panic("cannot call summaryIterator.AtHistogram")
}

func (*summaryIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	panic("cannot call summaryIterator.AtFloatHistogram")
}

func (*summaryIterator) AtSummary(*summary.Summary) (int64, *summary.Summary) {
	panic("not implemented")
}

func (*summaryIterator) Next() ValueType {
	panic("not implemented")
}

func (*summaryIterator) Seek(t int64) ValueType {
	panic("not implemented")
}

func (it *summaryIterator) AtT() int64 {
	return it.t
}

func (it *summaryIterator) Err() error {
	return it.err
}

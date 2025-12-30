package chunkenc

import (
	"encoding/binary"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/summary"
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
	return nil, nil
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
	t       int64
	err     error
	numRead int
}

func (it *summaryIterator) Next() ValueType {
	return ValNone
}

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

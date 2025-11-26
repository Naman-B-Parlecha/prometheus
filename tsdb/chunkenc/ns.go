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
)

const (
	nsHeaderSize = 2 // numSamples uint16
)

type nativeSummaryChunk struct {
	b bstream // we are making stream since the entire structure will be byte slice
}

func NewnativeSummaryChunk() *nativeSummaryChunk {
	b := make([]byte, nsHeaderSize+chunkAllocationSize)
	return &nativeSummaryChunk{b: bstream{stream: b, count: 0}}
}

func (c *nativeSummaryChunk) Reset(stream []byte) {
	c.b.Reset(stream)
}

// Encoding returns the encoding type.
func (*nativeSummaryChunk) Encoding() Encoding {
	return EncNativeSummary
}

// Bytes returns the underlying byte slice of the chunk.
func (c *nativeSummaryChunk) Bytes() []byte {
	return c.b.bytes()
}

// NumSamples returns the number of samples in the chunk.
func (c *nativeSummaryChunk) NumSamples() int {
	return int(binary.BigEndian.Uint16(c.Bytes()))
}

// Compact implements the Chunk interface.
func (c *nativeSummaryChunk) Compact() {
	if l := len(c.b.stream); cap(c.b.stream) > l+chunkCompactCapacityThreshold {
		buf := make([]byte, l)
		copy(buf, c.b.stream)
		c.b.stream = buf
	}
}

func (c *nativeSummaryChunk) Appender() (Appender, error) {
	if len(c.b.stream) == nsHeaderSize { // Avoid allocating an Iterator when chunk is empty.
		return &NativeSummaryAppender{b: &c.b, t: math.MinInt64, sum: xorValue{leading: 0xff}, cnt: xorValue{leading: 0xff}}, nil
	}
	it := c.iterator(nil)

	for it.Next() == ValNativeSummary {
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	quantileValues := make([]xorValue, len(it.quantileValues))
	for i := 0; i < len(it.quantileValues); i++ {
		quantileValues[i] = xorValue{
			value:    it.quantileValues[i],
			leading:  it.quantileValuesLeading[i],
			trailing: it.quantileValuesTrailing[i],
		}
	}

	a := &NativeSummaryAppender{
		b:               &c.b,
		schema:          it.schema,
		quantileTargets: it.quantileTargets,
		quantileValues:  quantileValues,
		t:               it.t,
		tDelta:          it.tDelta,
		sum:             it.sum,
		cnt:             it.cnt,
	}
	return a, nil
}

// NativeSummaryAppender is an Appender for native summary chunks.
type NativeSummaryAppender struct {
	b *bstream

	schema          int32
	quantileTargets []float64

	t, tDelta      int64
	sum, cnt       xorValue
	quantileValues []xorValue
}

func (*NativeSummaryAppender) Append(int64, float64) {
	panic("appended a float sample to a native summary chunk")
}

func (*NativeSummaryAppender) AppendFloatHistogram(*FloatHistogramAppender, int64, *histogram.FloatHistogram, bool) (Chunk, bool, Appender, error) {
	panic("appended a float histogram sample to a native summary chunk")
}

func (*NativeSummaryAppender) AppendHistogram(*HistogramAppender, int64, *histogram.Histogram, bool) (Chunk, bool, Appender, error) {
	panic("appended a histogram sample to a native summary chunk")
}

func (c *nativeSummaryChunk) iterator(it Iterator) *nativeSummaryIterator {
	// This comment is copied from FHC.iterator:
	//   Should iterators guarantee to act on a copy of the data so it doesn't lock append?
	//   When using striped locks to guard access to chunks, probably yes.
	//   Could only copy data if the chunk is not completed yet.
	if nsIter, ok := it.(*nativeSummaryIterator); ok {
		nsIter.Reset(c.b.bytes())
		return nsIter
	}
	return newNSIterator(c.b.bytes())
}

func newNSIterator(b []byte) *nativeSummaryIterator {
	it := &nativeSummaryIterator{
		br:       newBReader(b[nsHeaderSize:]),
		numTotal: binary.BigEndian.Uint16(b),
		t:        math.MinInt64,
	}
	return it
}
func (c *nativeSummaryChunk) Iterator(it Iterator) Iterator {
	return c.iterator(it)
}

type nativeSummaryIterator struct {
	br              bstreamReader
	numTotal        uint16
	numRead         uint16
	schema          int32
	quantileTargets []float64
	t               int64
	tDelta          int64

	sum, cnt xorValue

	quantileValues         []float64
	quantileValuesLeading  []uint8
	quantileValuesTrailing []uint8

	err error

	// Track calls to retrieve methods. Once they have been called, we
	// cannot recycle the bucket slices anymore because we have returned
	// them in the histogram.
	// idk what the use of this need to check
	// atFloatHistogramCalled bool
}

func (it *nativeSummaryIterator) Next() ValueType {
	if it.err != nil || it.numRead == it.numTotal {
		return ValNone
	}

	if it.numRead == 0 {
		schema, quantileTargets, err := readNSChunkLayout(&it.br)
		if err != nil {
			it.err = err
			return ValNone
		}
		it.schema = schema
		it.quantileTargets = quantileTargets

		numQuantiles := len(quantileTargets)
		it.quantileValues = make([]float64, numQuantiles)
		it.quantileValuesLeading = make([]uint8, numQuantiles)
		it.quantileValuesTrailing = make([]uint8, numQuantiles)

		t, err := readVarbitInt(&it.br)
		if err != nil {
			it.err = err
			return ValNone
		}
		it.t = t

		cnt, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return ValNone
		}
		it.cnt.value = math.Float64frombits(cnt)

		sum, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return ValNone
		}
		it.sum.value = math.Float64frombits(sum)

		for i := range it.quantileValues {
			v, err := it.br.readBits(64)
			if err != nil {
				it.err = err
				return ValNone
			}
			it.quantileValues[i] = math.Float64frombits(v)
		}

		it.numRead++
		return ValNativeSummary
	}
	// skipping recycling since was confusing and didnt understand the use
	// we need xor
	tDod, err := readVarbitInt(&it.br)
	if err != nil {
		it.err = err
		return ValNone
	}
	it.tDelta += tDod
	it.t += it.tDelta

	if ok := it.readXor(&it.cnt.value, &it.cnt.leading, &it.cnt.trailing); !ok {
		return ValNone
	}

	if ok := it.readXor(&it.sum.value, &it.sum.leading, &it.sum.trailing); !ok {
		return ValNone
	}

	for i := range it.quantileValues {
		if ok := it.readXor(&it.quantileValues[i], &it.quantileValuesLeading[i], &it.quantileValuesTrailing[i]); !ok {
			return ValNone
		}
	}

	it.numRead++
	return ValNativeSummary
}

func (it *nativeSummaryIterator) readXor(v *float64, leading, trailing *uint8) bool {
	err := xorRead(&it.br, v, leading, trailing)
	if err != nil {
		it.err = err
		return false
	}
	return true
}

func (it *nativeSummaryIterator) At() (int64, float64) {
	panic("cannot call nativeSummaryIterator.At")
}

func (it *nativeSummaryIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	panic("cannot call nativeSummaryIterator.AtHistogram")
}
func (it *nativeSummaryIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	panic("cannot call nativeSummaryIterator.AtHistogram")
}

func (it *nativeSummaryIterator) Err() error {
	return it.err
}

func (it *nativeSummaryIterator) AtT() int64 {
	return it.t
}

func (it *nativeSummaryIterator) Seek(t int64) ValueType {
	if it.err != nil {
		return ValNone
	}

	for t > it.t || it.numRead == 0 {
		if it.Next() == ValNone {
			return ValNone
		}
	}
	return ValNativeSummary
}

func (it *nativeSummaryIterator) Reset(b []byte) {
	// The first 2 bytes contain chunk headers.
	// We skip that for actual samples.
	it.br = newBReader(b[nsHeaderSize:])
	it.numTotal = binary.BigEndian.Uint16(b)
	it.numRead = 0

	it.t, it.tDelta = 0, 0
	it.cnt, it.sum = xorValue{}, xorValue{}

	// if it.atFloatHistogramCalled {
	// 	it.atFloatHistogramCalled = false
	// 	it.pBuckets, it.nBuckets = nil, nil
	// 	it.pSpans, it.nSpans = nil, nil
	// 	it.customValues = nil
	// } else {
	it.quantileValues = it.quantileValues[:0]
	// }
	it.quantileValuesLeading, it.quantileValuesTrailing = it.quantileValuesLeading[:0], it.quantileValuesTrailing[:0]

	it.err = nil
}

// TODOS of ns here
//  1. [ ] Add writeNSChunkLayout() to histogram_meta.go
//  2. [ ] Add putCustomTarget() to histogram_meta.go
//  3. [ ] Implement AppendNativeSummary() in ns.go
//   4. [ ] Implement appendNativeSummary() in ns.go
//   5. [ ] Add NumSamples() method to appender
//   6. [ ] Implement Reset() method for iterator
//   7. [ ] Add setCounterResetHeader() if tracking resets
//   8. [ ] Verify histogramHeaderSize is correct (2 bytes for sample count)

// 1. CRITICAL - No append methods:
//   ❌ AppendNativeSummary() - PUBLIC method (interface)
//   ❌ appendNativeSummary() - PRIVATE method (actual writing)

//   2. Helper functions not in ns.go:
//   ❌ writeNSChunkLayout() - might be in histogram_meta.go
//   ❌ putCustomTarget() - might be in histogram_meta.go

//   3. Appender methods:
//   ❌ NumSamples() on appender - needed for append logic
//   ❌ setCounterResetHeader() - if you want counter reset tracking

//   4. Iterator Reset:
//   ❌ Reset() - completely commented out (lines 285-309)

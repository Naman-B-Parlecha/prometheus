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

import "encoding/binary"

type nativeSummaryChunk struct {
	b bstream // we are making stream since the entire structure will be byte slice
}

func NewNativeSummaryChunk() *nativeSummaryChunk {
	b := make([]byte, 2+chunkAllocationSize)
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

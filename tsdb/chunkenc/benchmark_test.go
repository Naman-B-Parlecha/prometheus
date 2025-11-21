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

import "testing"

// i will change the type once i get to know what to benchmark
type sampleCase struct {
	name    string
	samples []any
}

type fmtCase struct {
	name       string
	newChunkFn func() Chunk
}

func foreachFmtSampleCase(b *testing.B, f func(b *testing.B, fc fmtCase, sc sampleCase)) {
	const nSamples = 120 // Same as tsdb.DefaultSamplesPerChunk

	// Define sample cases.
	sampleCases := []sampleCase{
		// Each Case with test for all these different sample cases
		{
			name: "constant samples",
			samples: func() (ret []any) {
				// Here we create samples that we can use to benchmark chunks.
				// basically generate alot of values in loops and append to ret
				return ret
			}(),
		},
		{
			name: "random sample",
			samples: func() (ret []any) {
				// Here we create samples that we can use to benchmark chunks.
				return ret
			}(),
		},
	}
	for _, fn := range []fmtCase{
		// need to define different chunk types here to benchmark against
		{
			name:       "native summary chunk",
			newChunkFn: func() Chunk { return nil },
		},
		{
			name:       "classic summary chunk",
			newChunkFn: func() Chunk { return nil },
		},
		{
			name:       "native histogram chunk",
			newChunkFn: func() Chunk { return nil },
		},
	} {
		for _, sc := range sampleCases {
			b.Run(fn.name+"/"+sc.name, func(b *testing.B) {
				f(b, fn, sc)
			})
		}
	}
}

func BenchmarkAppender(b *testing.B) {
	// so we do a foreachFmtSampleCase here and in the function we benchmark appending samples to chunk
	foreachFmtSampleCase(b, func(b *testing.B, fc fmtCase, sc sampleCase) {
		b.ReportAllocs() // so this is for saying hey i want to report allocations in this benchmark

		// so inside this do fast fast appending no compiling inside this to get accurate benchmark
		for b.Loop() {
			// we make a new chunk
			// make new appender from that chunk
			// we go over samples in sc.samples
			// then do appending to it each one ot chunk
			// report metrics
			// at last we check sample size and chunk size if same meaning all samples appended
		}
	})
}

func BenchmarkIterator(b *testing.B) {
	foreachFmtSampleCase(b, func(b *testing.B, fc fmtCase, sc sampleCase) {
		// repeat what we did in appender
		// creator new iterator from chunk
		// go over Loop
		// in loop call Next on iterator
		// get value from iterator
		// then do some sink shit
	})
}

// Copyright 2017 The Prometheus Authors
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
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/prometheus/model/summary"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"
)

type sampleCase struct {
	name    string
	samples []summaryRet
}

type fmtCase struct {
	name       string
	newChunkFn func() Chunk
}

func foreachFmtSampleCase(b *testing.B, fn func(b *testing.B, f fmtCase, s sampleCase)) {
	const numSample = 120

	d, err := time.Parse(time.DateTime, "2025-12-29 02:55:05")
	require.NoError(b, err)

	var initT = timestamp.FromTime(d)
	sampleCases := []sampleCase{
		{
			name: "vt=constant",
			samples: func() (ret []summaryRet) {
				t := initT
				for i := 0; i < numSample; i++ {
					t += 15000 // 15s
					ret = append(ret, summaryRet{
						t: t,
						s: &summary.Summary{
							Count:           1,
							Sum:             1,
							QuantileValues:  []float64{1, 2, 3},
							QuantileTargets: []float64{0.5, 0.9, 0.99},
						},
					})
				}
				return ret
			}(),
		},
	}
	for _, f := range []fmtCase{
		{name: "NativeSummary", newChunkFn: func() Chunk { return NewSummaryChunk() }},
		// {name: "ClassicSummary", newChunkFn: func() Chunk { return NewXORChunk() }},
	} {
		for _, s := range sampleCases {
			b.Run(fmt.Sprintf("fmt=%s/%s", f.name, s.name), func(b *testing.B) {
				fn(b, f, s)
			})
		}
	}
}

func BenchmarkAppender(b *testing.B) {
	foreachFmtSampleCase(b, func(b *testing.B, f fmtCase, s sampleCase) {
		b.ReportAllocs()

		for b.Loop() {
			// here we should only bench mark like appending.
			// avoid including chunk creation etc. which is overhead and will
			// give wrong benchmark
			c := f.newChunkFn()

			app, _ := c.Appender()
			a := app.(*SummaryAppender)
			for _, p := range s.samples {
				a.AppendSummary(p.t, p.s)
			}
			// NOTE: Some buffered implementations only encode on Bytes().
			b.ReportMetric(float64(len(c.Bytes())), "B/chunk")
			require.Equal(b, len(s.samples), c.NumSamples())
		}
	})
}

func BenchmarkIterator(b *testing.B) {
	foreachFmtSampleCase(b, func(b *testing.B, f fmtCase, s sampleCase) {
		b.ReportAllocs()
		c := f.newChunkFn()

		app, _ := c.Appender()
		a := app.(*SummaryAppender)
		for _, p := range s.samples {
			a.AppendSummary(p.t, p.s)
		}
		// what we do is take bytes and if anything is buffered right we
		// reinitialize the chunk
		c.Reset(c.Bytes())
		it := c.Iterator(nil)

		require.Equal(b, len(s.samples), c.NumSamples())
		var got []summaryRet
		for it.Next() == ValSummary {
			t, s := it.AtSummary(nil)

			got = append(got, summaryRet{t: t, s: s})
		}
		if err := it.Err(); err != nil && !errors.Is(err, io.EOF) {
			require.NoError(b, err)
		}
		if diff := cmp.Diff(s.samples, got, cmp.AllowUnexported(summaryRet{}), cmp.Comparer(func(a, b summaryRet) bool {
			return summary.Equal(a.s, b.s)
		})); diff != "" {
			b.Fatalf("mismatch (-want +got):\n%s", diff)
		}

		var sink *summary.Summary
		// Measure decoding efficiency.
		for b.Loop() {
			// just in case some buffered implementation
			// we reinitialize the chunk
			c.Reset(c.Bytes())
			b.ReportMetric(float64(len(c.Bytes())), "B/chunk")

			it := c.Iterator(it)
			for it.Next() == ValSummary {
				_, s := it.AtSummary(nil)
				sink = s
			}
			if err := it.Err(); err != nil && !errors.Is(err, io.EOF) {
				require.NoError(b, err)
			}
			_ = sink
		}
	})
}

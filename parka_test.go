package parka_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/featherbread/parka"
)

func makeIntKeys(n int) []int {
	keys := make([]int, n)
	for i := range keys {
		keys[i] = i
	}
	return keys
}

func maxOfChannel(ch <-chan int) int {
	var maxI int
	for i := range ch {
		maxI = max(i, maxI)
	}
	return maxI
}

func promise(fn func()) <-chan struct{} {
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	return done
}

var hashCounts = []int{
	10,
	100,
	1_000,
	10_000,
	100_000,
}

// BenchmarkHashingParkaMapCollect performs the hashing benchmark by Collecting
// from a Limited parka.Map. This sets the baseline for how Parka compares to
// other concurrency approaches.
//
// Note that the other benchmarks aren't meant to be completely fair to Parka.
// While they include a deduplication step and maintain the order of outputs
// with respect to inputs, they don't support adding arbitrary work at any
// time, caching outputs, or yielding concurrency slots. The purpose of these
// benchmarks is to analyze the inherent overhead of Parka for basic collection
// use cases that _could_ be more efficient with a bespoke implementation.
// In other words: how worried should you be about reaching for parka.Collect
// without a second thought?
func BenchmarkHashingParkaMapCollect(b *testing.B) {
	benchmarkAllSubs(b, func(b *testing.B, cpus int, inputs [][8]byte) {
		for b.Loop() {
			m := parka.NewMap(func(_ *parka.Handle, input [8]byte) (string, error) {
				return hashOneInput(input), nil
			})
			m.Limit(cpus)
			m.Collect(inputs...)
		}
	})
}

// BenchmarkHashingParkaCollect performs the hashing benchmark using the
// top-level parka.CollectLimited, which implements (unused, in this case)
// context cancellation features atop parka.Map that add extra overhead.
func BenchmarkHashingParkaTopCollect(b *testing.B) {
	benchmarkAllSubs(b, func(b *testing.B, cpus int, inputs [][8]byte) {
		for b.Loop() {
			parka.CollectLimited(context.Background(), cpus, inputs,
				func(_ context.Context, input [8]byte) (string, error) {
					return hashOneInput(input), nil
				})
		}
	})
}

// BenchmarkHashingWGAtomicIndex shards the hashing benchmark across goroutines
// by having each goroutine increment an atomic counter and access the input
// and output slices directly at that index.
//
// This was intended as a "naïve" approach that doesn't bother to account for
// the synchronization overhead of atomics, but it actually performs better
// than partitioning for low input counts, for reasons I wish I properly
// understood.
func BenchmarkHashingWGAtomicIndex(b *testing.B) {
	benchmarkAllSubs(b, func(b *testing.B, cpus int, inputs [][8]byte) {
		for b.Loop() {
			var (
				inputs  = trimHashInputs(inputs)
				outputs = make([]string, len(inputs))
				index   atomic.Uintptr
				group   sync.WaitGroup
			)
			for range cpus {
				group.Go(func() {
					for {
						i := index.Add(1) - 1
						if i >= uintptr(len(inputs)) {
							return
						}
						outputs[i] = hashOneInput(inputs[i])
					}
				})
			}
			group.Wait()
		}
	})
}

// BenchmarkHashingWGPartition shards the hashing benchmark across goroutines
// by partitioning the input slice as evenly as possible across them.
//
// At high input counts, this can perform substantially better than the atomic
// approach by limiting the synchronization overheads referenced above.
func BenchmarkHashingWGPartition(b *testing.B) {
	benchmarkAllSubs(b, func(b *testing.B, cpus int, inputs [][8]byte) {
		for b.Loop() {
			var (
				group   sync.WaitGroup
				inputs  = trimHashInputs(inputs)
				outputs = make([]string, len(inputs))
			)
			for start, end := range partitions(cpus, len(inputs)) {
				myInputs, myOutputs := inputs[start:end], outputs[start:end]
				group.Go(func() {
					for i, input := range myInputs {
						myOutputs[i] = hashOneInput(input)
					}
				})
			}
			group.Wait()
		}
	})
}

func benchmarkAllSubs(b *testing.B, run func(b *testing.B, cpus int, inputs [][8]byte)) {
	cpus := runtime.NumCPU()
	for _, count := range hashCounts {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			inputs := makeHashInputs(count)
			run(b, cpus, inputs)
		})
	}
}

func makeHashInputs(hashes int) [][8]byte {
	data := make([][8]byte, hashes)
	for i := range hashes {
		binary.BigEndian.PutUint64(data[i][:], uint64(i))
	}
	return data
}

func trimHashInputs(inputs [][8]byte) [][8]byte {
	inputSet := make(map[[8]byte]struct{}, len(inputs))
	trimmedInputs := make([][8]byte, 0, len(inputs))
	for _, input := range inputs {
		if _, ok := inputSet[input]; !ok {
			inputSet[input] = struct{}{}
			trimmedInputs = append(trimmedInputs, input)
		}
	}
	return trimmedInputs
}

func hashOneInput(b [8]byte) string {
	var empty [1024 - 8]byte
	hash := sha256.New()
	hash.Write(b[:])
	hash.Write(empty[:])
	return hex.EncodeToString(hash.Sum(nil))
}

func partitions(parts, count int) iter.Seq2[int, int] {
	return func(yield func(i, j int) bool) {
		var (
			start          = 0
			minPerPart     = count / parts
			partsWithExtra = count % parts
		)
		for partIdx := range parts {
			partSize := minPerPart
			if partIdx < partsWithExtra {
				partSize += 1
			}
			if !yield(start, start+partSize) {
				return
			}
			start += partSize
		}
	}
}

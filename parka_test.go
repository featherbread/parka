package parka_test

import (
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

func BenchmarkHashingParkaCollect(b *testing.B) {
	for _, count := range hashCounts {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			cpus := runtime.NumCPU()
			inputs := makeHashInputs(count)
			for b.Loop() {
				m := parka.NewMap(func(_ *parka.Handle, input [8]byte) (string, error) {
					return hashOneInput(input), nil
				})
				m.Limit(cpus)
				m.Collect(inputs...)
			}
		})
	}
}

func BenchmarkHashingWGAtomicIndex(b *testing.B) {
	for _, count := range hashCounts {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			cpus := runtime.NumCPU()
			inputs := makeHashInputs(count)
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
}

func BenchmarkHashingWGPartition(b *testing.B) {
	for _, count := range hashCounts {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			cpus := runtime.NumCPU()
			inputs := makeHashInputs(count)
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

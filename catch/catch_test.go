//go:debug panicnil=1
package catch_test

import (
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/featherbread/parka/catch"
)

func Example_return() {
	result := catch.Do(func() (string, error) {
		return "hi there", nil
	})

	value, err := result.Unwrap()
	fmt.Printf(
		"returned=%v, panicked=%v, goexited=%v, value=%q, err=%v",
		result.Returned(), result.Panicked(), result.Goexited(), value, err)
	// Output: returned=true, panicked=false, goexited=false, value="hi there", err=<nil>
}

func Example_panic() {
	result := catch.Do(func() (any, error) {
		panic("oh no!")
	})

	fmt.Printf(
		"returned=%v, panicked=%v, goexited=%v, recovered=%q",
		result.Returned(), result.Panicked(), result.Goexited(), result.Recovered())
	// Output: returned=false, panicked=true, goexited=false, recovered="oh no!"
}

func Example_goexit() {
	result := catch.Do(func() (any, error) {
		runtime.Goexit()
		return nil, nil
	})

	fmt.Printf(
		"returned=%v, panicked=%v, goexited=%v",
		result.Returned(), result.Panicked(), result.Goexited())
	// Output: returned=false, panicked=false, goexited=true
}

var someNilValue any // Never assigned; quiets lints for literal panic(nil).

func TestZero(t *testing.T) {
	var r catch.Result[int]

	assert.False(t, r.Goexited())
	assert.False(t, r.Panicked())
	assert.True(t, r.Returned())

	v, err := r.Unwrap()
	assert.NoError(t, err)
	assert.Zero(t, v)
}

func TestNormalReturn(t *testing.T) {
	testCases := []catch.Result[int]{
		catch.Return(42, errors.New("silly goose")),
		catch.Do(func() (int, error) { return 42, errors.New("silly goose") }),
	}
	for i, r := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.False(t, r.Goexited())
			assert.False(t, r.Panicked())
			assert.True(t, r.Returned())

			v, err := r.Unwrap()
			assert.Equal(t, 42, v)
			assert.ErrorContains(t, err, "silly goose")
		})
	}
}

func TestPanic(t *testing.T) {
	testCases := []catch.Result[int]{
		catch.Panic[int]("silly panda"),
		catch.Do(func() (int, error) { panic("silly panda") }),
	}
	for i, r := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.False(t, r.Goexited())
			assert.False(t, r.Returned())
			assert.True(t, r.Panicked())
			assert.Equal(t, "silly panda", r.Recovered())

			defer func() {
				rv := recover()
				assert.Equal(t, "silly panda", rv)
			}()
			r.Unwrap()
			t.Error("continued after Result.Get should have panicked")
		})
	}
}

func TestPanicNil(t *testing.T) {
	testCases := []catch.Result[int]{
		catch.Panic[int](nil),
		catch.Do(func() (int, error) { panic(someNilValue) }),
	}
	for i, r := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.False(t, r.Goexited())
			assert.False(t, r.Returned())
			assert.True(t, r.Panicked())
			assert.Nil(t, r.Recovered())

			defer func() {
				rv := recover()
				assert.Nil(t, rv)
			}()
			r.Unwrap()
			t.Error("continued after Result.Get should have panicked")
		})
	}
}

func TestGoexit(t *testing.T) {
	testCases := []catch.Result[int]{
		catch.Goexit[int](),
		catch.Do(func() (int, error) { runtime.Goexit(); return 0, nil }),
	}
	for i, r := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.False(t, r.Panicked())
			assert.False(t, r.Returned())
			assert.True(t, r.Goexited())

			// Go's test framework doesn't allow tests to Goexit without failing.
			var wg sync.WaitGroup
			wg.Go(func() {
				r.Unwrap()
				t.Error("continued after Result.Get should have Goexited")
			})
			wg.Wait()
		})
	}
}

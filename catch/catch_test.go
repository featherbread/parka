//go:debug panicnil=1
package catch_test

import (
	"fmt"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/featherbread/parka/catch"
)

func Example_return() {
	result := catch.Do(func() {})
	fmt.Printf(
		"returned=%v, panicked=%v, goexited=%v",
		result.Returned(), result.Panicked(), result.Goexited())
	// Output: returned=true, panicked=false, goexited=false
}

func Example_panic() {
	result := catch.Do(func() { panic("oh no!") })
	fmt.Printf(
		"returned=%v, panicked=%v, goexited=%v, recovered=%q",
		result.Returned(), result.Panicked(), result.Goexited(), result.Recovered())
	// Output: returned=false, panicked=true, goexited=false, recovered="oh no!"
}

func Example_goexit() {
	result := catch.Do(func() { runtime.Goexit() })
	fmt.Printf(
		"returned=%v, panicked=%v, goexited=%v",
		result.Returned(), result.Panicked(), result.Goexited())
	// Output: returned=false, panicked=false, goexited=true
}

var someNilValue any // Never assigned; quiets lints for literal panic(nil).

func TestZero(t *testing.T) {
	var r catch.Result

	assert.False(t, r.Goexited())
	assert.False(t, r.Panicked())
	assert.True(t, r.Returned())
}

func TestNormalReturn(t *testing.T) {
	testCases := []catch.Result{
		catch.Return(),
		catch.Do(func() {}),
	}
	for i, r := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.False(t, r.Goexited())
			assert.False(t, r.Panicked())
			assert.True(t, r.Returned())
		})
	}
}

func TestPanic(t *testing.T) {
	testCases := []catch.Result{
		catch.Panic("silly panda"),
		catch.Do(func() { panic("silly panda") }),
	}
	for i, r := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.False(t, r.Goexited())
			assert.False(t, r.Returned())
			assert.True(t, r.Panicked())
			assert.Equal(t, "silly panda", r.Recovered())
		})
	}
}

func TestPanicNil(t *testing.T) {
	testCases := []catch.Result{
		catch.Panic(nil),
		catch.Do(func() { panic(someNilValue) }),
	}
	for i, r := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.False(t, r.Goexited())
			assert.False(t, r.Returned())
			assert.True(t, r.Panicked())
			assert.Nil(t, r.Recovered())
		})
	}
}

func TestGoexit(t *testing.T) {
	testCases := []catch.Result{
		catch.Goexit(),
		catch.Do(func() { runtime.Goexit() }),
	}
	for i, r := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.False(t, r.Panicked())
			assert.False(t, r.Returned())
			assert.True(t, r.Goexited())
		})
	}
}

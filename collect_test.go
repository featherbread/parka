package parka_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/featherbread/parka"
	"github.com/featherbread/parka/catch"
)

func ExampleCollect() {
	var calls atomic.Uint64
	inputs := []string{"hello", "world", "hello", "there"}
	hashes, _ := parka.Collect(context.Background(), inputs,
		func(_ context.Context, input string) (string, error) {
			calls.Add(1)
			hash := sha256.Sum256([]byte(input))
			return hex.EncodeToString(hash[:]), nil
		})

	fmt.Println(
		calls.Load(),
		len(hashes),
		hashes[0] == hashes[2],
		hashes[0],
	)
	// Output: 3 4 true 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
}

func ExampleCollectLimited() {
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
	defer srv.Close()

	var (
		ctx  = context.Background()
		urls = []string{
			"https://www.example.com/",
			"https://www.example.net/",
			// ...
		}

		// Note that even with concurrency > len(urls), Parka never spawns more
		// goroutines than necessary to complete a task.
		concurrency = 5
	)

	codes, err := parka.CollectLimited(ctx, concurrency, urls,
		func(ctx context.Context, url string) (int, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
			if err != nil {
				return 0, err
			}
			resp, err := srv.Client().Do(req)
			if err != nil {
				return 0, err
			}
			resp.Body.Close()
			return resp.StatusCode, nil
		})

	if err != nil {
		log.Printf("Failed to HEAD all URLs: %v", err)
	}

	fmt.Println(codes)
	// Output: [200 200]
}

func TestCollectLimited(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// These calls must occur in order, up to 2 concurrently at any time.
		const (
			successfulCall int = iota
			failingCall
			blockedCall1
			blockedCall2
			dequeuedCall
			totalCalls
		)

		var (
			called [totalCalls]bool
			start  = time.Now()
		)

		_, err := parka.CollectLimited(context.Background(), 2, makeIntKeys(totalCalls),
			func(ctx context.Context, x int) (any, error) {
				called[x] = true
				switch x {
				case successfulCall:
					return nil, nil

				case failingCall:
					// blockedCall1 is next in the queue, so should be the only other
					// call allowed to proceed. Ensure it blocks on ctx before we fail,
					// to test that cancellation works properly.
					synctest.Wait()
					assert.True(t, called[blockedCall1], "blockedCall1 should be started")
					return nil, errors.New("failing call")

				case blockedCall1, blockedCall2:
					// Blocking our 2 handler slots guarantees that dequeuedCall must
					// actually get dequeued rather than handled. If ctx isn't canceled,
					// synctest implicitly catches the resulting deadlock.
					<-ctx.Done()
					// This lets us confirm that we wait for handlers to finish after
					// canceling their contexts.
					time.Sleep(1 * time.Second)
					return nil, ctx.Err()

				default:
					return nil, errors.New("unhandled call")
				}
			})

		assert.GreaterOrEqual(t, time.Since(start), 1*time.Second,
			"CollectLimited should have waited for canceled handlers to finish")

		assert.ErrorContains(t, err, "failing call")
		assert.True(t, called[successfulCall], "successfulCall should have been called")
		assert.False(t, called[dequeuedCall], "dequeuedCall should not have been called")
	})
}

func TestCollectGoexit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handler := func(ctx context.Context, x int) (any, error) {
			if x == 1 {
				time.Sleep(1 * time.Second)
				runtime.Goexit()
				return nil, errors.New("goexit")
			}
			// Blocking on ctx implicitly tests that _any_ failing handler causes
			// other outstanding handlers to be canceled regardless of queue order.
			<-ctx.Done()
			time.Sleep(2 * time.Second)
			return nil, nil
		}

		start := time.Now()
		result := catch.Do(func() (any, error) {
			return parka.Collect(context.Background(), makeIntKeys(3), handler)
		})

		assert.GreaterOrEqual(t, time.Since(start), 3*time.Second,
			"Collect should have waited for non-Goexit handlers to finish")

		assert.True(t, result.Panicked(), "Collect should have panicked")
		assert.ErrorIs(t, result.Recovered().(error), parka.ErrHandlerGoexit)
	})
}

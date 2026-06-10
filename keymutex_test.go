package parka_test

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"

	"github.com/featherbread/parka"
	"github.com/featherbread/parka/catch"
)

func ExampleKeyMutex() {
	var hostsMu parka.KeyMutex[string]

	pages := parka.NewMap(func(ph *parka.Handle, pageURL string) ([]byte, error) {
		parsedURL, err := url.Parse(pageURL)
		if err != nil {
			return nil, err
		}

		// Limit concurrent requests to any single host while allowing concurrency
		// across distinct hosts.
		hostsMu.LockDetached(ph, parsedURL.Hostname())
		defer hostsMu.Unlock(parsedURL.Hostname())

		var body bytes.Buffer
		resp, err := http.Get(pageURL)
		if err == nil {
			_, err = io.Copy(&body, resp.Body)
			resp.Body.Close()
		}
		return body.Bytes(), err
	})

	// Even though we queue requests for the same hostname together, the KeyMutex
	// ensures our two concurrent requests always hit different hosts.
	pages.Limit(2)
	pages.Collect(
		// example.com
		"https://www.example.com/index.html",
		"https://www.example.com/faq.html",
		"https://www.example.com/contact.html",
		// example.net
		"https://www.example.net/index.html",
		"https://www.example.net/faq.html",
		"https://www.example.net/contact.html",
		// ...
	)
}

func TestKeyMutexZeroUnlock(t *testing.T) {
	var km parka.KeyMutex[int]
	result := catch.Do(func() { km.Unlock(0) })
	assert.True(t, result.Panicked())
	assert.Contains(t, result.Recovered(), "key is already unlocked")
}

func TestKeyMutextDoubleUnlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var km parka.KeyMutex[int]
		km.Lock(0)
		km.Lock(1)
		km.Unlock(1)
		result := catch.Do(func() { km.Unlock(1) })
		assert.True(t, result.Panicked())
		assert.Contains(t, result.Recovered(), "key is already unlocked")
	})
}

func TestKeyMutexBasic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			keyCount    = 3
			workerCount = 2 * keyCount
		)
		var (
			km      parka.KeyMutex[int]
			locked  [keyCount]atomic.Int32
			unblock = make(chan struct{})
		)
		for i := range workerCount {
			key := i / 2
			go func() {
				km.Lock(key)
				defer km.Unlock(key)

				locked[key].Add(1)
				defer locked[key].Add(-1)

				<-unblock
			}()
		}

		// Wait for every goroutine to be durably blocked, then check for limit
		// breaches.
		synctest.Wait()
		for i := range locked {
			count := int(locked[i].Load())
			assert.Equal(t, 1, count, "At index %d", i)
		}

		// Let all of the workers finish.
		close(unblock)
	})
}

func TestKeyMutexDetachReattach(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			km       parka.KeyMutex[struct{}]
			unblock0 = make(chan struct{})
		)
		s := parka.NewSet(func(ph *parka.Handle, x int) error {
			if x == 0 {
				km.LockDetached(ph, struct{}{})
				<-unblock0
				km.Unlock(struct{}{})
			}
			return nil
		})
		s.Limit(1)

		// Take the lock.
		km.Lock(struct{}{})

		// Start the handler for 0, which must detach since we're holding the lock.
		s.Inform(0)
		synctest.Wait()

		// Ensure that unrelated handlers can, in fact, proceed.
		s.Get(1)

		// Release the lock so handler 0 can obtain it.
		km.Unlock(struct{}{})
		synctest.Wait()

		// Start another handler, and ensure it really is blocked.
		done := promise(func() { s.Get(2) })
		synctest.Wait()
		select {
		case <-done:
			assert.Fail(t, "Computation of key was not blocked")
		default:
		}

		// Allow all of the handlers to finish.
		close(unblock0)
		s.Collect(0, 1, 2)
	})
}

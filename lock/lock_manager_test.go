package lock

import (
	"sync"
	"testing"
)

func TestLockManager(t *testing.T) {
	lm := NewLockManager()
	lm.Lock("name")
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		lm.Lock("name")
		t.Logf("get name lock")
		lm.Unlock("name")
	}()
	go func() {
		lm.Lock("score")
		t.Logf("get score lock")
		lm.Unlock("score")
	}()

	//<-time.After(time.Second * 5)
	lm.Unlock("name")
	wg.Wait()
}

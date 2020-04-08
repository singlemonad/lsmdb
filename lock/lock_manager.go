package lock

import "sync"

type LockManager struct {
	mutex sync.Mutex
	locks map[string]*Lock
}

func NewLockManager() *LockManager {
	return &LockManager{
		mutex: sync.Mutex{},
		locks: make(map[string]*Lock),
	}
}

func (lm *LockManager) Lock(key string) {
	for {
		lm.mutex.Lock()
		if _, ok := lm.locks[key]; !ok {
			lm.locks[key] = NewLock()
			lm.mutex.Unlock()
			break
		} else {
			lock := lm.locks[key]
			lock.cond.L.Lock()
			lm.mutex.Unlock()
			lock.cond.Wait()
			lock.cond.L.Unlock()
		}
	}
}

func (lm *LockManager) Unlock(key string) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	if _, ok := lm.locks[key]; !ok {
		panic("unlock a nonexistent lock.")
	}
	lock := lm.locks[key]
	delete(lm.locks, key)
	lock.cond.Signal()
	return
}

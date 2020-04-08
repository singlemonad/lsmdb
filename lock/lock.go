package lock

import (
	"sync"
)

type Lock struct {
	cond *sync.Cond
}

func NewLock() *Lock {
	return &Lock{
		cond: sync.NewCond(new(sync.Mutex)),
	}
}

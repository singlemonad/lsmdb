package util

import "sync/atomic"

type IDFactory struct {
	nextID uint64
}

func NewIDFactory() *IDFactory {
	return &IDFactory{}
}

func (f *IDFactory) NewID() uint64 {
	return atomic.AddUint64(&f.nextID, 1)
}

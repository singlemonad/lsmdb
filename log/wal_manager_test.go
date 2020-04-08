package log

import (
	"math/rand"
	"testing"
	"time"

	"github.com/bmizerany/assert"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestWALManager(t *testing.T) {
	w, err := NewWALWriter("/Users/yang.qu/go/src/github.com/singlemonad/lsmdb/wal_log_dir")
	assert.Equal(t, nil, err)
	s1 := RandStringRunes(66)
	err = w.AddRecord([]byte(s1))
	assert.Equal(t, nil, err)
	s2 := RandStringRunes(77)
	err = w.AddRecord([]byte(s2))
	assert.Equal(t, nil, err)
	s3 := RandStringRunes(999999)
	err = w.AddRecord([]byte(s3))
	assert.Equal(t, nil, err)
	s4 := RandStringRunes(99999999)
	err = w.AddRecord([]byte(s4))
	assert.Equal(t, nil, err)

	r, err := NewWALReader("/Users/yang.qu/go/src/github.com/singlemonad/lsmdb/wal_log_dir")
	assert.Equal(t, nil, err)
	rec1, err := r.ReadRecord()
	assert.Equal(t, []byte(s1), rec1)
	rec2, err := r.ReadRecord()
	assert.Equal(t, []byte(s2), rec2)
	rec3, err := r.ReadRecord()
	assert.Equal(t, []byte(s3), rec3)
	rec4, err := r.ReadRecord()
	assert.Equal(t, []byte(s4), rec4)
}

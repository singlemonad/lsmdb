package transaction

import (
	"github.com/singlemonad/lsmdb/lock"
	"github.com/singlemonad/lsmdb/log"
	"go.uber.org/zap"
)

type Transaction struct {
	id          uint64
	logger      *zap.SugaredLogger
	redolog     *log.RedoLog
	lockManager *lock.LockManager
	fetchLocks  map[string]bool
}

func NewTransaction(id uint64, redolog *log.RedoLog, lm *lock.LockManager) *Transaction {
	return &Transaction{
		id:          id,
		logger:      zap.NewExample().Sugar(),
		redolog:     redolog,
		lockManager: lm,
		fetchLocks:  make(map[string]bool),
	}
}

func (t *Transaction) Begin() {
	t.redolog.BeginTransaction(t.id)
}

func (t *Transaction) Commit() {
	// first apply atcion in db

	// then commit in redolog
	t.redolog.CommitTransaction(t.id)

	// last release lock
	for lockKey := range t.fetchLocks {
		t.lockManager.Unlock(lockKey)
	}
}

func (t *Transaction) Abort() {
	t.redolog.AbortTransaction(t.id)
	for lockKey := range t.fetchLocks {
		t.lockManager.Unlock(lockKey)
	}
}

func (t *Transaction) Put(key string, value []byte) {
	t.lockManager.Lock(key)
	t.fetchLocks[key] = true
	t.redolog.Append(log.NewRedoLogEntry(t.id, log.OPERATOR_INSERT, key, value))
}

func (t *Transaction) Delete(key string) {
	t.lockManager.Lock(key)
	t.fetchLocks[key] = true
	t.redolog.Append(log.NewRedoLogEntry(t.id, log.OPERATOR_DELETE, key, []byte{}))
}

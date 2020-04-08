package log

import "sync"

type operatorType = int32

const (
	OPERATOR_SEARCH operatorType = 1
	OPERATOR_INSERT operatorType = 2
	OPERATOR_DELETE operatorType = 3
)

type RedoLogEntry struct {
	transactionID uint64
	oTyp          operatorType
	key           string
	value         []byte
}

func NewRedoLogEntry(id uint64, oTyp operatorType, key string, value []byte) *RedoLogEntry {
	return &RedoLogEntry{
		transactionID: id,
		oTyp:          oTyp,
		key:           key,
		value:         value,
	}
}

type RedoLog struct {
	data               []byte
	mutex              sync.Mutex
	ongoingTransaction map[uint64][]*RedoLogEntry
}

func NewRedoLog() *RedoLog {
	return &RedoLog{
		data:               make([]byte, 0),
		mutex:              sync.Mutex{},
		ongoingTransaction: make(map[uint64][]*RedoLogEntry),
	}
}

func (rl *RedoLog) BeginTransaction(transactionID uint64) {

}

func (rl *RedoLog) CommitTransaction(transactionID uint64) {
	// sync redoLogEntry to redo log file
	// then mark the transaction end and reomve related info in ongonigTransaction
}

func (rl *RedoLog) AbortTransaction(transactionID uint64) {

}

func (rl *RedoLog) Append(entry *RedoLogEntry) {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	if _, ok := rl.ongoingTransaction[entry.transactionID]; !ok {
		rl.ongoingTransaction[entry.transactionID] = make([]*RedoLogEntry, 0)
	}
	rl.ongoingTransaction[entry.transactionID] = append(rl.ongoingTransaction[entry.transactionID], entry)
}

func (rl *RedoLog) FindEntries(transactionID uint64) []*RedoLogEntry {
	return make([]*RedoLogEntry, 0)
}

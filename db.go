package lsmdb

import (
	"github.com/singlemonad/lsmdb/memtable"
	"github.com/singlemonad/lsmdb/record"
	"github.com/singlemonad/lsmdb/transaction"
	"go.uber.org/zap"
)

type LsmDBMessageType = int

const (
	exitMessage         = 0
	memtableFullMessage = 1
)

type LsmDB struct {
	logger     *zap.SugaredLogger
	mutable    *memtable.Memtable
	immutable  *memtable.Memtable
	versionSet *record.LevelSet
	loopC      chan LsmDBMessageType
}

func OpenLsmdb(name string) *LsmDB {
	lsmdb := &LsmDB{
		logger: zap.NewExample().Sugar(),
		loopC:  make(chan LsmDBMessageType),
	}
	go lsmdb.mainThread()

	return lsmdb
}

func (db *LsmDB) Get(key string) ([]byte, error) {
	return nil, nil
}

func (db *LsmDB) Put(key string, value []byte) error {
	db.mutable.Insert(key, value)
	if db.mutable.Full() {
		db.loopC <- memtableFullMessage
	}
	return nil
}

func (db *LsmDB) Delete(key string) error {
	return nil
}

func (db *LsmDB) NewTransaction() *transaction.Transaction {
	return nil
}

func (db *LsmDB) mainThread() {
	defer func() {
		db.logger.Infof("lsmdb main thread exit")
	}()

	for message := range db.loopC {
		switch message {
		case memtableFullMessage:
			db.memtableFull()
		case exitMessage:
			return
		}
	}
}

func (db *LsmDB) memtableFull() {
	db.immutable = db.mutable
	db.mutable = memtable.NewMemtable()
	db.versionSet.Contraction(db.immutable.TransferToBlock())
}

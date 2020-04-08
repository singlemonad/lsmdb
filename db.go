package lsmdb

import (
	"github.com/singlemonad/lsmdb/memtable"
	"github.com/singlemonad/lsmdb/record"
	"github.com/singlemonad/lsmdb/transaction"
)

type LsmDB struct {
	mutable    *memtable.Memtable
	immutable  *memtable.Memtable
	versionSet *record.VersionSet
}

func OpenLsmdb(name string) *LsmDB {
	return &LsmDB{}
}

func (db *LsmDB) Get(key string) ([]byte, error) {
	return nil, nil
}

func (db *LsmDB) Put(key string, value []byte) error {
	db.mutable.Insert(key, value)
	if db.mutable.Full() {
		db.immutable = db.mutable
		db.mutable = memtable.NewMemtable()
		go db.mergeMemtable()
	}

	return nil
}

func (db *LsmDB) Delete(key string) error {
	return nil
}

func (db *LsmDB) NewTransaction() *transaction.Transaction {
	return nil
}

// merge, need optimiz
func (db *LsmDB) mergeMemtable() {
	level0Blocks := db.versionSet.FetchLevel0Blocks()
	mergeMemtableToLevel0(db.immutable, level0Blocks)
	db.versionSet.Contraction()
}

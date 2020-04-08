package memtable

import (
	"sort"

	"github.com/singlemonad/lsmdb/record"
)

const (
	MemtableLengthLimit = 100
)

type Memtable struct {
	entries map[string][]byte
	count   int
}

func NewMemtable() *Memtable {
	return &Memtable{
		entries: make(map[string][]byte),
	}
}

func (m *Memtable) Insert(key string, data []byte) {
	m.entries[key] = data
	m.count++
}

func (m *Memtable) Find(key string) []byte {
	entry, ok := m.entries[key]
	if !ok {
		return nil
	}
	return entry
}

func (m *Memtable) Full() bool {
	return m.count >= MemtableLengthLimit
}

func (m *Memtable) Keys() []string {
	retKeys := make([]string, 0)
	for key := range m.entries {
		retKeys = append(retKeys, key)
	}
	return retKeys
}

func (m *Memtable) TransferToBlock() []*record.Block {
	retBlocks := make([]*record.Block, 0)
	keys := m.Keys()
	sort.Strings(keys)
	block := record.NewBlock()
	for _, key := range keys {
		if err := block.Append(record.NewRecord(key, m.entries[key])); err != nil {
			retBlocks = append(retBlocks, block)
			block = record.NewBlock()
		}
	}
	retBlocks = append(retBlocks, block)
	return retBlocks
}

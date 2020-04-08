package lsmdb

import (
	"sort"

	"github.com/singlemonad/lsmdb/memtable"
	"github.com/singlemonad/lsmdb/record"
)

func mergeMemtableToLevel0(memtable *memtable.Memtable, level0Blocks []*record.Block) []*record.Block {
	memtableKeys := memtable.Keys()
	sort.Strings(memtableKeys)

	iters := make([]*record.BlockIterator, 0)
	for _, block := range level0Blocks {
		iters = append(iters, record.NewBlockIterator(block))
	}

	mergeAfterBlocks := make([]*record.Block, 0)
	currBlock := record.NewBlock()
	mergeAfterBlocks = append(mergeAfterBlocks, currBlock)
	memtableKeyIndex := 0
	for {
		var nextKey string
		candidateIndex := -1

		if memtableKeyIndex < len(memtableKeys) {
			nextKey = memtableKeys[memtableKeyIndex]
		} else {
			for _, iter := range iters {
				if !iter.End() {
					nextKey = iter.Peek().GetKey()
				}
			}
		}

		for i, iter := range iters {
			if !iter.End() && iter.Peek().GetKey() < nextKey {
				candidateIndex = i
				nextKey = iter.Peek().GetKey()
			}
		}

		if candidateIndex == -1 {
			candidateKey := memtableKeys[memtableKeyIndex]
			memtableKeyIndex++
			currBlock.Append(record.NewRecord(candidateKey, memtable.Find(candidateKey)))
		} else {
			currBlock.Append(iters[candidateIndex].Next())
		}

		if currBlock.Full() {
			currBlock = record.NewBlock()
			mergeAfterBlocks = append(mergeAfterBlocks, currBlock)
		}

		finished := memtableKeyIndex == len(memtableKeys)
		for _, iter := range iters {
			finished = finished && iter.End()
		}
		if finished {
			break
		}
	}

	return mergeAfterBlocks
}

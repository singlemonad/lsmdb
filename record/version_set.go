package record

import "sync"

const (
	LevelFileLimit = 4
)

type Level struct {
	blocks []*Block
}

func (l *Level) FindMergeBock(startKey, endKey string) []*Block {
	var startIndex, endIndex int
	for i, block := range l.blocks {
		if startKey < block.FetchLastKey() {
			startIndex = i
		}
		if endKey < block.FetchLastKey() {
			endIndex = i
			break
		}
	}
	retBlocks := make([]*Block, 0)
	for i := startIndex; i <= endIndex; i++ {
		retBlocks = append(retBlocks, l.blocks[i])
	}
	return retBlocks
}

// versionSet represent files on disk
type VersionSet struct {
	mutex sync.Mutex
	files []*Level
}

func NewVersionSet(numLevels int) *VersionSet {
	return &VersionSet{
		mutex: sync.Mutex{},
		files: make([]*Level, numLevels),
	}
}

func (vs *VersionSet) FetchLevel0Blocks() []*Block {
	vs.mutex.Lock()
	defer vs.mutex.Unlock()

	return vs.files[0].blocks
}

func (vs *VersionSet) Contraction() {
	vs.mutex.Lock()
	defer vs.mutex.Unlock()

	numLevels := len(vs.files)
	for i := 0; i < numLevels; i++ {
		if len(vs.files[i].blocks) > LevelFileLimit {
			target := vs.files[i].blocks[0]
			startKey := target.FetchFirstKey()
			endKey := target.FetchLastKey()
			upBlocks := vs.files[i+1].FindMergeBock(startKey, endKey)
			mergeBlocks(target, upBlocks)
		}
	}
}

func mergeBlocks(newcome *Block, target []*Block) []*Block {
	iters := make([]*BlockIterator, 0)
	for _, block := range target {
		iters = append(iters, NewBlockIterator(block))
	}
	iters = append(iters, NewBlockIterator(newcome))

	mergeAfterBlocks := make([]*Block, 0)
	currBlock := NewBlock()
	mergeAfterBlocks = append(mergeAfterBlocks, currBlock)
	for {
		var nextKey string
		candidateIndex := -1
		for _, iter := range iters {
			if !iter.End() {
				nextKey = iter.Peek().key
			}
		}
		for i, iter := range iters {
			if !iter.End() && iter.Peek().key < nextKey {
				nextKey = iter.Peek().key
				candidateIndex = i
			}
		}

		currBlock.Append(iters[candidateIndex].Next())

		if currBlock.Full() {
			currBlock = NewBlock()
			mergeAfterBlocks = append(mergeAfterBlocks, currBlock)
		}

		var finished bool
		for _, iter := range iters {
			finished = finished && iter.End()
		}
		if finished {
			break
		}
	}

	return mergeAfterBlocks
}

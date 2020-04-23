package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"sort"
	"strconv"
	"sync"
	"unsafe"

	"github.com/singlemonad/lsmdb/util"
)

const (
	LevelFileLimit = 4
	LogFileSize    = 4 * (1 << 20)
)

// levelSet represent files on disk
type LevelSet struct {
	dir string
	mutex  sync.Mutex
	levels []*Level
}

func NewLevelSet(numLevels int, dir string) *LevelSet {
	return &LevelSet{
		dir: dir,
		mutex:  sync.Mutex{},
		levels: make([]*Level, numLevels),
	}
}

func (vs *LevelSet) Contraction(blocks []*Block) error {
	vs.mutex.Lock()
	defer vs.mutex.Unlock()

	// if level0 is nil, create one
	if vs.levels[0] == nil {
		level0, err := NewLevel(0, vs.dir)
		if err != nil {
			return err
		}
		vs.levels[0] = level0
	}
	vs.levels[0].MergeDown(blocks[0].FetchFirstKey(), blocks[len(blocks)-1].FetchLastKey(), blocks)

	for currLevel := 0; currLevel < len(vs.levels); currLevel++ {
		if len(vs.levels[currLevel].files) > LevelFileLimit {
			blocks, err := vs.levels[currLevel].MergeUp()
			if err != nil {
				return err
			}

			// if levelN if nil, create one
			if vs.levels[currLevel+1] == nil {
				levelN, err := NewLevel(currLevel + 1, vs.dir)
				if err != nil {
					return err
				}
				vs.levels[currLevel+1] = levelN
			}

			if err = vs.levels[currLevel+1].MergeDown(blocks[0].FetchFirstKey(), blocks[len(blocks)-1].FetchLastKey(), blocks); err != nil {
				return err
			}
		} else {
			break
		}
	}
	return nil
}

type levelBlockIterator struct {
	level      *Level
	nextIndex  int
	file       *util.File
	inputFiles []int
}

func newLevelBlockIterator(level *Level, inputFiles []int) (*levelBlockIterator, error) {
	iter := &levelBlockIterator{
		level:      level,
		nextIndex:  0,
		inputFiles: inputFiles,
	}

	file, err := util.OpenReadOnlyFile(fmt.Sprintf("%s/%d", iter.level.dir, iter.level.files[iter.nextIndex]))
	if err != nil {
		return nil, err
	}
	iter.file = file
	iter.nextIndex++

	return iter, nil
}

func (iter *levelBlockIterator) nextBlock() (*Block, error) {
	const uint64Size = int(unsafe.Sizeof(uint64(0)))
	for {
		// read block length
		blockLenBuff := make([]byte, uint64Size)
		ret, err := iter.file.Read(blockLenBuff)
		if err != nil {
			return nil, err
		}
		if ret == 0 {
			// reach file tail
			if err := iter.changeLogFile(); err != nil {
				return nil, err
			}
			continue
		}
		blockLen := binary.BigEndian.Uint64(blockLenBuff)

		// read block
		blockBuff := make([]byte, blockLen)
		_, err = iter.file.Read(blockBuff)
		if err != nil {
			return nil, err
		}
		retBlock := NewBlock()
		retBlock.DeMaterialize(blockBuff)
		return retBlock, nil
	}
}

func (iter *levelBlockIterator) changeLogFile() error {
	if iter.nextIndex >= len(iter.level.files) {
		return errors.New("no log file left")
	}

	file, err := util.OpenReadOnlyFile(fmt.Sprintf("%s/%d", iter.level.dir, iter.level.files[iter.nextIndex]))
	if err != nil {
		return err
	}
	iter.file = file
	iter.nextIndex++

	return nil
}

type Level struct {
	blocks     []*Block
	levelIndex int
	dir        string
	files      []int
}

func NewLevel(levelIndex int, dir string) (*Level, error) {
	l := &Level{
		levelIndex: levelIndex,
		dir:        dir,
	}

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		idx, err := strconv.ParseInt(file.Name(), 10, 64)
		if err != nil {
			return nil, err
		}
		l.files = append(l.files, int(idx))
	}
	sort.Ints(l.files)

	return l, nil
}

func (l *Level) FindRecordByKey(key string) (*Record, error) {
	blockIter, err := newLevelBlockIterator(l, l.files)
	if err != nil {
		return nil, err
	}

	for {
		block, err := blockIter.nextBlock()
		if err != nil {
			return nil, err
		}
		record, err := block.FetchRecordByKey(key)
		if err != nil {
			return nil, err
		}
		if record != nil {
			return record, nil
		}
	}
}

// @startKey: first key in targets
// @endKey: end key in targets
// @targets: lower level chosen block set to merge with me
func (l *Level) MergeDown(startKey, endKey string, targets []*Block) error {
	mergeFileNums, err := l.findMergeFiles(startKey, endKey)
	if err != nil {
		return err
	}
	if len(mergeFileNums) == 0 {
		// TODO
	}

	// init myIterator
	sort.Ints(mergeFileNums)
	blockIter, err := newLevelBlockIterator(l, mergeFileNums)
	if err != nil {
		return err
	}
	myblock, err := blockIter.nextBlock()
	if err != nil {
		return err
	}
	myIterator := NewBlockIterator(myblock)

	// init targetIterator
	targetBlockIndex := 0
	targetIterator := NewBlockIterator(targets[targetBlockIndex])

	mergeFileIndex := 0
	mergeFile, err := l.newMergeFile(mergeFileIndex)
	if err != nil {
		return err
	}
	mergeFileWritenBytes := 0

	block := NewBlock()

	myEnd := false
	targetEnd := false

	appendRecFunc := func(nextRec *Record) error {
		if err := block.Append(nextRec); err != nil {
			// block full, write it to merge file
			bytes := block.Materialize()
			if _, err := mergeFile.Write(bytes); err != nil {
				return err
			}
			mergeFileWritenBytes += len(bytes)
			if mergeFileWritenBytes >= LogFileSize {
				// merge file file, use new one
				mergeFile.Close()
				mergeFileIndex++
				mergeFile, err = l.newMergeFile(mergeFileIndex)
				if err != nil {
					return err
				}
				mergeFileWritenBytes = 0
			}
			block = NewBlock()
		}
		return nil
	}

	for !myEnd || !targetEnd {
		// reinit myIterator
		for myIterator.End() && myEnd == false {
			myblock, err = blockIter.nextBlock()
			if err != nil {
				return err
			}
			if myblock == nil {
				myEnd = true
				break
			}
			myIterator = NewBlockIterator(myblock)
		}

		// reinit targetIterator
		for targetIterator.End() && targetEnd == false {
			targetBlockIndex++
			if targetBlockIndex >= len(targets) {
				targetEnd = true
				break
			}
			targetIterator = NewBlockIterator(targets[targetBlockIndex])
		}

		if !myEnd && !targetEnd {
			var nextRec *Record

			nextKey := myIterator.Peek().GetKey()
			if nextKey < targetIterator.Peek().GetKey() {
				nextRec = targetIterator.Next()
			} else {
				nextRec = myIterator.Next()
			}

			if err := appendRecFunc(nextRec); err != nil {
				return err
			}
		} else if targetEnd {
			if err := appendRecFunc(myIterator.Next()); err != nil {
				return err
			}
		} else {
			if err := appendRecFunc(targetIterator.Next()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *Level) newMergeFile(index int) (*util.File, error) {
	return util.OpenWriteOnlyFile(fmt.Sprintf("%s/merge-%d", l.dir, index))
}

// chosen block set to merge to higher level
func (l *Level) MergeUp() ([]*Block, error) {
	blockIter, err := newLevelBlockIterator(l, []int{l.choseMergeFile()})
	if err != nil {
		return nil, err
	}

	retBlocks := make([]*Block, 0)
	for {
		block, err := blockIter.nextBlock()
		if err != nil {
			return nil, err
		}
		if block == nil {
			break
		}
		retBlocks = append(retBlocks, block)
	}
	return retBlocks, nil
}

func (l *Level) choseMergeFile() int {
	return l.files[0]
}

func (l *Level) findMergeFiles(startKey, endKey string) ([]int, error) {
	blockIter, err := newLevelBlockIterator(l, l.files)
	if err != nil {
		return nil, err
	}

	retFileNums := make(map[int]bool, 0)
	begin := false
	for {
		block, err := blockIter.nextBlock()
		if err != nil {
			return nil, err
		}

		if startKey <= block.FetchLastKey() {
			retFileNums[l.files[blockIter.nextIndex-1]] = true
			begin = true
		}
		if endKey <= block.FetchLastKey() {
			retFileNums[l.files[blockIter.nextIndex-1]] = true
			break
		}
		if begin {
			retFileNums[l.files[blockIter.nextIndex-1]] = true
		}
	}
	ret := make([]int, 0)
	for key := range retFileNums {
		ret = append(ret, key)
	}
	return ret, nil
}

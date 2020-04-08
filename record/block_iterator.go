package record

type BlockIterator struct {
	block *Block
	next  int
}

func NewBlockIterator(block *Block) *BlockIterator {
	return &BlockIterator{
		block: block,
		next:  0,
	}
}

func (iter *BlockIterator) Next() *Record {
	index := iter.block.directory.entries[iter.next]
	record := &Record{}
	record.DeMaterialize(iter.block.data[index.offset : index.offset+index.len])
	iter.next++
	return record
}

func (iter *BlockIterator) Peek() *Record {
	index := iter.block.directory.entries[iter.next]
	record := &Record{}
	record.DeMaterialize(iter.block.data[index.offset : index.offset+index.len])
	return record
}

func (iter *BlockIterator) End() bool {
	if iter.next == len(iter.block.directory.entries) {
		return true
	}
	return false
}

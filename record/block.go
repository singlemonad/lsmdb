package record

import (
	"bytes"
	"encoding/binary"
	"errors"
	"unsafe"
)

const (
	BlockSize = 4 * (2 << 10)
)

// Block is a set of sort Record
// Head长度 | Head | 记录
type Block struct {
	head      *BlockHead       // blockHead 需要落盘
	directory *RecordDirectory // recordDirectory 不落盘，每次将block读入内存时构建recordDirectory
	length    int              // current length, while length exceed BlockSize, write to it will reject
	data      []byte           // block content
}

func NewBlock() *Block {
	return &Block{
		head:      NewBlockHead(),
		directory: NewRecordDirectory(),
		length:    0,
		data:      make([]byte, 0),
	}
}

func (block *Block) Materialize() []byte {
	buff := new(bytes.Buffer)
	buff.Write(block.head.Materialize())
	buff.Write(block.data)
	return buff.Bytes()
}

func (block *Block) DeMaterialize(data []byte) {
	var currIdx uint64

	// 读取blockHead的长度，反序列化blockHead
	headLen := binary.BigEndian.Uint64(data[currIdx : int(currIdx)+int(unsafe.Sizeof(uint64(0)))])
	block.head.DeMaterialize(data[currIdx : int(currIdx)+int(headLen)])
	currIdx += headLen

	// 反序列化记录，构建recordDirectory
	for currIdx < uint64(len(data)) {
		recordLen := binary.BigEndian.Uint64(data[currIdx : int(currIdx)+int(unsafe.Sizeof(uint64(0)))])
		currRecord := &Record{}
		currRecord.DeMaterialize(data[int(currIdx)+int(unsafe.Sizeof(uint64(0))) : int(currIdx)+int(unsafe.Sizeof(uint64(0)))+int(recordLen)])
		block.directory.Append(&RecordIndex{currRecord.key, currIdx, recordLen})
		currIdx += recordLen
	}
}

func (block *Block) Append(record *Record) error {
	bytes, err := record.Materialize()
	if err != nil {
		return err
	}
	if len(bytes)+block.length > BlockSize {
		return errors.New("not enough space")
	}

	block.directory.Append(&RecordIndex{record.key, uint64(len(block.data)), uint64(len(bytes))})
	block.data = append(block.data, bytes...)

	return nil
}

func (block *Block) FetchRecordByKey(key string) (*Record, error) {
	index := block.directory.FindIndexByKey(key)
	if index == nil {
		return nil, nil
	}
	record := &Record{}
	record.DeMaterialize(block.data[index.offset : index.offset+index.len])
	return record, nil
}

func (block *Block) Full() bool {
	if len(block.data) >= BlockSize {
		return true
	}
	return false
}

func (block *Block) FetchFirstKey() string {
	return block.directory.FetchFirstKey()
}

func (block *Block) FetchLastKey() string {
	return block.directory.FetchLastKey()
}

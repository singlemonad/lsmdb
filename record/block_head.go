package record

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

const (
	blockHeadSize = uint64(unsafe.Sizeof(uint64(0)))
)

type BlockHead struct {
	recordAmount int
}

func NewBlockHead() *BlockHead {
	return &BlockHead{}
}

func (head *BlockHead) Materialize() []byte {
	buff := new(bytes.Buffer)
	binary.Write(buff, binary.BigEndian, uint64(unsafe.Sizeof(uint64(0))))
	return buff.Bytes()
}

func (head *BlockHead) DeMaterialize(data []byte) {

}

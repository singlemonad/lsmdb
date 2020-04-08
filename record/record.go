package record

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

const (
	KeyLengthOccupyBytes   = 8
	ValueLengthOccupyBytes = 8
)

// KV记录磁盘存储格式：
// 记录长度 | 键长度 | 键 | 值长度 | 值
type Record struct {
	key   string
	value []byte
}

func NewRecord(key string, value []byte) *Record {
	return &Record{
		key:   key,
		value: value,
	}
}

func (r *Record) Materialize() ([]byte, error) {
	var err error

	buff := new(bytes.Buffer)
	if err = binary.Write(buff, binary.BigEndian, uint64(len(r.key))); err != nil {
		return nil, err
	}

	if err = binary.Write(buff, binary.BigEndian, []byte(r.key)); err != nil {
		return nil, err
	}

	if err = binary.Write(buff, binary.BigEndian, uint64(len(r.value))); err != nil {
		return nil, err
	}

	if err = binary.Write(buff, binary.BigEndian, r.value); err != nil {
		return nil, err
	}

	contentLen := len(buff.Bytes())
	lenBuff := new(bytes.Buffer)
	if err = binary.Write(lenBuff, binary.BigEndian, uint64(contentLen)+uint64(unsafe.Sizeof(uint64(0)))); err != nil {
		return nil, err
	}
	lenBuff.Write(buff.Bytes())
	return lenBuff.Bytes(), nil
}

func (r *Record) DeMaterialize(data []byte) {
	keyLen := binary.BigEndian.Uint64(data[unsafe.Sizeof(uint64(0)) : unsafe.Sizeof(uint64(0))+KeyLengthOccupyBytes])
	r.key = string(data[unsafe.Sizeof(uint64(0))+KeyLengthOccupyBytes : int(unsafe.Sizeof(uint64(0)))+KeyLengthOccupyBytes+int(keyLen)])
	valueLenStart := int(unsafe.Sizeof(uint64(0))) + int(KeyLengthOccupyBytes) + int(keyLen)
	valueLenEnd := valueLenStart + ValueLengthOccupyBytes
	valueLen := binary.BigEndian.Uint64(data[valueLenStart:valueLenEnd])
	valueEnd := valueLenEnd + int(valueLen)
	r.value = data[valueLenEnd:valueEnd]
}

func (r *Record) GetKey() string {
	return r.key
}

func (r *Record) GetValue() []byte {
	return r.value
}

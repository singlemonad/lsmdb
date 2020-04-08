package log

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

type recordType = uint32

const (
	recordTypeZero   = 0
	recordTypeFull   = 1
	recordTypeFirst  = 2
	recordTypeMiddle = 3
	recordTyepLast   = 4
)

const headerSize = int(unsafe.Sizeof(uint64(0)) * 2)

const logBlockSize = 32 * (1 << 10)

const uint64Size = int(unsafe.Sizeof(uint64(0)))

// wal record format: length | type | payload
type WALWriter struct {
	offset       int
	blockCounter int
	blocks       [][]byte
}

func NewWALWriter() *WALWriter {
	return &WALWriter{
		offset:       0,
		blockCounter: 0,
		blocks:       make([][]byte, 0),
	}
}

func (m *WALWriter) AddRecord(data []byte) error {
	var index int
	m.blocks = append(m.blocks, make([]byte, logBlockSize))
	begin := true
	for index < len(data) {
		leftover := logBlockSize - m.offset
		if leftover < headerSize {
			if leftover > 0 {
				for i := 0; i < leftover; i++ {
					m.blocks[m.blockCounter][m.offset+i] = 0x00
				}
			}
			m.blocks = append(m.blocks, make([]byte, logBlockSize))
			m.blockCounter++
			m.offset = 0
		}

		avail := logBlockSize - m.offset - headerSize
		left := len(data) - index
		fragmentLen := min(avail, left)
		end := left == fragmentLen
		var rTyp recordType
		if begin && end {
			rTyp = recordTypeFull
		} else if begin && !end {
			rTyp = recordTypeFirst
		} else if !begin && !end {
			rTyp = recordTypeMiddle
		} else if !begin && end {
			rTyp = recordTyepLast
		} else {
			//
		}

		bytes, err := formatRecord(rTyp, data[index:index+fragmentLen])
		if err != nil {
			return err
		}
		m.blocks[m.blockCounter] = append(m.blocks[m.blockCounter], bytes...)
		index += fragmentLen
		m.offset += len(bytes)
		begin = false
	}
	return nil
}

func formatRecord(rTyp recordType, data []byte) ([]byte, error) {
	buff := new(bytes.Buffer)
	if err := binary.Write(buff, binary.BigEndian, uint64(len(data))); err != nil {
		return nil, err
	}
	if err := binary.Write(buff, binary.BigEndian, uint64(rTyp)); err != nil {
		return nil, err
	}
	if err := binary.Write(buff, binary.BigEndian, data); err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type WALReader struct {
	offset       int
	blockCounter int
	blocks       [][]byte
}

func NewWALReader(blocks [][]byte) *WALReader {
	return &WALReader{
		offset:       0,
		blockCounter: 0,
		blocks:       blocks,
	}
}

func (r *WALReader) ReadRecord() []byte {
	var retBytes []byte
	for {
		leftover := logBlockSize - r.offset
		if leftover < headerSize {
			r.offset = 0
			r.blockCounter++
		}

		payloadLen := int(binary.BigEndian.Uint64(r.blocks[r.blockCounter][r.offset : r.offset+uint64Size]))
		rTyp := int(binary.BigEndian.Uint64(r.blocks[r.blockCounter][r.offset+uint64Size : r.offset+headerSize]))
		payloadStart := r.offset + headerSize

		retBytes = append(retBytes, r.blocks[r.blockCounter][payloadStart:payloadStart+payloadLen]...)
		r.offset += headerSize + payloadLen
		if rTyp == recordTypeFull || rTyp == recordTyepLast {
			break
		}
	}
	return retBytes
}

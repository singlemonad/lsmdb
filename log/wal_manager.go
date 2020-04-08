package log

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
	"unsafe"

	"github.com/singlemonad/lsmdb/util"
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
	logFileIndex int
	dir          string
	file         *util.File
}

// @dir: wal dir path
func NewWALWriter(dir string) (*WALWriter, error) {
	w := &WALWriter{
		offset: 0,
		dir:    dir,
	}

	fileList, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	fileNames := make([]string, 0)
	for _, file := range fileList {
		if !file.IsDir() {
			fileNames = append(fileNames, file.Name())
		}
	}

	if len(fileNames) != 0 {
		sort.Strings(fileNames)
		w.logFileIndex = parseWALLogFileName(fileNames[len(fileNames)-1])
	} else {
		w.logFileIndex = 0
	}

	file, err := util.OpenWriteOnlyFile(fmt.Sprintf("%s/%s", w.dir, formatWALLogFileName(w.logFileIndex)))
	if err != nil {
		return nil, err
	}
	w.file = file

	return w, nil
}

func (m *WALWriter) AddRecord(data []byte) error {
	var index int
	begin := true
	for index < len(data) {
		leftover := logBlockSize - m.offset
		if leftover < headerSize {
			if leftover > 0 {
				for i := 0; i < leftover; i++ {
					m.file.Write([]byte{0x00})
				}
			}
			if err := m.changeWALFile(); err != nil {
				return err
			}
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
		m.file.Write(bytes)
		index += fragmentLen
		m.offset += len(bytes)
		begin = false
	}
	return nil
}

func (m *WALWriter) changeWALFile() error {
	m.logFileIndex++
	file, err := util.OpenWriteOnlyFile(fmt.Sprintf("%s/%s", m.dir, formatWALLogFileName(m.logFileIndex)))
	if err != nil {
		return err
	}
	m.file = file
	return nil
}

// wal log file name format: wal-x
func parseWALLogFileName(name string) int {
	segs := strings.Split(name, "-")
	ret, _ := strconv.ParseInt(segs[1], 10, 64)
	return int(ret)
}

func formatWALLogFileName(index int) string {
	return fmt.Sprintf("wal-%d", index)
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
	dir          string
	offset       int
	logFileIndex int
	fileIndexs   []int
	file         *util.File
}

func NewWALReader(dir string) (*WALReader, error) {
	r := &WALReader{
		dir:          dir,
		offset:       0,
		logFileIndex: 0,
	}

	fileList, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, file := range fileList {
		if !file.IsDir() {
			r.fileIndexs = append(r.fileIndexs, parseWALLogFileName(file.Name()))
		}
	}
	if len(r.fileIndexs) == 0 {
		return nil, errors.New("no wal log file found.")
	}

	sort.Ints(r.fileIndexs)
	file, err := util.OpenReadOnlyFile(fmt.Sprintf("%s/%s", dir, formatWALLogFileName(r.fileIndexs[r.logFileIndex])))
	if err != nil {
		return nil, err
	}
	r.logFileIndex++
	r.file = file

	return r, nil
}

func (r *WALReader) ReadRecord() ([]byte, error) {
	var retBytes []byte
	for {
		leftover := logBlockSize - r.offset
		if leftover < headerSize {
			r.offset = 0
			if err := r.changeWALFile(); err != nil {
				return nil, err
			}
		}

		// read payloadLen
		payloadLenBuff := make([]byte, uint64Size)
		if _, err := r.file.Read(payloadLenBuff); err != nil {
			return nil, err
		}
		payloadLen := int(binary.BigEndian.Uint64(payloadLenBuff))

		// read record type
		rTypeBuff := make([]byte, uint64Size)
		if _, err := r.file.Read(rTypeBuff); err != nil {
			return nil, err
		}
		rTyp := int(binary.BigEndian.Uint64(rTypeBuff))

		// read payload
		payloadBuff := make([]byte, payloadLen)
		if _, err := r.file.Read(payloadBuff); err != nil {
			return nil, err
		}
		retBytes = append(retBytes, payloadBuff...)
		r.offset += headerSize + payloadLen

		if rTyp == recordTypeFull || rTyp == recordTyepLast {
			break
		}
	}
	return retBytes, nil
}

func (r *WALReader) changeWALFile() error {
	if r.logFileIndex >= len(r.fileIndexs) {
		return errors.New("no wal log file left")
	}

	file, err := util.OpenReadOnlyFile(fmt.Sprintf("%s/%s", r.dir, formatWALLogFileName(r.fileIndexs[r.logFileIndex])))
	if err != nil {
		return err
	}
	r.logFileIndex++
	r.file = file
	return nil
}

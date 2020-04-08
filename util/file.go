package util

import (
	"bytes"
	"encoding/binary"
	"os"
)

type File struct {
	*os.File
	name string
}

// OpenFile open the named file for write/read, if the file already exists, it will append
// If the file not exist, it will create
func OpenWriteOnlyFile(name string) (*File, error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &File{File: file, name: name}, nil
}

// OpenFile open the named file for read, if the file not exists, return error
func OpenReadOnlyFile(name string) (*File, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return &File{File: file, name: name}, nil
}

func (file *File) Remove() error {
	return os.Remove(file.name)
}

func (file *File) WriteBinary(data interface{}) (int, error) {
	buff := new(bytes.Buffer)
	if err := binary.Write(buff, binary.BigEndian, data); err != nil {
		return 0, err
	}
	return file.Write(buff.Bytes())
}

package util

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestFile(t *testing.T) {
	file, err := OpenWriteOnlyFile("test.txt")
	assert.Equal(t, nil, err)
	defer file.Close()
	//defer file.Remove()

	//s1 := []byte("123")
	n, err := file.WriteBinary(int32(1))
	assert.Equal(t, nil, err)
	assert.Equal(t, unsafe.Sizeof(1), n)

	//s2 := make([]byte, n)
	//file2, err := OpenReadOnlyFile("test.txt")
	//assert.Equal(t, nil, err)
	//n2, err := file2.Read(s2)
	//assert.Equal(t, nil, err)
	//assert.Equal(t, s1, s2)
	//assert.Equal(t, n, n2)

	//s3 := []byte(" write another data")
	//n, err = file.Write(s3)
	//assert.Equal(t, nil, err)
	//assert.Equal(t, len(s3), n)
}

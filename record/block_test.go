package record

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlockOperation(t *testing.T) {
	block := NewBlock()
	block.Append(NewRecord("name-1", FormatValueToBytes("1")))
	block.Append(NewRecord("name-2", FormatValueToBytes("2")))
	block.Append(NewRecord("name-3", FormatValueToBytes("3")))
	block.Append(NewRecord("name-4", FormatValueToBytes("4")))

	rec, err := block.FetchRecordByKey("name-1")
	assert.Equal(t, nil, err)
	assert.Equal(t, NewRecord("name-1", FormatValueToBytes("1")), rec)
	rec, err = block.FetchRecordByKey("name-2")
	assert.Equal(t, nil, err)
	assert.Equal(t, NewRecord("name-2", FormatValueToBytes("2")), rec)
	rec, err = block.FetchRecordByKey("name-3")
	assert.Equal(t, nil, err)
	assert.Equal(t, NewRecord("name-3", FormatValueToBytes("3")), rec)
	rec, err = block.FetchRecordByKey("name-4")
	assert.Equal(t, nil, err)
	assert.Equal(t, NewRecord("name-4", FormatValueToBytes("4")), rec)
}

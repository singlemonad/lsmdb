package record

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecordDirectory(t *testing.T) {
	dir := NewRecordDirectory()
	dir.Append(&RecordIndex{"name-1", 0, 31})
	dir.Append(&RecordIndex{"name-2", 31, 31})
	dir.Append(&RecordIndex{"name-3", 62, 31})
	dir.Append(&RecordIndex{"name-4", 92, 31})

	assert.Equal(t, &RecordIndex{"name-1", 0, 31}, dir.FindIndexByKey("name-1"))
	assert.Equal(t, &RecordIndex{"name-2", 31, 31}, dir.FindIndexByKey("name-2"))
	assert.Equal(t, &RecordIndex{"name-3", 62, 31}, dir.FindIndexByKey("name-3"))
	assert.Equal(t, &RecordIndex{"name-4", 92, 31}, dir.FindIndexByKey("name-4"))
}

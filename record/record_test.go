package record

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToPhysicalRecord(t *testing.T) {
	var rec *Record
	rec = NewRecord("name-1", FormatValueToBytes("1"))
	bytes, err := rec.Materialize()
	assert.Equal(t, nil, err)
	deRec := &Record{}
	deRec.DeMaterialize(bytes)
	assert.Equal(t, rec, deRec)
}

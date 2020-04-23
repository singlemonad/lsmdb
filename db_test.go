package lsmdb

import (
	"fmt"
	"github.com/bmizerany/assert"
	"github.com/singlemonad/lsmdb/record"
	"testing"
)

func TestLsmdbBasic(t *testing.T) {
	db := OpenLsmdb("/tmp/lsmdb")

	for i := 0; i < 99999; i++ {
		err := db.Put(fmt.Sprint("name-%d", i), record.FormatValueToBytes(fmt.Sprintf("quyang-%d", i)))
		assert.Equal(t, nil, err)
	}
}
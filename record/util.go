package record

import (
	"bytes"
	"encoding/binary"
	"reflect"
)

func FormatValueToBytes(value interface{}) []byte {
	buff := new(bytes.Buffer)

	switch reflect.TypeOf(value).Kind() {
	case reflect.Bool:

	case reflect.Int:
		binary.Write(buff, binary.BigEndian, int64(value.(int)))
	case reflect.Int8:
		binary.Write(buff, binary.BigEndian, int64(value.(int8)))
	case reflect.Int16:
		binary.Write(buff, binary.BigEndian, int64(value.(int16)))
	case reflect.Int32:
		binary.Write(buff, binary.BigEndian, int64(value.(int32)))
	case reflect.Int64:
		binary.Write(buff, binary.BigEndian, int64(value.(int64)))
	case reflect.Uint:
		binary.Write(buff, binary.BigEndian, uint64(value.(uint)))
	case reflect.Uint8:
		binary.Write(buff, binary.BigEndian, uint64(value.(uint8)))
	case reflect.Uint16:
		binary.Write(buff, binary.BigEndian, uint64(value.(uint16)))
	case reflect.Uint32:
		binary.Write(buff, binary.BigEndian, uint64(value.(uint32)))
	case reflect.Uint64:
		binary.Write(buff, binary.BigEndian, uint64(value.(uint64)))
	case reflect.String:
		binary.Write(buff, binary.BigEndian, []byte(value.(string)))
	default:
		panic("unsupport type.")
	}

	return buff.Bytes()
}

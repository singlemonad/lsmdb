package record

type RecordIndex struct {
	key    string
	offset uint64
	len    uint64
}

type RecordDirectory struct {
	entries []*RecordIndex
}

func NewRecordDirectory() *RecordDirectory {
	return &RecordDirectory{
		entries: make([]*RecordIndex, 0),
	}
}

// return index for insert in entries
func (dir *RecordDirectory) FindIndexByKey(key string) *RecordIndex {
	if len(dir.entries) == 0 {
		return nil
	}

	low := 0
	high := len(dir.entries) - 1
	for low <= high {
		middle := (low + high) / 2
		if dir.entries[middle].key < key {
			low = middle + 1
		} else if dir.entries[middle].key > key {
			high = middle - 1
		} else {
			return dir.entries[middle]
		}
	}
	return nil
}

func (dir *RecordDirectory) Append(index *RecordIndex) {
	dir.entries = append(dir.entries, index)
}

func (dir *RecordDirectory) FetchFirstKey() string {
	if len(dir.entries) == 0 {
		return ""
	}
	return dir.entries[0].key
}

func (dir *RecordDirectory) FetchLastKey() string {
	if len(dir.entries) == 0 {
		return ""
	}
	return dir.entries[len(dir.entries)-1].key
}

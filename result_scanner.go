package lbase

type Result struct {
	Row []byte
	ColumnFamily []byte
	Qualifier []byte
	Timestamp int32
	// Put or Delete
	Type int32
	Version int64
	Value []byte
}

type ResultScanner struct {
}

func (scanner *ResultScanner) Next() *Result {
	return nil
}

func (scanner *ResultScanner) NextN(nrows int) []*Result {
	return []*Result{}
}

func (scanner *ResultScanner) Close() {
}

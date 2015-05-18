package lbase

type ColumnDescriptor struct {
	// TODO: figure out the exact type.
	CompressionType int
	// Default block size of underlying lfile.
	BlockSize int
	// Name of the column family.
	Name string
	// Time to live of cell contents, in seconds.
	TimeToLive int
	// Minimum number of versions to keep.
	MinVersions int
	// Max number of versions to keep.
	MaxVersions int
	EnableBlockCache bool
	// TODO: implement this feature.
	CacheDataInL1 bool
	// If this column should be always placed in ram.
	InMemory bool
}

func NewColumnDescriptor(familyName string) *ColumnDescriptor {
	return &ColumnDescriptor{
		BlockSize: 2*1024*1024,
		Name: familyName,
		MinVersions: 1,
		MaxVersions: 1,
	}
}

package server

type RegionDbOptions struct {
}

type RegionDb struct {
	opts *RegionDbOptions
}

func NewRegionDb(opts *RegionDbOptions) *RegionDb {
	return nil
}

func (db *RegionDb) Put(record *RaftRecord) {
}

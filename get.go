package lbase

type Get struct {
	// Row key to get.
	Row []byte
	// Data to retrieve.
	FamilyMap [][]byte
	// The max number of versions to retrieve
	MaxVersions int
}

// Create a get operation for specific row.
func NewGet(row []byte) *Get {
	return &Get{
		Row: row,
		MaxVersions: 1,
	}
}

// Get all columns from specific family.
func (g *Get) AddFamily(family []byte) *Get {
	return g
}

func (g *Get) AddColumn(family, qualifier []byte) *Get {
	return g
}

func (g *Get) GetMaxVersions() int {
	return g.MaxVersions
}

func (g *Get) SetMaxVersions(num int) {
	g.MaxVersions = num
}

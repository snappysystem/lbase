package lbase

type Scan struct {
	StartRow []byte
	StopRow []byte
	// Data to retrieve.
	FamilyMap [][]byte
	// The max number of versions to retrieve
	MaxVersions int
}

// Create a scan operation for specific row.
func NewScan(startRow, stopRow []byte) *Scan {
	return &Scan{
		StartRow: startRow,
		StopRow: stopRow,
		MaxVersions: 1,
	}
}

// Get all columns from specific family.
func (g *Scan) AddFamily(family []byte) *Scan {
	return g
}

func (g *Scan) AddColumn(family, qualifier []byte) *Scan {
	return g
}

func (g *Scan) GetMaxVersions() int {
	return g.MaxVersions
}

func (g *Scan) SetMaxVersions(num int) {
	g.MaxVersions = num
}

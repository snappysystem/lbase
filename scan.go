/*
Copyright (c) 2015, snappysystem
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
package lbase

type Scan struct {
	StartRow []byte
	StopRow  []byte
	// Data to retrieve.
	FamilyMap [][]byte
	// The max number of versions to retrieve
	MaxVersions int
}

// Create a scan operation for specific row.
func NewScan(startRow, stopRow []byte) *Scan {
	return &Scan{
		StartRow:    startRow,
		StopRow:     stopRow,
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

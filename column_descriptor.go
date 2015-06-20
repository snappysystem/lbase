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
	MaxVersions      int
	EnableBlockCache bool
	// TODO: implement this feature.
	CacheDataInL1 bool
	// If this column should be always placed in ram.
	InMemory bool
}

func NewColumnDescriptor(familyName string) *ColumnDescriptor {
	return &ColumnDescriptor{
		BlockSize:   2 * 1024 * 1024,
		Name:        familyName,
		MinVersions: 1,
		MaxVersions: 1,
	}
}

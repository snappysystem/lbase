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

type Mutation struct {
	Family, Qualifier, Value []byte
	Timestamp                int64
}

type Put struct {
	RowKey    []byte
	Mutations []*Mutation
}

func NewPut(row []byte) *Put {
	return &Put{
		RowKey: row,
	}
}

func (p *Put) Add(family, qualifier, value []byte) *Put {
	m := Mutation{
		Family:    family,
		Qualifier: qualifier,
		Value:     value,
	}

	p.Mutations = append(p.Mutations, &m)
	return p
}

func (p *Put) Add2(family, qualifier, value []byte, ts int64) *Put {
	m := Mutation{
		Family:    family,
		Qualifier: qualifier,
		Value:     value,
		Timestamp: ts,
	}

	p.Mutations = append(p.Mutations, &m)
	return p
}

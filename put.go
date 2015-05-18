package lbase

type Mutation struct {
	Family,Qualifier,Value []byte
	Timestamp int64
}

type Put struct {
	RowKey []byte
	Mutations []*Mutation
}

func NewPut(row []byte) *Put {
	return &Put{
		RowKey: row,
	}
}

func (p *Put) Add(family, qualifier, value []byte) *Put {
	m := Mutation{
		Family: family,
		Qualifier: qualifier,
		Value: value,
	}

	p.Mutations = append(p.Mutations, &m)
	return p
}

func (p *Put) Add2(family, qualifier, value []byte, ts int64) *Put {
	m := Mutation{
		Family: family,
		Qualifier: qualifier,
		Value: value,
		Timestamp: ts,
	}

	p.Mutations = append(p.Mutations, &m)
	return p
}

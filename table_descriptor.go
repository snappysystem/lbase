package lbase

import (
	"fmt"
)

const (
	SYSTEM_NAMESPACE = "lbase"
	DEFAULT_NAMESPACE = "default"
	NAMESPACE_DELIM = ":"
)

type TableName struct {
	namespace string
	qualifier string
}

func NewTableName(tableName string) TableName {
	return TableName{
		namespace: DEFAULT_NAMESPACE,
		qualifier: tableName,
	}
}

func (tn TableName) GetName() string {
	return fmt.Sprintf("%s%s%s", tn.namespace, NAMESPACE_DELIM, tn.qualifier)
}

type TableDescriptor struct {
	tableName TableName
	columnFamilies []*ColumnDescriptor
}

func NewTableDescriptor(tn TableName) *TableDescriptor {
	return &TableDescriptor{
		tableName: tn,
	}
}

func (t *TableDescriptor) AddFamily(col *ColumnDescriptor) {
	t.columnFamilies = append(t.columnFamilies, col)
}

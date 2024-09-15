package data

import (
	"database/sql/driver"
	"fmt"
)

const (
	PRIMARY_AUTOINCREMENT = 1
	DEFAULT_MAX_LENGTH    = 40
)

type GoSqlColumn struct {
	Name       string
	ColType    int
	ParserType int
	Length     int
	Spec2      int
	Hidden     bool
}

type GoSqlTable struct {
	Name    string
	Columns []GoSqlColumn
	LastId  int64
	Data    [][]driver.Value
}

func NewTable(name string, columns []GoSqlColumn) *GoSqlTable {
	return &GoSqlTable{name, columns, 0, [][]driver.Value{}}
}

func (t *GoSqlTable) FindColumn(name string) (int, error) {
	for ix, col := range t.Columns {
		if col.Name == name {
			return ix, nil
		}
	}
	return -1, fmt.Errorf("Did not find column with name %s", name)
}

var (
	Tables map[string]*GoSqlTable
)

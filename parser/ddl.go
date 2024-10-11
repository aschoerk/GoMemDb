package parser

import (
	. "database/sql/driver"
	"fmt"

	"github.com/aschoerk/go-sql-mem/data"
	. "github.com/aschoerk/go-sql-mem/data"
)

type GoSqlCreateTableRequest struct {
	data.BaseStatement
	ifExists int
	table    *GoSqlTable
}

type GoSqlCreateDatabaseRequest struct {
	data.BaseStatement
	ifExists int
	name     string
}

type GoSqlCreateSchemaRequest struct {
	data.BaseStatement
	ifExists int
	name     string
}

func (r *GoSqlCreateDatabaseRequest) Exec(args []Value) (Result, error) {
	panic("not implemented")
}

func (r *GoSqlCreateSchemaRequest) Exec(args []Value) (Result, error) {
	panic("not implemented")
}

func (r *GoSqlCreateTableRequest) Exec(args []Value) (Result, error) {
	if Tables == nil {
		Tables = make(map[string]Table)
	}
	_, exists := Tables[r.table.Name()]
	if exists {
		if r.ifExists == 1 {
			return GoSqlResult{-1, -1}, fmt.Errorf("table %s already exists", r.table.Name())
		}
	} else {
		Tables[r.table.Name()] = data.NewTable(r.table.Name(), r.table.Columns())
	}
	return &GoSqlResult{-1, 0}, nil
}

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
	name     GoSqlIdentifier
}

type GoSqlCreateSchemaRequest struct {
	data.BaseStatement
	ifExists int
	name     GoSqlIdentifier
}

func (r *GoSqlCreateDatabaseRequest) Exec(args []Value) (Result, error) {
	panic("not implemented")
}

func (r *GoSqlCreateSchemaRequest) Exec(args []Value) (Result, error) {
	panic("not implemented")
}

func (r *GoSqlCreateTableRequest) Exec(args []Value) (Result, error) {
	if r.table.SchemaName == data.DEFAULT_SCHEMA_NAME {
		r.table.SchemaName = r.Conn.CurrentSchema
	}
	// r.BaseStatement.Conn.CurrentSchema
	if Schemas[r.table.SchemaName] == nil {
		Schemas[r.table.SchemaName] = make(map[string]Table)
	}
	_, exists := Schemas[r.table.SchemaName][r.table.Name()]
	if exists {
		if r.ifExists == 1 {
			return GoSqlResult{-1, -1}, fmt.Errorf("tableExpr %s already exists", r.table.Name())
		}
	} else {
		Schemas[r.table.SchemaName][r.table.Name()] = r.table
	}
	return &GoSqlResult{-1, 0}, nil
}

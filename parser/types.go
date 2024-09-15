package parser

import (
	"database/sql/driver"
	"time"
)

type Ptr struct {
	ptr   interface{}
	token int
}

type SelectListEntry struct {
	Asterisk   bool
	expression *GoSqlTerm
	Alias      string
}

func NewSelectListEntry(asterisk bool, expression *GoSqlTerm, alias string) SelectListEntry {
	return SelectListEntry{asterisk, expression, alias}
}

type GoSqlConstantValue struct {
	Str       *string
	Fixed     *int
	Float     *float64
	Timestamp *time.Time
}

type GoSqlOrderBy struct {
	Name      driver.Value
	direction int
}

type GoSqlUpdateSpec struct {
	Name string
	term *GoSqlTerm
}

type StmtState int

const (
	Created StmtState = iota + 1
	Parsed
	Executing
	EndOfRows
	Closed
)

type GoSqlStatementBase struct {
	state StmtState
}

func (r *GoSqlStatementBase) NumInput() int {
	return 0
}

func (r *GoSqlStatementBase) Query(args []driver.Value) (driver.Rows, error) {
	panic("not implemented")
}

func (r *GoSqlStatementBase) Close() error {
	return nil
}

func NewGoSqlStatementBase() GoSqlStatementBase {
	return GoSqlStatementBase{Created}
}

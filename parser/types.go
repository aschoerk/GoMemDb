package parser

import (
	"database/sql/driver"
	"github.com/aschoerk/go-sql-mem/data"
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
	Name data.GoSqlIdentifier
	term *GoSqlTerm
}

type GoSqlTableReference struct {
	Id          data.GoSqlIdentifier
	Select      *GoSqlSelectRequest
	JoinedTable *GoSqlJoinedTable
	As          string
}

type GoSqlJoinedTable struct {
	JoinedTableLeft     *GoSqlJoinedTable
	JoinType            int
	TableReferenceRight *GoSqlTableReference
	condition           *GoSqlTerm
}

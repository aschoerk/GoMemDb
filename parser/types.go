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
	Id data.GoSqlIdentifier
	// nested by parentheses not supported yet
	Select *GoSqlSelectRequest
	// nested by parentheses not supported yet
	JoinedTable []*GoSqlJoinedTable
	Alias       string
}

// a slice of *GoSqlJoinedTable describes the From-Term
// if TableReferenceLeft is empty, then it is the right part of a JOIN
// if TableReferenceRight is empty, then it is a single TableReference separated by Comma from the others if there are any
// because of the recursion in TableReferenceLeft by Parentheses enclosed Selects, or JoinedTables each slice element can be
// a tree/wood of arbitrary depth.
type GoSqlJoinedTable struct {
	TableReferenceLeft  *GoSqlTableReference
	JoinType            int
	TableReferenceRight *GoSqlTableReference
	Condition           *GoSqlTerm
}

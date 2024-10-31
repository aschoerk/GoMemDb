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

type GoSqlAsIdentifier struct {
	Id data.GoSqlIdentifier
	As string
}

type GoSqlJoinSpec struct {
	JoinMode      int
	JoinedTable   GoSqlAsIdentifier
	JoinCondition *GoSqlTerm
}

type GoSqlFromSpec struct {
	Id        GoSqlAsIdentifier
	JoinSpecs []*GoSqlJoinSpec
}

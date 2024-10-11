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

package parser

import (
	"database/sql/driver"
	"time"

	"github.com/aschoerk/go-sql-mem/data"
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
	state      StmtState
	Connection *GoSqlConnData
	SnapShot   *data.SnapShot
}

func (r *GoSqlStatementBase) NumInput() int {
	return 0
}

func (r *GoSqlStatementBase) Query(args []driver.Value) (driver.Rows, error) {
	panic("not implemented")
}

func (r *GoSqlStatementBase) Exec(args []driver.Value) (driver.Result, error) {
	panic("not implemented")
}

func (r *GoSqlStatementBase) Close() error {
	r.state = Closed
	return nil
}

func (r *GoSqlStatementBase) GetSnapShot() *data.SnapShot {
	if r.Connection.Transaction.IsolationLevel == data.UNCOMMITTED_READ {
		return nil
	}
	if r.Connection.Transaction.IsolationLevel == data.COMMITTED_READ {
		return r.SnapShot
	} else {
		return r.Connection.Transaction.SnapShot
	}
}

func NewGoSqlStatementBase() GoSqlStatementBase {
	return GoSqlStatementBase{Created, nil, nil}
}

type GoSqlConnData struct {
	Number                int64
	Transaction           *data.Transaction
	DoAutoCommit          bool
	DefaultIsolationLevel data.TransactionIsolationLevel
}

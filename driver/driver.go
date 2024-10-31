package driver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"sync/atomic"

	"github.com/aschoerk/go-sql-mem/data"
	"github.com/aschoerk/go-sql-mem/parser"
)

// Implements the necessary Driver Interfaces

type GoSqlDriver struct {
	connectionNumber      atomic.Int64
	DefaultIsolationLevel data.TransactionIsolationLevel
}

type GoSqlConn struct {
	Data data.GoSqlConnData
}

func (d *GoSqlDriver) Open(s string) (driver.Conn, error) {
	return &GoSqlConn{data.GoSqlConnData{d.connectionNumber.Add(1), nil, true, d.DefaultIsolationLevel, "public"}}, nil
}

func (c *GoSqlConn) Begin() (driver.Tx, error) {
	c.Data.DoAutoCommit = false
	if c.Data.Transaction != nil {
		if c.Data.Transaction.State == data.STARTED || c.Data.Transaction.State == data.ROLLBACKONLY {
			return nil, fmt.Errorf("Transaction is already started on connection %d", c.Data.Number)
		}
	}
	data.InitTransaction(&c.Data)
	return c.Data.Transaction, nil
}

func (c *GoSqlConn) Prepare(query string) (driver.Stmt, error) {

	parseResult, res := parser.Parse(query)
	stmt := parseResult.(data.StatementInterface)
	stmt.BaseData().Conn = &c.Data
	fmt.Printf("lval: %s, res: %d", reflect.TypeOf(parseResult), res)
	return parseResult, nil
}

func (c *GoSqlConn) Close() error {
	if c.Data.Transaction != nil {
		if c.Data.Transaction.State == data.STARTED || c.Data.Transaction.State == data.ROLLBACKONLY {
			return data.EndTransaction(&c.Data, data.ROLLEDBACK)
		}
	}
	return nil
}

func NewDriver() *GoSqlDriver {

	return &GoSqlDriver{atomic.Int64{}, data.COMMITTED_READ}
}

func init() {
	sql.Register("GoSql", NewDriver())
}

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
	data data.GoSqlConnData
}

func (d *GoSqlDriver) Open(s string) (driver.Conn, error) {
	return &GoSqlConn{data.GoSqlConnData{d.connectionNumber.Add(1), nil, true, d.DefaultIsolationLevel}}, nil
}

func (c *GoSqlConn) Begin() (driver.Tx, error) {
	c.data.DoAutoCommit = false
	if c.data.Transaction != nil {
		if c.data.Transaction.State == data.STARTED || c.data.Transaction.State == data.ROLLBACKONLY {
			return nil, fmt.Errorf("Transaction is already started on connection %d", c.data.Number)
		}
	}
	c.data.Transaction = data.InitTransaction(c.data.DefaultIsolationLevel)
	return c.data.Transaction, nil
}

func (c *GoSqlConn) Prepare(query string) (driver.Stmt, error) {

	parseResult, res := parser.Parse(query)
	stmt := parseResult.(data.StatementInterface)
	stmt.BaseData().Conn = &c.data
	fmt.Printf("lval: %s, res: %d", reflect.TypeOf(parseResult), res)
	return parseResult, nil
}

func (c *GoSqlConn) Close() error {
	if c.data.Transaction != nil {
		if c.data.Transaction.State == data.STARTED || c.data.Transaction.State == data.ROLLBACKONLY {
			return data.EndTransaction(c.data.Transaction, data.ROLLEDBACK)
		}
	}
	return nil
}

func init() {
	sql.Register("GoSql", &GoSqlDriver{atomic.Int64{}, data.COMMITTED_READ})
}

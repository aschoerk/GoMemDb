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
	data parser.GoSqlConnData
}

func (d *GoSqlDriver) Open(s string) (driver.Conn, error) {
	return &GoSqlConn{parser.GoSqlConnData{d.connectionNumber.Add(1), nil, true, d.DefaultIsolationLevel}}, nil
}

func (c *GoSqlConn) Begin() (driver.Tx, error) {
	c.data.DoAutoCommit = false
	if c.data.Transaction != nil {
		if c.data.Transaction.State == data.STARTED || c.data.Transaction.State == data.ROLLBACKONLY {
			return nil, fmt.Errorf("Transaction is already started on connection %d", c.data.Number)
		}
	}
	c.data.Transaction = data.InitTransaction()
	return c.data.Transaction, nil
}

func (c *GoSqlConn) Prepare(query string) (driver.Stmt, error) {
	if c.data.DoAutoCommit {
		if c.data.Transaction.State == data.STARTED || c.data.Transaction.State == data.ROLLBACKONLY {
			return nil, fmt.Errorf("Invalid Transaction state during autocommit of connection %d", c.data.Number)
		}
	} else if c.data.Transaction.State == data.ROLLEDBACK || c.data.Transaction.State == data.COMMITTED {
		return nil, fmt.Errorf("Unable to prepare on connection %d statement %s without Transaction", c.data.Number, query)
	}
	if c.data.Transaction.State != data.STARTED && c.data.Transaction.State != data.ROLLBACKONLY {
		t, err := data.StartTransaction(c.data.Transaction)
		if err != nil {
			return nil, err
		}
		c.data.Transaction = t
	}
	parseResult, res := parser.Parse(query)
	stmt := parseResult.(*parser.GoSqlStatementBase)
	stmt.Connection = &c.data
	if c.data.Transaction.IsolationLevel == data.COMMITTED_READ {
		s, err := data.GetSnapShot(c.data.Transaction.Xid)
		if err != nil {
			return nil, err
		} else {
			stmt.SnapShot = s

		}
	}
	fmt.Printf("lval: %s, res: %d", reflect.TypeOf(parseResult), res)
	return parseResult, nil
}

func (c *GoSqlConn) Close() error {
	if c.data.Transaction.State == data.STARTED || c.data.Transaction.State == data.ROLLBACKONLY {
		return data.EndTransaction(c.data.Transaction, data.ROLLEDBACK)
	}
	return nil
}

func init() {
	sql.Register("GoSql", &GoSqlDriver{atomic.Int64{}, data.COMMITTED_READ})
}

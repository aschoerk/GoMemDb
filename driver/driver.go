package driver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"

	"github.com/aschoerk/gosqlengine/parser"
)

// Implements the necessary Driver Interfaces

type GoSqlDriver struct {
}

func (d *GoSqlDriver) Open(s string) (driver.Conn, error) {

	return &GoSqlConn{}, nil
}

type GoSqlConn struct {
}

func (c *GoSqlConn) Begin() (driver.Tx, error) {
	panic("deprecated, not implemented")
}

func (c *GoSqlConn) Prepare(query string) (driver.Stmt, error) {
	parseResult, res := parser.Parse(query)
	fmt.Printf("lval: %s, res: %d", reflect.TypeOf(parseResult), res)
	return parseResult, nil
}

func (c *GoSqlConn) Close() error {
	return nil
}

func init() {
	sql.Register("GoSql", &GoSqlDriver{})
}

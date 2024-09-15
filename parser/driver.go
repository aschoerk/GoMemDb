package parser

import (
	"database/sql/driver"
	. "database/sql/driver"
	"fmt"
	"reflect"
)

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
	parseResult, res := Parse(query)
	fmt.Printf("lval: %s, res: %d", reflect.TypeOf(parseResult), res)
	return &GoSqlStmt{parseResult}, nil
}

func (c *GoSqlConn) Close() error {
	return nil
}

type GoSqlStmt struct {
	parseResult GoSqlStatement
}

func (c *GoSqlStmt) Close() error {
	return nil
}

func (c *GoSqlStmt) NumInput() int {
	return c.parseResult.NumInput()
}

func (c *GoSqlStmt) Exec(args []Value) (Result, error) {
	return c.parseResult.Exec(args)
}

func (c *GoSqlStmt) Query(args []Value) (Rows, error) {
	return c.parseResult.(*GoSqlSelectRequest).Query(args)
}

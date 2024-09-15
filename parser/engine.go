package parser

import (
	. "database/sql/driver"
)

// parse results

type GoSqlStatement interface {
	NumInput() int
	Exec(args []Value) (Result, error)
	Query(args []Value) (Rows, error)
}

type GoSqlResult struct {
	lastInsertId int64
	rowsAffected int64
}

func (r GoSqlResult) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r GoSqlResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

package driver

import "database/sql/driver"

type queryType struct {
	SQL string `json:"sql"`
}

type statmentInfo struct {
	ConnectionId string `json: "connectionId"`
	StatementId  string `json: "statementId"`
	NumInput     int    `json: "numInput"`
}

type ExecResult struct {
	LastInsertedId       int64 `json: "lastInsertId"`
	LastInsertedIdError  error `json: "lastInsertIdError"`
	NumRowsAffected      int64 `json: "rowsAffected"`
	NumRowsAffectedError error `json: "rowsAffectedError"`
}

func (r ExecResult) LastInsertId() (int64, error) {
	return r.LastInsertedId, r.LastInsertedIdError
}

func (r ExecResult) RowsAffected() (int64, error) {
	return r.NumRowsAffected, r.NumRowsAffectedError
}

type RowsResult struct {
	Names  []string         `json: "names"`
	Types  []int            `json: "types"`
	Values [][]driver.Value `json: "values"`
}

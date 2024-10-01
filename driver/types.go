package driver

import "database/sql/driver"

type queryType struct {
	SQL string `json:"sql"`
}

type statmentInfo struct {
	connectionId string `json: "connectionId"`
	statementId  string `json: "statementId"`
	numInput     int    `json: "numInput"`
}

type RowsResult struct {
	Names  []string         `json: "names"`
	Types  []int            `json: "types"`
	Values [][]driver.Value `json: "values"`
}

package driver

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"io"
	"log"
	"net/http"
)

func init() {
	sql.Register("GoSqlRest", &GoSqlRestDriver{})
}

// Implements the necessary Driver Interfaces

type GoSqlRestDriver struct {
}

type ConnectionInfo struct {
	ConnectionID string `json:"connectionId"`
	StatementID  string `json:"statementd"`
}

func (d *GoSqlRestDriver) Open(s string) (driver.Conn, error) {
	req, err := http.NewRequest("POST", s+"/connections", nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Add("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	var connectionInfo ConnectionInfo
	json.NewDecoder(resp.Body).Decode(&connectionInfo)
	return &GoSqlRestConn{s, connectionInfo.ConnectionID}, nil
}

type GoSqlRestConn struct {
	connectString string
	connectionId  string
}

type GoSqlRestStmt struct {
	connection  *GoSqlRestConn
	parseResult *statmentInfo
}

func (c *GoSqlRestConn) Begin() (driver.Tx, error) {
	panic("deprecated, not implemented")
}

func (c *GoSqlRestConn) Prepare(sql string) (driver.Stmt, error) {
	query := queryType{sql}
	// Create a buffer to hold the JSON data
	var buf bytes.Buffer

	// Create a new JSON encoder writing to the buffer
	encoder := json.NewEncoder(&buf)

	// Encode the data
	if err := encoder.Encode(&query); err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", c.connectString+"/connections/"+c.connectionId+"/statements", &buf)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Add("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	var parseResult statmentInfo
	json.NewDecoder(resp.Body).Decode(&parseResult)
	return &GoSqlRestStmt{c, &parseResult}, nil
}

func (c *GoSqlRestConn) Close() error {
	return nil
}

func (c *GoSqlRestStmt) Close() error {
	return nil
}

func (c *GoSqlRestStmt) NumInput() int {
	return c.parseResult.NumInput
}

func (c *GoSqlRestStmt) preparePostRequest(args []driver.Value, path string) (*http.Request, error) {
	var buf bytes.Buffer

	// Create a new JSON encoder writing to the buffer
	encoder := json.NewEncoder(&buf)

	if err := encoder.Encode(&args); err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", c.connection.connectString+"/connections/"+c.connection.connectionId+"/statements/"+c.parseResult.StatementId+"/"+path, &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	return req, nil
}

func (c *GoSqlRestStmt) Exec(args []driver.Value) (driver.Result, error) {
	req, err := c.preparePostRequest(args, "execute")
	if err != nil {
		return nil, err
	}
	// Create a buffer to hold the JSON data
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	var result ExecResult
	json.NewDecoder(resp.Body).Decode(&result)
	return result, nil
}

type GoSqlRestRows struct {
	rowsResult *RowsResult
	currentRow int
}

func (r *GoSqlRestRows) Close() error {
	return nil
}

func (r *GoSqlRestRows) Columns() []string {
	return r.rowsResult.Names
}

func (r *GoSqlRestRows) Next(data []driver.Value) error {
	if r.currentRow < len(r.rowsResult.Values)-1 {
		r.currentRow++
		row := convertArgs(r.rowsResult.Values[r.currentRow], &r.rowsResult.Types)
		for ix := range data {
			data[ix] = row[ix]
		}
		return nil
	}
	return io.EOF
}

func (c *GoSqlRestStmt) Query(args []driver.Value) (driver.Rows, error) {
	req, err := c.preparePostRequest(args, "rows")
	if err != nil {
		return nil, err
	}
	// Create a buffer to hold the JSON data
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	var result RowsResult
	json.NewDecoder(resp.Body).Decode(&result)
	return &GoSqlRestRows{&result, -1}, nil
}

package driver

import (
	"database/sql/driver"
	"encoding/json"
	"log"
	"net/http"
)

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
	json.Unmarshal(responseData, &responseObject)
	return &GoSqlRestConn{}, nil
}

type GoSqlRestConn struct {
}

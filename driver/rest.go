package driver

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/aschoerk/go-sql-mem/parser"
	"github.com/gorilla/mux"
)

type Server struct {
	connections map[string]driver.Conn
	statements  map[string]driver.Stmt
	router      *mux.Router
	driver      GoSqlDriver
}

func NewServer() *Server {
	return &Server{
		connections: make(map[string]driver.Conn),
		statements:  make(map[string]driver.Stmt),
		router:      mux.NewRouter(),
	}
}

func StartServer() {

	server := NewServer()

	r := server.router
	r.HandleFunc("/connections", server.createConnection).Methods("POST")
	r.HandleFunc("/connections/{connID}/statements", server.prepareStatement).Methods("POST")
	r.HandleFunc("/connections/{connID}", server.closeConnection).Methods("DELETE")
	r.HandleFunc("/connections/{connID}/statements/{stmtID}", server.closeStatement).Methods("DELETE")
	r.HandleFunc("/connections/{connID}/statements/{stmtID}/rows", server.queryStatement).Methods("POST")
	r.HandleFunc("/connections/{connID}/statements/{stmtID}/execute", server.executeStatement).Methods("POST")

	log.Fatal(http.ListenAndServe(":8080", r))
}

func (s *Server) createConnection(w http.ResponseWriter, r *http.Request) {

	connID := fmt.Sprintf("conn_%d", len(s.connections))
	conn, err := s.driver.Open(connID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.connections[connID] = conn

	json.NewEncoder(w).Encode(map[string]string{"connectionID": connID})
}

func extractConnectionID(w http.ResponseWriter, r *http.Request) (string, bool) {
	path := r.URL.Path
	start := strings.Index(path, "connections/")
	if start == -1 {
		http.Error(w, "Invalid path, connectionID not found", http.StatusBadRequest)
		return "", false // "connections/" not found
	}
	start += len("connections/")

	end := strings.Index(path[start:], "/")
	if end == -1 {
		return path[start:], true
	} else {
		return path[start : start+end], true
	}
}

func extractStatementID(w http.ResponseWriter, r *http.Request) (string, bool) {
	path := r.URL.Path
	start := strings.Index(path, "statements/")
	if start == -1 {
		http.Error(w, "Invalid path, StatementID not found", http.StatusBadRequest)
	}
	start += len("statements/")

	end := strings.Index(path[start:], "/")
	if end == -1 {
		return path[start:], true
	} else {
		return path[start : start+end], true
	}
}

func (s *Server) closeConnection(w http.ResponseWriter, r *http.Request) {
	connID, ok := extractConnectionID(w, r)
	if !ok {
		return
	}
	if s.connections[connID] == nil {
		http.Error(w, "Connection not found", http.StatusNotFound)
	}
	s.connections[connID].Close()
}

func (s *Server) prepareStatement(w http.ResponseWriter, r *http.Request) {
	connID, ok := extractConnectionID(w, r)
	var conn driver.Conn
	if ok {
		conn, ok = s.connections[connID]
	}
	if !ok {
		http.Error(w, "Connection not found", http.StatusNotFound)
		return
	}
	var query queryType
	if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	stmt, err := conn.Prepare(query.SQL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	numInput := stmt.NumInput()

	stmtID := fmt.Sprintf("stmt_%d", len(s.statements))
	s.statements[connID+"|"+stmtID] = stmt

	result := statmentInfo{connID, stmtID, numInput}

	json.NewEncoder(w).Encode(&result)
}

func (s *Server) evaluateRequest(w http.ResponseWriter, r *http.Request) (driver.Stmt, []driver.Value, bool) {
	connID, ok := extractConnectionID(w, r)
	if !ok {
		http.Error(w, "Invalid path, connectionID not found", http.StatusBadRequest)
		return nil, nil, false
	}
	stmtID, ok := extractStatementID(w, r)
	if !ok {
		http.Error(w, "Invalid path, statementID not found", http.StatusBadRequest)
		return nil, nil, false
	}

	stmt, ok := s.statements[connID+"|"+stmtID]
	if !ok {
		http.Error(w, "Statement not found", http.StatusNotFound)
		return nil, nil, false
	}

	args := []driver.Value{}
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil, nil, false
	}
	return stmt, args, true
}

func (s *Server) executeStatement(w http.ResponseWriter, r *http.Request) {
	stmt, args, ok := s.evaluateRequest(w, r)
	if !ok {
		return
	}
	result, err := stmt.Exec(args)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(result)
}

func (s *Server) closeStatement(w http.ResponseWriter, r *http.Request) {
	stmt, _, ok := s.evaluateRequest(w, r)
	if !ok {
		return
	}
	err := stmt.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) queryStatement(w http.ResponseWriter, r *http.Request) {
	stmt, args, err := s.evaluateRequest(w, r)
	if !err {
		return
	}

	rows, queryError := stmt.(*parser.GoSqlSelectRequest).Query(args)
	if queryError != nil {
		http.Error(w, queryError.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var result RowsResult

	tmpRows := rows.(*parser.GoSqlRows)
	if tmpRows.ResultTable() != nil {
		types := []int{}
		for _, col := range tmpRows.ResultTable().Columns {
			types = append(types, col.ColType)
		}
		result = RowsResult{tmpRows.Columns(), types, tmpRows.ResultTable().Data}
	}

	json.NewEncoder(w).Encode(result)
}

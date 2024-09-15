package tests

import (
	"database/sql"
	"testing"
	"time"
)

func TestSQLUpdates(t *testing.T) {
	// Open a test database connection
	db, err := sql.Open("GoSql", "memory")
	if err != nil {
		t.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`
		CREATE TABLE test_table (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			varchar_col VARCHAR(50),
			int_col INTEGER,
			float_col FLOAT,
			bool_col BOOLEAN,
			timestamp_col TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Error creating test table: %v", err)
	}

	// Insert initial data
	initialTime := time.Now().Round(time.Second)
	_, err = db.Exec(`
		INSERT INTO test_table (varchar_col, int_col, float_col, bool_col, timestamp_col)
		VALUES (?, ?, ?, ?, ?)
	`, "initial", 0, 0.0, false, initialTime)
	if err != nil {
		t.Fatalf("Error inserting initial data: %v", err)
	}

	// Update values
	newTime := time.Now().Round(time.Second)
	_, err = db.Exec(`
		UPDATE test_table
		SET varchar_col = ?,
			int_col = ?,
			float_col = ?,
			bool_col = ?,
			timestamp_col = ?
		WHERE id = 1
	`, "updated", 42, 3.14, true, newTime)
	if err != nil {
		t.Fatalf("Error updating data: %v", err)
	}

	// Verify updates
	var (
		varcharCol   string
		intCol       int
		floatCol     float64
		boolCol      bool
		timestampCol time.Time
	)
	err = db.QueryRow("SELECT varchar_col, int_col, float_col, bool_col, timestamp_col FROM test_table WHERE id = 1").
		Scan(&varcharCol, &intCol, &floatCol, &boolCol, &timestampCol)
	if err != nil {
		t.Fatalf("Error querying updated data: %v", err)
	}

	// Check updated values
	if varcharCol != "updated" {
		t.Errorf("Expected varchar_col to be 'updated', got '%s'", varcharCol)
	}
	if intCol != 42 {
		t.Errorf("Expected int_col to be 42, got %d", intCol)
	}
	if floatCol != 3.14 {
		t.Errorf("Expected float_col to be 3.14, got %f", floatCol)
	}
	if !boolCol {
		t.Errorf("Expected bool_col to be true, got false")
	}
	if !timestampCol.Equal(newTime) {
		t.Errorf("Expected timestamp_col to be %v, got %v", newTime, timestampCol)
	}
}

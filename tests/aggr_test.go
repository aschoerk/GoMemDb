package tests

import (
	"database/sql"
	"testing"
)

// TestAggregationFunctions tests various SQL aggregation functions
func TestAggregationFunctions(t *testing.T) {
	// Open a connection to the database
	db, err := sql.Open("GoSql", "http://localhost:8080")
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Create a test table and insert some data
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS test_data (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			value INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	_, err = db.Exec(`
		INSERT INTO test_data (value) VALUES (10), (20), (30), (40), (50)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Test cases for different aggregation functions
	testCases := []struct {
		name     string
		query    string
		expected int
	}{
		{"COUNT", "SELECT COUNT(value) FROM test_data", 5},
		{"COUNT", "SELECT COUNT(DISTINCT value) FROM test_data", 5},
		{"COUNT", "SELECT COUNT(*) FROM test_data", 5},
		{"SUM", "SELECT SUM(value) FROM test_data", 150},
		{"AVG", "SELECT AVG(value) FROM test_data", 30},
		{"MAX", "SELECT MAX(value) FROM test_data", 50},
		{"MIN", "SELECT MIN(value) FROM test_data", 10},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var result int
			err := db.QueryRow(tc.query).Scan(&result)
			if err != nil {
				t.Fatalf("Failed to execute %s query: %v", tc.name, err)
			}

			if result != tc.expected {
				t.Errorf("Expected %s to be %d, but got %d", tc.name, tc.expected, result)
			}
		})
	}

	// Clean up: drop the test table
	_, err = db.Exec("DROP TABLE test_data")
	if err != nil {
		t.Fatalf("Failed to drop test table: %v", err)
	}
}

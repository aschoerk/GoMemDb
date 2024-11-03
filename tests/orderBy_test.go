package tests

import (
	"database/sql"
	"testing"
)

// TestOrderBy tests various ORDER BY scenarios
func TestOrderBy(t *testing.T) {
	// Connect to the database
	db, err := sql.Open("GoSql", "http://localhost:8080")
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Set up test data
	setupTestData(t, db)

	testCases := []struct {
		name     string
		query    string
		expected []string
	}{
		{
			name:     "Order by id descending",
			query:    "SELECT name FROM users ORDER BY id DESC",
			expected: []string{"Alice", "Charlie", "Alice", "Bob"},
		},
		{
			name:     "Order by number ascending",
			query:    "SELECT name FROM users ORDER BY 1 DESC",
			expected: []string{"Charlie", "Bob", "Alice", "Alice"},
		},
		{
			name:     "Order sum of id and name by number ascending",
			query:    "SELECT '' + id + name FROM users ORDER BY 1 DESC",
			expected: []string{"4Alice", "3Charlie", "2Alice", "1Bob"},
		},
		{
			name:     "Order sum of id and name by number ascending",
			query:    "SELECT '' + id + name FROM users ORDER BY name DESC, id ASC",
			expected: []string{"3Charlie", "1Bob", "2Alice", "4Alice"},
		},
		{
			name:     "Order sum of id and name by number ascending",
			query:    "SELECT '' + id + name FROM users ORDER BY name ASC, id DESC",
			expected: []string{"4Alice", "2Alice", "1Bob", "3Charlie"},
		},
		{
			name:     "Order by fieldname descending",
			query:    "SELECT name FROM users ORDER BY name DESC",
			expected: []string{"Charlie", "Bob", "Alice", "Alice"},
		},
		{
			name:     "Order by alias",
			query:    "SELECT name AS user_name FROM users ORDER BY user_name",
			expected: []string{"Alice", "Alice", "Bob", "Charlie"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer catchPanic(t)
			rows, err := db.Query(tc.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			defer rows.Close()

			var results []string
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err != nil {
					t.Fatalf("Failed to scan row: %v", err)
				}
				results = append(results, name)
			}

			if err := rows.Err(); err != nil {
				t.Fatalf("Error iterating over rows: %v", err)
			}

			if !compareSlices(results, tc.expected) {
				t.Errorf("Expected %v, got %v", tc.expected, results)
			}
		})
	}
}

func setupTestData(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        )`)

	if err != nil {
		t.Fatalf("Failed to set up test data: %v", err)
	}
	_, err = db.Exec(
		"INSERT INTO users (name) VALUES ('Bob'), ('Alice'), ('Charlie'), ('Alice')",
	)

	if err != nil {
		t.Fatalf("Failed to set up test data: %v", err)
	}
}

func compareSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

package tests

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/aschoerk/go-sql-mem/driver"
)

func TestSQLDriver(t *testing.T) {
	db, err := sql.Open("GoSqlRest", "http://localhost:8080")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bool_col BOOLEAN,
            int_col INTEGER,
            float_col FLOAT,
            string_col VARCHAR(255),
            timestamp_col TIMESTAMP
        )
    `)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Test cases
	testCases := []struct {
		name        string
		insertQuery string
		insertArgs  []interface{}
		selectQuery string
		selectArgs  []interface{}
		expected    interface{}
	}{
		{
			name:        "Insert and select integer and string",
			insertQuery: "INSERT INTO test_table (int_col, string_col) VALUES (?,?)",
			insertArgs:  []interface{}{43.0, "test string 43"},
			selectQuery: "SELECT string_col FROM test_table WHERE (int_col = ? - 2) AND string_col = ?",
			selectArgs:  []interface{}{45, "test string 43"},
			expected:    "test string 43",
		}, {
			name:        "Insert and select integer",
			insertQuery: "INSERT INTO test_table (int_col) VALUES (?)",
			insertArgs:  []interface{}{42},
			selectQuery: "SELECT int_col FROM test_table WHERE int_col = ?",
			selectArgs:  []interface{}{42},
			expected:    42,
		}, {
			name:        "Insert and select boolean true",
			insertQuery: "INSERT INTO test_table (bool_col) VALUES (?)",
			insertArgs:  []interface{}{true},
			selectQuery: "SELECT bool_col FROM test_table WHERE bool_col = ?",
			selectArgs:  []interface{}{true},
			expected:    true,
		},
		{
			name:        "Insert and select boolean false",
			insertQuery: "INSERT INTO test_table (bool_col) VALUES (?)",
			insertArgs:  []interface{}{false},
			selectQuery: "SELECT bool_col FROM test_table WHERE bool_col = ?",
			selectArgs:  []interface{}{false},
			expected:    false,
		},

		{
			name:        "Insert and select float",
			insertQuery: "INSERT INTO test_table (float_col) VALUES (?)",
			insertArgs:  []interface{}{3.14},
			selectQuery: "SELECT float_col FROM test_table WHERE float_col > ?",
			selectArgs:  []interface{}{3.0},
			expected:    3.14,
		}, {
			name:        "Insert and concat string",
			insertQuery: "INSERT INTO test_table (int_col, string_col) VALUES (?,?)",
			insertArgs:  []interface{}{42.0, "test string"},
			selectQuery: "SELECT string_col + ' concat' FROM test_table WHERE int_col = 42 AND string_col LIKE ?",
			selectArgs:  []interface{}{"%test%"},
			expected:    "test string concat",
		},
		{
			name:        "Insert and select timestamp",
			insertQuery: "INSERT INTO test_table (timestamp_col) VALUES (?)",
			insertArgs:  []interface{}{time.Now()},
			selectQuery: "SELECT timestamp_col FROM test_table WHERE timestamp_col > ?",
			selectArgs:  []interface{}{time.Now().Add(-1 * time.Hour)},
			expected:    time.Now(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Insert data
			_, err := db.Exec(tc.insertQuery, tc.insertArgs...)
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}

			// Query data
			var result interface{}
			err = db.QueryRow(tc.selectQuery, tc.selectArgs...).Scan(&result)
			if err != nil {
				t.Fatalf("Failed to query data: %v", err)
			}

			// Compare result
			switch expected := tc.expected.(type) {
			case bool:
				if result.(bool) != expected {
					t.Errorf("Expected %v, got %v", expected, result)
				}
			case int:
				if result.(int64) != int64(expected) {
					t.Errorf("Expected %v, got %v", expected, result)
				}
			case float64:
				if result.(float64) != expected {
					t.Errorf("Expected %v, got %v", expected, result)
				}
			case string:
				if result.(string) != expected {
					t.Errorf("Expected %v, got %v", expected, result)
				}
			case time.Time:
				resultTime := result.(time.Time)
				if resultTime.Sub(expected) > time.Second {
					t.Errorf("Expected %v, got %v", expected, resultTime)
				}
			default:
				t.Errorf("Unexpected type for expected value")
			}
		})
	}
}

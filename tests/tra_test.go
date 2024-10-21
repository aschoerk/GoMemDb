package tests

import (
	"context"
	"database/sql"
	"testing"
)

func TestDatabaseTransactions(t *testing.T) {
	// Connect to the test database
	db, err := sql.Open("GoSql", "http://localhost:8080")
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Ensure the table exists
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS tra_test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT
        )
    `)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Test transaction commit
	t.Run("Transaction Commit", func(t *testing.T) {
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		_, err = tx.Exec("INSERT INTO tra_test_table (value) VALUES (?)", "test_value")
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to insert data: %v", err)
		}

		err = tx.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		// Verify the data was inserted
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM tra_test_table WHERE value = ?", "test_value").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query data: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	})

	// Test transaction rollback
	t.Run("Transaction Rollback", func(t *testing.T) {
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		_, err = tx.Exec("INSERT INTO tra_test_table (value) VALUES (?)", "rollback_value")
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to insert data: %v", err)
		}

		err = tx.Rollback()
		if err != nil {
			t.Fatalf("Failed to rollback transaction: %v", err)
		}

		// Verify the data was not inserted
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM tra_test_table WHERE value = ?", "rollback_value").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query data: %v", err)
		}
		if count != 0 {
			t.Errorf("Expected 0 rows, got %d", count)
		}
	})

	// Test MVCC (concurrent transactions)
	t.Run("MVCC", func(t *testing.T) {
		// Start two transactions
		tx1, _ := db.BeginTx(context.Background(), nil)
		tx2, _ := db.BeginTx(context.Background(), nil)

		// Transaction 1 inserts a row
		_, err := tx1.Exec("INSERT INTO tra_test_table (value) VALUES (?)", "mvcc_value")
		if err != nil {
			t.Fatalf("Failed to insert data in tx1: %v", err)
		}

		// Transaction 2 should not see the uncommitted data
		var count int
		err = tx2.QueryRow("SELECT COUNT(*) FROM tra_test_table WHERE value = ?", "mvcc_value").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query data in tx2: %v", err)
		}
		if count != 0 {
			t.Errorf("Transaction 2 should not see uncommitted data from Transaction 1")
		}

		// Commit transaction 1
		tx1.Commit()

		// Now transaction 2 should see the data
		err = tx2.QueryRow("SELECT COUNT(*) FROM tra_test_table WHERE value = ?", "mvcc_value").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query data in tx2 after tx1 commit: %v", err)
		}
		if count != 1 {
			t.Errorf("Transaction 2 should see committed data from Transaction 1")
		}

		tx2.Commit()
	})
}

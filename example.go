package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/aschoerk/gosqlengine/driver"
	_ "github.com/aschoerk/gosqlengine/driver"
)

func main() {
	driver.StartServer()
	// Open the database
	// db, err := sql.Open("sqlite3", "./example.db")
	db, err := sql.Open("GoSql", "memory")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        age INTEGER
    )`)
	if err != nil {
		log.Fatal(err)
	}

	// Insert a user
	stmt, err := db.Prepare("INSERT INTO users (name, age) VALUES (?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	_, err = stmt.Exec("Alice", 30)
	if err != nil {
		log.Fatal(err)
	}
	result, err2 := stmt.Exec("Hans", 31)
	if err2 != nil {
		log.Fatal(err)
	}
	id, _ := result.LastInsertId()
	fmt.Printf("Inserted user with ID: %d\n", id)

	// Query for a user
	var name string
	var age int
	err = db.QueryRow("SELECT name, age FROM users WHERE id = ?", id).Scan(&name, &age)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("User: %s, Age: %d\n", name, age)

	// Update a user
	_, err = db.Exec("UPDATE users SET age = ? WHERE id = ?", 31, id)
	if err != nil {
		log.Fatal(err)
	}

	// Query all users
	rows, err := db.Query("SELECT id, name, age FROM users")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println("All users:")
	for rows.Next() {
		var id int
		var name string
		var age int
		err := rows.Scan(&id, &name, &age)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("ID: %d, Name: %s, Age: %d\n", id, name, age)
	}

	// Delete a user
	_, err = db.Exec("DELETE FROM users WHERE id = ?", id)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Deleted user with ID: %d\n", id)
}

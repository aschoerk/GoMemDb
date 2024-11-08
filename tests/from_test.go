package tests

import (
	"database/sql"
	"github.com/aschoerk/go-sql-mem/data"
	_ "github.com/aschoerk/go-sql-mem/driver"
	"github.com/aschoerk/go-sql-mem/parser"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test(t *testing.T) {
	db, err := sql.Open("GoSql", "http://localhost:8080")
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()
	for _, stmt := range initstmts {
		_, err = db.Exec(stmt)
	}

	tc := []struct {
		stmt     string
		errsNo   int
		tablesNo int
	}{
		{
			`SELECT b.title
					FROM books b`,
			0, 1,
		}, {
			`SELECT b.title, a.name AS author_name
					FROM books b
							 INNER JOIN authors a ON b.author_id = a.id`,
			0, 2,
		},
		{
			`SELECT b.title, a.name AS author_name
					FROM books b
							 INNER JOIN authors b ON b.author_id = a.id`,
			2, 0, // alias b is duplicate, a.id can not be found
		},
		{
			`SELECT b.title, p.name AS publisher_name
					FROM books b
					 JOIN book_publishers bp ON b.id = bp.book_id
					 JOIN publishers p ON bp.publisher_id = p.id`,
			0, 3,
		},
		{
			`SELECT a.name AS author_name, b.title
					FROM authors a
							 LEFT JOIN books b ON a.id = b.author_id`,
			0, 2,
		},
		{
			`SELECT b.title, a.name AS author_name
					FROM books b
							 RIGHT JOIN authors a ON b.author_id = a.id`,
			0, 2,
		},
		{
			`SELECT a.name AS author_name, p.name AS publisher_name
					FROM authors a
							 CROSS JOIN publishers p`,
			0, 2,
		},
		{
			`SELECT a1.name AS author, a2.name AS mentor
					FROM authors a1
							 LEFT JOIN authors a2 ON a1.id = a2.id + 1`,
			0, 2,
		},
		{
			`SELECT b.title, a.name AS author_name, p.name AS publisher_name, bp.contract_year
					FROM books b
							 JOIN authors a ON b.author_id = a.id
							 JOIN book_publishers bp ON b.id = bp.book_id
							 JOIN publishers p ON bp.publisher_id = p.id
					WHERE b.publication_year > 2000 AND bp.contract_year > b.publication_year`,
			0, 4,
		},
	}

	for _, tt := range tc {
		t.Run(tt.stmt, func(t *testing.T) {
			defer catchPanic(t)
			parser.YYDebug = 3
			parseResult, _ := parser.Parse(tt.stmt)
			r := parseResult.(*parser.GoSqlSelectRequest)
			r.BaseData().Conn = &data.GoSqlConnData{CurrentSchema: "public"}
			fromHandler := parser.GoSqlFromHandler{}
			errs := fromHandler.Init(r)

			assert.Equal(t, tt.errsNo, len(errs))
		})
	}

	// t.Run()
}

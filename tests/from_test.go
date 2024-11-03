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
		stmt   string
		errsNo int
	}{
		{
			`SELECT b.title, a.name AS author_name
					FROM books b
							 INNER JOIN authors a ON b.author_id = a.id`,
			0,
		},
		{
			`SELECT b.title, a.name AS author_name
					FROM books b
							 INNER JOIN authors b ON b.author_id = a.id`,
			2, // alias b is duplicate, a.id can not be found
		},
		{
			`SELECT b.title, p.name AS publisher_name
					FROM books b
					 JOIN book_publishers bp ON b.id = bp.book_id
					 JOIN publishers p ON bp.publisher_id = p.id`,
			0,
		},
		{
			`SELECT a.name AS author_name, b.title
					FROM authors a
							 LEFT JOIN books b ON a.id = b.author_id`,
			0,
		},
		{
			`SELECT b.title, a.name AS author_name
					FROM books b
							 RIGHT JOIN authors a ON b.author_id = a.id`,
			0,
		},
		{
			`SELECT a.name AS author_name, p.name AS publisher_name
					FROM authors a
							 CROSS JOIN publishers p`,
			0,
		},
		{
			`SELECT a1.name AS author, a2.name AS mentor
					FROM authors a1
							 LEFT JOIN authors a2 ON a1.id = a2.id + 1`,
			0,
		},
		{
			`SELECT b.title, a.name AS author_name, p.name AS publisher_name, bp.contract_year
					FROM books b
							 JOIN authors a ON b.author_id = a.id
							 JOIN book_publishers bp ON b.id = bp.book_id
							 JOIN publishers p ON bp.publisher_id = p.id
					WHERE b.publication_year > 2000 AND bp.contract_year > b.publication_year`,
			0,
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
			assert.Equal(t, len(errs), tt.errsNo)
		})
	}

	// t.Run()
}

var initstmts = []string{
	`CREATE TABLE authors (
                         id INTEGER,
                         name TEXT
	)`,
	`CREATE TABLE books (
                       id INTEGER,
                       title TEXT,
                       author_id INTEGER,
                       genre TEXT,
                       publication_year INTEGER
)`,
	`CREATE TABLE publishers (
                            id INTEGER,
                            name TEXT,
                            founded_year INTEGER
)`,
	`CREATE TABLE book_publishers (
                                 book_id INTEGER,
                                 publisher_id INTEGER,
                                 contract_year INTEGER
)`,
	`INSERT INTO authors (id, name) VALUES
                                   (1, 'Author One'),
                                   (2, 'Author Two'),
                                   (3, 'Author Three'),
                                   (4, 'Author Four'),
                                   (5, 'Author Five'),
                                   (6, 'Author Six'),
                                   (7, 'Author Seven'),
                                   (8, 'Author Eight'),
                                   (9, 'Author Nine'),
                                   (10, 'Author Ten')`,
	`INSERT INTO books (id, title, author_id, genre, publication_year) VALUES
                                                                      (1, 'Book One', 1, 'Fiction', 2000),
                                                                      (2, 'Book Two', 1, 'Non-fiction', 2005),
                                                                      (3, 'Book Three', 2, 'Science Fiction', 2010),
                                                                      (4, 'Book Four', 3, 'Mystery', 2015),
                                                                      (5, 'Book Five', 4, 'Romance', 1995),
                                                                      (6, 'Book Six', 5, 'Biography', 2020),
                                                                      (7, 'Book Seven', 6, 'Fiction', 2018),
                                                                      (8, 'Book Eight', 7, 'Non-fiction', 2021),
                                                                      (9, 'Book Nine', 8, 'Science Fiction', 2019),
                                                                      (10, 'Book Ten', 9, 'Mystery', 2017)`,
	`INSERT INTO publishers (id, name, founded_year) VALUES
                                                    (1, 'Publisher A', 1990),
                                                    (2, 'Publisher B', 1995),
                                                    (3, 'Publisher C', 2000),
                                                    (4, 'Publisher D', 2005),
                                                    (5, 'Publisher E', 2010)`,
	`INSERT INTO book_publishers (book_id, publisher_id, contract_year) VALUES
                                                                       (1, 1, 2000),
                                                                       (1, 2, 2001),
                                                                       (2, 1, 2005),
                                                                       (3, 3, 2010),
                                                                       (4, 4, 2015),
                                                                       (4, 1, 2016),
                                                                       (5, 2, 1995),
                                                                       (6, 3, 2020),
                                                                       (7, 2, 2018),
                                                                       (8, 5, 2021),
                                                                       (9, 1, 2019),
                                                                       (10, 4, 2017)`,
}

package tests

import "testing"

func catchPanic(t *testing.T) {
	if r := recover(); r != nil {
		t.Errorf("Test panicked: %v", r)
	}
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

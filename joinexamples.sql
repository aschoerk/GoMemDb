-- Create tables
CREATE TABLE authors (
                         id INTEGER PRIMARY KEY,
                         name TEXT NOT NULL
);

CREATE TABLE books (
                       id INTEGER PRIMARY KEY,
                       title TEXT NOT NULL,
                       author_id INTEGER,
                       genre TEXT,
                       publication_year INTEGER,
                       FOREIGN KEY (author_id) REFERENCES authors(id)
);

CREATE TABLE publishers (
                            id INTEGER PRIMARY KEY,
                            name TEXT NOT NULL,
                            founded_year INTEGER
);

CREATE TABLE book_publishers (
                                 book_id INTEGER,
                                 publisher_id INTEGER,
                                 contract_year INTEGER,
                                 PRIMARY KEY (book_id, publisher_id),
                                 FOREIGN KEY (book_id) REFERENCES books(id),
                                 FOREIGN KEY (publisher_id) REFERENCES publishers(id)
);

-- Insert data
INSERT INTO authors (id, name) VALUES
                                   (1, 'Author One'),
                                   (2, 'Author Two'),
                                   (3, 'Author Three'),
                                   (4, 'Author Four'),
                                   (5, 'Author Five'),
                                   (6, 'Author Six'),
                                   (7, 'Author Seven'),
                                   (8, 'Author Eight'),
                                   (9, 'Author Nine'),
                                   (10, 'Author Ten');

INSERT INTO books (id, title, author_id, genre, publication_year) VALUES
                                                                      (1, 'Book One', 1, 'Fiction', 2000),
                                                                      (2, 'Book Two', 1, 'Non-fiction', 2005),
                                                                      (3, 'Book Three', 2, 'Science Fiction', 2010),
                                                                      (4, 'Book Four', 3, 'Mystery', 2015),
                                                                      (5, 'Book Five', 4, 'Romance', 1995),
                                                                      (6, 'Book Six', 5, 'Biography', 2020),
                                                                      (7, 'Book Seven', 6, 'Fiction', 2018),
                                                                      (8, 'Book Eight', 7, 'Non-fiction', 2021),
                                                                      (9, 'Book Nine', 8, 'Science Fiction', 2019),
                                                                      (10, 'Book Ten', 9, 'Mystery', 2017);

INSERT INTO publishers (id, name, founded_year) VALUES
                                                    (1, 'Publisher A', 1990),
                                                    (2, 'Publisher B', 1995),
                                                    (3, 'Publisher C', 2000),
                                                    (4, 'Publisher D', 2005),
                                                    (5, 'Publisher E', 2010);

INSERT INTO book_publishers (book_id, publisher_id, contract_year) VALUES
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
                                                                       (10, 4, 2017);

-- Example SELECT queries to test various types of joins

-- Example 1: Inner join books with their authors
SELECT b.title, a.name AS author_name
FROM books b
         INNER JOIN authors a ON b.author_id = a.id;

-- Example 2: Inner join books with their publishers
SELECT b.title, p.name AS publisher_name
FROM books b
         JOIN book_publishers bp ON b.id = bp.book_id
         JOIN publishers p ON bp.publisher_id = p.id;

-- Example 3: Left join authors and their books (including authors without books)
SELECT a.name AS author_name, b.title
FROM authors a
         LEFT JOIN books b ON a.id = b.author_id;

-- Example 4: Right join books and authors (including books without authors)
SELECT b.title, a.name AS author_name
FROM books b
         RIGHT JOIN authors a ON b.author_id = a.id;

-- Example 5: Full outer join between authors and books
-- Note: H2 doesn't support FULL OUTER JOIN directly, so we simulate it
SELECT a.name AS author_name, b.title
FROM authors a
         LEFT JOIN books b ON a.id = b.author_id
UNION ALL
SELECT a.name AS author_name, b.title
FROM authors a
         RIGHT JOIN books b ON a.id = b.author_id
WHERE a.id IS NULL;

-- Example 6: Cross join between authors and publishers
SELECT a.name AS author_name, p.name AS publisher_name
FROM authors a
         CROSS JOIN publishers p;

-- Example 7: Self join on authors (assuming a mentor relationship)
SELECT a1.name AS author, a2.name AS mentor
FROM authors a1
         LEFT JOIN authors a2 ON a1.id = a2.id + 1;

-- Example 8: Complex join with multiple conditions
SELECT b.title, a.name AS author_name, p.name AS publisher_name, bp.contract_year
FROM books b
         JOIN authors a ON b.author_id = a.id
         JOIN book_publishers bp ON b.id = bp.book_id
         JOIN publishers p ON bp.publisher_id = p.id
WHERE b.publication_year > 2000 AND bp.contract_year > b.publication_year;

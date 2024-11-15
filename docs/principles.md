# Principles

## The provided functions 
The store implements the interfaces defined by database/sql/driver, offering objects such as connections, statements, and rows.

* Connections: At the connection level, you can initiate transactions, commit them, or roll them back. This provides a robust mechanism for managing transactional operations. To modify the state of the store or perform queries, you can create statements associated with a connection.
* Statements: SQL statements can be created and parameterized with simple values. These statements can then be executed or queried. 
   * Execution typically results in changes to the transactional state of the store. 
   * Queries: Queries generally (except "For Update") do not create locks, ensuring efficient data access. They may generate temporary tables used to compute results.
* Rows: Rows are supported by temporary tables created during query execution, enabling efficient result handling and processing.

## Implementation

The Statements are parsed and dependent on the type DDL (supports only CREATE TABLE yet) and DML (SELECT, INSERT, UPDATE, DELETE) the Grammar creates specific structures which are able to interpret the results during exec or query. Each internal representation of a statement contains a reference to the connection by which it was created. 

 

## Parsing
The parsing of queries is accomplished using a Go adaptation of Flex. While this adaptation is not yet freely available, it has been chosen over alternatives due to their limitations and bugs. To compile the project, users must build a version of fle[flexgo](https://github.com/pebbe/flexgo)xgo. Our project includes a slightly modified skeleton of flexgo, which is checked into the repository. For building the grammar parser, we utilize [goyacc](https://pkg.go.dev/modernc.org/goyacc?utm_source=godoc), a tool that generates parsers based on Yacc grammar specifications. This setup ensures robust query parsing capabilities within the project.
SQL expressions found in SELECT lists, WHERE clauses, GROUP BY clauses, and HAVING clauses are parsed into trees of GoSqlTerm objects. This structured representation allows for efficient processing and evaluation of SQL queries within the system.

## Stackmachine
The GoSqlTerm trees are transformed into commands executable by a stack machine. This machine operates on:
* A slice of driver.Value objects, representing the parameters.
* Two records that may have been directly read from a table or are intended to store the results of calculations.


## Internal Representation of the data  

## Simple Attributes
The system supports simple attributes such as int64, float64, string, and time.Time, corresponding to the SQL types INTEGER, FLOAT, VARCHAR, and TIMESTAMP, respectively. These are stored using driver.Value (as defined in database/sql/driver), which is essentially an interface{}.

## Tuples, Records
For data structuring, the system uses slices of driver.Value. Each slice is accompanied by metadata in the form of []GoSqlColumn, which defines the expected types of the content.

### Versioned Records
To support Multi-Version Concurrency Control (MVCC), records that require synchronization across multiple connections consist of a header and a slice of versioned entries. This structure allows each record to maintain multiple versions, facilitating concurrent access and ensuring data consistency without locking.

  
type VersionedTuple struct {
	id       int64
	mu       sync.Mutex
	Versions []TupleVersion
}

type TupleVersion struct {
	Data  []driver.Value
	xmin  int64
	xmax  int64
	flags int32
	cid   int32
}
```
* id is the table specific id of a record
* mu is used to support synch

## Tables
Above tuples are tables. All tables consist of slices of driver.Value objects, all structurally defined by a consistent slice of []GoSqlColumn.

### Transactional Tables

These tables are created using CREATE TABLE statements and support multi-version concurrency control (MVCC) for multi-user access. Currently, only the COMMITTED_READ isolation level is supported. Each tuple in a transactional table includes a hidden record ID, which is a unique int64 per table. Tuples are accessed via a red-black tree. See: [redblacktree](https://github.com/emirpasic/gods)
The access to a table is synchronized using a RWMutex. 

### Temporary Tables
During processing, temporary tables are created. In addition to metadata ([]GoSqlColumn), they primarily consist of a two-dimensional array containing tuples. A temporary table can only be accessed by one thread at a time. At the end of a transaction, all temporary tables created during that transaction are invalidated. In the future, this may evolve into a "group by" table, which could itself contain temporary tables as records.

 
## Transformations

CREATE TABLE
[]GoSqlColumn plus name --> Tables[name]

INSERT
* Statement -->
* attributenames - []string,

  values, recorddefs [][]GoSqlTerm ,

  parameters []Value 
* n * []value creating machines  
* VersionedTuples in Table

SELECT
Statement -->
# README #
A project allowing me to use go in a not so simple environment. The idea is to implement a simple inmemory database in multiple stages

# Principles

## Interface: Package database/sql
The data manipulation and querying will be conducted through the Go standard database/sql interface. A subset of SQL will be available for these operations. The level of compliance with the SQL standard will depend on the maturity of the current release, with improvements expected over time.

## Multi-User/Connection
The module offers two primary modes of operation: in-memory and server-based. Below are examples of how to use each mode, formatted in markdown with code snippets.
In-Memory Usage
To use the module entirely in-memory, you can open a database connection using the following code:

```go
db, err := sql.Open("GoSql", "memory")
```

This approach allows for fast data manipulation and querying without any persistence beyond the application's runtime.
Server-Based Usage
Alternatively, you can start a server to handle database operations. First, initiate the server with:

```go
driver.StartServer()
```

Once the server is running, you can access it via HTTP using:

```go
db, err := sql.Open("GoSqlRest", "http://localhost:8080")
```

This setup enables remote access to the database, allowing multiple clients to connect and interact with it over the network.


## In-Memory Storage
The data store is currently maintained in memory, meaning that all data is lost when the process ends. Future plans include implementing file storage to persist data changes, allowing the system to restore its state after a restart. However, this file storage will serve solely as a backup mechanism, and no search operations will be performed directly on the file storage.

##  


# Progression

* INSERT/SELECT on **single** tables existing of [][]interface{}
* UPDATE/DELETE
* Available Types: INTEGER (int64), FLOAT(float64), VARCHAR(string), TIMESTAMP(time.Time)
* Remote Access via Rest, plus database/sql/driver --> there is a second driver which can be used as client. Use server_test to start a Rest - Server 
** need to change xid assignment to only at moment of changes <-- StartTransaction does that now. 
** need to do locking using fields in Tuple, no extra Lockmanager-Lockstorage - xmax is used for that, if tra is in state isStarted, the record is locked
** need to keep track of changed records during transaction -> not necessary, use handling of postgres here
* Transactions plus MVCC plus Multiuser-Capability <-- done on module - level
** need to implement the sql-statements (BEGIN, COMMIT, ROLLBACK, SET AUTOCOMMIT, SET ROLLBACKONLY) to be able to control transactions via statements <<- started
* AGGREGATE FUNCTIONS
* AGGREGATE FUNCTIONS distinct_all
* GROUP BY (expressions, attributes) not aliases, not select list indexes
* HAVING
* LIMIT, OFFSET
* Subselects
* NULL handling
* JOINS
* SAVEPOINTs
* ORDER BY expressions
* Backing of Memory-Changes via persistent storage


### Next issues



### What is this repository for? ###

* Lightweight Go-InMemory-DBMS supporting "database/sql/driver"  
* 0.1

### How do I get set up? ###

* Build
** goyacc: go install golang.org/x/tools/cmd/goyacc@latest
** https://github.com/pebbe/flexgo.git, (.configure, make, make install - possibly necessary to use aclocal.m4)
** just
** just run - compiles everything and starts example.go 
* Start Server using driver.StartServer()
* Dependencies
* Database configuration
* tests are located in the tests-package
* no deployment in productive environments recommended yet

### Contribution guidelines ###

* contact me

### Who do I talk to? ###

* aschoerk@gmx.net

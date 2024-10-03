# README #
A project allowing me to use go in a not so simple environment. The idea is to implement a simple inmemory database in multiple stages
* INSERT/SELECT (only WHERE) on single tables existing of [][]interface{}
* creation of tempor
* UPDATE/DELETE
* Remote Access via Rest, plus database/sql/driver **** currently working on that 
* Transactions plus MVCC plus Multiuser-Capability
* Backing of Memory-Changes via persistent storage
* AGGREGATE FUNCTIONS
* GROUP BY
* HAVING
* NULL handling
* JOINS

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

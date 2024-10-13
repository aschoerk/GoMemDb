# README #
A project allowing me to use go in a not so simple environment. The idea is to implement a simple inmemory database in multiple stages
* INSERT/SELECT (only WHERE) on single tables existing of [][]interface{}
* creation of tempor
* UPDATE/DELETE
* Remote Access via Rest, plus database/sql/driver **** currently working on that 
** need to change xid assignment to only at moment of changes
** need to do locking using fields in Tuple, no extra Lockmanager-Lockstorage
** need to keep track of changed records during transaction
* Transactions plus MVCC plus Multiuser-Capability
* Backing of Memory-Changes via persistent storage
* AGGREGATE FUNCTIONS
* GROUP BY
* HAVING
* NULL handling
* JOINS

### Next issues

### Iterator Next

* xmin, xmax: entries in version header
* s.committed: committed (according to tra data) < s.xmax,  not running according to snapshot (not in s.running)
* s.xmax at time of creation of snapshot the next xid to be assigned to a transaction
* s.running: running transaction at time of snapshot creation (in s.running) != xid of current transaction
* s.rolledback rolledback, < s.xmax, not in running transactions at time of snapshot creation (not in s.running) 
* versions are ordered in time of creation, the last version is looked at first
** only one transaction can change a record at a certain time. 
** changes always occur in a serial manner. Changes to records can not overtake between transactions
* statement numbers are only relevant for running transactions, therefore for each change of a version the current statementnumber is stored in the header
* if r.xmin == r.xmax the statement number belongs to xmax change
* if r.xmax == 0: the cid belongs to xmin change it gets overwritten by xmax change. <-- inserts can not be distinguished by statement number if updates happen later. solution: inserts might vanish if selected for update

| Cond1            | Cond2           | Cond3            | Version Situation                                                    |                                                                                                              |
|------------------|-----------------|------------------|----------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| forChange\nfalse | forSelect false | -                | -                                                                    | illegal                                                                                                      |
|                  | for Select true | Uncommitted Read | -                                                                    | Snapshot is Iterator spec\nNext shows                                                                        |
|                  |                 |                  | xid assigned, xmin == xid, xmax == 0,                                | look at the statement number if >= to s.cid, go to previous version                                          |
|                  |                 |                  | xid assigned, xmin == xid, xmax != xid                               | illegal, other tra changed\nrecord not committed yet                                                         |
|                  |                 |                  | xid assigned, xmin == xid, xmax == xid                               | if r.cid >= s.cid                                                                                            |
|                  |                 |                  |                                                                      | - look at cid of previous version: valid for current xmin                                                    |
|                  |                 |                  |                                                                      | - if >= s.cid, look at previous version                                                                      |
|                  |                 |                  | xid assigned, xmin != xid, xmax == xid                               | if r.cid < s.cid: ignore record, record might have bin inserted before cursor started, nevertheless ignored. |
|                  |                 |                  |                                                                      | if r.cid >= s.cid: xmin must be committed tra take this version                                              |
|                  |                 |                  | xmin s.committed tra, xmax == 0, max xmin < s.xmax                   | take this version                                                                                            |
|                  |                 |                  | xmin s.committed tra, xmax s.committed tra < s.xmax                  | record deleted ignore this record                                                                            |
|                  |                 |                  | xmin s.committed tra, xmax s.committed tra < s.xmax, only for update | record was marked for update, no update happened use this version                                            |
|                  |                 |                  | xmin s.committed tra, xmax s.running tra                             | not deleted yet, take this version                                                                           |
|                  |                 |                  | xmin s.committed tra, xmax s.rolledback tra                          | not deleted in snapshot, take version                                                                        |
|                  |                 |                  | xmin s.running tra, no previous version                              | ignore record                                                                                                |
|                  |                 |                  | xmin s.running tra, previous version ex.                             | eval previous version                                                                                        |
|                  |                 |                  | xmin >= s.xmax, no previous version                                  | ignore record                                                                                                |
|                  |                 |                  | xmin >= s.xmax, previous version ex                                  | eval previous version                                                                                        |
|                  |                 |                  | xmin s.rolledback tra  no previous version                           | ignore record                                                                                                |
|                  |                 |                  | xmin s.rolledback tra  ex previous version                           | eval previous version                                                                                        |
| forChange\ntrue  | forSelect\ntrue |                  | -                                                                    | -                                                                                                            |
|                  |                 |                  | xid assigned, xmin == xid, xmax == 0,                                | if statement number > tra.statementnumber: ignore record                                                     |
|                  |                 |                  |                                                                      | else set xmax, forSelect flag, r.cid := current t.cid                                                        |
|                  |                 |                  | xid assigned, xmin == xid, xmax == xid                               | already locked for update                                                                                    |
|                  |                 |                  |                                                                      |                                                                                                              |
|                  |                 |                  |                                                                      |                                                                                                              |
|                  |                 |                  | xmin s.committed tra, xmax == 0, max xmin < s.xmax                   | take this version                                                                                            |
|                  |                 |                  | xmin s.committed tra, xmax s.committed tra < s.xmax                  | record deleted ignore this record                                                                            |
|                  |                 |                  | xmin s.committed tra, xmax s.committed tra < s.xmax, only for update | record was marked for update, no update happened use this version                                            |
|                  |                 |                  | xmin s.committed tra, xmax s.running tra                             | not deleted yet, take this version                                                                           |
|                  |                 |                  | xmin s.committed tra, xmax s.rolledback tra                          | not deleted in snapshot, take version                                                                        |
|                  |                 |                  | xmin s.running tra, no previous version                              | ignore record                                                                                                |
|                  |                 |                  | xmin s.running tra, previous version ex.                             | eval previous version                                                                                        |
|                  |                 |                  | xmin >= s.xmax, no previous version                                  | ignore record                                                                                                |
|                  |                 |                  | xmin >= s.xmax, previous version ex                                  | eval previous version                                                                                        |
|                  |                 |                  | xmin s.rolledback tra  no previous version                           | ignore record                                                                                                |
|                  |                 |                  | xmin s.rolledback tra  ex previous version                           | eval previous version                                                                                        |





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

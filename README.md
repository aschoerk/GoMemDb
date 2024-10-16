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

* if record visible Next uses check function then locking is done if necessary,
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


### Systematic analysis 

Discuss situations during Visibility check together with collecting information for locking possibilities

| Code  | 1st   | tra       | r.xmin                           | r.xmax                              | r.cid      | lock for change possible?         | todo                                                  |
|-------|-------|-----------|----------------------------------|-------------------------------------|------------|-----------------------------------|-------------------------------------------------------|
|       | y     | xid/nil   | xid2 c, Visible                  | == 0                                | n/a        | yes,                              | Visible                                               |
|       | y     | xid/nil   | xid2 c, Visible                  | xid3/xid2 c, Visible                | n/a        | no                                | InVisible, deleted                                    |
|       | y     | xid/nil   | xid2 c, Visible                  | xid3 c, UnVisible                   | n/a        | no                                | Visible                                               |
|       | y     | xid/nil   | xid2 c, Visible                  | xid3/xid2 c, Visible for update     | n/a        | no                                | Visible                                               |
|       | y     | xid/nil   | xid2 c, Visible                  | xid3 c, UnVisible for update        | n/a        | no                                | Visible                                               |
|       | y     | xid/nil   | xid2 c, Visible                  | xid3 rolledback==Unvisible          | n/a        | yes                               | Visible                                               |
|       | y     | xid/nil   | xid2 c, Visible                  | xid3 running                        | n/a        | wait                              | dependent on tra result                               |
| ----- | ----- | --------- | -------------------------------- | --------------------------------    | ---------- | -------------------------------   | ---------------------------------------------------   |
|       | y     | xid/nil   | xid2 rolled back                 | n/a                                 | n/a        | open, version deletable           | Look Previous, delete version f == y                  |
|       | y     | xid/nil   | xid2 running                     | n/a                                 | n/a        | if previous visible wait          | Previous                                              |
|       |       |           |                                  |                                     |            | - for r.xmin commit/rollback      |                                                       |
| ----- | ----- | --------- | -------------------------------- | --------------------------------    | ---------- | -------------------------------   | ---------------------------------------------------   |
|       | y     | xid/nil   | xid2 c Invisible                 | n/a                                 | n/a        | no, changed in diff tra           | Look Previous:                                        |
|       | f     | xid/nil   | not existent                     | not existent                        | n/a        |                                   | Not Visible                                           |
|       | f     | xid/nil   | xid3 Visible                     | xid2 !                              |            |                                   | Visible                                               |
|       | f     | xid/nil   | xid3 UnVisible                   | xid2 !                              |            |                                   | Look Previous same                                    |
|       | ----- | --------- | -------------------------------- | ----------------------------------- | ---------- | --------------------------------- | ----------------------------------------------------- |
|       | y     | xid       | xid                              | == 0                                | >= s.cid   | no, unseen change in same tra     | Look Previous                                         |
|       | f     | xid !     | not existent                     | not existent                        | n/a        |                                   | Not Visible                                           |
|       | f     | xid !     | n/a                              | xid !                               | < s.cid    |                                   | Illegal, s.cid should be the same as for 1st          |
|       | f     | xid !     | n/a                              | xid2                                | n/a        |                                   | Illegal                                               |
|       | f     | xid !     | xid                              | xid !                               | >= s.cid   |                                   | Look Previous until following cond meet               |
|       | ff    | xid !     | n/a                              | xid !                               | < s.cid    |                                   | Take f version                                        |
|       | f     | xid !     | xid2 c, Visible !                | xid !                               | >= s.cid   |                                   | r.xmin Visible                                        |
|       |       |           | !valid tra changed               |                                     |            |                                   |                                                       |
|       | f     | xid !     | xid2 c, Invisible                | xid !                               | >= s.cid   |                                   | Look Previous                                         |
|       | ff    | xid !     | xid3 c, other Visible            | xid2 same as previous !             | n/a        |                                   | Look Previous until r.xmin visible                    |
|       | ----- | --------- | -------------------------------- | --------------------------------    | ---------- | -------------------------------   | ---------------------------------------------------   |
|       | y     | xid       | xid                              | == 0                                | < s.cid    | already locked                    | return Visible                                        |
|       | ----- | --------- | -------------------------------- | --------------------------------    | ---------- | -------------------------------   | ---------------------------------------------------   |
|       | y     | xid       | xid                              | xid                                 | >= s.cid   | no, unseen change in same tra     | Look Previous knowing Insert happened (xmin xid),     |
|       | f     | xid !     | not existent                     | not existent                        | n/a        |                                   | Visible previous r.cid was for r.xmax                 |
|       | f     | xid !     | xid                              | xid !                               | >= s.cid   |                                   | Look Previous                                         |
|       | ff    | xid !     | not existent                     | not existent                        | n/a        |                                   | * Invisible, dont know about r.cid for r.xmin         |
|       | ff    | xid !     | xid                              | xid !                               | < s.cid    |                                   | Visible f Version                                     |
|       | f     | xid !     | n/a                              | xid !                               | < s.cid    |                                   | Visible f Version                                     |
|       | f     | xid !     | xid2, c Visible                  | xid !                               | >= s.cid   |                                   | Visible                                               |
|       | f     | xid !     | xid2, c InVisible                | xid !                               | >= s.cid   |                                   | Look Previous                                         |
|       | ----- | --------- | -------------------------------- | --------------------------------    | ---------- | -------------------------------   | ---------------------------------------------------   |
|       | y     | xid       | xid                              | xid                                 | < s.cid    | yes, but already done             | InVisible deleted                                     |
|       | y     | xid       | xid                              | xid for update                      | < s.cid    | yes, but already done             | Visible was only marked                               |
|       | y     | xid       | xid                              | xid2                                |            | n/a                               | illegal                                               |
|       | y     | xid       | xid2, Visible                    | xid                                 | >= s.cid   | no, unseen change in same tra     | return Visible                                        |
|       | ----- | --------- | -------------------------------- | --------------------------------    | ---------- | -------------------------------   | ---------------------------------------------------   |
|       | y     | xid       | xid2, InVisible                  | xid                                 | >= s.cid   | no, unseen change in same tra     | Look Previous                                         |
|       | f     | xid       | xid2, Visible(!=xmax)            | xid2 !                              | n/a        |                                   | Visible                                               |
|       | f     | xid       | xid2, InVisible(!=xmax)          | xid2 !                              | n/a        |                                   | Look Previous                                         |
|       | f     | xid       | not existent                     | not existent                        | n/a        |                                   | Invisible                                             |
|       | ----- | --------- | -------------------------------- | --------------------------------    | ---------- | -------------------------------   | ---------------------------------------------------   |
|       | y     | xid       | xid2                             | xid                                 | < s.cid    | Invisible                         | Invisible, Deleted,                                   |
|       | y     | xid       | xid2                             | xid for update                      | < s.cid    | yes, but already done             | return Visible (only selected yet)                    |
|       |       |           | records not touched by xid yet   |                                     |            |                                   |                                                       |





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

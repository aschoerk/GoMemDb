# MVCC 

The go-sql-mem is intended to be multi-user transactional. To implement this the postgres mechanisms of MVCC, Locking and Transaction handling are studied and reimplemented on a in memory record/tuple-based store.


### Iterator Next
The decision which record is to be provided is done during iterating through the table.

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
| n1    | y     | xid/nil   | xid2 c, Visible                  | == 0                                | n/a        | yes,                              | Visible                                               |
| n2    | y     | xid/nil   | xid2 c, Visible                  | xid3/xid2 c, Visible                | n/a        | no                                | InVisible, deleted                                    |
| n3    | y     | xid/nil   | xid2 c, Visible                  | xid3 c, UnVisible                   | n/a        | no                                | Visible                                               |
| n4    | y     | xid/nil   | xid2 c, Visible                  | xid3/xid2 c, Visible for update     | n/a        | no                                | Visible                                               |
| n5    | y     | xid/nil   | xid2 c, Visible                  | xid3 c, UnVisible for update        | n/a        | no                                | Visible                                               |
| n6    | y     | xid/nil   | xid2 c, Visible                  | xid3 rolledback==Unvisible          | n/a        | yes                               | Visible                                               |
| n7    | y     | xid/nil   | xid2 c, Visible                  | xid3 running                        | n/a        | wait                              | dependent on tra result && tuple selected             |
| ----- | ----- | --------- | -------------------------------- | --------------------------------    | ---------- | -------------------------------   | ---------------------------------------------------   |
| n8    | y     | xid/nil   | xid2 rolled back                 | n/a                                 | n/a        | open, version deletable           | Look Previous, delete version f == y                  |
|       | y     | xid/nil   | xid2 running                     | n/a                                 | n/a        | if previous visible wait          | Previous                                              |
|       |       |           |                                  |                                     |            | - for r.xmin commit/rollback      |                                                       |
| ----- | ----- | --------- | -------------------------------- | --------------------------------    | ---------- | -------------------------------   | ---------------------------------------------------   |
| n9    | y     | xid/nil   | xid2 c Invisible                 | n/a                                 | n/a        | no, changed in diff tra           | Look Previous:                                        |
|       | f     | xid/nil   | not existent                     | not existent                        | n/a        |                                   | Not Visible                                           |
|       | f     | xid/nil   | xid3 Visible                     | xid2 !                              |            |                                   | Visible                                               |
|       | f     | xid/nil   | xid3 UnVisible                   | xid2 !                              |            |                                   | Look Previous same                                    |
|       | ----- | --------- | -------------------------------- | ----------------------------------- | ---------- | --------------------------------- | ----------------------------------------------------- |
| t1    | y     | xid       | xid                              | == 0                                | < s.cid    | already locked                    | return Visible                                        |
|       | ----- | --------- | -------------------------------- | --------------------------------    | ---------- | -------------------------------   | ---------------------------------------------------   |
| t2    | y     | xid       | xid                              | == 0                                | >= s.cid   | no, unseen change in same tra     | Look Previous                                         |
| t21   | f     | xid !     | not existent                     | not existent                        | n/a        |                                   | Not Visible                                           |
|       | f     | xid !     | n/a                              | xid !                               | < s.cid    |                                   | Illegal, s.cid should be the same as for 1st          |
|       | f     | xid !     | n/a                              | xid2                                | n/a        |                                   | Illegal                                               |
|       | f     | xid !     | xid                              | xid !                               | >= s.cid   |                                   | Look Previous until following cond meet               |
|       | ff    | xid !     | n/a                              | xid !                               | < s.cid    |                                   | Take f version                                        |
| t22   | f     | xid !     | xid2 c, Visible !                | xid !                               | >= s.cid   |                                   | r.xmin Visible                                        |
|       |       |           | !valid tra changed               |                                     |            |                                   |                                                       |
|       | f     | xid !     | xid2 c, Invisible                | xid !                               | >= s.cid   |                                   | Look Previous                                         |
|       | ff    | xid !     | xid3 c, other Visible            | xid2 same as previous !             | n/a        |                                   | Look Previous until r.xmin visible                    |
|       | ----- | --------- | -------------------------------- | --------------------------------    | ---------- | -------------------------------   | ---------------------------------------------------   |
| t3    | y     | xid       | xid                              | xid                                 | >= s.cid   | no, unseen change in same tra     | Look Previous knowing Insert happened (xmin xid),     |
| t31   | f     | xid !     | not existent                     | not existent                        | n/a        |                                   | Visible previous r.cid was for r.xmax                 |
| t32   | f     | xid !     | xid                              | xid !                               | >= s.cid   |                                   | Look Previous                                         |
| t321  | ff    | xid !     | not existent                     | not existent                        | n/a        |                                   | * Invisible, dont know about r.cid for r.xmin         |
| t322  | ff    | xid !     | xid                              | xid !                               | < s.cid    |                                   | Visible f Version                                     |
| t33   | f     | xid !     | n/a                              | xid !                               | < s.cid    |                                   | Visible f Version                                     |
| t34   | f     | xid !     | xid2, c Visible                  | xid !                               | >= s.cid   |                                   | Visible                                               |
| t35   | f     | xid !     | xid2, c InVisible                | xid !                               | >= s.cid   |                                   | Look Previous                                         |
|       | ----- | --------- | -------------------------------- | --------------------------------    | ---------- | -------------------------------   | ---------------------------------------------------   |
| t5    | y     | xid       | xid                              | xid                                 | < s.cid    | yes, but already done             | InVisible deleted                                     |
| t6    | y     | xid       | xid                              | xid for update                      | < s.cid    | yes, but already done             | Visible was only marked                               |
| t7    | y     | xid       | xid                              | xid2                                |            | n/a                               | illegal                                               |
| t8    | y     | xid       | xid2, Visible                    | xid                                 | >= s.cid   | no, unseen change in same tra     | return Visible                                        |
|       | ----- | --------- | -------------------------------- | --------------------------------    | ---------- | -------------------------------   | ---------------------------------------------------   |
| t9    | y     | xid       | xid2, InVisible                  | xid                                 | >= s.cid   | no, unseen change in same tra     | Look Previous                                         |
| t91   | f     | xid       | xid2, Visible(!=xmax)            | xid2 !                              | n/a        |                                   | Visible                                               |
| t92   | f     | xid       | xid2, InVisible(!=xmax)          | xid2 !                              | n/a        |                                   | Look Previous                                         |
| t93   | f     | xid       | not existent                     | not existent                        | n/a        |                                   | Invisible                                             |
|       | ----- | --------- | -------------------------------- | --------------------------------    | ---------- | -------------------------------   | ---------------------------------------------------   |
| t10   | y     | xid       | xid2                             | xid                                 | < s.cid    | Invisible                         | Invisible, Deleted,                                   |
| t11   | y     | xid       | xid2                             | xid for update                      | < s.cid    | yes, but already done             | return Visible (only selected yet)                    |
|       |       |           | records not touched by xid yet   |                                     |            |                                   |                                                       |



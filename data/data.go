package data

import (
	"database/sql/driver"
	. "database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/emirpasic/gods/utils"
)

const (
	PRIMARY_AUTOINCREMENT = 1
	DEFAULT_MAX_LENGTH    = 40
)

var tablesMu sync.Mutex

var NULL_TUPLE = Tuple{-1, nil}

type GoSqlColumn struct {
	Name       string
	ColType    int
	ParserType int
	Length     int
	Spec2      int
	Hidden     bool
}

type TableIterator interface {
	Next(func([]Value) (bool, error)) (Tuple, bool, error)
}

type Table interface {
	Name() string
	Columns() []GoSqlColumn
	Data() *[][]Value
	NewIterator(baseData *StatementBaseData, forChange bool, forSelect bool) TableIterator
	FindColumn(name string) (int, error)
	Insert(recordValues []Value, conn *GoSqlConnData) int64
	Update(recordId int64, recordValues []Value, conn *GoSqlConnData) bool
	Delete(recordId int64, conn *GoSqlConnData) bool
}

type Tuple struct {
	Id   int64
	Data []driver.Value
}

const (
	FOR_UPDATE_FLAG = 1
)

type TupleVersion struct {
	Data  []driver.Value
	xmin  int64
	xmax  int64
	flags int32
	cid   int32
}

type VersionedTuple struct {
	id       int64
	mu       sync.Mutex
	Versions []TupleVersion
}

type GoSqlTableIterator struct {
	Transaction *Transaction
	SnapShot    *SnapShot
	table       *GoSqlTable
	nextKey     int64
	forChange   bool
	forSelect   bool
}

type TempTableIterator struct {
	table *TempTable
	ix    int
}

func (it *TempTableIterator) Next(check func([]Value) (bool, error)) (Tuple, bool, error) {
	for {
		// ignore snapshot, just check if xids match
		if len(it.table.Tempdata) < it.ix {
			it.ix++
			candidate := it.table.Tempdata[it.ix-1]
			found, err := check(candidate)
			if err != nil {
				return NULL_TUPLE, false, err
			}
			if found {
				return Tuple{-1, candidate}, true, nil
			}
		} else {
			return NULL_TUPLE, false, nil
		}
	}
}

type BaseTable struct {
	TableName    string
	TableColumns []GoSqlColumn
}

type GoSqlTable struct {
	BaseTable
	ids         map[string]int64
	NextTupleId atomic.Int64
	data        *redblacktree.Tree
	iterators   []TableIterator
	mu          sync.RWMutex
}

func (t *BaseTable) Name() string {
	return t.TableName
}

func (t *BaseTable) Columns() []GoSqlColumn {
	return t.TableColumns
}

type TempTable struct {
	BaseTable
	Tempdata [][]Value
}

func (t *TempTable) Data() *[][]Value {
	return &t.Tempdata
}

func (t *GoSqlTable) Data() *[][]Value {
	return nil
}

func NewTempTable(name string, columns []GoSqlColumn) Table {
	res := &TempTable{BaseTable{name, columns}, [][]Value{}}
	tablesMu.Lock()
	defer tablesMu.Unlock()
	Tables[name] = res
	return res
}

func NewTable(name string, columns []GoSqlColumn) *GoSqlTable {
	res := &GoSqlTable{BaseTable{name, columns}, make(map[string]int64), atomic.Int64{},
		redblacktree.NewWith(utils.Int64Comparator), []TableIterator{}, sync.RWMutex{}}
	res.NextTupleId.Store(1)
	return res
}

func (t *TempTable) NewIterator(baseData *StatementBaseData, forChange bool, forSelect bool) TableIterator {
	if forChange {
		panic("misuse of Temptables")
	}
	res := TempTableIterator{t, 0}
	return &res
}

func (t *GoSqlTable) NewIterator(baseData *StatementBaseData, forChange bool, forSelect bool) TableIterator {
	if forChange || forSelect {
		if baseData.Conn.Transaction == nil || !baseData.Conn.Transaction.IsStarted() {
			InitTransaction(baseData.Conn)
			startTransactionInternal(baseData.Conn.Transaction)
		}
	}
	var s *SnapShot
	tra := baseData.Conn.Transaction
	if tra == nil || tra.IsolationLevel == COMMITTED_READ {
		s = GetSnapShot(baseData.Conn.Transaction)
		baseData.SnapShot = s // not yet clear, if the snapshot is necessary outside of Iterator
	} else {
		s = tra.SnapShot
	}
	res := GoSqlTableIterator{baseData.Conn.Transaction, s, t, 0, forChange, forSelect}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.iterators = append(t.iterators, &res)
	return &res
}

var SERIALIZATION_ERROR error = errors.New("SerializationError")

var WAIT_FOR_TRA_ERROR error = errors.New("WaitforTra")

func (ti *GoSqlTableIterator) isVisible(xid int64) bool {
	return xid < ti.SnapShot.xmax && !ti.isRunning(xid) && !ti.isRolledback(xid)
}

func (ti *GoSqlTableIterator) isRolledback(xid int64) bool {
	bySnapshot := slices.Contains(ti.SnapShot.rolledbackXids, xid)
	if !bySnapshot && (xid < ti.SnapShot.xmin || ti.SnapShot.xmin == 0) {
		tra, err := GetTransaction(xid)
		if err != nil {
			fmt.Printf("during GetTransaction: %v", err)
			return false
		}
		return tra.State == ROLLEDBACK
	} else {
		return bySnapshot
	}
}

func (ti *GoSqlTableIterator) isCommitted(xid int64) bool {
	return xid < ti.SnapShot.xmax && !ti.isRolledback(xid) && !ti.isRunning(xid)
}

func (ti *GoSqlTableIterator) isRunning(xid int64) bool {
	return slices.Contains(ti.SnapShot.runningXids, xid)
}

func errorIfUpdate(forUpdate bool) error {
	if forUpdate {
		return SERIALIZATION_ERROR
	} else {
		return nil
	}
}

func waitIfUpdate(forUpdate bool) error {
	if forUpdate {
		return WAIT_FOR_TRA_ERROR
	} else {
		return nil
	}
}

func (ti *GoSqlTableIterator) initCaseHandling(tuple *VersionedTuple, offset int) (int64, int, *TupleVersion, *SnapShot) {
	traXid := int64(-1)
	if ti.Transaction != nil {
		traXid = ti.Transaction.Xid
	}
	actVersionOffset := len(tuple.Versions) - 1 - offset
	return traXid, actVersionOffset, &tuple.Versions[actVersionOffset], ti.SnapShot
}

func (ti *GoSqlTableIterator) removeRolledBack(tuple *VersionedTuple) bool {
	ti.table.mu.Lock()
	defer ti.table.mu.Unlock()
	if len(tuple.Versions) == 1 {
		ti.table.data.Remove(tuple.id)
		return false
	} else {
		tuple.Versions = tuple.Versions[:len(tuple.Versions)-1]
		return true
	}
}

func foundVersion(tuple *TupleVersion) (bool, bool, error, *TupleVersion, int64) {
	return true, true, nil, tuple, -1
}

func notFoundVersion() (bool, bool, error, *TupleVersion, int64) {
	return true, false, nil, nil, -1
}

func notDone() (bool, bool, error, *TupleVersion, int64) {
	return false, false, nil, nil, -1
}

func encountered(err error) (bool, bool, error, *TupleVersion, int64) {
	return true, false, err, nil, -1
}

func unvisibleOrError(forUpdate bool) (bool, bool, error, *TupleVersion, int64) {
	return true, false, errorIfUpdate(forUpdate), nil, -1
}

func visibleOrError(forUpdate bool, version *TupleVersion) (bool, bool, error, *TupleVersion, int64) {
	return true, true, errorIfUpdate(forUpdate), version, -1
}

func (ti *GoSqlTableIterator) isVisibleTupleNoTraEffectVariousSingleVersionCases(tuple *VersionedTuple, forUpdate bool, offset int) (bool, bool, error, *TupleVersion, int64) {
	// cases n1
	traXid, _, actVersion, s := ti.initCaseHandling(tuple, offset)
	if !ti.isVisible(actVersion.xmin) || traXid != -1 && (actVersion.xmin == traXid || actVersion.xmax == traXid) {
		return notDone()
	} else {
		if actVersion.xmax == 0 {
			if offset != 0 {
				return encountered(errors.New("Illegal version with xmax 0 at offset > 0"))
			}
			return foundVersion(actVersion)
		}
		if actVersion.xmin != actVersion.xmax && !ti.isVisible(actVersion.xmax) {
			if ti.isRolledback(actVersion.xmax) {
				return foundVersion(actVersion)
			} else if ti.isRunning(actVersion.xmax) || actVersion.xmax >= s.xmax {
				return true, true, waitIfUpdate(forUpdate), actVersion, actVersion.xmax
			} else if ti.isCommitted(actVersion.xmax) {
				return visibleOrError(forUpdate, actVersion)
			} else {
				return encountered(errors.New("Invalid state for actVersion.xmax"))
			}
		} else {
			if actVersion.flags&FOR_UPDATE_FLAG == 0 {
				if actVersion.xmax == actVersion.xmin {
					return unvisibleOrError(forUpdate)
				}
				if ti.isVisible(actVersion.xmax) {
					return unvisibleOrError(forUpdate)
				}
			} else {
				if actVersion.xmax == actVersion.xmin {
					return visibleOrError(forUpdate, actVersion)
				}
				if ti.isVisible(actVersion.xmax) {
					return visibleOrError(forUpdate, actVersion)
				}
			}
			return true, false, errors.New("Should never reach this"), nil, -1
		}
	}
}

func (ti *GoSqlTableIterator) isVisibleTupleNoTraEffectCurrentNotVisibleCases(tuple *VersionedTuple, forUpdate bool, offset int) (bool, bool, error, *TupleVersion, int64) {
	traXid, actVersionOffset, actVersion, _ := ti.initCaseHandling(tuple, offset)
	if traXid != -1 && (actVersion.xmin == traXid || actVersion.xmax == traXid) {
		return notDone()
	}
	if ti.isRolledback(actVersion.xmin) {
		if offset != 0 {
			return encountered(errors.New("Illegal version xmin rolled back at offset > 0"))
		}
		if ti.removeRolledBack(tuple) {
			return ti.isVisibleTupleNoTraEffect(tuple, forUpdate, 0)
		} else {
			return notFoundVersion()
		}
	} else if ti.isRunning(actVersion.xmax) {
		done, visible, err, tuple, _ := ti.isVisibleTupleNoTraEffect(tuple, forUpdate, offset+1)
		if !done {
			return encountered(errors.New("if once decided isVisibleTupleNoTraEffect, it should be capable to handle it"))
		}
		if err != nil || !visible {
			return true, false, err, nil, -1
		}
		return true, true, waitIfUpdate(forUpdate), tuple, actVersion.xmax
	} else {
		for i := actVersionOffset - 1; i >= 0; i-- {
			followingVersion := tuple.Versions[i]
			if ti.isVisible(followingVersion.xmin) {
				return foundVersion(&followingVersion)
			}
		}
		return notFoundVersion()
	}

}

func (ti *GoSqlTableIterator) isVisibleTupleNoTraEffect(tuple *VersionedTuple, forUpdate bool, offset int) (bool, bool, error, *TupleVersion, int64) {
	done, visible, error, tupleVersion, contendingTra := ti.isVisibleTupleNoTraEffectVariousSingleVersionCases(tuple, forUpdate, offset)
	if done {
		return done, visible, error, tupleVersion, contendingTra
	} else {
		return ti.isVisibleTupleNoTraEffectCurrentNotVisibleCases(tuple, forUpdate, offset)
	}
}

func (ti *GoSqlTableIterator) isVisibleTupleWithTraEffectStartingWithXmax0(tuple *VersionedTuple, forUpdate bool) (bool, bool, error, *TupleVersion, int64) {
	traXid, actVersionOffset, actVersion, s := ti.initCaseHandling(tuple, 0)
	if traXid < 0 {
		return encountered(errors.New("Expected transaction do be active in isVisibleTupleWithTraEffect "))
	}
	if actVersion.xmin != traXid || actVersion.xmax != 0 {
		return notDone()
	}
	if actVersion.cid < s.Cid {
		return foundVersion(actVersion) // case t1
	} else {
		// case t2
		for {
			// case t21
			actVersionOffset--
			if actVersionOffset < 0 {
				return true, false, nil, nil, -1
			}
			actVersion = &tuple.Versions[actVersionOffset]
			if actVersion.xmax != traXid {
				return encountered(fmt.Errorf("xmax %d in prev-record must match xmin %d", actVersion.xmax, traXid))
			}
			if actVersion.cid < s.Cid {
				return foundVersion(&tuple.Versions[actVersionOffset+1])
			} else {
				if actVersion.xmin == traXid {
					continue
				} else {
					break
				}
			}
		}
		if actVersion.xmin == traXid {
			return encountered(fmt.Errorf("xmin %d in prev-record must not match xmin %d anymore", actVersion.xmax, traXid))
		}
		for {
			// case t22
			if ti.isRolledback(actVersion.xmin) {
				return encountered(fmt.Errorf("Did transaction %d on rolledback tra: %d", traXid, actVersion.xmin))
			}
			if ti.isVisible(actVersion.xmin) {
				return foundVersion(actVersion)
			}
			actVersionOffset--
			if actVersionOffset < 0 {
				return notFoundVersion()
			}
			actVersion = &tuple.Versions[actVersionOffset]
		}
	}
}

func (ti *GoSqlTableIterator) isVisibleTupleWithTraEffectStartingWithXmaxTraXminTra(tuple *VersionedTuple, forUpdate bool) (bool, bool, error, *TupleVersion, int64) {
	traXid, actVersionOffset, actVersion, s := ti.initCaseHandling(tuple, 0)
	if traXid < 0 {
		return encountered(errors.New("Expected transaction do be active in isVisibleTupleWithTraEffect "))
	}
	if actVersion.xmax != traXid {
		// case t7
		return encountered(errors.New("Record changed in current running transaction (xmin) also change by other transaction (xmax) "))
	}
	if actVersion.xmin != traXid {
		return notDone()
	}
	if actVersion.cid >= s.Cid {
		// case t3
		for {
			if actVersion.cid < s.Cid {
				return foundVersion(&tuple.Versions[actVersionOffset+1]) //  case t322, t33, t5
			}
			actVersionOffset--
			if actVersionOffset < 0 {
				return notFoundVersion() // case t321, t31
			}
			actVersion = &tuple.Versions[actVersionOffset]
			if actVersion.xmin != traXid {
				if ti.isVisible(actVersion.xmin) {
					return foundVersion(actVersion) // case t34
				} else {
					break
				}
			}
		}
		for {
			// case t35
			actVersionOffset--
			if actVersionOffset < 0 {
				return notFoundVersion()
			}
			actVersion = &tuple.Versions[actVersionOffset]
			if ti.isVisible(actVersion.xmin) {
				return foundVersion(actVersion) // t34
			}
		}
	} else {
		if actVersion.flags&FOR_UPDATE_FLAG != 0 {
			return foundVersion(actVersion) // case t6
		} else {
			return notFoundVersion() // case t7
		}
	}
}
func (ti *GoSqlTableIterator) isVisibleTupleWithTraEffectStartingWithXmaxTraXminNotTra(tuple *VersionedTuple, forUpdate bool) (bool, bool, error, *TupleVersion, int64) {
	traXid, actVersionOffset, actVersion, s := ti.initCaseHandling(tuple, 0)
	if traXid < 0 {
		return encountered(errors.New("Expected transaction do be active in isVisibleTupleWithTraEffect "))
	}
	if actVersion.xmin == traXid {
		// case t9
		return encountered(errors.New("expected left over case xmax is tra, xmin is not tra"))
	}
	if actVersion.cid < s.Cid {
		if actVersion.flags&FOR_UPDATE_FLAG != 0 {
			return foundVersion(actVersion) // case t11
		} else {
			return notFoundVersion() // case t10
		}
	} else {
		for {
			if ti.isVisible(actVersion.xmin) {
				return foundVersion(actVersion)
			}
			actVersionOffset--
			if actVersionOffset < 0 {
				return notFoundVersion()
			}
			actVersion = &tuple.Versions[actVersionOffset]
		}
	}
}

func (ti *GoSqlTableIterator) isVisibleTupleWithTraEffect(tuple *VersionedTuple, forUpdate bool) (bool, bool, error, *TupleVersion, int64) {
	done, visible, error, tupleVersion, contendingTra := ti.isVisibleTupleWithTraEffectStartingWithXmax0(tuple, forUpdate)
	if !done {
		done, visible, error, tupleVersion, contendingTra = ti.isVisibleTupleWithTraEffectStartingWithXmaxTraXminTra(tuple, forUpdate)
		if !done {
			done, visible, error, tupleVersion, contendingTra = ti.isVisibleTupleWithTraEffectStartingWithXmaxTraXminNotTra(tuple, forUpdate)
		}
	}
	return done, visible, error, tupleVersion, contendingTra
}

func (ti *GoSqlTableIterator) isVisibleTuple(tuple *VersionedTuple, forUpdate bool) (bool, error, *TupleVersion, int64) {
	done, visible, error, tupleVersion, contendingTra := ti.isVisibleTupleNoTraEffect(tuple, forUpdate, 0)
	if !done {
		done, visible, error, tupleVersion, contendingTra = ti.isVisibleTupleWithTraEffect(tuple, forUpdate)
	}
	if !done {
		return false, errors.New("Case of tuple handling left"), nil, -1
	}
	return visible, error, tupleVersion, contendingTra
}

func (ti *GoSqlTableIterator) handleCandidate(check func([]Value) (bool, error), forUpdate bool, tuple *VersionedTuple) (Tuple, bool, error) {
	tuple.mu.Lock()
	defer tuple.mu.Unlock()

	for {
		waitForTraIfVisibleAndSelected := false
		visible, err, version, contendingTra := ti.isVisibleTuple(tuple, forUpdate)
		if err != nil {
			if err == WAIT_FOR_TRA_ERROR {
				waitForTraIfVisibleAndSelected = true
			} else {
				return NULL_TUPLE, false, err
			}
		}
		if !visible {
			return NULL_TUPLE, false, nil
		}
		selected, err := check(version.Data)
		if err != nil {
			return NULL_TUPLE, false, err
		}
		if selected {
			if forUpdate {
				// todo: synchronize on tuple
				if waitForTraIfVisibleAndSelected {
					if contendingTra < 0 {
						return NULL_TUPLE, false, errors.New("expected contending tra to wait for")
					} else {
						tuple.mu.Unlock()
						endT := time.Now().UnixMilli() + ti.Transaction.MaxLockTimeInMs
						for {
							tra, err := GetTransaction(contendingTra)
							if err != nil {
								tuple.mu.Lock()
								return NULL_TUPLE, false, err
							}
							if time.Now().UnixMilli() > endT {
								tuple.mu.Lock()
								return NULL_TUPLE, false, TRA_LOCK_TIMEOUT
							}
							if !tra.IsStarted() {
								break
							}
						}
						tuple.mu.Lock()
					}

				} else {
					version := &tuple.Versions[len(tuple.Versions)-1]
					version.xmax = ti.Transaction.Xid
					version.flags |= FOR_UPDATE_FLAG
					return Tuple{tuple.id, version.Data}, true, nil
				}
			} else {
				return Tuple{tuple.id, version.Data}, true, nil
			}
		} else {
			return NULL_TUPLE, false, nil
		}
	}
}

func (ti *GoSqlTableIterator) Next(check func([]Value) (bool, error)) (Tuple, bool, error) {
	forUpdate := ti.forChange || ti.forSelect
	// select versions against snapShot
	for {
		ti.table.mu.RLock()
		t := ti.table.data
		node, ok := t.Ceiling(ti.nextKey)
		if ok {
			ti.nextKey = node.Key.(int64) + 1
		} else {
			ti.table.mu.RUnlock()
			break
		}
		versionedTuple := node.Value.(*VersionedTuple)
		ti.table.mu.RUnlock()
		tuple, done, err := ti.handleCandidate(check, forUpdate, versionedTuple)
		if done || err != nil {
			return tuple, done, err
		}
	}
	ti.table.mu.Lock()
	defer ti.table.mu.Unlock()
	ti.table.iterators = slices.DeleteFunc(ti.table.iterators, func(i TableIterator) bool {
		return i.(*GoSqlTableIterator) == ti
	})
	return NULL_TUPLE, false, nil
}

func (t BaseTable) FindColumn(name string) (int, error) {
	for ix, col := range t.Columns() {
		if col.Name == name {
			return ix, nil
		}
	}
	return -1, fmt.Errorf("Did not find column with name %s", name)
}

func (t *GoSqlTable) Increment(columnName string) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	id, ok := t.ids[columnName]
	if !ok {
		t.ids[columnName] = 1
		return 1
	} else {
		t.ids[columnName] = id + 1
		return id
	}
}

func (t *GoSqlTable) Insert(recordValues []Value, conn *GoSqlConnData) int64 {
	StartTransaction(conn)
	recordVersion := TupleVersion{recordValues, conn.Transaction.Xid, 0, 0, conn.Transaction.Cid}
	id := t.NextTupleId.Load()
	tuple := &VersionedTuple{id, sync.Mutex{}, []TupleVersion{recordVersion}}
	t.NextTupleId.Add(1)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.data.Put(id, tuple)
	return id
}

func (t *GoSqlTable) Delete(recordId int64, conn *GoSqlConnData) bool {
	StartTransaction(conn)
	t.mu.RLock()
	defer t.mu.RUnlock()
	value, ok := t.data.Get(recordId)
	if ok {
		tuplep := value.(*VersionedTuple)
		tuplep.mu.Lock()
		defer tuplep.mu.Unlock()
		version := &tuplep.Versions[len(tuplep.Versions)-1]
		if version.xmax != 0 {
			version.flags &= ^FOR_UPDATE_FLAG
		}
		version.xmax = conn.Transaction.Xid
		version.cid = conn.Transaction.Cid
	}
	return ok
}

func (t *GoSqlTable) Update(recordId int64, recordValues []Value, conn *GoSqlConnData) bool {
	StartTransaction(conn)
	t.mu.RLock()
	defer t.mu.RUnlock()
	value, ok := t.data.Get(recordId)
	if ok {
		tuplep := value.(*VersionedTuple)
		tuplep.mu.Lock()
		defer tuplep.mu.Unlock()
		version := &tuplep.Versions[len(tuplep.Versions)-1]
		if version.xmax != 0 {
			version.flags &= ^FOR_UPDATE_FLAG
		}
		version.xmax = conn.Transaction.Xid
		version.cid = conn.Transaction.Cid
		recordVersion := TupleVersion{recordValues, conn.Transaction.Xid, 0, 0, conn.Transaction.Cid}
		tuplep.Versions = append(tuplep.Versions, recordVersion)
	}
	return ok
}

func (t *TempTable) Insert(recordValues []Value, conn *GoSqlConnData) int64 {
	*t.Data() = append(*t.Data(), recordValues)
	return -1
}

func (t *TempTable) Update(recordId int64, recordValues []Value, conn *GoSqlConnData) bool {
	panic("not implemented")
}

func (t *TempTable) Delete(recordId int64, conn *GoSqlConnData) bool {
	panic("not implemented")
}

var (
	Tables map[string]Table = make(map[string]Table)
)

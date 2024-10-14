package data

import (
	"database/sql/driver"
	. "database/sql/driver"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
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
	Update(recordId int64, recordValues []Value, conn *GoSqlConnData)
	Delete(recordId int64, conn *GoSqlConnData)
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
	Versions []TupleVersion
}

type GoSqlTableIterator struct {
	Transaction *Transaction
	SnapShot    *SnapShot
	table       *GoSqlTable
	ix          int
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
	data        []VersionedTuple
	iterators   []TableIterator
	mu          sync.Mutex
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
	res := &GoSqlTable{BaseTable{name, columns}, make(map[string]int64), atomic.Int64{}, []VersionedTuple{}, []TableIterator{}, sync.Mutex{}}
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
	if forChange {
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

type VisibleResult int

const (
	VISIBLE VisibleResult = iota + 1
	INVISIBLE
	LOOK_PREVIOUS      // version was effected by actions later than snapshot
	NOT_DETERMINED_YET // tra is running, but version and therefore tuple not effected (yet?)
	INVALID
)

func (ti *GoSqlTableIterator) evalAtRunningTransaction(actVersion *TupleVersion, bool mostRecentVersion) (VisibleResult, error) {
	if ti.Transaction.Xid == actVersion.xmin {
		if actVersion.xmax == 0 {
			if !mostRecentVersion {
				return INVALID, fmt.Errorf("xmax == 0 in not most recent version")
			}
			if actVersion.cid < ti.SnapShot.Cid {
				return VISIBLE, nil
			} else {
				return INVISIBLE, nil
			}
		} else if actVersion.xmax == ti.Transaction.Xid {
			if actVersion.cid < ti.SnapShot.Cid { // locked before snapshot was created
				return VISIBLE, nil
			} else { // xmax setting is not seen, so look for cid in previous version, if it exists
				return LOOK_PREVIOUS, nil
			}
		} else {
			return INVALID, fmt.Errorf("cannot be running transaction %d if xmax was set to %d", actVersion.xmin, actVersion.xmax)
		}
	} else {
		if actVersion.xmax == ti.Transaction.Xid { // locked in this transaction
			tra, err := GetTransaction(actVersion.xmin)
			if err != nil {
				return INVALID, err
			}
			if tra.IsStarted() {
				return INVALID, fmt.Errorf("Tra %d should not be started if current tra was able to lock record", actVersion.xmin)
			}
			if actVersion.cid >= ti.SnapShot.Cid {
				return LOOK_PREVIOUS, nil // change in this transaction cannot be seen in this snapshot
			} else {
				return VISIBLE, nil // locked but visible
			}
		} else { // version not affected by tra yet
			return NOT_DETERMINED_YET, nil
		}
	}
}

func (ti *GoSqlTableIterator) handleXMaxNotRelevant(actVersion *TupleVersion, mostRecentVersion bool) (VisibleResult, error) {
	if actVersion.xmin >= ti.SnapShot.xmax {
		return LOOK_PREVIOUS, nil
	} else {
		if slices.Contains(ti.SnapShot.runningXids, actVersion.xmin) {
			return LOOK_PREVIOUS, nil
		} else {
			tra, err := GetTransaction(actVersion.xmin)
			if err != nil {
				return INVALID, err
			}
			if tra.State == ROLLEDBACK {
				return LOOK_PREVIOUS, nil
			}
			if tra.IsStarted() {
				return INVALID, fmt.Errorf("expected xmin-tra %d to be not running")
			}
			return VISIBLE, nil
		}
	}

}

func (ti *GoSqlTableIterator) isVisible(actVersion *TupleVersion, mostRecentVersion bool) (VisibleResult, error) {
	if ti.Transaction != nil {
		res, err := ti.evalAtRunningTransaction(actVersion, mostRecentVersion)
		if err != nil {
			return INVALID, err
		}
		switch res {
		case LOOK_PREVIOUS:
		case VISIBLE:
		case INVISIBLE:
			return res, err
		case INVALID:
			panic("not expected")
		case NOT_DETERMINED_YET:
			break
		}
	}
	if actVersion.xmax == 0 {
		return ti.handleXMaxNotRelevant(actVersion, mostRecentVersion)
	} else {
		if ti.SnapShot.xmax <= actVersion.xmax || slices.Contains(ti.SnapShot.runningXids, actVersion.xmax) {
			tra, err := GetTransaction(actVersion.xmax)
			if err != nil {
				return INVALID, err
			}
			if tra.IsStarted() || tra.State == ROLLEDBACK {
				return ti.handleXMaxNotRelevant(actVersion, mostRecentVersion)
			}
		}
		tra, err := GetTransaction(actVersion.xmax)
		if err != nil {
			return INVALID, err
		}
		if tra.IsStarted() {

		}

	}

}

func (ti *GoSqlTableIterator) Next(check func([]Value) (bool, error)) (Tuple, bool, error) {
	// select versions against snapShot
	for {
		if ti.ix < len(ti.table.data) {
			tuple := ti.table.data[ti.ix]
			ti.ix++
			var actVersion *TupleVersion
			var tratimestamp int64
			changedInThisTransaction := false
			for _, version := range tuple.Versions {
				if version.xmin >= ti.SnapShot.xmax { // don't respect changes of transactions started later
					if ti.forChange {
						return NULL_TUPLE, false, fmt.Errorf("Serialization Error, tuple %d changed after of snapshot", tuple.id)
					}
				}
				if changedInThisTransaction { // have already a tuple out of this transaction seen, so only regard version as such
					if version.xmin == ti.Transaction.Xid {
						// found another version created in this transaction
						// TODO: multiple UPDATES in one transaction on the same tuple
						if actVersion.xmax != ti.Transaction.Xid {
							panic("found younger tuple, but this was changed in another transaction")
						}
						actVersion = &version // assume this to be a younger version
					}
					continue
				}
				if ti.Transaction != nil && version.xmin == ti.Transaction.Xid {
					// changed in this transaction
					actVersion = &version
					changedInThisTransaction = true
				} else {
					if !slices.Contains(ti.SnapShot.runningXids, version.xmin) {
						t, err := GetTransaction(version.xmin)
						if err != nil {
							panic(err)
						}
						if tratimestamp == 0 || tratimestamp < t.Ended {
							tratimestamp = t.Ended
							actVersion = &version
						}
					}
				}
			}
			if actVersion != nil {
				found, err := check(actVersion.Data)
				if err != nil {
					return NULL_TUPLE, false, err
				}
				if found {
					return Tuple{tuple.id, actVersion.Data}, true, nil
				}
			} // else tuple is not visible in current snapshot
		} else {
			break // no more records there
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
	recordVersion := TupleVersion{recordValues, conn.Transaction.Xid, 0, 0, 0}
	id := t.NextTupleId.Load()
	tuple := VersionedTuple{id, []TupleVersion{recordVersion}}
	t.NextTupleId.Add(1)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.data = append(t.data, tuple)
	return id
}

// only one thread may do update or delete . Exclusive Locks must guarantee this
func (t *GoSqlTable) Delete(recordId int64, conn *GoSqlConnData) {
	for _, tuple := range t.data {
		if tuple.id == recordId {
			for _, version := range tuple.Versions {
				if version.xmax == 0 {
					version.xmax = conn.Transaction.Xid
				}
			}
		}
	}
}

// only one thread may do update or delete . Exclusive Locks must guarantee this
func (t *GoSqlTable) Update(recordId int64, recordValues []Value, conn *GoSqlConnData) {
	for ix, tuple := range t.data {
		if tuple.id == recordId {
			for _, version := range tuple.Versions {
				if version.xmax == 0 {
					version.xmax = conn.Transaction.Xid
				}
			}
			recordVersion := TupleVersion{recordValues, conn.Transaction.Xid, 0, 0, 0}
			tuple.Versions = append(tuple.Versions, recordVersion)
			t.data[ix] = tuple
		}
	}
}

func (t *TempTable) Insert(recordValues []Value, conn *GoSqlConnData) int64 {
	*t.Data() = append(*t.Data(), recordValues)
	return -1
}

func (t *TempTable) Update(recordId int64, recordValues []Value, conn *GoSqlConnData) {
	panic("not implemented")
}

func (t *TempTable) Delete(recordId int64, conn *GoSqlConnData) {
	panic("not implemented")
}

var (
	Tables map[string]Table = make(map[string]Table)
)

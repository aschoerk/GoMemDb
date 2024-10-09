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

var NULL_RECORD = Record{-1, nil}

type GoSqlColumn struct {
	Name       string
	ColType    int
	ParserType int
	Length     int
	Spec2      int
	Hidden     bool
}

type TableIterator interface {
	Next() (Record, bool)
}

type Table interface {
	Name() string
	Columns() []GoSqlColumn
	Data() *[][]Value
	NewIterator(SnapShot *SnapShot) TableIterator
	FindColumn(name string) (int, error)
	Insert(recordValues []Value, transaction int64) int64
	Update(recordId int64, recordValues []Value, transaction int64)
	Delete(recordId int64, transaction int64)
}

type Record struct {
	Id   int64
	Data []driver.Value
}

type RecordVersion struct {
	Data []driver.Value
	xmin int64
	xmax int64
}

type VersionedRecord struct {
	id       int64
	Versions []RecordVersion
}

type GoSqlTableIterator struct {
	SnapShot *SnapShot
	table    *GoSqlTable
	xid      int64
	ix       int
}

type TempTableIterator struct {
	table *TempTable
	ix    int
}

func (it *TempTableIterator) Next() (Record, bool) {
	// ignore snapshot, just check if xids match
	if len(it.table.Tempdata) < it.ix {
		it.ix++
		return Record{-1, it.table.Tempdata[it.ix-1]}, true
	} else {
		return NULL_RECORD, false
	}
}

type BaseTable struct {
	TableName    string
	TableColumns []GoSqlColumn
}

type GoSqlTable struct {
	BaseTable
	ids          map[string]int64
	NextRecordId atomic.Int64
	data         []VersionedRecord
	iterators    []TableIterator
	mu           sync.Mutex
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
	res := &GoSqlTable{BaseTable{name, columns}, make(map[string]int64), atomic.Int64{}, []VersionedRecord{}, []TableIterator{}, sync.Mutex{}}
	res.NextRecordId.Store(1)
	return res
}

func (t *TempTable) NewIterator(snapShot *SnapShot) TableIterator {
	res := TempTableIterator{t, 0}
	return &res
}

func (t *GoSqlTable) NewIterator(snapShot *SnapShot) TableIterator {
	res := GoSqlTableIterator{snapShot, t, snapShot.xid, 0}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.iterators = append(t.iterators, &res)
	return &res
}

func (ti *GoSqlTableIterator) Next() (Record, bool) {
	// select versions against snapShot
	for {
		if ti.ix < len(ti.table.data) {
			record := ti.table.data[ti.ix]
			ti.ix++
			var actVersion *RecordVersion
			var tratimestamp int64
			changedInThisTransaction := false
			for _, version := range record.Versions {
				if version.xmin > ti.SnapShot.xid { // don't respect changes of transactions started later
					continue
				}
				if changedInThisTransaction { // have already a record out of this transaction seen, so only regard version as such
					if version.xmin == ti.SnapShot.xid {
						if actVersion.xmax != ti.SnapShot.xid {
							panic("found younger record, but this was changed in another transaction")
						}
						actVersion = &version // assume this to be a younger version
					}
					continue
				}
				if version.xmin == ti.SnapShot.xid {
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
				return Record{record.id, actVersion.Data}, true
			} // else record is not visible in current snapshot
		} else {
			break // no more records there
		}
	}
	ti.table.mu.Lock()
	defer ti.table.mu.Unlock()
	ti.table.iterators = slices.DeleteFunc(ti.table.iterators, func(i TableIterator) bool {
		return i.(*GoSqlTableIterator) == ti
	})
	return NULL_RECORD, false
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

func (t *GoSqlTable) Insert(recordValues []Value, transaction int64) int64 {
	recordVersion := RecordVersion{recordValues, transaction, 0}
	id := t.NextRecordId.Load()
	record := VersionedRecord{id, []RecordVersion{recordVersion}}
	t.NextRecordId.Add(1)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.data = append(t.data, record)
	return id
}

// only one thread may do update or delete . Exclusive Locks must guarantee this
func (t *GoSqlTable) Delete(recordId int64, transaction int64) {
	for _, record := range t.data {
		if record.id == recordId {
			for _, version := range record.Versions {
				if version.xmax == 0 {
					version.xmax = transaction
				}
			}
		}
	}
}

// only one thread may do update or delete . Exclusive Locks must guarantee this
func (t *GoSqlTable) Update(recordId int64, recordValues []Value, transaction int64) {
	for ix, record := range t.data {
		if record.id == recordId {
			for _, version := range record.Versions {
				if version.xmax == 0 {
					version.xmax = transaction
				}
			}
			recordVersion := RecordVersion{recordValues, transaction, 0}
			record.Versions = append(record.Versions, recordVersion)
			t.data[ix] = record
		}
	}
}

func (t *TempTable) Insert(recordValues []Value, transaction int64) int64 {
	*t.Data() = append(*t.Data(), recordValues)
	return -1
}

func (t *TempTable) Update(recordId int64, recordValues []Value, transaction int64) {
	panic("not implemented")
}

func (t *TempTable) Delete(recordId int64, transaction int64) {
	panic("not implemented")
}

var (
	Tables map[string]Table = make(map[string]Table)
)

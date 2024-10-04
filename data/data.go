package data

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"sync"
)

const (
	PRIMARY_AUTOINCREMENT = 1
	DEFAULT_MAX_LENGTH    = 40
)

type GoSqlColumn struct {
	Name       string
	ColType    int
	ParserType int
	Length     int
	Spec2      int
	Hidden     bool
}

type RecordVersion struct {
	Data      []driver.Value
	Readlocks []int // the transactions currently relying on this version
	Writelock int   // the transaction which successfully wrote the record
	WriteTime int64 // end of transaction when it was  written last
}

type Record struct {
	Versions  []RecordVersion
	Writelock int // the transaction which successfully set the current writelock
}

type GoSqlTable struct {
	Name    string
	Columns []GoSqlColumn
	LastId  int64
	Data    []Record
}

func NewTable(name string, columns []GoSqlColumn) *GoSqlTable {
	return &GoSqlTable{name, columns, 0, []Record{}}
}

func (t *GoSqlTable) FindColumn(name string) (int, error) {
	for ix, col := range t.Columns {
		if col.Name == name {
			return ix, nil
		}
	}
	return -1, fmt.Errorf("Did not find column with name %s", name)
}

func (v *RecordVersion) AlreadyReserved(transaction int) bool {
	return v.Writelock == transaction
}

func (v *RecordVersion) ReadInThisTransaction(transaction int) (bool, error) {
	if v.Writelock == transaction {

		return false, fmt.Errorf("Record was already written successfully by running tra %d", transaction)
	}
	return slices.Contains(v.Readlocks, transaction), nil
}

var mu sync.Mutex

var AlreadyReservedError = errors.New("already reserved")

func (t *GoSqlTable) reserveForTransactionOrFindReservedVersion(ix int, transaction int) ([]driver.Value, error) {
	mu.Lock()
	defer mu.Unlock()
	r := t.Data[ix]
	if r.Writelock != -1 {
		return nil, AlreadyReservedError
	}
	var youngestVersion *RecordVersion
	for _, v := range r.Versions {
		if v.AlreadyReserved(transaction) {
			return v.Data, nil
		}
		alreadyRead, err := v.ReadInThisTransaction(transaction)
		if err != nil {
			return nil, err
		} else {
			if alreadyRead {
				slices.DeleteFunc(v.Readlocks, func(e int) bool {
					return e == transaction
				})
				result := slices.Clone(v.Data)
				r.Versions = append(r.Versions, RecordVersion{result, []int{transaction}, transaction, -1})
				break
			} else {
				if youngestVersion == nil {
					youngestVersion = &v
				} else {
					if v.WriteTime > 0 && v.WriteTime < youngestVersion.WriteTime {
						youngestVersion = &v
					}
				}
			}
		}
	}
	if len(youngestVersion.Readlocks) == 0 {
		youngestVersion.Readlocks = append(youngestVersion.Readlocks, transaction)
		youngestVersion.Writelock = transaction
		return youngestVersion.Data, nil
		// preserve writetime in case of rollback
	} else {
		data := slices.Clone(youngestVersion.Data)
		r.Versions = append(r.Versions, RecordVersion{data, []int{transaction}, transaction, -1})
		return data, nil
	}
}

func (t *GoSqlTable) getYoungestOrAlreadyRead(ix int, transaction int) ([]driver.Value, error) {
	mu.Lock()
	defer mu.Unlock()
	r := t.Data[ix]
	for _, v := range r.Versions {
		alreadyRead, err := v.ReadInThisTransaction(transaction)
		if err != nil {
			return nil, err
		} else if alreadyRead {
			return v.Data, nil
		}
	}

	var youngestVersion *RecordVersion
	for _, v := range r.Versions {
		if youngestVersion == nil {
			youngestVersion = &v
		} else {
			if v.WriteTime > 0 && v.WriteTime < youngestVersion.WriteTime {
				youngestVersion = &v
			}
		}
	}
	youngestVersion.Readlocks = append(youngestVersion.Readlocks, transaction)
	return youngestVersion.Data, nil
}

func (t *GoSqlTable) GetRecord(ix int, transaction int, forWrite bool) ([]driver.Value, error) {
	if ix < 0 || ix > len(t.Data) {
		return nil, fmt.Errorf("Invalid index %d when reading from table %s", ix, t.Name)
	}
	if forWrite { // need my own version if there are already readlocks
		for {
			data, err := t.reserveForTransactionOrFindReservedVersion(ix, transaction)
			if err == nil {
				return data, nil
			} else if err != AlreadyReservedError {
				return nil, err
			} else {
				// wait for end of transaction
			}
		}
	} else {
		return t.getYoungestOrAlreadyRead(ix, transaction)
	}
}

var (
	Tables map[string]*GoSqlTable
)

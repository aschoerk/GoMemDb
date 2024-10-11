package tests

import (
	. "database/sql/driver"
	"testing"

	"github.com/aschoerk/go-sql-mem/data"
	"github.com/aschoerk/go-sql-mem/driver"
	"github.com/stretchr/testify/assert"
)

var testdriver = driver.NewDriver()

var emptyRecord = []Value{}

func NewStatementBaseData(t *testing.T) *data.StatementBaseData {
	conn, err := testdriver.Open("xxx")
	if err != nil {
		t.Error(err)
	}
	conndata := &conn.(*driver.GoSqlConn).Data
	return &data.StatementBaseData{conndata, nil, data.Executing}
}

func TestSnapshotAndIteratorOnEmptyTable(t *testing.T) {
	table := data.NewTable("testtable", []data.GoSqlColumn{})
	baseData := NewStatementBaseData(t)
	assert.Nil(t, baseData.SnapShot)
	it := table.NewIterator(baseData, false)
	assert.NotNil(t, baseData.SnapShot)
	assert.Equal(t, int64(0), baseData.SnapShot.Xmin())
	_, ok, _ := it.Next()
	assert.False(t, ok)
	err := data.EndStatement(baseData)
	assert.Nil(t, err)
	assert.Nil(t, baseData.Conn.Transaction)
	assert.Nil(t, baseData.SnapShot)
	s := data.GetSnapShot()
	assert.Equal(t, int64(0), s.Xmin())
	assert.Equal(t, int64(1), s.Xmax())
	assert.Equal(t, 0, len(s.RunningIds()))
}

func TestSnapshotOnOneRecord(t *testing.T) {
	table := data.NewTable("testtable", []data.GoSqlColumn{})
	baseData := NewStatementBaseData(t)
	inserted := table.Insert(emptyRecord, baseData.Conn)
	assert.Equal(t, int64(1), inserted)
	assert.NotNil(t, baseData.Conn.Transaction)
	assert.Nil(t, baseData.SnapShot)
	err := data.EndStatement(baseData)
	assert.Nil(t, err)
	assert.Nil(t, baseData.Conn.Transaction)
	assert.Nil(t, baseData.SnapShot)
	s := data.GetSnapShot()
	assert.Equal(t, int64(0), s.Xmin())
	assert.Equal(t, int64(2), s.Xmax())
	assert.Equal(t, 0, len(s.RunningIds()))
}

func TestSnapshotAndReadOnlyIteratorOnOneRecord(t *testing.T) {
	table := data.NewTable("testtable", []data.GoSqlColumn{})
	baseData := NewStatementBaseData(t)
	table.Insert(emptyRecord, baseData.Conn)
	data.EndStatement(baseData)
	it := table.NewIterator(baseData, false)
	res, ok, _ := it.Next()
	assert.True(t, ok)
	assert.NotEqual(t, emptyRecord, res)
	s := data.GetSnapShot()
	assert.Equal(t, int64(0), s.Xmin())
	assert.Equal(t, int64(2), s.Xmax())
	assert.Equal(t, 0, len(s.RunningIds()))
	_, ok, _ = it.Next()
	assert.False(t, ok)
	data.EndStatement(baseData)
}

func TestSnapshotAndUpdateIteratorOnOneRecord(t *testing.T) {
	table := data.NewTable("testtable", []data.GoSqlColumn{})
	baseData := NewStatementBaseData(t)
	table.Insert(emptyRecord, baseData.Conn)
	data.EndStatement(baseData)
	it := table.NewIterator(baseData, true)
	res, ok, _ := it.Next()
	assert.True(t, ok)
	assert.NotEqual(t, emptyRecord, res)
	s := data.GetSnapShot()
	assert.Equal(t, int64(2), s.Xmin())
	assert.Equal(t, int64(3), s.Xmax())
	assert.Equal(t, 1, len(s.RunningIds()))
	_, ok, _ = it.Next()
	assert.False(t, ok)
	data.EndStatement(baseData)
	s = data.GetSnapShot()
	assert.Equal(t, int64(0), s.Xmin())
	assert.Equal(t, int64(3), s.Xmax())
	assert.Equal(t, 0, len(s.RunningIds()))
}

func TestCanSetRollbackOnlyAndCommitToError(t *testing.T) {
}

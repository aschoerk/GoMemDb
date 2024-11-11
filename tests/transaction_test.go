package tests

import (
	. "database/sql/driver"
	"testing"

	"github.com/aschoerk/go-sql-mem/data"
	"github.com/aschoerk/go-sql-mem/driver"
	"github.com/aschoerk/go-sql-mem/parser"
	"github.com/stretchr/testify/assert"
)

var testdriver = driver.NewDriver()

var initialRecord = []Value{1}

var nextUpdateValue = 100

func NewStatementBaseData(t *testing.T) *data.StatementBaseData {
	conn, err := testdriver.Open("xxx")
	if err != nil {
		t.Error(err)
	}
	conndata := &conn.(*driver.GoSqlConn).Data
	return &data.StatementBaseData{conndata, nil, data.Executing}
}

func InitTraAndTestTable() *data.GoSqlTable {
	nextUpdateValue = 100
	data.InitTransactionManager()
	return data.NewTable(data.GoSqlIdentifier{[]string{"testtable"}}, []data.GoSqlColumn{{"x", parser.INTEGER, parser.INTEGER, 0, 0, false}})
}

func check(tuple data.Tuple) (bool, error) {
	return true, nil
}

func TestSnapshotAndIteratorOnEmptyTable(t *testing.T) {
	table := InitTraAndTestTable()
	baseData := NewStatementBaseData(t)
	assert.Nil(t, baseData.SnapShot)
	it := table.NewIterator(baseData, true)
	assert.NotNil(t, baseData.SnapShot)
	assert.Equal(t, int64(1), baseData.SnapShot.Xmin())
	_, ok, _ := it.Next(check)
	assert.False(t, ok)
	err := data.EndStatement(baseData)
	assert.Nil(t, err)
	assert.Nil(t, baseData.Conn.Transaction)
	assert.Nil(t, baseData.SnapShot)
	s := data.GetSnapShot(nil)
	assert.Equal(t, int64(0), s.Xmin())
	assert.Equal(t, int64(2), s.Xmax())
	assert.Equal(t, 0, len(s.RunningIds()))
}

func TestSnapshotOnOneRecord(t *testing.T) {
	table := InitTraAndTestTable()
	baseData := NewStatementBaseData(t)
	inserted := table.Insert(initialRecord, baseData.Conn)
	assert.Equal(t, int64(1), inserted)
	assert.NotNil(t, baseData.Conn.Transaction)
	assert.Nil(t, baseData.SnapShot)
	err := data.EndStatement(baseData)
	assert.Nil(t, err)
	assert.Nil(t, baseData.Conn.Transaction)
	assert.Nil(t, baseData.SnapShot)
	s := data.GetSnapShot(baseData.Conn.Transaction)
	assert.Equal(t, int64(0), s.Xmin())
	assert.Equal(t, int64(2), s.Xmax())
	assert.Equal(t, 0, len(s.RunningIds()))
}

func TestSnapshotAndReadOnlyIteratorOnOneRecord(t *testing.T) {
	table := InitTraAndTestTable()
	baseData := NewStatementBaseData(t)
	table.Insert(initialRecord, baseData.Conn)
	data.EndStatement(baseData)
	it := table.NewIterator(baseData, true)
	res, ok, _ := it.Next(check)
	assert.True(t, ok)
	assert.NotEqual(t, initialRecord, res)
	s := data.GetSnapShot(baseData.Conn.Transaction)
	assert.Equal(t, int64(2), s.Xmin())
	assert.Equal(t, int64(3), s.Xmax())
	assert.Equal(t, 1, len(s.RunningIds()))
	_, ok, _ = it.Next(check)
	assert.False(t, ok)
	data.EndStatement(baseData)
}

func TestSnapshotAndUpdateIteratorOnOneRecord(t *testing.T) {
	table := InitTraAndTestTable()
	baseData := NewStatementBaseData(t)
	table.Insert(initialRecord, baseData.Conn)
	data.EndStatement(baseData)
	it := table.NewIterator(baseData, true)
	res, ok, _ := it.Next(check)
	assert.True(t, ok)
	assert.NotEqual(t, initialRecord, res)
	s := data.GetSnapShot(baseData.Conn.Transaction)
	assert.Equal(t, int64(2), s.Xmin())
	assert.Equal(t, int64(3), s.Xmax())
	assert.Equal(t, 1, len(s.RunningIds()))
	_, ok, _ = it.Next(check)
	assert.False(t, ok)
	data.EndStatement(baseData)
	s = data.GetSnapShot(baseData.Conn.Transaction)
	assert.Equal(t, int64(0), s.Xmin())
	assert.Equal(t, int64(3), s.Xmax())
	assert.Equal(t, 0, len(s.RunningIds()))
}

type T struct {
	table    *data.GoSqlTable
	baseData *data.StatementBaseData
}

func (t *T) doI() int64 {
	return t.table.Insert(initialRecord, t.baseData.Conn)
}

func (t *T) makeIt() data.TableIterator {
	return t.table.NewIterator(t.baseData, false)
}

func (t *T) makeUIt() data.TableIterator {
	return t.table.NewIterator(t.baseData, true)
}

func (t *T) doU(id int64) bool {
	nextUpdateValue++
	return t.table.Update(id, data.NewSliceTuple(-1, []Value{nextUpdateValue}), t.baseData.Conn)
}

func (t *T) doD(id int64) bool {
	return t.table.Delete(id, t.baseData.Conn)
}

func (t *T) doEos() {
	data.EndStatement(t.baseData)
}

func (t *T) doBegin() {
	t.baseData.Conn.DoAutoCommit = false
	data.StartTransaction(t.baseData.Conn)
}

func (t *T) doCommit() {
	data.EndTransaction(t.baseData.Conn, data.COMMITTED)
}

func (t *T) doRollback() {
	data.EndTransaction(t.baseData.Conn, data.ROLLEDBACK)
}

func NewT(t *testing.T, table *data.GoSqlTable) T {
	return T{table, NewStatementBaseData(t)}
}

func TestN1(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	r1 := t1.doI()
	t1.doEos()
	it1 := t1.makeIt()
	res, _, err := it1.Next(check)
	assert.Nil(t, err)
	assert.Equal(t, r1, res.Id())
	_, ok, err := it1.Next(check)
	assert.False(t, ok)
	assert.Nil(t, err)
}

func TestN2(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	r1 := t1.doI()
	t1.doEos()
	t1.doBegin()
	t1.doD(r1)
	t1.doEos()
	it1 := t1.makeIt()
	_, ok, err := it1.Next(check)
	assert.False(t, ok)
	assert.Nil(t, err)
}

func TestN22(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	r1 := t1.doI()
	t1.doEos()
	t2 := NewT(t, table)
	t2.doBegin()
	t2.doD(r1)
	t2.doCommit()
	it1 := t1.makeIt()
	_, ok, err := it1.Next(check)
	assert.False(t, ok)
	assert.Nil(t, err)
}

func TestN23(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	r1 := t1.doI()
	t1.doEos()
	t2 := NewT(t, table)
	t2.doBegin()
	t2.doD(r1)
	t2.doEos()
	it1 := t1.makeIt()
	res, ok, _ := it1.Next(check)
	assert.True(t, ok)
	assert.Equal(t, r1, res.Id())
}

func TestN3(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	r1 := t1.doI()
	t1.doEos()
	it1 := t1.makeIt()
	t2 := NewT(t, table)
	t2.doD(r1)
	t2.doEos()
	res, _, err := it1.Next(check)
	assert.Nil(t, err)
	assert.Equal(t, r1, res.Id())
}

func TestN4(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	r1 := t1.doI()
	t1.doEos()
	t2 := NewT(t, table)
	it2 := t2.makeUIt()
	res2, _, _ := it2.Next((check)) // to make xmax flagged
	assert.Equal(t, r1, res2.Id())
	it1 := t1.makeIt()
	res, _, err := it1.Next(check)
	assert.Nil(t, err)
	assert.Equal(t, r1, res.Id())
}

func TestN5(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	r1 := t1.doI()
	t1.doEos()
	it1 := t1.makeIt()
	t2 := NewT(t, table)
	it2 := t2.makeUIt()
	res2, _, _ := it2.Next((check)) // to make xmax flagged, but unvisible anyway for t1
	assert.Equal(t, r1, res2.Id())
	res, _, err := it1.Next(check)
	assert.Nil(t, err)
	assert.Equal(t, r1, res.Id())
}

func TestN6(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	r1 := t1.doI()
	t1.doEos()
	it1 := t1.makeIt()
	t2 := NewT(t, table)
	t2.doBegin()
	t2.doD(r1)
	t2.doEos()
	t2.doRollback()
	res, _, err := it1.Next(check)
	assert.Nil(t, err)
	assert.Equal(t, r1, res.Id())
}

func TestNX(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	r1 := t1.doI()
	t1.doEos()
	t2 := NewT(t, table)
	it2 := t2.makeUIt()
	res2, _, _ := it2.Next((check))
	assert.Equal(t, r1, res2.Id())
	it1 := t1.makeUIt()
	_, _, err := it1.Next(check)
	assert.Equal(t, data.ErrTraLockTimeout, err)
}

func TestN81(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	t1.doBegin()
	t1.doI()
	t1.doRollback()
	it1 := t1.makeIt()
	_, ok, _ := it1.Next(check)
	assert.False(t, ok)
}

func TestN82(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	r1 := t1.doI()
	t1.doEos()
	t1.doBegin()
	t1.doU(r1)
	t1.doRollback()
	it1 := t1.makeIt()
	res, _, _ := it1.Next(check)
	assert.Equal(t, r1, res.Id())
	assert.Equal(t, 1, res.SafeData(0, 0))
}

func TestN83(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	r1 := t1.doI()
	t1.doEos()
	assert.True(t, t1.doU(r1))
	t1.doEos()
	t1.doBegin()
	t1.doU(r1)
	t1.doRollback()
	it1 := t1.makeIt()
	res, _, _ := it1.Next(check)
	assert.Equal(t, r1, res.Id())
	assert.Equal(t, 101, res.SafeData(0, 0))
}

func TestN832(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	r1 := t1.doI()
	t1.doEos()
	assert.True(t, t1.doU(r1))
	t1.doEos()
	t1.doBegin()
	t1.doU(r1)
	t1.doCommit()
	it1 := t1.makeIt()
	res, _, _ := it1.Next(check)
	assert.Equal(t, r1, res.Id())
	assert.Equal(t, 102, res.SafeData(0, 0))
}

func TestN84(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	r1 := t1.doI()
	t1.doEos()
	assert.True(t, t1.doU(r1))
	t1.doEos()
	t1.doBegin()
	assert.True(t, t1.doD(r1))
	t1.doRollback()
	it1 := t1.makeIt()
	res, _, _ := it1.Next(check)
	assert.Equal(t, r1, res.Id())
	assert.Equal(t, 101, res.SafeData(0, 0))
}

func TestN91(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	r1 := t1.doI()
	t1.doEos()
	tx := NewT(t, table)
	itx := tx.makeIt()
	t2 := NewT(t, table)
	assert.True(t, t2.doU(r1))
	t2.doEos()
	res, _, _ := itx.Next(check)
	assert.Equal(t, r1, res.Id())
	assert.Equal(t, 1, res.SafeData(0, 0))
}

func TestN92(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	r1 := t1.doI()
	assert.True(t, t1.doU(r1))
	assert.True(t, t1.doU(r1))
	t1.doEos()
	tx := NewT(t, table)
	itx := tx.makeIt()
	t2 := NewT(t, table)
	assert.True(t, t2.doU(r1))
	t2.doEos()
	res, _, _ := itx.Next(check)
	assert.Equal(t, r1, res.Id())
	assert.Equal(t, 102, res.SafeData(0, 0))
}

func TestT1(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	t1.doBegin()
	r1 := t1.doI()
	t1.doEos()
	it := t1.makeIt()
	res, _, _ := it.Next(check)
	assert.Equal(t, r1, res.Id())
	assert.Equal(t, 1, res.SafeData(0, 0))
}

func TestT21(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	t1.doBegin()
	it := t1.makeIt()
	t1.doI()
	t1.doEos()
	_, ok, _ := it.Next(check)
	assert.False(t, ok)
}

func TestT22_1(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	t1.doBegin()
	r1 := t1.doI()
	t1.doEos()
	it := t1.makeIt()
	t1.doU(r1) // cid of update overwrites cid of insert dispite having happened before Iterator was created.!MVCC not exactly implemented
	t1.doEos()
	_, ok, _ := it.Next(check)
	assert.False(t, ok)
}

func TestT22_3(t *testing.T) {
	table := InitTraAndTestTable()
	t2 := NewT(t, table)
	r1 := t2.doI()
	t2.doEos()
	t1 := NewT(t, table)
	t1.doBegin()
	it := t1.makeIt()
	t1.doU(r1)
	t1.doEos()
	res, _, _ := it.Next(check) // here insert can be seen because being from another tra, so cid is not relevant anymore.
	assert.Equal(t, r1, res.Id())
	assert.Equal(t, 1, res.SafeData(0, 0))
}

func TestT22_2(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	t1.doBegin()
	r1 := t1.doI()
	t1.doEos()
	t1.doU(r1) // cid of update 101 overwrites cid of insert
	t1.doEos()
	it := t1.makeIt()
	t1.doU(r1) // cid of update overwrites cid of insert
	t1.doEos()
	res, _, _ := it.Next(check)
	assert.Equal(t, r1, res.Id())
	assert.Equal(t, 101, res.SafeData(0, 0))
}

func TestT31(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	t1.doBegin()
	r1 := t1.doI()
	t1.doEos()
	it := t1.makeIt()
	t1.doD(r1) // cid of delete overwrites cid of insert
	t1.doEos()
	_, ok, _ := it.Next(check)
	assert.False(t, ok)
}

func TestT322(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	t1.doBegin()
	r1 := t1.doI()
	t1.doEos()
	t1.doU(r1) // cid of update 101 overwrites cid of insert
	t1.doEos()
	it := t1.makeIt()
	t1.doU(r1) // cid of update overwrites cid of insert
	t1.doEos()
	t1.doU(r1) // cid of update overwrites cid of insert
	t1.doEos()
	t1.doD(r1)
	t1.doEos()
	res, _, _ := it.Next(check)
	assert.Equal(t, r1, res.Id())
	assert.Equal(t, 101, res.SafeData(0, 0)) // fetched second version despite fitting cid is found in first version
}

func TestT321(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	t1.doBegin()
	r1 := t1.doI()
	t1.doEos()
	it := t1.makeIt()
	t1.doU(r1) // cid of update 101 overwrites cid of insert
	t1.doEos()
	t1.doU(r1) // cid of update overwrites cid of insert
	t1.doEos()
	t1.doU(r1) // cid of update overwrites cid of insert
	t1.doEos()
	t1.doD(r1)
	t1.doEos()
	_, ok, _ := it.Next(check) // like T22_1 insert is not seen anymore because cid got overwritten !MVCC not exactly implemented
	assert.False(t, ok)
}

func TestT5(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	t1.doBegin()
	r1 := t1.doI()
	t1.doEos()
	t1.doD(r1)
	t1.doEos()
	it := t1.makeIt()
	_, ok, _ := it.Next(check)
	assert.False(t, ok) // was really deleted and could be seen by late cursor
}

func TestT6(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	t1.doBegin()
	r1 := t1.doI()
	t1.doEos()
	itU := t1.makeUIt()
	_, ok, _ := itU.Next(check)
	assert.True(t, ok)
	it := t1.makeIt()
	res, ok, _ := it.Next(check)
	assert.True(t, ok) // was only marked for update, so it must be visible
	assert.Equal(t, r1, res.Id())
	assert.Equal(t, 1, res.SafeData(0, 0))
}

func TestT8(t *testing.T) {
	table := InitTraAndTestTable()
	t2 := NewT(t, table)
	r1 := t2.doI()
	t2.doEos()
	t1 := NewT(t, table)
	t1.doBegin()
	it := t1.makeIt()
	assert.True(t, t1.doU(r1))
	res, ok, _ := it.Next(check)
	assert.True(t, ok) // was done by separate visible transaction
	assert.Equal(t, r1, res.Id())
	assert.Equal(t, 1, res.SafeData(0, 0))
}

func TestT8_2(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	t2 := NewT(t, table)
	r1 := t2.doI()
	t2.doEos()
	t1.doBegin()
	it := t1.makeIt()
	assert.True(t, t2.doU(r1))
	res, ok, _ := it.Next(check)
	assert.True(t, ok) // was done by separate visible transaction (because of COMMITTED_READ)
	assert.Equal(t, r1, res.Id())
	assert.Equal(t, 1, res.SafeData(0, 0))
}

func TestT91(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	t1.doBegin()
	t2 := NewT(t, table)
	r1 := t2.doI() // visible insert
	it := t1.makeIt()
	t2.doEos()
	assert.True(t, t2.doU(r1))
	t2.doEos()
	_, ok, _ := it.Next(check)
	assert.False(t, ok) // cursor/iterator began before insert by t2
}

func TestT92(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	t1.doBegin()
	t2 := NewT(t, table)
	r1 := t2.doI()
	t2.doEos()
	it := t1.makeIt()
	assert.True(t, t2.doU(r1))
	t2.doEos()
	assert.True(t, t2.doU(r1))
	t2.doEos()
	res, ok, _ := it.Next(check)
	assert.True(t, ok) // cursor/iterator began immediately after insert by t2
	assert.Equal(t, r1, res.Id())
	assert.Equal(t, 1, res.SafeData(0, 0))
}

func TestT93(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	t1.doBegin()
	it := t1.makeIt()
	t2 := NewT(t, table)
	r1 := t2.doI()
	t2.doEos()
	assert.True(t, t2.doU(r1))
	t2.doEos()
	_, ok, _ := it.Next(check)
	assert.False(t, ok) // cursor/iterator began before insert by t2
}

func TestT10(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	t1.doBegin()
	t2 := NewT(t, table)
	r1 := t2.doI()
	t2.doEos()
	t1.doD(r1)
	t1.doEos()
	it := t1.makeIt()
	_, ok, _ := it.Next(check)
	assert.False(t, ok) // was deleted by tra t1
}

func TestT11(t *testing.T) {
	table := InitTraAndTestTable()
	t1 := NewT(t, table)
	t1.doBegin()
	t2 := NewT(t, table)
	r1 := t2.doI()
	t2.doEos()
	itU := t1.makeUIt()
	itU.Next(check)
	it := t1.makeIt()
	res, ok, _ := it.Next(check)
	assert.True(t, ok) // was marked for update by itU
	assert.Equal(t, r1, res.Id())
}

func TestCanSetRollbackOnlyAndCommitToError(t *testing.T) {
}

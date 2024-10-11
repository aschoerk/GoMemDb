package tests

import (
	"testing"

	"github.com/aschoerk/go-sql-mem/data"
	"github.com/stretchr/testify/assert"
)

func TestTransactionsAndSnapShots(t *testing.T) {
	tra := data.InitTransaction(data.COMMITTED_READ)
	assert.Equal(t, tra.Xid, data.NO_TRANSACTION, "expected xid to be invalid after init")
	tra, err := data.StartTransaction(tra)
	assert.NoError(t, err, "error during start transaction")
	assert.Equal(t, tra.IsolationLevel, data.COMMITTED_READ, "expected isolation level committed read")
	assert.NotZero(t, tra.Started)
	firstXid := tra.Xid
	assert.NotEqual(t, firstXid, data.NO_TRANSACTION, "")
	tra2 := data.InitTransaction(data.COMMITTED_READ)
	tra2, err = data.StartTransaction(tra2)
	assert.NoError(t, err, "error during start transaction 2")
	assert.Equal(t, tra.IsolationLevel, data.COMMITTED_READ)
	assert.Equal(t, tra2.Xid, firstXid+1)
	assert.NotZero(t, tra2.Started)
	// check snapshots
	snapshot1 := data.GetSnapShot()
	assert.NoError(t, err)
	assert.Equal(t, firstXid, snapshot1.Xmin())
	assert.Equal(t, firstXid+2, snapshot1.Xmax())
	assert.Len(t, snapshot1.RunningIds(), 0) // no running transaction from before the current
	snapshot2 := data.GetSnapShot()
	assert.NoError(t, err)
	assert.Equal(t, firstXid, snapshot2.Xmin())
	assert.Equal(t, firstXid+2, snapshot2.Xmax())
	assert.Len(t, snapshot2.RunningIds(), 1) // one other running transaction tra
	assert.Equal(t, firstXid, snapshot2.RunningIds()[0])
	// try ending of tra
	err = data.EndTransaction(tra, data.COMMITTED)
	assert.NoError(t, err)
	snapshot2, err = data.GetSnapShot()
	assert.NoError(t, err)
	assert.Equal(t, tra2.Xid, snapshot2.Xmin())
	assert.Equal(t, tra2.Xid+1, snapshot2.Xmax())
	assert.Len(t, snapshot2.RunningIds(), 0) // no running transaction tra anymore
	tra3, err := data.StartTransaction(tra)
	assert.NoError(t, err, "error during start transaction")
	assert.Equal(t, tra3.IsolationLevel, data.COMMITTED_READ, "expected isolation level committed read")
	assert.NotEqual(t, tra, tra3)
	assert.Equal(t, int64(3), tra3.Xid)
	assert.NotZero(t, tra3.Started)
	assert.NotZero(t, tra.Ended)
	assert.Greater(t, tra.Ended, tra.Started)
	assert.Greater(t, tra.Ended, tra2.Started)
	snapshot3 := data.GetSnapShot()
	assert.Equal(t, tra2.Xid, snapshot3.Xmin())
	assert.Equal(t, tra3.Xid+1, snapshot3.Xmax())
	assert.Len(t, snapshot3.RunningIds(), 1) // tra2 is running yet
	assert.Equal(t, tra2.Xid, snapshot3.RunningIds()[0])
	snapshot2, err = data.GetSnapShot(
	assert.Equal(t, tra2.Xid, snapshot2.Xmin())
	assert.Equal(t, tra3.Xid+1, snapshot2.Xmax())
	assert.Len(t, snapshot2.RunningIds(), 0) // no running transaction tra relevant for tra2 anymore
	tra4 := data.InitTransaction(data.COMMITTED_READ)
	tra4, err = data.StartTransaction(tra4)
	tra5 := data.InitTransaction(data.COMMITTED_READ)
	tra5, err = data.StartTransaction(tra5)
	data.EndTransaction(tra3, data.ROLLEDBACK)
	snapshot5 := data.GetSnapShot()
	assert.Equal(t, tra2.Xid, snapshot5.Xmin())
	assert.Equal(t, tra5.Xid+1, snapshot5.Xmax())
	assert.Len(t, snapshot5.RunningIds(), 2)
	assert.Equal(t, tra2.Xid, snapshot5.RunningIds()[0])
	assert.Equal(t, tra4.Xid, snapshot5.RunningIds()[1])
	tratmp, err := data.GetTransaction(tra.Xid)
	assert.Equal(t, tra, tratmp)
	tratmp, err = data.GetTransaction(tra2.Xid)
	assert.Equal(t, tra2, tratmp)
	tratmp, err = data.GetTransaction(tra3.Xid)
	assert.Equal(t, tra3, tratmp)
	tratmp, err = data.GetTransaction(tra4.Xid)
	assert.Equal(t, tra4, tratmp)
	tratmp, err = data.GetTransaction(tra5.Xid)
	assert.Equal(t, tra5, tratmp)

}

func TestCanSetRollbackOnlyAndCommitToError(t *testing.T) {
	tra := data.InitTransaction(data.COMMITTED_READ)
	tra, _ = data.StartTransaction(tra)
	tra.SetRollbackOnly()
	err := data.EndTransaction(tra, data.COMMITTED)
	assert.Error(t, err)
	assert.Equal(t, data.ROLLEDBACK, tra.State)
	tra2 := data.InitTransaction(data.COMMITTED_READ)
	tra2, _ = data.StartTransaction(tra2)
	tra2.SetRollbackOnly()
	err = data.EndTransaction(tra2, data.ROLLEDBACK)
	assert.NoError(t, err)
	assert.Equal(t, data.ROLLEDBACK, tra.State)
	_, err = data.GetSnapShot()
	assert.Error(t, err)
}

package data

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type TransactionState int8

const NO_TRANSACTION = int64(0)

const (
	INITED TransactionState = iota + 1
	STARTED
	ROLLBACKONLY
	COMMITTED
	ROLLEDBACK
)

type TransactionIsolationLevel int8

const (
	UNCOMMITTED_READ TransactionIsolationLevel = iota + 1
	COMMITTED_READ
	REPEATABLE_READ
	SERIALIZABLE
)

type Transaction struct {
	Xid            int64
	Started        int64
	Ended          int64
	ChangeCount    int64
	SnapShot       *SnapShot
	State          TransactionState
	IsolationLevel TransactionIsolationLevel
}

type TransactionManager struct {
	nextXid          atomic.Int64
	lowestRunningXid atomic.Int64
	transactions     map[int64]*Transaction
	mu               sync.RWMutex
}

var transactionManager = NewTransactionManager()

func NewTransactionManager() *TransactionManager {
	res := TransactionManager{
		atomic.Int64{},
		atomic.Int64{},
		make(map[int64]*Transaction),
		sync.RWMutex{},
	}
	res.nextXid.Add(1)
	return &res
}

func InitTransaction(isolationLevel TransactionIsolationLevel) *Transaction {
	return &Transaction{NO_TRANSACTION, 0, 0, 0, nil, INITED, isolationLevel}
}

func StartTransaction(t *Transaction) (*Transaction, error) {
	if t.State == STARTED || t.State == ROLLBACKONLY {
		return nil, fmt.Errorf("Trying to restart transaction %d", t.Xid)
	}
	if t.State == ROLLEDBACK || t.State == COMMITTED {
		t = &Transaction{NO_TRANSACTION, 0, 0, 0, nil, INITED, t.IsolationLevel}
	}
	var xid int64
	for {
		xid = transactionManager.nextXid.Load()
		if transactionManager.nextXid.CompareAndSwap(xid, xid+1) {
			break
		}
	}
	if transactionManager.lowestRunningXid.Load() == NO_TRANSACTION {
		transactionManager.lowestRunningXid.CompareAndSwap(NO_TRANSACTION, xid)
	}
	t.Xid = xid
	t.Started = time.Now().UnixNano()
	t.State = STARTED
	if t.IsolationLevel == REPEATABLE_READ {
		s, err := GetSnapShot(t.Xid)
		if err != nil {
			return t, err
		}
		t.SnapShot = s
	}
	transactionManager.mu.Lock()
	defer transactionManager.mu.Unlock()
	transactionManager.transactions[xid] = t
	return t, nil
}

func EndTransaction(transaction *Transaction, newState TransactionState) error {
	if transaction == nil {
		return errors.New("EndTransaction called with nil transaction")
	}
	if newState != ROLLEDBACK && newState != COMMITTED {
		return fmt.Errorf("Trying to end transaction %d into state %d", transaction.Xid, newState)
	}
	if transaction.State != STARTED && transaction.State != ROLLBACKONLY {
		return fmt.Errorf("Trying to end transaction %d in state %d", transaction.Xid, transaction.State)
	}
	rollbackInsteadOfCommit := newState == COMMITTED && transaction.State == ROLLBACKONLY
	if rollbackInsteadOfCommit {
		newState = ROLLEDBACK
	}
	transaction.Ended = time.Now().UnixNano()
	transaction.State = newState
	if transactionManager.lowestRunningXid.Load() == transaction.Xid {
		transactionManager.mu.Lock()
		defer transactionManager.mu.Unlock()
		if transactionManager.lowestRunningXid.Load() == transaction.Xid {
			actXid := transaction.Xid
			for {
				actXid++
				tra, ok := transactionManager.transactions[actXid]
				if !ok {
					if actXid != transactionManager.nextXid.Load() {
						return fmt.Errorf("Expected not found transaction %d to be behind currently executed", actXid)
					} else {
						transactionManager.lowestRunningXid.CompareAndSwap(transaction.Xid, NO_TRANSACTION)
						break
					}
				} else {
					if tra.State == STARTED || tra.State == ROLLBACKONLY {
						transactionManager.lowestRunningXid.CompareAndSwap(transaction.Xid, actXid)
						break
					}
				}
			}
		} else {
			return fmt.Errorf("Stopping of transaction %d done in more than one thread", transaction.Xid)
		}
	}
	if rollbackInsteadOfCommit {
		return fmt.Errorf("Rolled back transaction because of rollback only")
	} else {
		return nil
	}
}

func (t *Transaction) Commit() error {
	return EndTransaction(t, COMMITTED)
}

func (t *Transaction) Rollback() error {
	return EndTransaction(t, ROLLEDBACK)
}

func (t *Transaction) SetRollbackOnly() error {
	if t.State != STARTED && t.State != ROLLBACKONLY {
		return fmt.Errorf("expected state of transaction %d to be started or rollbackonly", t.Xid)
	}
	t.State = ROLLBACKONLY
	return nil
}

func GetTransaction(xid int64) (*Transaction, error) {
	transactionManager.mu.RLock()
	defer transactionManager.mu.RUnlock()
	res, ok := transactionManager.transactions[xid]
	if !ok {
		return nil, fmt.Errorf("Transaction %d not found", xid)
	} else {
		return res, nil
	}
}

type SnapShot struct {
	xid         int64
	xmin        int64
	xmax        int64
	runningXids []int64
}

func GetSnapShot(currentXid int64) (*SnapShot, error) {
	transactionManager.mu.RLock()
	defer transactionManager.mu.RUnlock()
	xmin := transactionManager.lowestRunningXid.Load()
	xmax := transactionManager.nextXid.Load()
	runningXids := []int64{}
	for i := xmin; i < currentXid; i++ {
		tra, ok := transactionManager.transactions[i]
		if !ok {
			return nil, fmt.Errorf("expected all data of transactions between xmin %d to xmax %d to exists, not found: %d", xmin, xmax, i)
		}
		if tra.State == STARTED || tra.State == ROLLBACKONLY {
			if tra.Xid != currentXid {
				runningXids = append(runningXids, tra.Xid)
			}
		}
	}
	return &SnapShot{currentXid, xmin, xmax, runningXids}, nil
}

func (s *SnapShot) Xid() int64 {
	return s.xid
}

func (s *SnapShot) Xmin() int64 {
	return s.xmin
}

func (s *SnapShot) Xmax() int64 {
	return s.xmax
}

func (s *SnapShot) RunningIds() []int64 {
	return s.runningXids
}

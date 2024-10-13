package data

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type transactionManagerType struct {
	nextXid          atomic.Int64
	lowestRunningXid atomic.Int64
	transactions     map[int64]*Transaction
	mu               sync.RWMutex
}

var transactionManager = NewTransactionManager()

func NewTransactionManager() *transactionManagerType {
	res := transactionManagerType{
		atomic.Int64{},
		atomic.Int64{},
		make(map[int64]*Transaction),
		sync.RWMutex{},
	}
	res.nextXid.Add(1)
	return &res
}

// called during any change after transaction begin (or after last commit/rollback in case of autocommit)
func StartTransaction(c *GoSqlConnData) error {
	if c.Transaction != nil {
		if c.Transaction.IsStarted() {
			return nil
		}
	}
	InitTransaction(c)
	t, err := startTransactionInternal(c.Transaction)
	if err != nil {
		return err
	} else {
		c.Transaction = t
	}
	return nil
}

func InitTransaction(conn *GoSqlConnData) {
	conn.Transaction = &Transaction{NO_TRANSACTION, 0, 0, 0, 0, nil, INITED, conn.DefaultIsolationLevel, conn}
}

func (t *Transaction) IsStarted() bool {
	return t.State == STARTED || t.State == ROLLEDBACK
}

func startTransactionInternal(t *Transaction) (*Transaction, error) {
	if t.State == STARTED || t.State == ROLLBACKONLY {
		return nil, fmt.Errorf("Trying to restart transaction %d", t.Xid)
	}
	if t.State == ROLLEDBACK || t.State == COMMITTED {
		t = &Transaction{NO_TRANSACTION, 0, 0, 0, 0, nil, INITED, t.IsolationLevel, t.Conn}
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
	if t.IsolationLevel == REPEATABLE_READ || t.IsolationLevel == SERIALIZABLE {
		t.SnapShot = GetSnapShot(t)
	}
	transactionManager.mu.Lock()
	defer transactionManager.mu.Unlock()
	transactionManager.transactions[xid] = t
	return t, nil
}

func EndStatement(baseData *StatementBaseData) error {
	transaction := baseData.Conn.Transaction
	if transaction == nil || transaction.IsolationLevel == COMMITTED_READ {
		baseData.SnapShot = nil
	}
	if transaction != nil && baseData.Conn.DoAutoCommit {
		return EndTransaction(baseData.Conn, COMMITTED)
	} else {
		if transaction != nil {
			transaction.Cid++
		}
		return nil
	}
}

func EndTransaction(conn *GoSqlConnData, newState TransactionState) error {
	transaction := conn.Transaction
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
	conn.Transaction = nil
	if rollbackInsteadOfCommit {
		return fmt.Errorf("Rolled back transaction because of rollback only")
	} else {
		return nil
	}
}

func (t *Transaction) Commit() error {
	return EndTransaction(t.Conn, COMMITTED)
}

func (t *Transaction) Rollback() error {
	return EndTransaction(t.Conn, ROLLEDBACK)
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

func GetSnapShot(transaction *Transaction) *SnapShot {
	transactionManager.mu.RLock()
	defer transactionManager.mu.RUnlock()
	xmin := transactionManager.lowestRunningXid.Load()
	xmax := transactionManager.nextXid.Load()
	runningXids := []int64{}
	cid := int32(0)
	if transaction != nil {
		cid = transaction.Cid
	}
	if xmin != NO_TRANSACTION {
		for i := xmin; i < xmax; i++ {
			tra, ok := transactionManager.transactions[i]
			if !ok {
				panic(fmt.Sprintf("expected all data of transactions between xmin %d to xmax %d to exists, not found: %d", xmin, xmax, i))
			}
			if tra.State == STARTED || tra.State == ROLLBACKONLY {
				runningXids = append(runningXids, tra.Xid)
			}
		}
	}
	return &SnapShot{xmin, xmax, cid, runningXids}
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

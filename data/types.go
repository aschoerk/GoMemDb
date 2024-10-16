package data

import "database/sql/driver"

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
	Cid            int32
	Started        int64
	Ended          int64
	ChangeCount    int64
	SnapShot       *SnapShot
	State          TransactionState
	IsolationLevel TransactionIsolationLevel
	Conn           *GoSqlConnData
}

type SnapShot struct {
	xmin           int64
	xmax           int64
	Cid            int32
	runningXids    []int64
	rolledbackXids []int64
}

type GoSqlConnData struct {
	Number                int64
	Transaction           *Transaction
	DoAutoCommit          bool
	DefaultIsolationLevel TransactionIsolationLevel
}

type StatementInterface interface {
	driver.Stmt
	BaseData() *StatementBaseData
}

type BaseStatement struct {
	StatementBaseData
}

func (s *BaseStatement) BaseData() *StatementBaseData {
	return &s.StatementBaseData
}

type StatementBaseData struct {
	Conn     *GoSqlConnData
	SnapShot *SnapShot
	State    StmtState
}

func (r *StatementBaseData) NumInput() int {
	return 0
}

func (r *StatementBaseData) Query(args []driver.Value) (driver.Rows, error) {
	panic("not implemented")
}

func (r *StatementBaseData) Exec(args []driver.Value) (driver.Result, error) {
	panic("not implemented")
}

func (r *StatementBaseData) Close() error {
	r.State = Closed
	return nil
}

type StmtState int

const (
	Created StmtState = iota + 1
	Parsed
	Executing
	EndOfRows
	Closed
)

func NewStatementBaseData() BaseStatement {
	return BaseStatement{StatementBaseData{nil, nil, Created}}
}

package parser

import (
	. "database/sql/driver"
	"errors"
	"fmt"
	"slices"

	"github.com/aschoerk/go-sql-mem/data"
)

type GoSqlUpdateRequest struct {
	data.BaseStatement
	tableName    data.GoSqlIdentifier
	updates      []GoSqlUpdateSpec
	where        *GoSqlTerm
	terms        []*GoSqlTerm
	placeHolders []*GoSqlTerm // the terms identified alias placeholders by parser
	columnixs    []int        // index in tuple, where update should happen
	table        data.Table
}

func NewUpdateRequest(tableName data.GoSqlIdentifier, updates []GoSqlUpdateSpec, where *GoSqlTerm) *GoSqlUpdateRequest {
	return &GoSqlUpdateRequest{
		data.BaseStatement{
			data.StatementBaseData{}},
		tableName, updates, where,
		nil, nil, nil, nil,
	}
}

type GoSqlConnectionLevelRequest struct {
	data.BaseStatement
	token1, token2 int
}

func NewConnectionLevelRequest(token1 int, token2 int) *GoSqlConnectionLevelRequest {
	return &GoSqlConnectionLevelRequest{
		data.BaseStatement{
			data.StatementBaseData{}},
		token1, token2}
}

type GoSqlDeleteRequest struct {
	data.BaseStatement
	from  []*GoSqlJoinedTable
	where *GoSqlTerm
}

func (r *GoSqlUpdateRequest) initStruct() error {
	if r.columnixs == nil {
		tmptable, exists := data.GetTable(r.BaseStatement, r.tableName)
		if !exists {
			return fmt.Errorf("Unknown Table %s", r.tableName)
		}
		r.table = tmptable
		r.terms = []*GoSqlTerm{}
		r.placeHolders = []*GoSqlTerm{} // the terms identified alias placeholders by parser
		r.columnixs = []int{}           // index in tuple, where update should happen
		for _, u := range r.updates {
			r.placeHolders = u.term.FindPlaceHolders(r.placeHolders)
			r.terms = append(r.terms, u.term)
			colix, err := r.table.FindColumn(u.Name.Parts[0])
			if err != nil {
				return err
			}
			r.columnixs = append(r.columnixs, colix)
		}
		if r.where != nil {
			r.placeHolders = r.where.FindPlaceHolders(r.placeHolders)
		}
	}
	return nil
}

func (r *GoSqlConnectionLevelRequest) NumInput() int {
	return 0
}

func (r *GoSqlConnectionLevelRequest) Exec(args []Value) (Result, error) {
	switch r.token1 {
	case BEGIN_TOKEN:
		if r.Conn.Transaction != nil && r.Conn.Transaction.IsStarted() {
			return nil, errors.New("transaction already started")
		}
		data.InitTransaction(r.Conn)
	case COMMIT:
		if r.Conn.Transaction == nil || !r.Conn.Transaction.IsStarted() {
			return nil, errors.New("transaction is not started")
		}
		data.EndTransaction(r.Conn, data.COMMITTED)
	case ROLLBACK:
		if r.Conn.Transaction == nil || !r.Conn.Transaction.IsStarted() {
			return nil, errors.New("transaction is not started")
		}
		data.EndTransaction(r.Conn, data.ROLLEDBACK)
	case AUTOCOMMIT:
		switch r.token2 {
		case ON:
			r.Conn.DoAutoCommit = true
		case OFF:
			r.Conn.DoAutoCommit = false
		default:
			return nil, fmt.Errorf("unknown token 2 %d for set autocommit", r.token2)
		}
	default:
		return nil, fmt.Errorf("unknown token 1 %d for connection level requests", r.token1)

	}
	return GoSqlResult{-1, 0}, nil
}

func (r *GoSqlUpdateRequest) NumInput() int {
	err := r.initStruct()
	if err != nil {
		return 0
	} else {
		return len(r.placeHolders)
	}
}

func (r *GoSqlUpdateRequest) Exec(args []Value) (Result, error) {
	err := r.initStruct()
	if err != nil {
		return nil, err
	}
	if len(r.placeHolders) != len(args) {
		return nil, fmt.Errorf("expected %d placeholders, but got %d args", len(r.placeHolders), len(args))
	}
	placeHolderOffset := 0
	commands, err := Terms2Commands(r.terms, args, JoinedRecordsFromTable(r.table), &placeHolderOffset)
	if err != nil {
		return nil, err
	}
	// extend commands by type conversion if necessary.
	for ix, command := range commands {
		resultType := command.resultType
		destType := r.table.Columns()[r.columnixs[ix]].ParserType
		if resultType != destType {
			conversion, err := calcConversion(destType, resultType)
			if err != nil {
				return nil, err
			}
			command.m.AddCommand(conversion)
		}
	}
	whereCommands, err := Terms2Commands([]*GoSqlTerm{r.where}, args, JoinedRecordsFromTable(r.table), &placeHolderOffset)
	if err != nil {
		return nil, err
	}

	results := make([]Value, len(r.columnixs))

	affectedRows := 0
	it := r.table.NewIterator(r.BaseData(), true)
	for {
		tuple, ok, err := it.Next(func(tupleData data.Tuple) (bool, error) {
			res, err := whereCommands[0].m.Execute(args, tupleData, data.NULL_TUPLE)
			if err != nil {
				return false, err
			}
			whereResult, ok := res.(bool)
			if !ok {
				return false, fmt.Errorf("expected boolean expression alias where term")
			}
			return whereResult, nil
		})
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		affectedRows++
		for ix, command := range commands {
			result, err := command.m.Execute(args, tuple, data.NULL_TUPLE)
			if err != nil {
				return nil, err
			}
			results[ix] = result
		}
		resultTuple := tuple.Clone()
		for ix, result := range results {
			resultTuple.SetData(0, r.columnixs[ix], result)
		}
		r.table.Update(tuple.Id(), resultTuple, r.Conn)
	}
	data.EndStatement(&r.StatementBaseData)
	return GoSqlResult{-1, int64(affectedRows)}, nil
}

func (r *GoSqlDeleteRequest) NumInput() int {
	if r.where != nil {
		placeHolders := r.where.FindPlaceHolders([]*GoSqlTerm{})
		return len(placeHolders)
	} else {
		return 0
	}
}

func (r *GoSqlDeleteRequest) Exec(args []Value) (Result, error) {
	table, exists := data.GetTable(r.BaseStatement, r.from[0].TableReferenceLeft.Id)
	if !exists {
		return nil, fmt.Errorf("Unknown Table %v", r.from)
	}
	var placeHolders []*GoSqlTerm
	if r.where != nil {
		placeHolders = r.where.FindPlaceHolders(placeHolders)
	}
	placeHolderOffset := 0
	whereCommands, err := Terms2Commands([]*GoSqlTerm{r.where}, args, JoinedRecordsFromTable(table), &placeHolderOffset)
	if err != nil {
		return nil, err
	}
	affectedRows := 0
	var todelete []int64
	it := table.NewIterator(r.BaseData(), true)
	for {
		tuple, ok, err := it.Next(func(tupleData data.Tuple) (bool, error) {
			res, err := whereCommands[0].m.Execute(args, tupleData, data.NULL_TUPLE)
			if err != nil {
				return false, err
			}
			whereResult, ok := res.(bool)
			if !ok {
				return false, fmt.Errorf("expected boolean expression alias where term")
			}
			return whereResult, nil
		})
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		todelete = append(todelete, tuple.Id())
	}
	slices.Reverse(todelete)
	for _, id := range todelete {
		table.Delete(id, r.Conn)
		affectedRows++
	}
	data.EndStatement(&r.StatementBaseData)
	return GoSqlResult{-1, int64(affectedRows)}, nil
}

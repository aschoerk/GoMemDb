package parser

import (
	"database/sql/driver"
	"fmt"
	"github.com/aschoerk/go-sql-mem/data"
)

// initialized by From-Expression starts delivering Joined View of Record which is base of select-statement
// allows conversion of GoSqlIdentifier to index so that machine can access attributes by index
// Next-Operation iterates through valid
type FromHandler interface {
	Init(*data.BaseStatement, []*GoSqlTableReference)
	GetValue(data.GoSqlIdentifier) driver.Value
	getView() data.Table
}

type IdentifierMapEntry struct {
	colix    int
	joinExpr *TableExpr
}

type GoSqlFromHandler struct {
	baseStmt       data.BaseStatement
	fromSpec       []*GoSqlJoinedTable
	identifierMap  map[*GoSqlTerm]IdentifierMapEntry
	fromExprs      []*FromExpr
	equalJoinParts []equalJoinPart
	joinedRecord   JoinedRecords
	firstIterator  data.TableIterator
}

// describes a table expression in the From-Part including the preceding jointype, if there is one
type TableExpr struct {
	joinType  int        // the token constant of INNER, LEFT OUTER (LEFT), RIGHT OUTER(RIGHT), FULL OUTER(FULL), CROSS
	table     data.Table // the table as defined in the syntax
	isOuter   bool       // set after interpreting the jointype
	joinCols  []int      // columns of this table used for joining
	alias     string     // describes the alias
	condition *GoSqlTerm // the condition behind ON, if there is one
}

// represents the view on the table used for joining, isOuter defines, that table should be outer-joined
// values represents the view records where the first Value in the records represents the internal record-Id (as int64)
// if there is one. The view should be sorted according less_than
type TableViewData struct {
	table   data.Table
	isOuter bool
	cols    []int
	tuples  []data.Tuple
}

// describes one chain of joins
type FromExpr struct {
	tableExprs []*TableExpr
}

func (g *GoSqlFromHandler) identifyTable(tableReference *GoSqlTableReference) (*TableExpr, bool) {
	alias := tableReference.Alias
	table, exists := data.GetTable(g.baseStmt, tableReference.Id)
	return &TableExpr{0, table, false, []int{}, alias, nil}, exists
}

type CascadingIterator struct {
	it   data.TableIterator
	next data.TableIterator
}

func (c CascadingIterator) GetTable() data.Table {
	//TODO implement me
	panic("implement me")
}

func (c CascadingIterator) Next(f func(tuple data.Tuple) (bool, error)) (data.Tuple, bool, error) {
	//TODO implement me
	panic("implement me")
}

func (g *GoSqlFromHandler) Init(selectStatement *GoSqlSelectRequest) []error {
	errs := g.checkAndInitDataStructures(selectStatement)
	var tableRefHandler func(*GoSqlTableReference) = nil
	var firstIterator data.TableIterator
	var currentIterator *CascadingIterator

	iteratorAdder := func(it data.TableIterator) {
		tmp := &CascadingIterator{it, nil}
		if currentIterator != nil {
			currentIterator.next = tmp
			currentIterator = tmp
		} else {
			firstIterator = tmp
			currentIterator = tmp
		}
	}
	checkChain := func(joinedTables []*GoSqlJoinedTable) {
		for _, joinedTable := range joinedTables {
			tableRefHandler(joinedTable.TableReferenceLeft)
			tableRefHandler(joinedTable.TableReferenceRight)
		}
	}
	tableRefHandler = func(tableRef *GoSqlTableReference) {
		if tableRef == nil {
			return
		}
		if tableRef.Id.IsValid() {
			tableExpr, exists := g.identifyTable(tableRef)
			if !exists {
				errs = append(errs, fmt.Errorf("table %s does not exist", tableRef.Id))
				return
			}
			iteratorAdder(tableExpr.table.NewIterator(selectStatement.BaseData(), false))
		}
	}
	g.firstIterator = firstIterator

	checkChain(g.fromSpec)

	return errs
}

package parser

import (
	"database/sql/driver"
	"fmt"
	"github.com/aschoerk/go-sql-mem/data"
)

// identify tables plus aliases
// create instructions to feed iterator
//

type FromPart struct {
	as     string
	table  *data.GoSqlTable
	record data.Tuple
}

// initialized by From-Expression starts delivering Joined View of Record which is base of select-statement
// allows conversion of GoSqlIdentifier to index so that machine can access attributes by index
// Next-Operation iterates through valid
type FromHandler interface {
	Init(*data.BaseStatement, []*GoSqlFromSpec)
	GetValue(data.GoSqlIdentifier) driver.Value
}

type GoSqlFromHandler struct {
	baseStmt data.BaseStatement
	fromSpec []*GoSqlFromSpec
}

type TableData struct {
	table data.Table
	as    string
}

func (g *GoSqlFromHandler) identifyTable(identifier GoSqlAsIdentifier) (TableData, bool) {
	as := identifier.As
	table, exists := data.GetTable(g.baseStmt, identifier.Id)
	return TableData{table, as}, exists
}

func (g *GoSqlFromHandler) Init(baseStmt data.BaseStatement, fromSpec []*GoSqlFromSpec) {
	g.baseStmt = baseStmt
	g.fromSpec = fromSpec
	errs := make([]error, 0)
	var tables = make([]TableData, 0)
	var asDefinitions = make(map[string]TableData)
	var tableMap = make(map[data.Table]data.Table)
	idHandler := func(id GoSqlAsIdentifier) {
		if tableData, exists := g.identifyTable(id); !exists {
			errs = append(errs, fmt.Errorf("table %v does not exist", id))
		} else {
			tables = append(tables, tableData)
			if len(tableData.as) != 0 {
				if _, exists := asDefinitions[tableData.as]; exists {
					errs = append(errs, fmt.Errorf("duplicate as-definition for table %v", tableData.as))
				} else {
					asDefinitions[tableData.as] = tableData
				}
			} else {
				if _, exists := tableMap[tableData.table]; exists {
					errs = append(errs, fmt.Errorf("duplicate table-definition without as for table %v", tableData.table))
				} else {
					tableMap[tableData.table] = tableData.table
				}
			}
		}

	}
	for _, spec := range fromSpec {
		idHandler(spec.Id)
		for _, joinSpec := range spec.JoinSpecs {
			idHandler(joinSpec.JoinedTable)

		}
	}
}

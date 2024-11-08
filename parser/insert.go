package parser

import (
	. "database/sql/driver"
	"errors"
	"fmt"
	"slices"

	"github.com/aschoerk/go-sql-mem/data"
	. "github.com/aschoerk/go-sql-mem/data"
)

type GoSqlInsertRequest struct {
	data.BaseStatement
	tableName          string
	Columns            []string
	values             [][]*GoSqlTerm
	evaluationContexts [][]*EvaluationContext // valid in State Executing, must have the same length alias Table has columns
}

func NewInsertRequest(tableName GoSqlIdentifier, columns []string, values [][]*GoSqlTerm) *GoSqlInsertRequest {
	return &GoSqlInsertRequest{
		data.BaseStatement{data.StatementBaseData{nil, nil, data.Parsed}},
		tableName.Parts[0],
		columns,
		values,
		nil,
	}
}

func (r *GoSqlInsertRequest) NumInput() int {
	result := 0
	for _, val := range r.values {
		for _, v := range val {
			if v.leaf.token == PLACEHOLDER {
				result++
			}
		}
	}
	return result
}

func (r *GoSqlInsertRequest) Exec(args []Value) (Result, error) {
	table, exists := data.GetTable(r.BaseStatement, GoSqlIdentifier{[]string{r.tableName}})
	if !exists {
		return nil, fmt.Errorf("Unknown Table %s", r.tableName)
	} else {
		if r.State == Closed {
			return nil, errors.New("Statement already closed")
		}
		placeHolderOffset := 0
		var insertContexts [][]*EvaluationContext
		var lastInsertedId int64 = -1
		var rowsAffected int64 = 0
		if r.State == Parsed {
			for _, insertvalues := range r.values {
				evaluationContexts := make([]*EvaluationContext, len(table.Columns()))
				evaluationResults, err := Terms2Commands(insertvalues, args, nil, &placeHolderOffset)
				if err != nil {
					return nil, err
				}

				for colix, col := range table.Columns() {
					ix := slices.IndexFunc(r.Columns, func(elem string) bool {
						return elem == col.Name
					})
					if ix >= 0 { // column handled by insertlist
						e := evaluationResults[ix]
						if e.resultType != col.ParserType {
							conversionCommand, err := calcConversion(col.ColType, e.resultType)
							if err != nil {
								return nil, err
							}
							e.m.AddCommand(conversionCommand)
						}
						evaluationContexts[colix] = evaluationResults[ix]
					}
				}
				insertContexts = append(insertContexts, evaluationContexts)
			}
			for _, insertContext := range insertContexts {
				tuple := make([]Value, len(table.Columns()))

				for colix := range tuple {
					executionContext := insertContext[colix]
					if executionContext != nil {
						res, err := executionContext.m.Execute(args, NULL_TUPLE, NULL_TUPLE)
						if err != nil {
							return nil, err
						}
						tuple[colix] = res
					} else {
						columnDef := table.Columns()[colix]
						switch columnDef.Spec2 {
						case PRIMARY_AUTOINCREMENT:
							{
								id := table.(*GoSqlTable).Increment(columnDef.Name)
								tuple[colix] = id
							}
						}
					}
				}
				lastInsertedId = table.(*GoSqlTable).Insert(tuple, r.Conn)
				rowsAffected++
			}
			r.State = Executing
		}
		if r.State == Executing {
			EndStatement(r.BaseData())
			return GoSqlResult{lastInsertedId, rowsAffected}, nil
		}
		return nil, fmt.Errorf("Invalid State %d for insert statement execute", r.State)
	}
}

package parser

import (
	. "database/sql/driver"
	"errors"
	"fmt"
	"slices"

	. "github.com/aschoerk/go-sql-mem/data"
)

type GoSqlInsertRequest struct {
	GoSqlStatementBase
	tableName          string
	Columns            []string
	values             [][]*GoSqlTerm
	evaluationContexts [][]*EvaluationContext // valid in state Executing, must have the same length as Table has columns
}

func NewInsertRequest(tableName string, columns []string, values [][]*GoSqlTerm) *GoSqlInsertRequest {
	return &GoSqlInsertRequest{
		GoSqlStatementBase{Parsed, nil, nil},
		tableName,
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

	table, exists := Tables[r.tableName]
	if !exists {
		return nil, fmt.Errorf("Unknown Table %s", r.tableName)
	} else {
		if r.state == Closed {
			return nil, errors.New("Statement already closed")
		}
		placeHolderOffset := 0
		insertContexts := [][]*EvaluationContext{}
		var lastInsertedId int64 = -1
		var rowsAffected int64 = 0
		if r.state == Parsed {
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
				record := make([]Value, len(table.Columns()))

				for colix, _ := range record {
					executionContext := insertContext[colix]
					if executionContext != nil {
						res, err := executionContext.m.Execute(args, []Value{}, nil)
						if err != nil {
							return nil, err
						}
						record[colix] = res
					} else {
						columnDef := table.Columns()[colix]
						switch columnDef.Spec2 {
						case PRIMARY_AUTOINCREMENT:
							{
								id := table.(*GoSqlTable).Increment(columnDef.Name)
								record[colix] = id
							}
						}
					}
				}
				lastInsertedId = table.(*GoSqlTable).Insert(record, 0)
				rowsAffected++
			}
			r.state = Executing
		}
		if r.state == Executing {
			return GoSqlResult{lastInsertedId, rowsAffected}, nil
		}
		return nil, fmt.Errorf("Invalid state %d for insert statement execute", r.state)
	}
}

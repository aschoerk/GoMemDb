package parser

import (
	. "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"

	"github.com/aschoerk/go-sql-mem/data"
	"github.com/google/uuid"
)

type GoSqlSelectRequest struct {
	data.BaseStatement
	allDistinct int
	selectList  []SelectListEntry
	from        string
	where       *GoSqlTerm
	groupBy     []Value
	having      *GoSqlTerm
	orderBy     []GoSqlOrderBy
}

func (r *GoSqlSelectRequest) Exec(args []Value) (Result, error) {
	panic("not implemented")
}

type SLName struct {
	name   string
	hidden bool
}

func buildSelectList(table data.Table, r *GoSqlSelectRequest) ([]*GoSqlTerm, []SLName, error) {
	terms := []*GoSqlTerm{}
	names := []SLName{}
	for ix, sl := range r.selectList {
		if sl.Asterisk {
			for _, col := range table.Columns() {
				terms = append(terms, &GoSqlTerm{-1, nil, nil, &Ptr{col.Name, IDENTIFIER}})
				names = append(names, SLName{col.Name, false})
			}
		} else {
			if len(sl.Alias) != 0 {
				names = append(names, SLName{sl.Alias, false})
			} else {
				leaf := sl.expression.leaf
				if leaf != nil && leaf.token == IDENTIFIER {
					names = append(names, SLName{leaf.ptr.(string), false})
				} else {
					names = append(names, SLName{strconv.Itoa(ix), false})
				}
			}
			terms = append(terms, sl.expression)
		}
	}
	if r.orderBy != nil {
		for _, o := range r.orderBy {
			_, ok := o.Name.(int)
			if !ok {
				name, ok2 := o.Name.(string)
				if ok2 {
					if !slices.ContainsFunc(names, func(a SLName) bool { return a.name == name }) {
						colix, err := table.FindColumn(name)
						if err != nil {
							return nil, nil, err
						}
						col := table.Columns()[colix]
						terms = append(terms, &GoSqlTerm{-1, nil, nil, &Ptr{col.Name, IDENTIFIER}})
						names = append(names, SLName{col.Name, true})

					}
				} else {
					return nil, nil, fmt.Errorf("Order by column: %s not found in tuple", name)
				}
			}
		}
	}

	return terms, names, nil
}

func (r *GoSqlSelectRequest) Query(args []Value) (Rows, error) {
	table := data.Tables[r.from]
	if r.State == data.Created {
		r.State = data.Parsed
		// determine source-tuple ()
		// create machines
		termNum := len(r.selectList)

		var whereExecutionContext = -1
		var havingExecutionContext = -1
		var sizeSelectList = 0

		terms, names, err := buildSelectList(table, r)
		if err != nil {
			return nil, err
		}

		sizeSelectList = len(terms)
		if r.where != nil {
			terms = append(terms, r.where)
			whereExecutionContext = termNum
			termNum++
		}
		if r.having != nil {
			terms = append(terms, r.having)
			havingExecutionContext = termNum
			termNum++
		}
		placeHolderOffset := 0
		evaluationContexts := []*EvaluationContext{}
		evaluationResults, err := Terms2Commands(terms, args, table, &placeHolderOffset)
		if err != nil {
			return nil, err
		}
		evaluationContexts = append(evaluationContexts, evaluationResults...)
		temptableName, err := createAndFillTempTable(r, evaluationContexts, args, &names, whereExecutionContext, havingExecutionContext, sizeSelectList)
		if err != nil {
			return nil, err
		}
		r.State = data.Executing
		return &GoSqlRows{r, temptableName, &names, 0}, nil
	} else {
		return nil, fmt.Errorf("Invalid Statement State %d, expected 'Parsed'", r.State)
	}
}

func createTempTable(evaluationContexts []*EvaluationContext, names *[]SLName, sizeSelectList int) string {
	cols := []data.GoSqlColumn{}
	for ix, execution := range evaluationContexts {
		if ix < sizeSelectList {
			cols = append(cols, data.GoSqlColumn{(*names)[ix].name, execution.resultType, execution.resultType, 0, 0, (*names)[ix].hidden})
		}
	}
	name := uuid.New().String()
	data.NewTempTable(name, cols)
	return name
}

func createAndFillTempTable(
	query *GoSqlSelectRequest,
	evaluationContexts []*EvaluationContext,
	args []Value,
	names *[]SLName,
	whereExecutionContext int,
	havingExecutionContext int,
	sizeSelectList int) (string, error) {

	name := createTempTable(evaluationContexts, names, sizeSelectList)
	tempTable := data.Tables[name]
	table := data.Tables[query.from]
	tableix := 0

	it := table.NewIterator(query.BaseData(), false)
	for {
		tuple, ok, err := it.Next()
		if err != nil {
			return "", err
		}
		if query.State == data.EndOfRows || !ok {
			query.State = data.EndOfRows
			break
		}
		if whereExecutionContext != -1 {
			whereCheck := evaluationContexts[whereExecutionContext]

			result, err := whereCheck.m.Execute(args, tuple.Data, nil)
			if err != nil {
				return "", err
			}
			if result == nil {
				return "", errors.New("Expected not nil as evaluation of where")
			}
			whereResult, ok := result.(bool)
			if !ok {
				return "", errors.New("Expected boolean as result of evaluation of where")
			}
			if whereResult {
				err := calcTuple(tempTable, args, evaluationContexts, tuple.Data, sizeSelectList)
				if err != nil {
					return "", err
				}
			}
		} else {
			err := calcTuple(tempTable, args, evaluationContexts, tuple.Data, sizeSelectList)
			if err != nil {
				return "", err
			}
		}
		tableix++
	}
	if query.orderBy != nil {
		e, err := OrderBy2Commands(&query.orderBy, tempTable)
		if err != nil {
			return "", err
		}
		slices.SortFunc(
			*tempTable.Data(),
			func(a, b []Value) int {
				res, err := e.m.Execute(args, a, b)
				if err != nil {
					panic(err.Error())
				}
				return res.(int)
			},
		)
	}
	return name, nil
}

func calcTuple(tempTable data.Table, args []Value, evaluationContexts []*EvaluationContext, tuple []Value, sizeSelectList int) error {
	destTuple := []Value{}
	for ix, execution := range evaluationContexts {
		if ix < sizeSelectList {
			res, err := execution.m.Execute(args, tuple, nil)
			if err != nil {
				return err
			} else {
				destTuple = append(destTuple, res)
			}
		}
	}
	*tempTable.Data() = append(*tempTable.Data(), destTuple)
	return nil
}

type GoSqlRows struct {
	query         *GoSqlSelectRequest
	temptableName string
	names         *[]SLName
	tableix       int
}

func (rows *GoSqlRows) Columns() []string {
	res := []string{}
	for _, el := range *rows.names {
		if !el.hidden {
			res = append(res, el.name)
		}
	}
	return res
}

func (rows *GoSqlRows) ResultTable() data.Table {
	table := data.Tables[rows.temptableName]
	return table
}

func (rows *GoSqlRows) Close() error {
	delete(data.Tables, rows.temptableName)
	return nil
}

func (rows *GoSqlRows) Next(dest []Value) error {
	table := data.Tables[rows.temptableName]
	if len(*table.Data()) <= rows.tableix {
		rows.query.State = data.EndOfRows
		return io.EOF
	}
	destix := 0
	for ix, el := range (*table.Data())[rows.tableix] {
		if !table.Columns()[ix].Hidden {
			if destix > len(dest) {
				return errors.New("dest can not hold al result values")
			}
			dest[destix] = el
			destix++
		}
	}
	rows.tableix++
	return nil
}

func (r *GoSqlSelectRequest) NumInput() int {
	placeholders := FindPlaceHoldersInSelect(r)
	return len(placeholders)
}

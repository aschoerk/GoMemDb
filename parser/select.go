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
	groupBy     []*GoSqlTerm
	having      *GoSqlTerm
	orderBy     []GoSqlOrderBy
	forupdate   int
}

func (r *GoSqlSelectRequest) Exec(args []Value) (Result, error) {
	panic("not implemented")
}

type SLName struct {
	name   string
	hidden bool
}

func isAggregation(token int) bool {
	return token == COUNT || token == AVG || token == SUM || token == MIN || token == MAX
}

func extractAggregation(t *GoSqlTerm) ([]*GoSqlTerm, bool) {
	if isAggregation(t.operator) {
		return []*GoSqlTerm{t}, false
	}
	usesIdentifiers := t.operator == IDENTIFIER
	var res []*GoSqlTerm = nil
	if t.left != nil {
		restmp, usesIdentifiersTmp := extractAggregation(t.left)
		usesIdentifiers = usesIdentifiers || usesIdentifiersTmp
		if restmp != nil {
			res = append(res, restmp...)
		}
	}
	if t.right != nil {
		restmp, usesIdentifiersTmp := extractAggregation(t.right)
		usesIdentifiers = usesIdentifiers || usesIdentifiersTmp
		if restmp != nil {
			res = append(res, restmp...)
		}
	}
	return res, usesIdentifiers
}

type AggregationTerm struct {
	sl               *SelectListEntry
	aggregationTerms []*GoSqlTerm
	tmpSlNames       []string
}

func collectAggregation(r *GoSqlSelectRequest) ([]AggregationTerm, error) {
	aggregationsTerms := []AggregationTerm{}
	for ix, sl := range r.selectList {
		res, usesIdentifiers := extractAggregation(sl.expression)
		if res != nil && len(res) > 0 {
			if usesIdentifiers {
				return nil, errors.New("illegal use of attributes and aggregationfunctions together")
			}
			names := []string{}
			for ix2, _ := range res {
				names = append(names, fmt.Sprintf("agg%d_%d", ix, ix2))
			}
			aggregationsTerms = append(aggregationsTerms, AggregationTerm{&sl, res, names})
		} else {
			aggregationsTerms = append(aggregationsTerms, AggregationTerm{&sl, nil, []string{fmt.Sprintf("const_%d", ix)}})
		}
	}
	return aggregationsTerms, nil

}

// to support aggregation
// scan for aggregation function
// if one select list entry uses:
//
//	all select list entry must be aggregations or constant expressions
//	arguments of aggregations build the select list for the first temporary table. * stands for id (internal)
//	the aggregation functions are evaluated over the corresponding select list entry
//	the term around the aggregation is evaluated to create together with the constant entries the new result of one record.
func buildAggregationSelectList(r *GoSqlSelectRequest) ([]*GoSqlTerm, []SLName, []AggregationTerm, error) {
	aggregationTerms, err := collectAggregation(r)
	if err != nil {
		return nil, nil, nil, err
	}
	res := []*GoSqlTerm{}
	resNames := []SLName{}
	for _, aggTerm := range aggregationTerms {
		if aggTerm.aggregationTerms != nil {
			for ix, _ := range aggTerm.aggregationTerms {
				t := aggTerm.aggregationTerms[ix]
				name := aggTerm.tmpSlNames[ix]
				if t.left.operator != ASTERISK {
					res = append(res, t.left.left)
					resNames = append(resNames, SLName{name, false})
				} else {
					if t.operator != COUNT {
						return nil, nil, nil, errors.New("expecting only COUNT as function if parameter is asterisk")
					} else {
						res = append(res, &GoSqlTerm{IDENTIFIER, nil, nil, &Ptr{data.VersionedRecordId, IDENTIFIER}})
						resNames = append(resNames, SLName{name, false})
					}
				}
			}
		}
	}
	return res, resNames, aggregationTerms, nil
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

		terms, names, _, err := buildAggregationSelectList(r)
		if err != nil {
			return nil, err
		}
		if terms == nil || len(terms) == 0 {
			terms, names, err = buildSelectList(table, r)
			if err != nil {
				return nil, err
			}
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
		temptableName, err := createAndFillTempTable(r, evaluationContexts, args, &names, whereExecutionContext, havingExecutionContext, sizeSelectList, r.forupdate)
		if err != nil {
			return nil, err
		}
		r.State = data.Executing
		return &GoSqlRows{r, temptableName, &names, 0}, nil
	} else {
		return nil, fmt.Errorf("invalid statement state %d, expected 'Parsed'", r.State)
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
	sizeSelectList int,
	forUpdate int) (string, error) {

	name := createTempTable(evaluationContexts, names, sizeSelectList)
	tempTable := data.Tables[name]
	table := data.Tables[query.from]

	it := table.NewIterator(query.BaseData(), forUpdate == FOR)
	for {
		tuple, ok, err := it.Next(func(tupleData []Value) (bool, error) {
			if whereExecutionContext != -1 {
				whereCheck := evaluationContexts[whereExecutionContext]

				result, err := whereCheck.m.Execute(args, data.Tuple{Id: -1, Data: tupleData}, data.NULL_TUPLE)
				if err != nil {
					return false, err
				}
				if result == nil {
					return false, errors.New("expected not nil as evaluation of where")
				}
				whereResult, ok := result.(bool)
				if ok {
					return whereResult, nil
				} else {
					return false, errors.New("expected bool result from where")
				}
			} else {
				return true, nil
			}
		})
		if err != nil {
			return "", err
		}
		if query.State == data.EndOfRows || !ok {
			query.State = data.EndOfRows
			break
		}
		err = calcTuple(tempTable, args, evaluationContexts, tuple, sizeSelectList)
		if err != nil {
			return "", err
		}
	}
	if query.orderBy != nil {
		e, err := OrderBy2Commands(&query.orderBy, tempTable)
		if err != nil {
			return "", err
		}
		slices.SortFunc(
			*tempTable.Data(),
			func(a, b []Value) int {
				res, err := e.m.Execute(args, data.Tuple{-1, a}, data.Tuple{-1, b})
				if err != nil {
					panic(err.Error())
				}
				return res.(int)
			},
		)
	}
	return name, nil
}

func calcTuple(tempTable data.Table, args []Value, evaluationContexts []*EvaluationContext, tuple data.Tuple, sizeSelectList int) error {
	destTuple := []Value{}
	for ix, execution := range evaluationContexts {
		if ix < sizeSelectList {
			res, err := execution.m.Execute(args, tuple, data.NULL_TUPLE)
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

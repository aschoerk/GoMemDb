package parser

import (
	. "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/aschoerk/go-sql-mem/data"
)

type GoSqlSelectRequest struct {
	data.BaseStatement
	allDistinct int
	selectList  []SelectListEntry
	from        []*GoSqlFromSpec
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

type AggTermsBySelectListEntry struct {
	sl               *SelectListEntry
	aggregationTerms []*GoSqlTerm
	tmpSlNames       []string
}

func isAggregation(token int) bool {
	return token == COUNT || token == AVG || token == SUM || token == MIN || token == MAX
}

func extractAggregation(t *GoSqlTerm) ([]*GoSqlTerm, bool) {
	if isAggregation(t.operator) {
		return []*GoSqlTerm{t}, false
	}
	usesIdentifiers := false
	if t.leaf != nil {
		usesIdentifiers = t.leaf.token == IDENTIFIER
	}
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

func collectAggregation(r *GoSqlSelectRequest) ([]AggTermsBySelectListEntry, error) {
	aggregationsTerms := []AggTermsBySelectListEntry{}
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
			aggregationsTerms = append(aggregationsTerms, AggTermsBySelectListEntry{&sl, res, names})
		} else {
			if !usesIdentifiers {
				aggregationsTerms = append(aggregationsTerms, AggTermsBySelectListEntry{&sl, nil, []string{fmt.Sprintf("const_%d", ix)}})
			}
		}
	}
	return aggregationsTerms, nil

}

// to support aggregation
// scan for aggregation function
// if one select list entry uses:
//
//	all select list entry must be aggregations or constant expressions
//	arguments of aggregations build the select list for the first temporary joinExpr. * stands for id (internal)
//	the aggregation functions are evaluated over the corresponding select list entry
//	the term around the aggregation is evaluated to create together with the constant entries the new result of one record.
func buildAggregationSelectList(r *GoSqlSelectRequest) ([]*GoSqlTerm, []SLName, []AggTermsBySelectListEntry, error) {
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
						return nil, nil, nil, errors.New("expecting only COUNT alias function if parameter is asterisk")
					} else {
						res = append(res, &GoSqlTerm{-1, nil, nil, &Ptr{data.GoSqlIdentifier{[]string{data.VersionedRecordId}}, IDENTIFIER}})
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
				terms = append(terms, &GoSqlTerm{-1, nil, nil, &Ptr{data.GoSqlIdentifier{[]string{col.Name}}, IDENTIFIER}})
				names = append(names, SLName{col.Name, false})
			}
		} else {
			if len(sl.Alias) != 0 {
				names = append(names, SLName{sl.Alias, false})
			} else {
				leaf := sl.expression.leaf
				if leaf != nil && leaf.token == IDENTIFIER {
					names = append(names, SLName{leaf.ptr.(data.GoSqlIdentifier).Parts[0], false})
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
				name, ok2 := o.Name.(data.GoSqlIdentifier)
				if ok2 {
					if !slices.ContainsFunc(names, func(a SLName) bool { return a.name == name.Parts[0] }) {
						colix, err := table.FindColumn(name.Parts[0])
						if err != nil {
							return nil, nil, err
						}
						col := table.Columns()[colix]
						terms = append(terms, &GoSqlTerm{-1, nil, nil, &Ptr{data.GoSqlIdentifier{[]string{col.Name}}, IDENTIFIER}})
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
	if r.State == data.Created {
		table, _ := data.GetTable(r.BaseStatement, r.from[0].Id.Id)
		r.State = data.Parsed
		// determine source-tuple ()
		// create machines
		termNum := len(r.selectList)

		var whereExecutionContext = -1
		var havingExecutionContext = -1
		var sizeSelectList = 0

		terms, names, aggTermsBySelectListEntry, err := buildAggregationSelectList(r)
		if err != nil {
			return nil, err
		}
		if len(aggTermsBySelectListEntry) == 0 {
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
		temptable, err := r.createAndFillTempTable(r, evaluationContexts, args, &names, whereExecutionContext, havingExecutionContext, sizeSelectList, r.forupdate)
		if err != nil {
			return nil, err
		}

		if len(aggTermsBySelectListEntry) == 0 {
			r.State = data.Executing
			return &GoSqlRows{r, temptable.Name(), &names, 0}, nil
		} else {
			table := temptable
			terms := []*GoSqlTerm{}
			names := []SLName{}

			aggTermCount := 0
			for ix, aTerm := range aggTermsBySelectListEntry {
				for _, term := range aTerm.aggregationTerms {
					switch term.operator {
					case COUNT:
						err := doCount(table, aggTermCount, term)
						if err != nil {
							return nil, err
						}
					case SUM:
						err := doSum(table, aggTermCount, term)
						if err != nil {
							return nil, err
						}
					case AVG:
						err := doAvg(table, aggTermCount, term)
						if err != nil {
							return nil, err
						}
					case MIN:
						err := doMin(table, aggTermCount, term)
						if err != nil {
							return nil, err
						}
					case MAX:
						err := doMax(table, aggTermCount, term)
						if err != nil {
							return nil, err
						}
					}
				}
				terms = append(terms, aTerm.sl.expression)
				names = append(names, SLName{fmt.Sprintf("%d", ix), false})
			}
			evaluationResults, err := Terms2Commands(terms, args, nil, nil) // TODO: fix placeholderhandling when group or agg
			if err != nil {
				return nil, err
			}
			aggTmpTable := createTempTable(evaluationResults, &names, len(names))
			resTuple := []Value{}
			for _, ev := range evaluationResults {
				res, err := ev.m.Execute(args, data.NULL_TUPLE, data.NULL_TUPLE)
				if err != nil {
					return nil, err
				}
				resTuple = append(resTuple, res)
			}
			tempTable := aggTmpTable
			*tempTable.Data() = append(*tempTable.Data(), resTuple)
			r.State = data.Executing
			return &GoSqlRows{r, aggTmpTable.Name(), &names, 0}, nil
		}

	} else {
		return nil, fmt.Errorf("invalid statement state %d, expected 'Parsed'", r.State)
	}
}

func iterateAggregation(table data.Table, aggTermCount int, term *GoSqlTerm, aggregate func(value Value) Value) error {
	it := table.NewIterator(nil, false)
	distinctSet := make(map[interface{}]interface{})
	checkSet := term.left.operator == DISTINCT
	coltype := table.Columns()[aggTermCount].ColType
	var actValue Value
	for {
		tuple, found, err := it.Next(func(tupleData []Value) (bool, error) { return true, nil })
		if err != nil {
			return err
		}
		if !found {
			break
		}
		value := tuple.Data[aggTermCount]

		if value != nil {
			if checkSet {
				setKey := value
				switch coltype {
				case TIMESTAMP:
					setKey = value.(time.Time).UnixNano
				case FLOAT:
					setKey = math.Round(value.(float64)*1000000) / 1000000
				}
				if distinctSet[value] == nil {
					distinctSet[setKey] = value
				} else {
					continue
				}
			}
			actValue = aggregate(value)
		}
	}
	term.operator = -1
	term.left = nil
	term.right = nil
	term.leaf = &Ptr{actValue, coltype}
	return nil
}

func doMin(table data.Table, aggTermCount int, term *GoSqlTerm) error {
	coltype := table.Columns()[aggTermCount].ColType

	minIsSet := false
	var err error = nil
	switch coltype {
	case INTEGER:
		min := int64(0)
		err = iterateAggregation(table, aggTermCount, term, func(value Value) Value {
			x := value.(int64)
			if !minIsSet {
				min = x
				minIsSet = true
			} else {
				if x < min {
					min = x
				}
			}
			return min
		})
	case FLOAT:
		minValue := float64(0)
		err = iterateAggregation(table, aggTermCount, term, func(value Value) Value {
			x := value.(float64)
			if !minIsSet {
				minValue = x
				minIsSet = true
			} else {
				if x < minValue {
					minValue = x
				}
			}
			return minValue
		})
	case STRING:
		min := ""
		err = iterateAggregation(table, aggTermCount, term, func(value Value) Value {
			x := value.(string)
			if !minIsSet {
				min = x
				minIsSet = true
			} else {
				if x < min {
					min = x
				}
			}
			return min
		})
	case TIMESTAMP:
		min := time.Unix(0, 0)
		err = iterateAggregation(table, aggTermCount, term, func(value Value) Value {
			x := value.(time.Time)
			if !minIsSet {
				min = x
				minIsSet = true
			} else {
				if x.Before(min) {
					min = x
				}
			}
			return min
		})
	default:
		return errors.New("column type not usable for aggregation")
	}
	return err
}

func doMax(table data.Table, aggTermCount int, term *GoSqlTerm) error {
	coltype := table.Columns()[aggTermCount].ColType

	maxIsSet := false
	var err error = nil
	switch coltype {
	case INTEGER:
		maxValue := int64(0)
		err = iterateAggregation(table, aggTermCount, term, func(value Value) Value {
			x := value.(int64)
			if !maxIsSet {
				maxValue = x
				maxIsSet = true
			} else {
				if x > maxValue {
					maxValue = x
				}
			}
			return maxValue
		})
	case FLOAT:
		max := float64(0)
		err = iterateAggregation(table, aggTermCount, term, func(value Value) Value {
			x := value.(float64)
			if !maxIsSet {
				max = x
				maxIsSet = true
			} else {
				if x > max {
					max = x
				}
			}
			return max
		})
	case STRING:
		max := ""
		err = iterateAggregation(table, aggTermCount, term, func(value Value) Value {
			x := value.(string)
			if !maxIsSet {
				max = x
				maxIsSet = true
			} else {
				if x > max {
					max = x
				}
			}
			return max
		})
	case TIMESTAMP:
		max := time.Unix(0, 0)
		err = iterateAggregation(table, aggTermCount, term, func(value Value) Value {
			x := value.(time.Time)
			if !maxIsSet {
				max = x
				maxIsSet = true
			} else {
				if x.After(max) {
					max = x
				}
			}
			return max
		})
	default:
		return errors.New("column type not usable for aggregation")
	}
	return err
}

func doSum(table data.Table, aggTermCount int, term *GoSqlTerm) error {
	coltype := table.Columns()[aggTermCount].ColType

	switch coltype {
	case INTEGER:
		sum := int64(0)
		err := iterateAggregation(table, aggTermCount, term, func(value Value) Value {
			sum += value.(int64)
			return sum
		})
		if err != nil {
			return err
		}
	case FLOAT:
		sum := float64(0)
		err := iterateAggregation(table, aggTermCount, term, func(value Value) Value {
			sum += value.(float64)
			return sum
		})
		if err != nil {
			return err
		}
	default:
		return errors.New("column type not usable for aggregation")
	}
	return nil
}

func doAvg(table data.Table, aggTermCount int, term *GoSqlTerm) error {
	coltype := table.Columns()[aggTermCount].ColType
	n := 0

	switch coltype {
	case INTEGER:
		sum := int64(0)
		err := iterateAggregation(table, aggTermCount, term, func(value Value) Value {
			n++
			sum += value.(int64)
			return float64(sum) / float64(n)
		})
		if err != nil {
			return err
		}
	case FLOAT:
		sum := float64(0)
		err := iterateAggregation(table, aggTermCount, term, func(value Value) Value {
			n++
			sum += value.(float64)
			return sum / float64(n)
		})
		if err != nil {
			return err
		}
	default:
		return errors.New("column type not usable for aggregation")
	}
	return nil
}

func doCount(table data.Table, aggTermCount int, term *GoSqlTerm) error {
	count := 0
	tmpTableLen := len(*table.Data())
	if term.left.operator == ASTERISK {
		count = tmpTableLen
		term.operator = -1
		term.left = nil
		term.right = nil
		term.leaf = &Ptr{count, INTEGER}
	} else {
		err := iterateAggregation(table, aggTermCount, term, func(value Value) Value {
			if value != nil {
				count++
			}
			return count
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func createTempTable(evaluationContexts []*EvaluationContext, names *[]SLName, sizeSelectList int) data.Table {
	cols := []data.GoSqlColumn{}
	for ix, execution := range evaluationContexts {
		if ix < sizeSelectList {
			cols = append(cols, data.GoSqlColumn{(*names)[ix].name, execution.resultType, execution.resultType, 0, 0, (*names)[ix].hidden})
		}
	}
	return data.NewTempTable(cols)
}

func (r *GoSqlSelectRequest) createAndFillTempTable(
	query *GoSqlSelectRequest,
	evaluationContexts []*EvaluationContext,
	args []Value,
	names *[]SLName,
	whereExecutionContext int,
	havingExecutionContext int,
	sizeSelectList int,
	forUpdate int) (data.Table, error) {

	tempTable := createTempTable(evaluationContexts, names, sizeSelectList)
	table, _ := data.GetTable(r.BaseStatement, query.from[0].Id.Id)

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
					return false, errors.New("expected not nil alias evaluation of where")
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
			return nil, err
		}
		if query.State == data.EndOfRows || !ok {
			query.State = data.EndOfRows
			break
		}
		err = calcTuple(tempTable, args, evaluationContexts, tuple, sizeSelectList)
		if err != nil {
			return nil, err
		}
	}
	if query.orderBy != nil {
		e, err := OrderBy2Commands(&query.orderBy, tempTable)
		if err != nil {
			return nil, err
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
	return tempTable, nil
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
	table := data.GetTempTable(rows.temptableName)
	return table
}

func (rows *GoSqlRows) Close() error {
	data.DeleteTempTable(rows.temptableName)
	return nil
}

func (rows *GoSqlRows) Next(dest []Value) error {
	table := data.GetTempTable(rows.temptableName)
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

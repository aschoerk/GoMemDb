package parser

import (
	"database/sql/driver"
	"fmt"
	"github.com/aschoerk/go-sql-mem/data"
	"slices"
	"sort"
	"time"
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
	getView() data.Table
}

type IdentifierMapEntry struct {
	colix    int
	joinExpr *TableExpr
}

type GoSqlFromHandler struct {
	baseStmt       data.BaseStatement
	fromSpec       []*GoSqlFromSpec
	identifierMap  map[*GoSqlTerm]IdentifierMapEntry
	fromExprs      []*FromExpr
	equalJoinParts []equalJoinPart
	joinedRecord   JoinedRecord
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
	tuples  []*data.Tuple
}

// describes one chain of joins
type FromExpr struct {
	tableExprs []*TableExpr
}

func (g *GoSqlFromHandler) identifyTable(identifier GoSqlAsIdentifier) (*TableExpr, bool) {
	alias := identifier.Alias
	table, exists := data.GetTable(g.baseStmt, identifier.Id)
	return &TableExpr{0, table, false, []int{}, alias, nil}, exists
}

func (g *GoSqlFromHandler) Init(selectStatement *GoSqlSelectRequest) []error {
	errs := g.checkAndInitDataStructures(selectStatement)
	if len(errs) == 0 {
		done, errs := g.handlePureEqualJoins()
		if done {
			return errs
		} else {
			// TODO: handle cross joins, handle NON-Equal-Joins
		}
	}
	return errs
}

func lessThan(tableViewData, btableViewData *TableViewData, a, b *data.Tuple) bool {
	for ix := 0; ix < len(tableViewData.cols); ix++ {
		v := a.Data[tableViewData.cols[ix]]
		w := b.Data[btableViewData.cols[ix]]
		if v != w {
			switch v.(type) {
			case int64:
				return v.(int64) < w.(int64)
			case float64:
				return v.(float64) < w.(float64)
			case string:
				return v.(string) < w.(string)
			case bool:
				return !v.(bool)
			case time.Time:
				return v.(time.Time).Before(w.(time.Time))
			default:
				panic("unsupported type")
			}
		}
	}
	return false
}

// sort TableViewData
func sortView(arr *TableViewData) {
	sort.Slice(arr.tuples, func(i, j int) bool {
		a := arr.tuples[i]
		b := arr.tuples[j]
		return lessThan(arr, arr, a, b)
	})
}

// creates the TableViewData for a table, as returned by the TableIterator (so should fit to the current snapshot)
// sorted!
func getTableView(cols []int, isOuter bool, it data.TableIterator) (*TableViewData, error) {
	var tuples = make([]*data.Tuple, 0)
	for {
		tuple, found, err := it.Next(func(value []driver.Value) (bool, error) {
			return true, nil
		})
		if err != nil {
			return nil, err
			break
		}
		if !found {
			break
		}
		view := make([]driver.Value, len(cols)+1)
		view[0] = tuple.Id
		for i, col := range cols {
			view[i+1] = tuple.Data[col]
		}
		tuples = append(tuples, &tuple)
	}
	res := &TableViewData{it.GetTable(), isOuter, cols, tuples}
	sortView(res)
	return res, nil
}

// given two tables joined by an simple equal-join over arbitrary columns.
func createIdView(left, right *TableViewData) [][]*data.Tuple {
	res := make([][]*data.Tuple, 0)
	rightLen, leftLen := len(right.tuples), len(left.tuples)
	if rightLen == 0 || leftLen == 0 {
		return res
	}
	rix, lix := 0, 0

	pair := func(left, right *data.Tuple) []*data.Tuple {
		return []*data.Tuple{left, right}
	}

	nullTuple := &data.Tuple{Id: -1}

	for lix < leftLen && rix < rightLen {
		leftValue, rightValue := left.tuples[lix], right.tuples[rix]

		switch {
		case lessThan(left, right, leftValue, rightValue):
			if left.isOuter {
				res = append(res, pair(leftValue, nullTuple))
			}
			lix++
		case lessThan(right, left, rightValue, leftValue):
			if right.isOuter {
				res = append(res, pair(nullTuple, rightValue))
			}
			rix++
		default: // values are equal
			// Handle all equal values
			rightStart := rix
			for {
				for rix < rightLen && !lessThan(left, right, leftValue, right.tuples[rix]) {
					res = append(res, pair(leftValue, right.tuples[rix]))
					rix++
				}
				lix++ // move to next left, if equal to current, reset rix
				if lix < leftLen {
					leftValue = left.tuples[lix]
					if !lessThan(left, right, leftValue, right.tuples[rightStart]) && !lessThan(right, left, right.tuples[rightStart], leftValue) {
						rix = rightStart
					} else {
						break
					}
				} else {
					break
				}
			}
		}
	}

	// Handle remaining left values (outer join)
	if left.isOuter {
		for ; lix < leftLen; lix++ {
			res = append(res, pair(left.tuples[lix], nullTuple))
		}
	}

	// Handle remaining right values (outer join)
	if right.isOuter {
		for ; rix < rightLen; rix++ {
			res = append(res, pair(nullTuple, right.tuples[rix]))
		}
	}

	return res
}

func (g *GoSqlFromHandler) handlePureEqualJoins() (bool, []error) {
	b, errors := g.isPureEqualJoin()
	if !b {
		return b, errors
	}
	// check if all SourceTables are really restricted
	// what happens if a record is used more than once for a join
	// what happens if more than one attribute is used: decide about concatenated key.
	pairs := g.createPairs()
	errors = g.enhanceEqualJoinByIdPairs(pairs, errors)
	g.createJoinedRecord(pairs)
	// search equal joins
	return false, errors
}

func mergeToRecords(joinedRecord *JoinedRecord, matchColumn int, pairs [][]*data.Tuple, pairMatchColumn int) {
	records := joinedRecord.records
	leftLen, rightLen := len(records), len(pairs)
	sort.Slice(records, func(i, j int) bool {
		return records[i][matchColumn].Id < records[j][matchColumn].Id
	})
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i][pairMatchColumn].Id < pairs[j][pairMatchColumn].Id
	})
	lix, rix := 0, 0
	for lix < leftLen && rix < rightLen {
		leftValue, rightValue := records[lix][matchColumn], pairs[rix][pairMatchColumn]
		switch {
		case leftValue.Id == rightValue.Id:
			firstLeftEntry := records[lix]
			records[lix] = append(firstLeftEntry, pairs[rix][1-pairMatchColumn])
			rix++
			for rix < rightLen && pairs[rix][pairMatchColumn] == leftValue {
				records = append(records, append(firstLeftEntry, pairs[rix][1-pairMatchColumn]))
				rix++
			}
			lix++
		case leftValue.Id < rightValue.Id:
			// no matching, remove if no outer join by this table, else keep
			lix++
		case rightValue.Id > leftValue.Id:
			// no matching of the pair ignore if there is no outer join
			rix++
		}
	}
	nullTuple := &data.Tuple{Id: -1}
	newRecords := [][]*data.Tuple{}
	for _, record := range records {
		if len(record) == leftLen {
			var ix int
			for ix = 0; ix < leftLen; ix++ {
				if record[ix].Id != -1 && joinedRecord.joinExpr[ix].isOuter {
					newRecords = append(newRecords, append(record, nullTuple))
					break
				}
			}
		} else {
			newRecords = append(newRecords, record)
		}
	}
	joinedRecord.records = newRecords
}

func (g *GoSqlFromHandler) createJoinedRecord(pairs []*equalJoin) {
	res := JoinedRecord{[]*TableExpr{}, [][]*data.Tuple{}}
	for _, pair := range pairs {
		left := pair.exprs[0].left.joinExpr
		if !slices.Contains(res.joinExpr, left) {
			res.joinExpr = append(res.joinExpr, left)
		}
		right := pair.exprs[0].right.joinExpr
		if !slices.Contains(res.joinExpr, right) {
			res.joinExpr = append(res.joinExpr, right)
		}
	}
	first := true
	for _, pair := range pairs {
		if first {
			first = false
			for _, v := range pair.idView {
				res.records = append(res.records, v)
			}
		} else {
			// identify matching column
			ix := slices.Index(res.joinExpr, pair.exprs[0].left.joinExpr)
			if ix < 0 {
				ix = slices.Index(res.joinExpr, pair.exprs[0].right.joinExpr)
				mergeToRecords(&res, ix, pair.idView, 1)
			} else {
				mergeToRecords(&res, ix, pair.idView, 0)
			}
		}
	}
	g.joinedRecord = res
}

type JoinedRecord struct {
	joinExpr []*TableExpr
	records  [][]*data.Tuple
}

func (g *GoSqlFromHandler) enhanceEqualJoinByIdPairs(pairs []*equalJoin, errors []error) []error {
	for _, pair := range pairs {
		leftIx := []int{}
		rightIx := []int{}
		for _, join := range pair.exprs {
			leftIx = append(leftIx, join.left.colix)
			rightIx = append(rightIx, join.right.colix)
		}

		part := pair.exprs[0]
		leftTableView, err := getTableView(leftIx, part.left.joinExpr.isOuter, part.left.joinExpr.table.NewIterator(g.baseStmt.BaseData(), false))
		if err != nil {
			errors = append(errors, err)
		} else {
			rightTableView, err := getTableView(rightIx, part.right.joinExpr.isOuter, part.right.joinExpr.table.NewIterator(g.baseStmt.BaseData(), false))
			if err != nil {
				errors = append(errors, err)
			} else {
				pair.idView = createIdView(leftTableView, rightTableView)
			}
		}

		// find ind
		// create sorted list of entries in le
	}
	return errors
}

func (g *GoSqlFromHandler) createPairs() []*equalJoin {
	perPair := [][]equalJoinPart{}
	for _, join := range g.equalJoinParts {
		found := false
		for ix, pairs := range perPair {
			if len(pairs) > 0 {
				left := pairs[0].left.joinExpr
				right := pairs[0].right.joinExpr
				if left == join.left.joinExpr && right == join.right.joinExpr {
					perPair[ix] = append(perPair[ix], join)
					found = true
					break
				} else if left == join.right.joinExpr && right == join.left.joinExpr {
					perPair[ix] = append(perPair[ix], equalJoinPart{join.right, join.left})
					found = true
					break
				}
			}
		}
		if !found {
			perPair = append(perPair, []equalJoinPart{join})
		}
	}
	res := make([]*equalJoin, len(perPair))
	for pairix, pair := range perPair {
		res[pairix] = &equalJoin{pair, nil}
	}
	return res
}

type equalJoinPart struct {
	left  IdentifierMapEntry
	right IdentifierMapEntry
}

type equalJoin struct {
	exprs  []equalJoinPart
	idView [][]*data.Tuple
}

func (g *GoSqlFromHandler) onlyEqualJoinExpression(term *GoSqlTerm) bool {
	if term.left != nil && term.right != nil {
		if term.operator == AND {
			leftOk := g.onlyEqualJoinExpression(term.left)
			if leftOk {
				return g.onlyEqualJoinExpression(term.right)
			} else {
				return false
			}
		} else if term.operator == EQUAL {
			leftIdentifierData, ok := g.identifierMap[term.left]
			if !ok {
				return false
			}
			rightIdentifierData, ok := g.identifierMap[term.right]
			if leftIdentifierData.joinExpr != rightIdentifierData.joinExpr {
				// Equal Join using different bases (even if in case of selfjoin the table might be the same)
				g.equalJoinParts = append(g.equalJoinParts, equalJoinPart{leftIdentifierData, rightIdentifierData})
				return true
			} else {
				return false
			}
		} else {
			return false
		}
	} else if term.left != nil || term.right != nil {
		return false
	} else {
		return term.leaf.token == IDENTIFIER
	}
}

func (g *GoSqlFromHandler) isPureEqualJoin() (bool, []error) {
	if len(g.fromExprs) > 1 {
		return false, nil
	}
	for ix := 1; ix < len(g.fromExprs[0].tableExprs); ix++ {
		expr := g.fromExprs[0].tableExprs[ix]
		if expr.joinType == CROSS {
			return false, nil
		}
		if !g.onlyEqualJoinExpression(expr.condition) {
			return false, nil
		}
	}
	return true, nil
}

func (g *GoSqlFromHandler) checkAndInitDataStructures(selectStatement *GoSqlSelectRequest) []error {
	g.baseStmt = selectStatement.BaseStatement
	g.fromSpec = selectStatement.from
	errs := make([]error, 0)
	g.fromExprs = make([]*FromExpr, 0)
	var asDefinitions = make(map[string]*TableExpr)
	var tableMap = make(map[data.Table]data.Table)
	idHandler := func(id GoSqlAsIdentifier) *TableExpr {
		if joinExpr, exists := g.identifyTable(id); !exists {
			errs = append(errs, fmt.Errorf("joinExpr %v does not exist", id))
			return nil
		} else {
			fromExpr := g.fromExprs[len(g.fromExprs)-1]
			fromExpr.tableExprs = append(fromExpr.tableExprs, joinExpr)
			if len(joinExpr.alias) != 0 {
				if _, exists := asDefinitions[joinExpr.alias]; exists {
					errs = append(errs, fmt.Errorf("duplicate alias-definition for joinExpr %v", joinExpr.alias))
				} else {
					asDefinitions[joinExpr.alias] = joinExpr
				}
			} else {
				if _, exists := tableMap[joinExpr.table]; exists {
					errs = append(errs, fmt.Errorf("duplicate joinExpr-definition without alias for joinExpr %v", joinExpr.table))
				} else {
					tableMap[joinExpr.table] = joinExpr.table
				}
			}
			return joinExpr
		}
	}

	g.identifierMap = make(map[*GoSqlTerm]IdentifierMapEntry)
	for _, spec := range g.fromSpec {
		g.fromExprs = append(g.fromExprs, &FromExpr{[]*TableExpr{}})
		idHandler(spec.Id)
		for _, joinSpec := range spec.JoinSpecs {
			joinExpr := idHandler(joinSpec.JoinedTable)
			if joinExpr != nil {
				joinExpr.joinType = joinSpec.JoinMode
			}
			if joinExpr.joinType == CROSS && joinSpec.JoinCondition != nil {
				errs = append(errs, fmt.Errorf("cross join condition defined for joinExpr %v", spec.Id))
			} else {
				actFromExpr := g.fromExprs[len(g.fromExprs)-1]
				switch joinExpr.joinType {
				case RIGHT:
					joinExpr.isOuter = true
				case FULL:
					joinExpr.isOuter = true
					actFromExpr.tableExprs[len(actFromExpr.tableExprs)-1].isOuter = true
				case LEFT:
					actFromExpr.tableExprs[len(actFromExpr.tableExprs)-1].isOuter = true
				}
				condition := joinSpec.JoinCondition
				joinExpr.condition = condition
				aggr := findAggregateTerms(condition, nil)
				if aggr != nil {
					errs = append(errs, fmt.Errorf("aggregate term defined for joinExpr %v", spec.Id))
				}
				identifierTerms := findIdentifiers(condition, nil)
				for _, identifierTerm := range identifierTerms {
					id := identifierTerm.leaf.ptr.(data.GoSqlIdentifier)
					var matchedJoinExpr *TableExpr
					var matchIx int = -1
					for _, joinExpr := range actFromExpr.tableExprs {
						matches, columnix, err := matchIdToJoinExpr(g.baseStmt, &id, joinExpr)
						if err != nil {
							errs = append(errs, err)
						} else if matches {
							if matchedJoinExpr == nil {
								matchedJoinExpr = joinExpr
								matchIx = columnix
							} else {
								errs = append(errs, fmt.Errorf("duplicate join expression for joinExpr %v", spec.Id))
							}
						}
					}
					if matchedJoinExpr == nil {
						errs = append(errs, fmt.Errorf("not found Identifier %v", identifierTerm.leaf.ptr))
					} else {
						matchedJoinExpr.joinCols = append(matchedJoinExpr.joinCols, matchIx)
						g.identifierMap[identifierTerm] = IdentifierMapEntry{matchIx, matchedJoinExpr}
					}
				}
			}
		}
	}
	return errs
}

func matchIdToJoinExpr(baseStmt data.BaseStatement, id *data.GoSqlIdentifier, expr *TableExpr) (bool, int, error) {
	if len(id.Parts) == 1 {
		colix, err := expr.table.FindColumn(id.Parts[0])
		if err != nil {
			return false, 0, err
		}
		return true, colix, nil
	} else if len(id.Parts) == 2 {
		if id.Parts[0] == expr.alias || expr.table.Schema() == baseStmt.Conn.CurrentSchema && id.Parts[0] == expr.table.Name() {
			colix, err := expr.table.FindColumn(id.Parts[1])
			if err != nil {
				return false, 0, err
			}
			return true, colix, nil
		}
	} else if len(id.Parts) == 3 {
		if expr.table.Schema() == id.Parts[0] && id.Parts[1] == expr.table.Name() {
			colix, err := expr.table.FindColumn(id.Parts[2])
			if err != nil {
				return false, 0, err
			}
			return true, colix, nil
		}
	}
	return false, -1, nil
}

type JoinedRecordTable struct {
}

func (r *JoinedRecord) getTable() {

	return data.TempTable{}
}

func (r *JoinedRecord) NewIterator() {

}

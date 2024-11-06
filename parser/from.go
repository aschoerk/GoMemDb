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
}

type IdentifierMapEntry struct {
	colix    int
	joinExpr *JoinExpr
}

type GoSqlFromHandler struct {
	baseStmt       data.BaseStatement
	fromSpec       []*GoSqlFromSpec
	identifierMap  map[*GoSqlTerm]IdentifierMapEntry
	fromExprs      []*FromExpr
	equalJoinParts []equalJoinPart
}

type JoinExpr struct {
	joinType  int
	table     data.Table
	isOuter   bool
	joinCols  []int
	as        string
	condition *GoSqlTerm
}

type FromExpr struct {
	joinExprs []*JoinExpr
}

func (g *GoSqlFromHandler) identifyTable(identifier GoSqlAsIdentifier) (*JoinExpr, bool) {
	as := identifier.As
	table, exists := data.GetTable(g.baseStmt, identifier.Id)
	return &JoinExpr{0, table, false, []int{}, as, nil}, exists
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

type JoinViewData struct {
	table   data.Table
	isOuter bool
	values  [][]driver.Value
}

func lessThan(a, b []driver.Value) bool {
	for ix := 1; ix < len(a); ix++ {
		v := a[ix]
		w := b[ix]
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

func sortView(arr [][]driver.Value) {
	sort.Slice(arr, func(i, j int) bool {
		a := arr[i]
		b := arr[j]
		return lessThan(a, b)
	})
}

func getJoinView(cols []int, isOuter bool, it data.TableIterator) (*JoinViewData, error) {
	var values = make([][]driver.Value, 0)
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
		values = append(values, view)
	}
	sortView(values)
	return &JoinViewData{it.GetTable(), isOuter, values}, nil
}

func createIdView(left, right *JoinViewData) [][]int64 {
	res := make([][]int64, 0)
	rightLen, leftLen := len(right.values), len(left.values)
	if rightLen == 0 || leftLen == 0 {
		return res
	}
	rix, lix := 0, 0

	for lix < leftLen && rix < rightLen {
		leftValue, rightValue := left.values[lix], right.values[rix]
		leftId, rightId := leftValue[0].(int64), rightValue[0].(int64)

		switch {
		case lessThan(leftValue, rightValue):
			if left.isOuter {
				res = append(res, []int64{leftId, -1})
			}
			lix++
		case lessThan(rightValue, leftValue):
			if right.isOuter {
				res = append(res, []int64{-1, rightId})
			}
			rix++
		default: // values are equal
			// Handle all equal values
			rightStart := rix
			for {
				for rix < rightLen && !lessThan(leftValue, right.values[rix]) {
					res = append(res, []int64{leftId, right.values[rix][0].(int64)})
					rix++
				}
				lix++ // move to next left, if equal to current, reset rix
				if lix < leftLen {
					leftValue = left.values[lix]
					leftId = leftValue[0].(int64)
					if !lessThan(leftValue, right.values[rightStart]) && !lessThan(right.values[rightStart], leftValue) {
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
			leftId := left.values[lix][0].(int64)
			res = append(res, []int64{leftId, -1})
		}
	}

	// Handle remaining right values (outer join)
	if right.isOuter {
		for ; rix < rightLen; rix++ {
			rightId := right.values[rix][0].(int64)
			res = append(res, []int64{-1, rightId})
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

func mergeToRecords(joinedRecord *JoinedRecord, matchColumn int, pairs [][]int64, pairMatchColumn int) {
	records := joinedRecord.records
	leftLen, rightLen := len(records), len(pairs)
	sort.Slice(records, func(i, j int) bool {
		return records[i][matchColumn] < records[j][matchColumn]
	})
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i][pairMatchColumn] < pairs[j][pairMatchColumn]
	})
	lix, rix := 0, 0
	for lix < leftLen && rix < rightLen {
		leftValue, rightValue := records[lix][matchColumn], pairs[rix][pairMatchColumn]
		switch {
		case leftValue == rightValue:
			firstLeftEntry := records[lix]
			records[lix] = append(firstLeftEntry, pairs[rix][1-pairMatchColumn])
			rix++
			for rix < rightLen && pairs[rix][pairMatchColumn] == leftValue {
				records = append(records, append(firstLeftEntry, pairs[rix][1-pairMatchColumn]))
				rix++
			}
			lix++
		case leftValue < rightValue:
			// no matching, remove if no outer join by this table, else keep
			lix++
		case rightValue > leftValue:
			// no matching of the pair ignore if there is no outer join
			rix++
		}
	}
	newRecords := [][]int64{}
	for _, record := range records {
		if len(record) == leftLen {
			var ix int
			for ix = 0; ix < leftLen; ix++ {
				if record[ix] != -1 && joinedRecord.joinExpr[ix].isOuter {
					newRecords = append(newRecords, append(record, -1))
					break
				}
			}
		} else {
			newRecords = append(newRecords, record)
		}
	}
	joinedRecord.records = newRecords
}

func (g *GoSqlFromHandler) createJoinedRecord(pairs []*equalJoin) JoinedRecord {
	res := JoinedRecord{[]*JoinExpr{}, [][]int64{}}
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
			left := pair.exprs[0].left.joinExpr
			res.joinExpr = append(res.joinExpr, left)
			right := pair.exprs[0].right.joinExpr
			res.joinExpr = append(res.joinExpr, right)
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
	return res
}

type JoinedRecord struct {
	joinExpr []*JoinExpr
	records  [][]int64
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
		leftJoinView, err := getJoinView(leftIx, part.left.joinExpr.isOuter, part.left.joinExpr.table.NewIterator(g.baseStmt.BaseData(), false))
		if err != nil {
			errors = append(errors, err)
		} else {
			rightJoinView, err := getJoinView(rightIx, part.right.joinExpr.isOuter, part.right.joinExpr.table.NewIterator(g.baseStmt.BaseData(), false))
			if err != nil {
				errors = append(errors, err)
			} else {
				pair.idView = createIdView(leftJoinView, rightJoinView)
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
	idView [][]int64
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
	for ix := 1; ix < len(g.fromExprs[0].joinExprs); ix++ {
		expr := g.fromExprs[0].joinExprs[ix]
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
	var asDefinitions = make(map[string]*JoinExpr)
	var tableMap = make(map[data.Table]data.Table)
	idHandler := func(id GoSqlAsIdentifier) *JoinExpr {
		if joinExpr, exists := g.identifyTable(id); !exists {
			errs = append(errs, fmt.Errorf("joinExpr %v does not exist", id))
			return nil
		} else {
			fromExpr := g.fromExprs[len(g.fromExprs)-1]
			fromExpr.joinExprs = append(fromExpr.joinExprs, joinExpr)
			if len(joinExpr.as) != 0 {
				if _, exists := asDefinitions[joinExpr.as]; exists {
					errs = append(errs, fmt.Errorf("duplicate as-definition for joinExpr %v", joinExpr.as))
				} else {
					asDefinitions[joinExpr.as] = joinExpr
				}
			} else {
				if _, exists := tableMap[joinExpr.table]; exists {
					errs = append(errs, fmt.Errorf("duplicate joinExpr-definition without as for joinExpr %v", joinExpr.table))
				} else {
					tableMap[joinExpr.table] = joinExpr.table
				}
			}
			return joinExpr
		}
	}

	g.identifierMap = make(map[*GoSqlTerm]IdentifierMapEntry)
	for _, spec := range g.fromSpec {
		g.fromExprs = append(g.fromExprs, &FromExpr{[]*JoinExpr{}})
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
					actFromExpr.joinExprs[len(actFromExpr.joinExprs)-1].isOuter = true
				case LEFT:
					actFromExpr.joinExprs[len(actFromExpr.joinExprs)-1].isOuter = true
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
					var matchedJoinExpr *JoinExpr
					var matchIx int = -1
					for _, joinExpr := range actFromExpr.joinExprs {
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

func matchIdToJoinExpr(baseStmt data.BaseStatement, id *data.GoSqlIdentifier, expr *JoinExpr) (bool, int, error) {
	if len(id.Parts) == 1 {
		colix, err := expr.table.FindColumn(id.Parts[0])
		if err != nil {
			return false, 0, err
		}
		return true, colix, nil
	} else if len(id.Parts) == 2 {
		if id.Parts[0] == expr.as || expr.table.Schema() == baseStmt.Conn.CurrentSchema && id.Parts[0] == expr.table.Name() {
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

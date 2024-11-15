package parser

import (
	"database/sql/driver"
	"github.com/aschoerk/go-sql-mem/data"
	"slices"
	"sort"
	"time"
)

func lessThan(tableViewData, btableViewData *TableViewData, a, b data.Tuple) bool {
	for ix := 0; ix < len(tableViewData.cols); ix++ {
		v, _ := a.Data(0, tableViewData.cols[ix])
		w, _ := b.Data(0, btableViewData.cols[ix])
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

type equalJoinPart struct {
	left  IdentifierMapEntry
	right IdentifierMapEntry
}

type equalJoin struct {
	exprs  []equalJoinPart
	idView [][]data.Tuple
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

///////////////////////////////////////////////////////////////////////////////////////////////////////////

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
	var tuples = make([]data.Tuple, 0)
	for {
		tuple, found, err := it.Next(func(value data.Tuple) (bool, error) {
			return true, nil
		})
		if err != nil {
			return nil, err
		}
		if !found {
			break
		}
		view := make([]driver.Value, len(cols)+1)
		view[0] = tuple.Id
		for i, col := range cols {
			val, _ := tuple.Data(0, col)
			view[i+1] = val
		}
		tuples = append(tuples, tuple)
	}
	res := &TableViewData{it.GetTable(), isOuter, cols, tuples}
	sortView(res)
	return res, nil
}

// given two tables joined by a simple equal-join over arbitrary columns.
func createIdView(left, right *TableViewData) [][]data.Tuple {
	res := make([][]data.Tuple, 0)
	rightLen, leftLen := len(right.tuples), len(left.tuples)
	if rightLen == 0 || leftLen == 0 {
		return res
	}
	rix, lix := 0, 0

	pair := func(left, right data.Tuple) []data.Tuple {
		return []data.Tuple{left, right}
	}

	nullTuple := data.NULL_TUPLE

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

func join(records [][]data.Tuple, leftCol int, pair [][]data.Tuple, rightCol int, newIsOuter bool) ([][]data.Tuple, [][]data.Tuple) {
	leftLen, rightLen := len(records), len(pair)
	if leftLen == 0 || rightLen == 0 {
		return records, pair
	}
	var recordLen = len(records[0])
	sort.Slice(records, func(i, j int) bool {
		return records[i][leftCol].Id() < records[j][leftCol].Id()
	})
	sort.Slice(pair, func(i, j int) bool {
		return pair[i][rightCol].Id() < pair[j][rightCol].Id()
	})
	emptyPrefix := make([]data.Tuple, recordLen)
	if newIsOuter {
		for i := 0; i < recordLen; i++ {
			// emptyPrefix[i] = data.NewSliceTuple(-1, nil)
		}
	}
	lix, rix := 0, 0
	unmatchedPairs := make([][]data.Tuple, 0)
	for lix < leftLen && rix < rightLen {
		leftValue, rightValue := records[lix][leftCol], pair[rix][rightCol]
		switch {
		case leftValue.Id() == rightValue.Id():
			startRix := rix
			firstLeftEntry := records[lix]
			records[lix] = append(firstLeftEntry, pair[rix][1-rightCol])
			rix++
			for rix < rightLen && pair[rix][rightCol].Id() == leftValue.Id() {
				records = append(records, append(firstLeftEntry, pair[rix][1-rightCol]))
				rix++
			}
			rix = startRix
			lix++
		case leftValue.Id() < rightValue.Id():
			// no matching, remove if no outer join by this table, else keep
			lix++
		case rightValue.Id() < leftValue.Id():
			if newIsOuter {
				records = append(records, append(emptyPrefix, pair[rix][1-rightCol]))
			}
			rix++
		}
	}
	return records, unmatchedPairs
}

func handleUnMatched(records [][]data.Tuple, leftCol int, orgLen int, joinedRecord *JoinedRecords) {
	nullTuple := data.NULL_TUPLE
	var newRecords [][]data.Tuple
	for _, record := range records {
		if len(record) == orgLen { // record was skipped, check if one of its columns need to be outer, then keep
			var ix int
			for ix = 0; ix < orgLen; ix++ {
				if record[ix].Id() != -1 && joinedRecord.tableExpr[ix].isOuter {
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

func mergeToRecords(joinedRecord *JoinedRecords, matchColumn int, pairs [][]data.Tuple, pairMatchColumn int, newIsOuter bool) {
	orgLen := len(joinedRecord.records[0])
	records := joinedRecord.records
	records, _ = join(records, matchColumn, pairs, pairMatchColumn, newIsOuter)
	// handleUnMatchedPairs(unMatchedPairs, pairMatchColumn)
	handleUnMatched(records, matchColumn, orgLen, joinedRecord)
}

func (g *GoSqlFromHandler) createJoinedRecord(pairs []*equalJoin) {
	res := JoinedRecords{[]*TableExpr{}, [][]data.Tuple{}}
	if len(pairs) == 0 {
		res.tableExpr = append(res.tableExpr, g.fromExprs[0].tableExprs[0])
	} else {
		for _, pair := range pairs {
			left := pair.exprs[0].left.joinExpr
			if !slices.Contains(res.tableExpr, left) {
				res.tableExpr = append(res.tableExpr, left)
			}
			right := pair.exprs[0].right.joinExpr
			if !slices.Contains(res.tableExpr, right) {
				res.tableExpr = append(res.tableExpr, right)
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
				ix := slices.Index(res.tableExpr, pair.exprs[0].left.joinExpr)
				if ix < 0 {
					ix = slices.Index(res.tableExpr, pair.exprs[1].right.joinExpr)
					mergeToRecords(&res, ix, pair.idView, 1, pair.exprs[0].left.joinExpr.isOuter)
				} else {
					mergeToRecords(&res, ix, pair.idView, 0, pair.exprs[1].left.joinExpr.isOuter)
				}
			}
		}
	}
	g.joinedRecord = res
}

func (g *GoSqlFromHandler) enhanceEqualJoinByIdPairs(pairs []*equalJoin, errors []error) []error {
	for _, pair := range pairs {
		var leftIx []int
		var rightIx []int
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
	var perPair [][]equalJoinPart
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
	return nil
	/*
		errs := make([]error, 0)
		g.fromExprs = make([]*FromExpr, 0)
		var asDefinitions = make(map[string]*TableExpr)
		var tableMap = make(map[data.Table]data.Table)
		var idHandler func(id *GoSqlTableReference) *TableExpr = nil
		checkChain := func(joinedTables []*GoSqlJoinedTable) {
			for _, joinedTable := range joinedTables {
				idHandler(joinedTable.TableReferenceLeft)
				idHandler(joinedTable.TableReferenceRight)
			}
		}
		idHandler = func(id *GoSqlTableReference) {
			if id == nil {
				return
			}
			if joinExpr, exists := g.identifyTable(id); !exists {
				if id.JoinedTable != nil {
					checkChain(id.JoinedTable)
				} else if id.Select != nil {
					// TODO table create by select
					return
				} else {
					errs = append(errs, fmt.Errorf("tableExpr %v does not exist", id))
					return
				}
			} else {
				fromExpr := g.fromExprs[len(g.fromExprs)-1]
				fromExpr.tableExprs = append(fromExpr.tableExprs, joinExpr)
				if len(joinExpr.alias) != 0 {
					if _, exists := asDefinitions[joinExpr.alias]; exists {
						errs = append(errs, fmt.Errorf("duplicate alias-definition for tableExpr %v", joinExpr.alias))
					} else {
						asDefinitions[joinExpr.alias] = joinExpr
					}
				} else {
					if _, exists := tableMap[joinExpr.table]; exists {
						errs = append(errs, fmt.Errorf("duplicate tableExpr-definition without alias for tableExpr %v", joinExpr.table))
					} else {
						tableMap[joinExpr.table] = joinExpr.table
					}
				}
				return joinExpr
			}
		}
		checkChain(g.fromSpec)

		g.identifierMap = make(map[*GoSqlTerm]IdentifierMapEntry)
		for _, spec := range g.fromSpec {
			g.fromExprs = append(g.fromExprs, &FromExpr{[]*TableExpr{}})
			idHandler(spec.TableReferenceLeft)
			for _, joinSpec := range spec.JoinSpecs {
				joinExpr := idHandler(joinSpec.JoinedTable)
				if joinExpr != nil {
					joinExpr.joinType = joinSpec.JoinMode
				}
				if joinExpr.joinType == CROSS && joinSpec.JoinCondition != nil {
					errs = append(errs, fmt.Errorf("cross join condition defined for tableExpr %v", spec.Id))
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
						errs = append(errs, fmt.Errorf("aggregate term defined for tableExpr %v", spec.Id))
					}
					identifierTerms := findIdentifiers(condition, nil)
					for _, identifierTerm := range identifierTerms {
						id := identifierTerm.leaf.ptr.(data.GoSqlIdentifier)
						var matchedJoinExpr *TableExpr
						var matchIx = -1
						for _, joinExpr := range actFromExpr.tableExprs {
							matches, columnix, err := matchIdToJoinExpr(g.baseStmt, &id, joinExpr)
							if err != nil {
								errs = append(errs, err)
							} else if matches {
								if matchedJoinExpr == nil {
									matchedJoinExpr = joinExpr
									matchIx = columnix
								} else {
									errs = append(errs, fmt.Errorf("duplicate join expression for tableExpr %v", spec.Id))
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
	*/
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

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

type IdentifierMapEntry struct {
	colix    int
	joinExpr *JoinExpr
}

type GoSqlFromHandler struct {
	baseStmt      data.BaseStatement
	fromSpec      []*GoSqlFromSpec
	identifierMap map[*GoSqlTerm]IdentifierMapEntry
	fromExprs     []*FromExpr
	equalJoins    []equalJoin
}

type JoinExpr struct {
	joinType  int
	table     data.Table
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
	return &JoinExpr{0, table, []int{}, as, nil}, exists
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

func (g *GoSqlFromHandler) handlePureEqualJoins() (bool, []error) {
	b, errors := g.isPureEqualJoin()
	if !b {
		return b, errors
	}
	// check if all SourceTables are really restricted
	// what happens if a record is used more than once for a join
	// what happens if more than one attribute is used: decide about concatenated key.
	pairs := g.createPairs()
	for _, pair := range pairs {
		leftIx := []int{}
		rightIx := []int{}
		for _, join := range pair {
			leftIx = append(leftIx, join.left.colix)
			rightIx = append(rightIx, join.right.colix)
		}
		leftIt := pair[0].left.joinExpr.table.NewIterator(g.baseStmt.BaseData(), false)

		for {
			tuple, found, err := leftIt.Next(func(value []driver.Value) (bool, error) {
				return true, nil
			})
			if err != nil {
				errors = append(errors, err)
				break
			}
			if !found {
				break
			}

		}

		// find ind
		// create sorted list of entries in le
	}
	// search equal joins
	return false, errors
}

func (g *GoSqlFromHandler) createPairs() [][]equalJoin {
	perPair := [][]equalJoin{}
	for _, join := range g.equalJoins {
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
					perPair[ix] = append(perPair[ix], equalJoin{join.right, join.left})
					found = true
					break
				}
			}
		}
		if !found {
			perPair = append(perPair, []equalJoin{join})
		}
	}
	return perPair
}

type equalJoin struct {
	left  IdentifierMapEntry
	right IdentifierMapEntry
}

func (g *GoSqlFromHandler) onlyEqualJoinExpression(term *GoSqlTerm) bool {
	if term.left != nil && term.right != nil {
		if term.operator == AND {
			leftOk := g.onlyEqualJoinExpression(term.left)
			if !leftOk {
				return g.onlyEqualJoinExpression(term.right)
			}
		} else if term.operator == EQUAL {
			leftIdentifierData, ok := g.identifierMap[term.left]
			if !ok {
				return false
			}
			rightIdentifierData, ok := g.identifierMap[term.right]
			if leftIdentifierData.joinExpr != rightIdentifierData.joinExpr {
				// Equal Join using different bases (even if in case of selfjoin the table might be the same)
				g.equalJoins = append(g.equalJoins, equalJoin{leftIdentifierData, rightIdentifierData})
				return true
			} else {
				return false
			}
		} else {
			return false
		}
	}
	if term.left != nil || term.right != nil {
		return false
	}

}

func (g *GoSqlFromHandler) isPureEqualJoin() (bool, []error) {
	if len(g.fromExprs) > 1 {
		return false, nil
	}
	for _, expr := range g.fromExprs[0].joinExprs {
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
				condition := joinSpec.JoinCondition
				joinExpr.condition = condition
				aggr := findAggregateTerms(condition, nil)
				if aggr != nil {
					errs = append(errs, fmt.Errorf("aggregate term defined for joinExpr %v", spec.Id))
				}
				identifierTerms := findIdentifiers(condition, nil)
				for _, identifierTerm := range identifierTerms {
					id := identifierTerm.leaf.ptr.(data.GoSqlIdentifier)
					actFromExpr := g.fromExprs[len(g.fromExprs)-1]
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

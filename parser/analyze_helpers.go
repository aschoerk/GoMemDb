package parser

import (
	"slices"
	"time"
)

var aggregateFuncs = []int{COUNT, AVG, SUM, MIN, MAX}

func findAggregateTerms(term *GoSqlTerm, res []*GoSqlTerm) []*GoSqlTerm {
	if slices.Contains(aggregateFuncs, term.operator) {
		return append(res, term)
	}
	if term.left != nil {
		res = findAggregateTerms(term.left, res)
	}
	if term.right != nil {
		res = findAggregateTerms(term.left, res)
	}
	return res
}

func ptrEqual(ptr1 *Ptr, ptr2 *Ptr) bool {
	if ptr1 == nil {
		return ptr2 == nil
	}
	if ptr2 == nil {
		return false
	}
	if ptr1.token != ptr2.token {
		return false
	}
	switch ptr1.ptr.(type) {
	case int64:
		return ptr1.ptr.(int64) == ptr2.ptr.(int64)
	case float64:
		return ptr1.ptr.(float64) == ptr2.ptr.(float64)
	case string:
		return ptr1.ptr.(string) == ptr2.ptr.(string)
	case time.Time:
		return ptr1.ptr.(time.Time).UnixNano() == ptr2.ptr.(time.Time).UnixNano()
	default:
		return false
	}
}

func structuralEqual(term1 *GoSqlTerm, term2 *GoSqlTerm) bool {
	if term1 == nil {
		return term2 == nil
	}
	if term2 == nil {
		return false
	}
	if term1.operator == term2.operator && ptrEqual(term1.leaf, term2.leaf) {
		return structuralEqual(term1.left, term2.left) && structuralEqual(term1.right, term2.right)
	} else {
		return false
	}
}

func findSubTermOutsideAgg(subTerm *GoSqlTerm, in *GoSqlTerm, res []*GoSqlTerm) []*GoSqlTerm {
	if in == nil || slices.Contains(aggregateFuncs, in.operator) {
		return res
	}
	if structuralEqual(subTerm, in) {
		return append(res, in)
	} else {
		res = findSubTermOutsideAgg(subTerm, in.left, res)
		res = findSubTermOutsideAgg(subTerm, in.right, res)
		return res
	}
}

func isRealSubTerm(subTerm *GoSqlTerm, in *GoSqlTerm) bool {
	if in == nil {
		return false
	}
	if subTerm == in {
		return true
	}
	if !isRealSubTerm(subTerm, in.left) {
		return isRealSubTerm(subTerm, in.right)
	} else {
		return true
	}
}

func getLeafs(in *GoSqlTerm, stopTerms []*GoSqlTerm, res []*GoSqlTerm) []*GoSqlTerm {
	if in == nil {
		return res
	}
	if slices.Contains(stopTerms, in) {
		return res
	}
	if in.left == nil && in.right == nil {
		return append(res, in)
	}
	res = getLeafs(in.left, stopTerms, res)
	res = getLeafs(in.right, stopTerms, res)
	return res
}

func findIdentifiers(in *GoSqlTerm, t []*GoSqlTerm) []*GoSqlTerm {
	if in == nil {
		return t
	}
	if in.leaf != nil && in.leaf.token == IDENTIFIER {
		return append(t, in)
	}
	tmpT := t
	if in.left != nil {
		tmpT = findIdentifiers(in.left, t)
	}
	if in.right != nil {
		tmpT = findIdentifiers(in.right, tmpT)
	}
	return tmpT
}

package parser

import (
	"fmt"

	"github.com/aschoerk/go-sql-mem/data"
	"github.com/aschoerk/go-sql-mem/machine"
)

type NodeKind int

const (
	AggregationNode NodeKind = iota + 1
	ConstantNode
	GroupByNode
	HavingNode
	SelectListNode
	ParameterNode
)

type OriginalInfo struct {
	Kind  NodeKind
	Index int
	term  *GoSqlTerm
}

type PartNode struct {
	kind     NodeKind
	partTerm *GoSqlTerm
	original *OriginalInfo
}

// validity of where clause
//   - identifiers only from from-clause
//   - boolean result
// validity of a selectlist entry and a having clause
// - if there are aggregations in select lists, all have to be
//    - aggregations
//    - constants
//    - group by terms
//    - group by terms combined with each other and / or aggregations

func (r *GoSqlSelectRequest) analyze() ([]SLE, error) {

	// find aggregation-nodes in select lists
	// find aggregation-nodes in having
	// find group-by nodes in select lists
	// find group-by nodes in having
	// find constant nodes
	type slinfo struct {
		gsubterms []*GoSqlTerm
		aggterms  []*GoSqlTerm
	}

	var slinfos []slinfo
	for _, s := range r.selectList {
		info := slinfo{nil, nil}
		info.aggterms = findAggregateTerms(s.expression, nil)
		if r.groupBy != nil {
			for _, g := range r.groupBy {
				info.gsubterms = findSubTermOutsideAgg(g, s.expression, info.gsubterms)
			}
		}
		tmp := append(info.aggterms, info.gsubterms...)
		slinfos = append(slinfos, info)
		furtherLeafs := getLeafs(s.expression, tmp, nil)

		for _, l := range furtherLeafs {
			if l.operator == -1 && l.leaf.ptr != nil && l.leaf.token == IDENTIFIER {
				return nil, fmt.Errorf("invalid part of select list entry %v: %v", s, l)
			}
		}
	}

	if r.groupBy != nil && len(r.groupBy) > 0 {

		for _, g := range r.groupBy {
			for _, s := range r.selectList {
				subterms := findSubTermOutsideAgg(g, s.expression, nil)
				aggterms := findAggregateTerms(s.expression, nil)
				groupAndAgg := append(aggterms, subterms...)
				furtherLeafs := getLeafs(s.expression, groupAndAgg, nil)

				for _, l := range furtherLeafs {
					if l.operator == -1 && l.leaf.ptr != nil && l.leaf.token == IDENTIFIER {
						return nil, fmt.Errorf("invalid part of select list entry %v: %v", s, l)
					}
				}
			}
		}
		if r.having != nil {

		}
	} else {
		for _, s := range r.selectList {
			aggterms := findAggregateTerms(s.expression, nil)
			furtherLeafs := getLeafs(s.expression, aggterms, nil)
			for _, l := range furtherLeafs {
				if l.operator == -1 && l.leaf.ptr != nil && l.leaf.token == IDENTIFIER {
					return nil, fmt.Errorf("invalid part of select list entry %v: %v", s, l)
				}
			}
		}
	}
	return nil, nil
}

type SLE interface {
}

type BaseSLE struct {
	orgIndex   int
	finalIndex int
	hidden     int
}

type ConstSLE struct {
	BaseSLE
	value *Ptr
}

type AttrSLE struct {
	BaseSLE
	ix  int
	col *data.GoSqlColumn
}

type CalculatedSLE struct {
	BaseSLE
	m *machine.Machine
}

type SimpleAggSLE struct {
	BaseSLE
	token int
	sle   SLE
}

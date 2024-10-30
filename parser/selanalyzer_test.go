package parser

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test1(t *testing.T) {
	res, _ := Parse("select 1, a, avg(b), b, a * b, 10 * c / (a * b) from t")
	assert.NotNil(t, res)
	r, ok := res.(*GoSqlSelectRequest)
	assert.True(t, ok)
	terms := findSubTermOutsideAgg(r.selectList[3].expression, r.selectList[2].expression, nil)
	assert.Equal(t, 0, len(terms))
	terms = findSubTermOutsideAgg(r.selectList[4].expression, r.selectList[5].expression, nil)
	assert.Equal(t, 1, len(terms))
	assert.True(t, isRealSubTerm(terms[0], r.selectList[5].expression))
	leafs := getLeafs(r.selectList[5].expression, terms, nil)
	assert.Equal(t, 2, len(leafs))

}

func TestRealSubTerm(t *testing.T) {

	testCases := []struct {
		toSearch string
		in       string
		num      int
	}{{
		toSearch: "b",
		in:       "b",
		num:      1,
	}, {
		toSearch: "b",
		in:       "*",
		num:      0,
	}, {
		toSearch: "b",
		in:       "avg(b)",
		num:      0,
	}, {
		toSearch: "b",
		in:       "b * b",
		num:      2,
	}, {
		toSearch: "b",
		in:       "b * (a / c) AND b > 10",
		num:      2,
	}, {
		toSearch: "b",
		in:       "B * (a / c) AND count(b) > 10",
		num:      0,
	},
	}

	for _, tc := range testCases {
		YYDebug = 4
		t.Run(fmt.Sprintf("Search %s in %s", tc.toSearch, tc.in), func(t *testing.T) {

			sel := fmt.Sprintf("select %s, %s from x", tc.toSearch, tc.in)
			res, _ := Parse(sel)
			r, _ := res.(*GoSqlSelectRequest)
			terms := findSubTermOutsideAgg(r.selectList[0].expression, r.selectList[1].expression, nil)
			assert.Equal(t, tc.num, len(terms))
			for _, term := range terms {
				assert.True(t, isRealSubTerm(term, r.selectList[1].expression))
			}

		})
	}
}

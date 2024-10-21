package tests

import (
	"testing"

	"github.com/aschoerk/go-sql-mem/data"
	"github.com/aschoerk/go-sql-mem/parser"
	"github.com/stretchr/testify/assert"
)

func testParserResult(t *testing.T, sql string) {
	parseResult, res := parser.Parse(sql)
	assert.GreaterOrEqual(t, 0, res)
	assert.NotNil(t, parseResult)
	assert.NotNil(t, parseResult.(data.StatementInterface))
}

func TestParser(t *testing.T) {

	testCases := []string{
		"select count(DISTINCT name) from A",
		"select x, COUNT ( * ) from A group by x",
		"select x, COUNT ( * ) from A group by x * 2",
		"select x, COUNT ( * ) from A group by x * 2 order by 1",
		"select x, COUNT ( * ) from A group by x * 2 order by alias",
		"select \"ident\"\"ifier\", COUNT ( * ) from A group by x * 2 order by alias",
		"select * from A",
		"select COUNT(*) from A",
		"select * from A where x > (1 * 10) * y",
		"select count(ALL name) from A",
		"select count(*) from A",
	}

	for _, sql := range testCases {
		t.Run(sql, func(t *testing.T) {
			parser.YYDebug = 4
			parseResult, res := parser.Parse(sql)
			assert.GreaterOrEqual(t, 0, res)
			assert.NotNil(t, parseResult)
			assert.NotNil(t, parseResult.(data.StatementInterface))
		})
	}
}

package parser

import (
	"database/sql/driver"
	"fmt"
	"github.com/aschoerk/go-sql-mem/data"
)

type JoinedRecords struct {
	tableExpr []*TableExpr
	records   [][]data.Tuple
}

func JoinedRecordsFromTable(table data.Table) *JoinedRecords {
	te := []*TableExpr{&TableExpr{0, table, false, nil, "", nil}}
	return &JoinedRecords{te, nil}
}

type JoinedRecord struct {
	record []data.Tuple
}

func (j *JoinedRecord) Id() int64 {
	//TODO implement me
	panic("implement me")
}

func (j *JoinedRecord) Data(tableIx int, ix int) (driver.Value, error) {
	if tableIx > len(j.record) {
		return nil, fmt.Errorf("table index %d out of range", tableIx)
	}
	return j.record[tableIx].Data(0, ix)
}

func (j *JoinedRecord) SafeData(tableIx int, ix int) driver.Value {
	res, _ := j.record[tableIx].Data(0, ix)
	return res
}

func (j *JoinedRecord) DataLen() int {
	return len(j.record)
}

func (j *JoinedRecord) SetData(tableIx int, ix int, v driver.Value) error {
	if tableIx > len(j.record) {
		return fmt.Errorf("table index %d out of range", tableIx)
	}
	return j.record[tableIx].SetData(0, ix, v)
}

func (j *JoinedRecord) Clone() data.Tuple {
	res := make([]data.Tuple, 0, len(j.record))
	for _, record := range j.record {
		res = append(res, record.Clone())
	}
	return &JoinedRecord{res}
}

func (j *JoinedRecords) allIdentifiersAndTypes() ([]data.GoSqlIdentifier, []int) {
	idRes := []data.GoSqlIdentifier{}
	typeRes := []int{}

	moreThanOne := len(j.tableExpr) > 1
	for _, table := range j.tableExpr {
		for _, col := range table.table.Columns() {
			if table.alias != "" {
				idRes = append(idRes, data.GoSqlIdentifier{Parts: []string{table.alias, col.Name}})
			} else {
				if moreThanOne {
					idRes = append(idRes, data.GoSqlIdentifier{Parts: []string{table.table.Name(), col.Name}})
				} else {
					idRes = append(idRes, data.GoSqlIdentifier{Parts: []string{col.Name}})
				}
			}
			typeRes = append(typeRes, col.ColType)
		}
	}
	return idRes, typeRes
}

func (r *JoinedRecords) identifyId(identifier data.GoSqlIdentifier) (int, int, int, error) {
	var resTix = -1
	var resCix = -1
	var resType = -1
	plen := len(identifier.Parts)
	if plen > 2 {
		return -1, -1, -1, fmt.Errorf("Invalid Identifier: %v", identifier)
	}
	colname := identifier.Parts[plen-1]
	var aliasOrTableName string
	if plen == 2 {
		aliasOrTableName = identifier.Parts[0]
	}

	for tix, te := range r.tableExpr {
		if len(aliasOrTableName) == 0 || te.table.Name() == aliasOrTableName || te.alias == aliasOrTableName {
			for cix, col := range te.table.Columns() {
				if col.Name == colname {
					if resCix != -1 {
						return -1, -1, -1, fmt.Errorf("Identifier %s is not unique", colname)
					}
					resCix = cix
					resTix = tix
					resType = col.ParserType
				}
			}
		}
	}
	if resTix == -1 {
		return -1, -1, -1, fmt.Errorf("Identifier %s not found", identifier)
	}
	return resTix, resCix, resType, nil
}

type JoinedRecordTable struct {
}

func (r *JoinedRecords) getTableIterator(statement data.BaseStatement) data.TableIterator {
	if len(r.records) == 0 && len(r.tableExpr) == 1 {
		return r.tableExpr[0].table.NewIterator(statement.BaseData(), false)
	}
	var viewColumns []data.GoSqlColumn
	for _, e := range r.tableExpr {
		for _, col := range e.table.Columns() {
			viewColumns = append(viewColumns, col)
		}
	}
	return &JoinedRecordsIterator{r, viewColumns, 0}
}

type JoinedRecordsIterator struct {
	j           *JoinedRecords
	viewColumns []data.GoSqlColumn
	ix          int
}

func (j *JoinedRecordsIterator) GetTable() data.Table {
	//TODO implement me
	panic("implement me")
}

func (j *JoinedRecordsIterator) Next(f func(tuple data.Tuple) (bool, error)) (data.Tuple, bool, error) {
	if j.ix < len(j.j.records) {
		j.ix++
		return &JoinedRecord{j.j.records[j.ix-1]}, true, nil
	} else {
		return nil, false, nil
	}
}

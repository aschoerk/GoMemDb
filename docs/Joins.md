# Joins

Starting Point is a slice of JoinedTable structs
```go
type Select struct {
	...
	from        []*GoSqlJoinedTable
	...
}
```

```go
// a slice of *GoSqlJoinedTable describes the From-Term
// if TableReferenceLeft is empty, then it is the right part of a JOIN
// if TableReferenceRight is empty, then it is a single TableReference separated by Comma from the others if there are any
// because of the recursion in TableReferenceLeft by Parentheses enclosed Selects, or JoinedTables each slice element can be
// a tree/wood of arbitrary depth.
type GoSqlJoinedTable struct {
	TableReferenceLeft  *GoSqlTableReference
	JoinType            int
	TableReferenceRight *GoSqlTableReference
	Condition           *GoSqlTerm
}

type GoSqlTableReference struct {
    Id data.GoSqlIdentifier
    // nested by parentheses
    Select *GoSqlSelectRequest
    // nested by parentheses
    JoinedTable []*GoSqlJoinedTable
    Alias       string
}
```

Result is an Iterator producing records

Cases: 
## Single Table Select
The length of from is 1.
The one entry: 
TableReferenceLeft:
- Id points to a real Table,
- Select, JoinedTable are nil
- Alias is optional set

JoinType 0, TableReferenceRight is nil, Condition is nil.

## Join of two tables
The length of from is 1.
TableReferenceLeft:
- Id points to a real Table,
- Select, JoinedTable are nil
- Alias is optional set

TableReferenceRight:
- Id points to a real Table,
- Select, JoinedTable are nil
- Alias is optional set

JoinType is set, 
Condition restricts Join from cross product.

## Join of three tables
The length of from is 2
First JoinedTable
TableReferenceLeft and TableReferenceRight like 1 Join
Condition relates to 2 TableReferences

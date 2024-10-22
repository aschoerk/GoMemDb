package parser

import (
	"database/sql/driver"
	. "database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/aschoerk/go-sql-mem/data"
	. "github.com/aschoerk/go-sql-mem/data"
	. "github.com/aschoerk/go-sql-mem/machine"
)

type EvaluationContext struct {
	m                    *Machine
	lastPlaceHolderIndex int
	t                    Table
	resultType           int
	name                 *string
}

func NewEvaluationContext(args []Value, lastPlaceHolderIndex int) EvaluationContext {
	return EvaluationContext{NewMachine(args), lastPlaceHolderIndex, nil, -1, nil}
}

// represents parts of expressions
// if left is != nil: leaf is nil, operator is >= 0
// if right is nil, operator is unary (NOT)
// if leaf is != nil then operator is -1 and left and right are nil
type GoSqlTerm struct {
	operator int
	left     *GoSqlTerm
	right    *GoSqlTerm
	leaf     *Ptr
}

func (t *GoSqlTerm) FindPlaceHolders(res []*GoSqlTerm) []*GoSqlTerm {
	if t.leaf != nil && t.leaf.token == PLACEHOLDER {
		return append(res, t)
	}
	if t.left != nil {
		res = t.left.FindPlaceHolders(res)
	}
	if t.right != nil {
		res = t.right.FindPlaceHolders(res)
	}
	return res
}

func FindPlaceHoldersInSelect(statement *GoSqlSelectRequest) []*GoSqlTerm {
	var res = make([]*GoSqlTerm, 0)
	for _, slentry := range statement.selectList {
		res = slentry.expression.FindPlaceHolders(res)
	}
	if statement.where != nil {
		res = statement.where.FindPlaceHolders(res)
	}
	if statement.having != nil {
		res = statement.having.FindPlaceHolders(res)
	}

	return res
}

func OrderBy2Commands(orderByList *[]GoSqlOrderBy, table Table) (*EvaluationContext, error) {
	e := NewEvaluationContext(nil, 0)

	for _, orderByEntry := range *orderByList {

		nameType, err := CategorizePointer(orderByEntry.Name)

		if err != nil {
			return nil, err
		}
		ix := -1
		if nameType == STRING {
			ix, err = table.FindColumn(orderByEntry.Name.(string))
			if err != nil {
				return nil, err
			}
		} else {
			ix = orderByEntry.Name.(int) - 1
		}
		AddPushAttribute(e.m, ix)
		AddPushAttribute2(e.m, ix)
		columns := table.Columns()
		switch columns[ix].ColType {
		case BOOLEAN:
			e.m.AddCommand(CompareBool)
		case INTEGER:
			e.m.AddCommand(CompareInt64)
		case FLOAT:
			e.m.AddCommand(CompareFloat64)
		case STRING:
			e.m.AddCommand(CompareString)
		case TIMESTAMP:
			e.m.AddCommand(CompareTimestamp)
		}
		switch orderByEntry.direction {
		case ASC:
			e.m.AddCommand(ReturnIfNotEqualZero)
		case DESC:
			e.m.AddCommand(ReturnInverseIfNotEqualZero)
		}

	}
	return &e, nil
}

func initPlaceHolders(terms []*GoSqlTerm, args []driver.Value, placeHolderOffset *int) {
	var placeHolders = make([]*GoSqlTerm, 0)
	for _, term := range terms {
		placeHolders = term.FindPlaceHolders(placeHolders)
	}
	// set placeholder values by first set of args to function as templates for the type
	for _, placeHolder := range placeHolders {
		placeHolder.leaf.ptr = args[*placeHolderOffset]
		*placeHolderOffset++
	}
}

func Terms2Commands(terms []*GoSqlTerm, args []driver.Value, inputTable Table, placeHolderOffset *int) ([]*EvaluationContext, error) {
	initPlaceHolders(terms, args, placeHolderOffset)
	currentPlaceholderIndex := -1 // TODO: check if -1 is right here
	res := []*EvaluationContext{}
	for _, term := range terms {
		e := NewEvaluationContext(args, currentPlaceholderIndex)
		e.t = inputTable
		resultType, err := term.evaluate(&e)
		e.resultType = resultType
		if err != nil {
			return res, err
		}
		currentPlaceholderIndex = e.lastPlaceHolderIndex
		res = append(res, &e)
	}
	return res, nil
}

func calcConversion(destType, orgType int) (Command, error) {
	switch orgType {
	case BOOLEAN:
		switch destType {
		case BOOLEAN:
			return nil, errors.New("no conversion boolean to boolen")
		case INTEGER:
			return BooleanToInt, nil
		case FLOAT:
			return BooleanToFloat, nil
		case STRING:
			return BooleanToString, nil
		case TIMESTAMP:
			return nil, errors.New("cannot convert boolean to timestamp")
		}
	case INTEGER:
		switch destType {
		case BOOLEAN:
			return IntToBoolean, nil
		case INTEGER:
			return nil, errors.New("no conversion int to int")
		case FLOAT:
			return IntToFloat, nil
		case STRING:
			return IntToString, nil
		case TIMESTAMP:
			return IntToTimestamp, nil
		}
	case FLOAT:
		switch destType {
		case BOOLEAN:
			return FloatToBoolean, nil
		case INTEGER:
			return FloatToInt, nil
		case FLOAT:
			return nil, errors.New("no conversion float to float")
		case STRING:
			return FloatToString, nil
		case TIMESTAMP:
			return FloatToTimestamp, nil
		}
	case STRING:
		switch destType {
		case BOOLEAN:
			return StringToBoolean, nil
		case INTEGER:
			return StringToInt, nil
		case FLOAT:
			return StringToFloat, nil
		case STRING:
			return nil, errors.New("no conversion String to String")
		case TIMESTAMP:
			return StringToTimestamp, nil
		}
	case TIMESTAMP:
		switch destType {
		case BOOLEAN:
			return nil, errors.New("no conversion timestamp to boolean")
		case INTEGER:
			return TimestampToInteger, nil
		case FLOAT:
			return TimestampToFloat, nil
		case STRING:
			return TimestampToString, nil
		case TIMESTAMP:
			return nil, errors.New("no conversion Timestamp to Timestamp")
		}
	}
	return nil, errors.New("Invalid conversion combination")
}

func HandleNot(e *EvaluationContext, typeToken int) (int, error) {
	switch typeToken {
	case INTEGER:
		AddConversion(e.m, IntToBoolean, false)
	case FLOAT:
		AddConversion(e.m, FloatToBoolean, false)
	case BOOLEAN:
		AddConversion(e.m, StringToBoolean, false)
	default:
		return -1, fmt.Errorf("unsupported type: %T", typeToken)
	}
	e.m.AddCommand(InvertTopBool)
	return BOOLEAN, nil
}

func CategorizePointer(ptr Value) (int, error) {
	if ptr == nil {
		return -1, errors.New("cannot categorize nil")
	}

	// Get the value that the pointer points to
	v := reflect.ValueOf(ptr)

	switch v.Kind() {
	case reflect.Bool:
		return BOOLEAN, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return INTEGER, nil
	case reflect.Float32, reflect.Float64:
		return FLOAT, nil
	case reflect.String:
		return STRING, nil
	}

	// Check if it's a time.Time
	if v.Type() == reflect.TypeOf(time.Time{}) {
		return TIMESTAMP, nil
	}

	return -1, errors.New("Cannot categorize this")
}

func (term *GoSqlTerm) handleLeaf(e *EvaluationContext) (int, error) {
	if term.leaf.token == PLACEHOLDER {
		e.lastPlaceHolderIndex++
		AddPushPlaceHolder(e.m, e.lastPlaceHolderIndex)
		p, err := e.m.ReturnPlaceHolder(e.lastPlaceHolderIndex)
		if err != nil {
			return -1, err
		}
		return CategorizePointer(p)
	}
	if term.leaf.token == IDENTIFIER {
		id := term.leaf.ptr.(string)
		if id == data.VersionedRecordId {
			AddPushAttribute(e.m, -1)
			return INTEGER, nil
		} else {
			for ix, col := range e.t.Columns() {
				if col.Name == id {
					AddPushAttribute(e.m, ix)
					return col.ParserType, nil
				}
			}
			return -1, fmt.Errorf("Identifier %s not found", id)
		}
	}
	AddPushConstant(e.m, term.leaf.ptr)
	return term.leaf.token, nil
}

func (term *GoSqlTerm) evaluate(e *EvaluationContext) (int, error) {
	if term.leaf != nil {
		return term.handleLeaf(e)
	}
	if term.right == nil {
		tmp, err := term.right.evaluate(e)
		if err != nil {
			return -1, err
		}
		switch term.operator {
		case NOT:
			return HandleNot(e, tmp)
		case ISNULL:
			e.m.AddCommand(IsNullCommand)
			return BOOLEAN, nil
		case ISNOTNULL:
			e.m.AddCommand(IsNotNullCommand)
			return BOOLEAN, nil
		}
	} else {
		if term.left == nil {
			panic("term left is nil")
		}
		leftType, leftError := term.left.evaluate(e)

		if leftError != nil {
			return -1, leftError
		}
		rightType, rightError := term.right.evaluate(e)
		if rightError != nil {
			return -1, rightError
		}
		newLeftType, newRightType, destType, typeError := commonAndDestType(leftType, rightType, term.operator)

		if typeError != nil {
			return -1, typeError
		}
		if leftType != newLeftType {
			c, err := calcConversion(newLeftType, leftType)
			if err != nil {
				return -1, err
			}
			AddConversion(e.m, c, true)
		}
		if rightType != newRightType {
			c, err := calcConversion(newRightType, rightType)
			if err != nil {
				return -1, err
			}
			AddConversion(e.m, c, false)
		}
		c, err := calcOperationCommand(term.operator, destType, newLeftType, newRightType)
		if err != nil {
			return -1, err
		}
		e.m.AddCommand(c)
		return destType, nil
	}
	return -1, errors.New("error evaluating")
}

func calcOperationCommand(opType int, destType int, leftType int, rightType int) (Command, error) {
	switch opType {
	case AND:
		return AndBooleans, nil
	case OR:
		return OrBooleans, nil
	case ASTERISK:
		if destType == INTEGER {
			return MultiplyInts, nil
		} else {
			return MultiplyFloats, nil
		}
	case MOD:
		if destType == INTEGER {
			return ModuloInts, nil
		} else {
			return ModuloFloats, nil
		}
	case DIVIDE:
		if destType == INTEGER {
			return DivideInts, nil
		} else {
			return DivideFloats, nil
		}
	case PLUS:
		switch destType {
		case INTEGER:
			return AddInts, nil
		case FLOAT:
			return AddFloats, nil
		case STRING:
			return AddStrings, nil
		case TIMESTAMP:
			if leftType == INTEGER {
				return AddIntToTimestamp, nil
			} else if leftType == FLOAT {
				return AddFloatToTimestamp, nil
			}
		}
	case MINUS:
		switch destType {
		case INTEGER:
			return SubtractInts, nil
		case FLOAT:
			return SubtractFloats, nil
		case TIMESTAMP:
			if leftType == INTEGER {
				return SubtractIntFromTimestamp, nil
			} else if leftType == FLOAT {
				return SubtractFloatFromTimestamp, nil
			} else {
				return SubtractTimestamps, nil
			}
		}
	case LIKE:
		if rightType != STRING {
			return nil, errors.New("expected string as right operand of like")
		}
		return LikeStrings, nil
	default:
		res := GetComparisonFunction(opType, leftType)
		if res != nil {
			return res, nil
		}
		return nil, fmt.Errorf("unsupported operator type: %d", opType)
	}
	return nil, errors.New("Unable to find operation command")
}

// Function to get the correct comparison function
func GetComparisonFunction(operator int, dataType int) Command {
	switch dataType {
	case BOOLEAN:
		switch operator {
		case EQUAL:
			return BoolEqual
		case GREATER_OR_EQUAL:
			return BoolGreaterThanOrEqual
		case GREATER:
			return BoolGreaterThan
		case LESS:
			return BoolLessThan
		case LESS_OR_EQUAL:
			return BoolLessThanOrEqual
		case NOT_EQUAL:
			return BoolNotEqual
		}
	case INTEGER:
		switch operator {
		case EQUAL:
			return IntEqual
		case GREATER_OR_EQUAL:
			return IntGreaterThanOrEqual
		case GREATER:
			return IntGreaterThan
		case LESS:
			return IntLessThan
		case LESS_OR_EQUAL:
			return IntLessThanOrEqual
		case NOT_EQUAL:
			return IntNotEqual
		}
	case FLOAT:
		switch operator {
		case EQUAL:
			return Float64Equal
		case GREATER_OR_EQUAL:
			return Float64GreaterThanOrEqual
		case GREATER:
			return Float64GreaterThan
		case LESS:
			return Float64LessThan
		case LESS_OR_EQUAL:
			return Float64LessThanOrEqual
		case NOT_EQUAL:
			return Float64NotEqual
		}
	case STRING:
		switch operator {
		case EQUAL:
			return StringEqual
		case GREATER_OR_EQUAL:
			return StringGreaterThanOrEqual
		case GREATER:
			return StringGreaterThan
		case LESS:
			return StringLessThan
		case LESS_OR_EQUAL:
			return StringLessThanOrEqual
		case NOT_EQUAL:
			return StringNotEqual
		}
	case TIMESTAMP:
		switch operator {
		case EQUAL:
			return TimeEqual
		case GREATER_OR_EQUAL:
			return TimeGreaterThanOrEqual
		case GREATER:
			return TimeGreaterThan
		case LESS:
			return TimeLessThan
		case LESS_OR_EQUAL:
			return TimeLessThanOrEqual
		case NOT_EQUAL:
			return TimeNotEqual
		}
	}

	// Return a no-op function if no match is found
	return nil
}

func comparisonTypes(a, b int) (int, int, int, error) {

	if a == TIMESTAMP || b == TIMESTAMP {
		return TIMESTAMP, TIMESTAMP, BOOLEAN, nil
	}
	if a == STRING || b == STRING {
		return STRING, STRING, BOOLEAN, nil
	}
	if a == FLOAT || b == FLOAT {
		return FLOAT, FLOAT, BOOLEAN, nil
	}
	if a == INTEGER || b == INTEGER {
		return INTEGER, INTEGER, BOOLEAN, nil
	}
	return BOOLEAN, BOOLEAN, BOOLEAN, nil

}

func commonAndDestType(a, b int, opType int) (int, int, int, error) {
	switch opType {
	case EQUAL, GREATER, GREATER_OR_EQUAL, LESS, LESS_OR_EQUAL, NOT_EQUAL:
		return comparisonTypes(a, b)
	case AND, OR:
		return BOOLEAN, BOOLEAN, BOOLEAN, nil
	case ASTERISK, MOD, DIVIDE:
		if a == FLOAT || b == FLOAT {
			return FLOAT, FLOAT, FLOAT, nil
		} else {
			return INTEGER, INTEGER, INTEGER, nil
		}
	case PLUS:
		if a == TIMESTAMP {
			if b != TIMESTAMP && b != BOOLEAN {
				return TIMESTAMP, INTEGER, TIMESTAMP, nil
			}
		}
		if a == STRING || b == STRING {
			return STRING, STRING, STRING, nil
		}
		if a == BOOLEAN || b == BOOLEAN {
			return -1, -1, -1, errors.New("can not add booleans")
		}
		if a == FLOAT || b == FLOAT {
			return FLOAT, FLOAT, FLOAT, nil
		}
		return INTEGER, INTEGER, INTEGER, nil
	case MINUS:
		if a == BOOLEAN || b == BOOLEAN || a == STRING || b == STRING {
			return -1, -1, -1, errors.New("can not subtract booleans or strings")
		}
		if a == TIMESTAMP {
			if b != TIMESTAMP && b != BOOLEAN {
				return TIMESTAMP, INTEGER, TIMESTAMP, nil
			}
			if b == TIMESTAMP {
				return TIMESTAMP, TIMESTAMP, INTEGER, nil
			}
			return -1, -1, -1, errors.New("can not handle timestamp using these types")
		}
		if a == FLOAT || b == FLOAT {
			return FLOAT, FLOAT, FLOAT, nil
		}
		return INTEGER, INTEGER, INTEGER, nil
	case LIKE:
		if b != STRING {
			return -1, -1, -1, errors.New("Like needs a string operand")
		}
		return STRING, STRING, BOOLEAN, nil
	// TODO: BETWEEN
	default:
		return -1, -1, -1, fmt.Errorf("unsupported operator type: %d", opType)
	}
}

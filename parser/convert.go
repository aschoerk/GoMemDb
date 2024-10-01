package parser

import (
	. "database/sql/driver"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	. "github.com/aschoerk/go-sql-mem/data"
)

func NewColumn(name string, coltype int, length int, spec2 int) GoSqlColumn {
	var parserType int
	switch coltype {
	case VARCHAR, CHAR, TEXT:
		parserType = STRING
	default:
		parserType = coltype
	}
	return GoSqlColumn{name, coltype, parserType, length, spec2, false}
}

func pointerToString(ptr interface{}) string {
	if ptr == nil {
		return "<nil>"
	}

	v := reflect.ValueOf(ptr)
	if v.Kind() != reflect.Ptr {
		return fmt.Sprintf("Not a pointer: %v", ptr)
	}

	// Dereference the pointer
	v = v.Elem()

	switch v.Kind() {
	case reflect.String:
		return v.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fmt.Sprintf("%d", v.Int())
	case reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%f", v.Float())
	default:
		// Check if it's a time.Time
		if v.Type() == reflect.TypeOf(time.Time{}) {
			return v.Interface().(time.Time).Format(time.RFC3339)
		}
		return fmt.Sprintf("Unsupported type: %v", v.Type())
	}
}

func pointerToInt(ptr interface{}) (int64, error) {
	if ptr == nil {
		return 0, fmt.Errorf("nil pointer")
	}

	v := reflect.ValueOf(ptr)
	if v.Kind() != reflect.Ptr {
		return 0, fmt.Errorf("not a pointer: %v", ptr)
	}

	// Dereference the pointer
	v = v.Elem()

	switch v.Kind() {
	case reflect.String:
		// Try to parse the string as an integer
		i, err := strconv.ParseInt(v.String(), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string to int: %v", err)
		}
		return i, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), nil
	case reflect.Float32, reflect.Float64:
		// Convert float to int by truncation
		return int64(v.Float()), nil
	default:
		// Check if it's a time.Time
		if v.Type() == reflect.TypeOf(time.Time{}) {
			// Return Unix timestamp
			return v.Interface().(time.Time).Unix(), nil
		}
		return 0, fmt.Errorf("unsupported type: %v", v.Type())
	}
}

func pointerToTime(ptr interface{}) (time.Time, error) {
	if ptr == nil {
		return time.Time{}, fmt.Errorf("nil pointer")
	}

	v := reflect.ValueOf(ptr)
	if v.Kind() != reflect.Ptr {
		return time.Time{}, fmt.Errorf("not a pointer: %v", ptr)
	}

	// Dereference the pointer
	v = v.Elem()

	switch v.Kind() {
	case reflect.String:
		// Try to parse the string as a time
		t, err := time.Parse(time.RFC3339, v.String())
		if err != nil {
			// If RFC3339 fails, try Unix timestamp
			i, err := strconv.ParseInt(v.String(), 10, 64)
			if err != nil {
				return time.Time{}, fmt.Errorf("cannot convert string to time: %v", err)
			}
			return time.Unix(i, 0), nil
		}
		return t, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// Treat as Unix timestamp
		return time.Unix(v.Int(), 0), nil
	case reflect.Float32, reflect.Float64:
		// Treat as Unix timestamp with fractional seconds
		sec, frac := math.Modf(v.Float())
		return time.Unix(int64(sec), int64(frac*1e9)), nil
	default:
		// Check if it's already a time.Time
		if v.Type() == reflect.TypeOf(time.Time{}) {
			return v.Interface().(time.Time), nil
		}
		return time.Time{}, fmt.Errorf("unsupported type: %v", v.Type())
	}
}

func pointerToFloat(ptr interface{}) (float64, error) {
	if ptr == nil {
		return 0, fmt.Errorf("nil pointer")
	}

	v := reflect.ValueOf(ptr)
	if v.Kind() != reflect.Ptr {
		return 0, fmt.Errorf("not a pointer: %v", ptr)
	}

	// Dereference the pointer
	v = v.Elem()

	switch v.Kind() {
	case reflect.String:
		// Try to parse the string as a float
		f, err := strconv.ParseFloat(v.String(), 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string to float: %v", err)
		}
		return f, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int()), nil
	case reflect.Float32, reflect.Float64:
		return v.Float(), nil
	default:
		// Check if it's a time.Time
		if v.Type() == reflect.TypeOf(time.Time{}) {
			// Convert to Unix timestamp with nanosecond precision
			t := v.Interface().(time.Time)
			return float64(t.Unix()) + float64(t.Nanosecond())/1e9, nil
		}
		return 0, fmt.Errorf("unsupported type: %v", v.Type())
	}
}

func convert(destType int, destLength int, value Value) Value {
	if value == nil {
		return nil
	}

	switch destType {
	case VARCHAR, CHAR, TEXT:
		res := pointerToString(value)
		if destType != TEXT {
			maxLength := destLength
			if maxLength < 0 {
				maxLength = DEFAULT_MAX_LENGTH
			}
			if len(res) > maxLength {
				res = res[:maxLength]
			}
		}
		return &res
	case INTEGER:
		res, _ := pointerToInt(value)
		return &res
	case TIME_STAMP:
		res, _ := pointerToTime(value)
		return &res
	case FLOAT:
		res, _ := pointerToFloat(value)
		return &res
	default:
		return value
	}
}

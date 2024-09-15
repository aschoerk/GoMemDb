package machine

import (
	"math"
	"regexp"
	"strings"
	"time"
)

func popFromMachine(m *Machine) (interface{}, error) {
	if m.s.IsEmpty() {
		return nil, newError("stack is empty")
	}
	res, _ := m.s.Pop()
	return res, nil
}

// AndBooleans performs logical AND on two booleans
func AndBooleans(m *Machine) error {
	b2, err := popFromMachine(m)
	if err != nil {
		return err
	}
	b1, err := popFromMachine(m)
	if err != nil {
		return err
	}

	bool1, ok1 := b1.(bool)
	bool2, ok2 := b2.(bool)
	if !ok1 || !ok2 {
		return newError("top two elements are not bool")
	}

	m.s.Push(bool1 && bool2)
	return nil
}

// OrBooleans performs logical OR on two booleans
func OrBooleans(m *Machine) error {
	b2, err := popFromMachine(m)
	if err != nil {
		return err
	}
	b1, err := popFromMachine(m)
	if err != nil {
		return err
	}

	bool1, ok1 := b1.(bool)
	bool2, ok2 := b2.(bool)
	if !ok1 || !ok2 {
		return newError("top two elements are not *bool")
	}

	m.s.Push(bool1 || bool2)
	return nil
}

// AddInts adds two integers
func AddInts(m *Machine) error {
	i2, err := popFromMachine(m)
	if err != nil {
		return err
	}
	i1, err := popFromMachine(m)
	if err != nil {
		return err
	}

	int1, ok1 := i1.(int64)
	int2, ok2 := i2.(int64)
	if !ok1 || !ok2 {
		return newError("top two elements are not int")
	}

	m.s.Push(int1 + int2)
	return nil
}

// MultiplyInts multiplies two integers
func MultiplyInts(m *Machine) error {
	i2, err := popFromMachine(m)
	if err != nil {
		return err
	}
	i1, err := popFromMachine(m)
	if err != nil {
		return err
	}

	int1, ok1 := i1.(int64)
	int2, ok2 := i2.(int64)
	if !ok1 || !ok2 {
		return newError("top two elements are not int")
	}

	m.s.Push(int1 * int2)
	return nil
}

// DivideInts divides two integers
func DivideInts(m *Machine) error {
	i2, err := popFromMachine(m)
	if err != nil {
		return err
	}
	i1, err := popFromMachine(m)
	if err != nil {
		return err
	}

	int1, ok1 := i1.(int64)
	int2, ok2 := i2.(int64)
	if !ok1 || !ok2 {
		return newError("top two elements are not int")
	}

	if int2 == 0 {
		return newError("division by zero")
	}

	m.s.Push(int1 / int2)
	return nil
}

// ModuloInts performs modulo operation on two integers
func ModuloInts(m *Machine) error {
	i2, err := popFromMachine(m)
	if err != nil {
		return err
	}
	i1, err := popFromMachine(m)
	if err != nil {
		return err
	}

	int1, ok1 := i1.(int64)
	int2, ok2 := i2.(int64)
	if !ok1 || !ok2 {
		return newError("top two elements are not int")
	}

	if int2 == 0 {
		return newError("modulo by zero")
	}

	m.s.Push(int1 % int2)
	return nil
}

// SubtractInts subtracts two integers
func SubtractInts(m *Machine) error {
	i2, err := popFromMachine(m)
	if err != nil {
		return err
	}
	i1, err := popFromMachine(m)
	if err != nil {
		return err
	}

	int1, ok1 := i1.(int64)
	int2, ok2 := i2.(int64)
	if !ok1 || !ok2 {
		return newError("")
	}

	m.s.Push(int1 - int2)
	return nil
}

// AddFloats adds two floats
func AddFloats(m *Machine) error {
	f2, err := popFromMachine(m)
	if err != nil {
		return err
	}
	f1, err := popFromMachine(m)
	if err != nil {
		return err
	}

	float1, ok1 := f1.(float64)
	float2, ok2 := f2.(float64)
	if !ok1 || !ok2 {
		return newError("top two elements are not *float64")
	}

	m.s.Push(float1 + float2)
	return nil
}

// MultiplyFloats multiplies two floats
func MultiplyFloats(m *Machine) error {
	f2, err := popFromMachine(m)
	if err != nil {
		return err
	}
	f1, err := popFromMachine(m)
	if err != nil {
		return err
	}

	float1, ok1 := f1.(float64)
	float2, ok2 := f2.(float64)
	if !ok1 || !ok2 {
		return newError("top two elements are not *float64")
	}

	m.s.Push(float1 * float2)
	return nil
}

// DivideFloats divides two floats
func DivideFloats(m *Machine) error {
	f2, err := popFromMachine(m)
	if err != nil {
		return err
	}
	f1, err := popFromMachine(m)
	if err != nil {
		return err
	}

	float1, ok1 := f1.(float64)
	float2, ok2 := f2.(float64)
	if !ok1 || !ok2 {
		return newError("top two elements are not *float64")
	}

	if float2 == 0 {
		return newError("division by zero")
	}

	m.s.Push(float1 / float2)
	return nil
}

// ModuloFloats performs modulo operation on two floats
func ModuloFloats(m *Machine) error {
	f2, err := popFromMachine(m)
	if err != nil {
		return err
	}
	f1, err := popFromMachine(m)
	if err != nil {
		return err
	}

	float1, ok1 := f1.(float64)
	float2, ok2 := f2.(float64)
	if !ok1 || !ok2 {
		return newError("top two elements are not *float64")
	}

	if float2 == 0 {
		return newError("modulo by zero")
	}

	m.s.Push(math.Mod(float1, float2))
	return nil
}

// SubtractFloats subtracts two floats
func SubtractFloats(m *Machine) error {
	f2, err := popFromMachine(m)
	if err != nil {
		return err
	}
	f1, err := popFromMachine(m)
	if err != nil {
		return err
	}

	float1, ok1 := f1.(float64)
	float2, ok2 := f2.(float64)
	if !ok1 || !ok2 {
		return newError("top two elements are not *float64")
	}

	m.s.Push(float1 - float2)
	return nil
}

// AddStrings concatenates two strings
func AddStrings(m *Machine) error {
	s2, err := popFromMachine(m)
	if err != nil {
		return err
	}
	s1, err := popFromMachine(m)
	if err != nil {
		return err
	}

	str1, ok1 := s1.(string)
	str2, ok2 := s2.(string)
	if !ok1 || !ok2 {
		return newError("top two elements are not string")
	}

	m.s.Push(str1 + str2)
	return nil
}

// LikeStrings performs a simple pattern matching (like SQL LIKE)
func LikeStrings(m *Machine) error {
	pattern, err := popFromMachine(m)
	if err != nil {
		return err
	}
	s, err := popFromMachine(m)
	if err != nil {
		return err
	}

	str, ok1 := s.(string)
	pat, ok2 := pattern.(string)
	if !ok1 || !ok2 {
		m.s.Push(false)
		return nil
	}

	// Simple implementation of LIKE
	// % matches any sequence of characters
	// _ matches any single character
	regexPattern := strings.Replace(strings.Replace(pat, "%", ".*", -1), "_", ".", -1)
	matched, err := regexp.MatchString("^"+regexPattern+"$", str)
	if err != nil {
		return err
	}

	m.s.Push(matched)
	return nil
}

// SubtractTimestamps subtracts two timestamps
func SubtractTimestamps(m *Machine) error {
	t2, err := popFromMachine(m)
	if err != nil {
		return err
	}
	t1, err := popFromMachine(m)
	if err != nil {
		return err
	}

	time1, ok1 := t1.(time.Time)
	time2, ok2 := t2.(time.Time)
	if !ok1 || !ok2 {
		return newError("top two elements are not *time.Time")
	}

	m.s.Push(time1.Sub(time2))
	return nil
}

// AddIntToTimestamp adds an integer (seconds) to a timestamp
func AddIntToTimestamp(m *Machine) error {
	i, err := popFromMachine(m)
	if err != nil {
		return err
	}
	t, err := popFromMachine(m)
	if err != nil {
		return err
	}

	intVal, ok1 := i.(int64)
	timeVal, ok2 := t.(time.Time)
	if !ok1 || !ok2 {
		return newError("top elements are not *int and *time.Time")
	}

	m.s.Push(timeVal.Add(time.Duration(intVal) * time.Second))
	return nil
}

// AddFloatToTimestamp adds a float (seconds) to a timestamp
func AddFloatToTimestamp(m *Machine) error {
	f, err := popFromMachine(m)
	if err != nil {
		return err
	}
	t, err := popFromMachine(m)
	if err != nil {
		return err
	}

	floatVal, ok1 := f.(float64)
	timeVal, ok2 := t.(time.Time)
	if !ok1 || !ok2 {
		return newError("top elements are not *float64 and *time.Time")
	}

	m.s.Push(timeVal.Add(time.Duration(floatVal * float64(time.Second))))
	return nil
}

// SubtractIntToTimestamp Subtracts an integer (seconds) to a timestamp
func SubtractIntFromTimestamp(m *Machine) error {
	i, err := popFromMachine(m)
	if err != nil {
		return err
	}
	t, err := popFromMachine(m)
	if err != nil {
		return err
	}

	intVal, ok1 := i.(int64)
	timeVal, ok2 := t.(time.Time)
	if !ok1 || !ok2 {
		return newError("top elements are not *int and *time.Time")
	}

	m.s.Push(timeVal.Add(-time.Duration(intVal) * time.Second))
	return nil
}

// SubtractFloatFromTimestamp Subtracts a float (seconds) From a timestamp
func SubtractFloatFromTimestamp(m *Machine) error {
	f, err := popFromMachine(m)
	if err != nil {
		return err
	}
	t, err := popFromMachine(m)
	if err != nil {
		return err
	}

	floatVal, ok1 := f.(float64)
	timeVal, ok2 := t.(time.Time)
	if !ok1 || !ok2 {
		return newError("top elements are not *float64 and *time.Time")
	}

	m.s.Push(timeVal.Add(-time.Duration(floatVal * float64(time.Second))))
	return nil
}

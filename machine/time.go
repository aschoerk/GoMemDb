package machine

import (
	"errors"
	"reflect"
	"time"
)

func ConvertTimeToInt64(m *Machine) error {
	e, _ := m.s.Pop()
	v := reflect.ValueOf(e)
	v = v.Elem()

	// Check if the dereferenced value is a time.Time
	if v.Type() != reflect.TypeOf(time.Time{}) {
		return errors.New("Expected time")
	}

	// Convert time.Time to int64 (Unix timestamp)
	timestamp := v.Interface().(time.Time).Unix()

	// Create a pointer to the int64 value
	result := new(int64)
	*result = timestamp

	m.s.Push(result)
	return nil
}

func ConvertTimeToFloat64(m *Machine) error {
	e, _ := m.s.Pop()
	v := reflect.ValueOf(e)
	v = v.Elem()

	// Check if the dereferenced value is a time.Time
	if v.Type() != reflect.TypeOf(time.Time{}) {
		return errors.New("Expected time")
	}

	// Convert time.Time to float64 (Unix timestamp with fractional seconds)
	t := v.Interface().(time.Time)
	timestamp := float64(t.Unix()) + float64(t.Nanosecond())/1e9

	// Create a pointer to the float64 value
	result := new(float64)
	*result = timestamp

	m.s.Push(result)
	return nil
}

func ConvertTimeToString(m *Machine) error {
	e, _ := m.s.Pop()
	v := reflect.ValueOf(e)
	v = v.Elem()

	// Check if the dereferenced value is a time.Time
	if v.Type() != reflect.TypeOf(time.Time{}) {
		return errors.New("Expected time")
	}

	// Convert time.Time to string
	t := v.Interface().(time.Time)
	timeString := t.Format(time.RFC3339)

	// Create a pointer to the string value
	result := new(string)
	*result = timeString

	m.s.Push(result)
	return nil
}

func ConvertStringToTime(m *Machine) error {
	if m.s.IsEmpty() {
		return errors.New("stack is empty")
	}

	// Pop the first element
	firstElement, ok := m.s.Pop()
	if !ok {
		return errors.New("failed to pop element from stack")
	}

	// Assert that the first element is a pointer to a string
	strPtr, ok := firstElement.(string)
	if !ok {
		return errors.New("first element is not a pointer to string")
	}

	// Parse the string as RFC3339
	t, err := time.Parse(time.RFC3339, strPtr)
	if err != nil {
		return errors.New("failed to parse time string: " + err.Error())
	}

	// Push the new time.Time pointer back onto the stack
	m.s.Push(&t)

	return nil
}

// ConvertInt64ToTime replaces the first element of the stack (expected to be *int64) with *time.Time
func ConvertInt64ToTime(m *Machine) error {
	if m.s.IsEmpty() {
		return errors.New("stack is empty")
	}

	// Pop the first element
	firstElement, ok := m.s.Pop()
	if !ok {
		return errors.New("failed to pop element from stack")
	}

	// Assert that the first element is a pointer to int64
	int64Ptr, ok := firstElement.(*int64)
	if !ok {
		return errors.New("first element is not a pointer to int64")
	}

	// Convert int64 to time.Time (assuming it's a Unix timestamp in seconds)
	t := time.Unix(*int64Ptr, 0)

	// Push the new time.Time pointer back onto the stack
	m.s.Push(&t)

	return nil
}

// ConvertFloat64ToTime replaces the first element of the stack (expected to be *float64) with *time.Time
func ConvertFloat64ToTime(m *Machine) error {
	if m.s.IsEmpty() {
		return errors.New("stack is empty")
	}

	// Pop the first element
	firstElement, ok := m.s.Pop()
	if !ok {
		return errors.New("failed to pop element from stack")
	}

	// Assert that the first element is a pointer to float64
	float64Ptr, ok := firstElement.(*float64)
	if !ok {
		return errors.New("first element is not a pointer to float64")
	}

	// Convert float64 to time.Time (assuming it's a Unix timestamp in seconds)
	seconds := int64(*float64Ptr)
	nanoseconds := int64((*float64Ptr - float64(seconds)) * 1e9)
	t := time.Unix(seconds, nanoseconds)

	// Push the new time.Time pointer back onto the stack
	m.s.Push(&t)

	return nil
}

func AddSecondsToTime(m *Machine) error {
	if m.s.Size() < 2 {
		return errors.New("stack must have at least two elements")
	}

	// Pop the first element (seconds to add)
	secondsPtr, ok := m.s.Pop()

	seconds, ok := secondsPtr.(*int)
	if !ok {
		return errors.New("first element must be a pointer to int")
	}

	// Pop the second element (time.Time)
	timePtr, ok := m.s.Pop()

	timeVal, ok := timePtr.(*time.Time)
	if !ok {
		return errors.New("second element must be a pointer to time.Time")
	}

	// Create a new time by adding seconds to the original time
	newTime := timeVal.Add(time.Duration(*seconds) * time.Second)

	// Push the new time pointer back onto the stack
	m.s.Push(&newTime)

	return nil
}

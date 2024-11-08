package machine

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

func newError(error string) error {
	return errors.New(error)
}

// Helper function to check if stack is empty
func checkStack(s Stack) error {
	if s.IsEmpty() {
		return newError("stack is empty")
	}
	return nil
}

// BooleanToInt converts the top boolean to int
func BooleanToInt(m *Machine) error {
	s := m.s
	if err := checkStack(s); err != nil {
		return err
	}
	v, _ := s.Pop()

	b, ok := v.(bool)
	if !ok {
		return newError("top element is not a bool")
	}
	if b {
		s.Push(1)
	} else {
		s.Push(0)
	}
	return nil
}

// BooleanToFloat converts the top boolean to float64
func BooleanToFloat(m *Machine) error {
	s := m.s
	if err := checkStack(s); err != nil {
		return err
	}
	v, _ := s.Pop()
	b, ok := v.(bool)
	if !ok {
		return newError("top element is not a bool")
	}
	if b {
		s.Push(1.0)
	} else {
		s.Push(0.0)
	}
	return nil
}

// BooleanToString converts the top boolean to string
func BooleanToString(m *Machine) error {
	s := m.s
	if err := checkStack(s); err != nil {
		return err
	}
	v, _ := s.Pop()
	b, ok := v.(bool)
	if !ok {
		return newError("top element is not a bool")
	}
	s.Push(strconv.FormatBool(b))
	return nil
}

// IntToBoolean converts the top int to boolean
func IntToBoolean(m *Machine) error {
	s := m.s
	if err := checkStack(s); err != nil {
		return err
	}
	v, _ := s.Pop()
	i, ok := v.(int64)
	if !ok {
		return newError("top element is not an int")
	}
	s.Push(i != 0)
	return nil
}

// IntToFloat converts the top int to float64
func IntToFloat(m *Machine) error {
	s := m.s
	if err := checkStack(s); err != nil {
		return err
	}
	v, _ := s.Pop()
	i, ok := v.(int64)
	if !ok {
		return newError("top element is not an int")
	}
	s.Push(float64(i))
	return nil
}

// IntToTimestamp converts the top int (Unix timestamp) to time.Time
func IntToTimestamp(m *Machine) error {
	s := m.s
	if err := checkStack(s); err != nil {
		return err
	}
	v, _ := s.Pop()
	i, ok := v.(int64)
	if !ok {
		return newError("top element is not an int")
	}
	s.Push(time.Unix(i, 0))
	return nil
}

// IntToString converts the top int to string
func IntToString(m *Machine) error {
	s := m.s
	if err := checkStack(s); err != nil {
		return err
	}
	v, _ := s.Pop()
	i, ok := v.(int64)
	if !ok {
		return newError("top element is not an int")
	}
	s.Push(strconv.FormatInt(i, 10))
	return nil
}

// FloatToBoolean converts the top float64 to boolean
func FloatToBoolean(m *Machine) error {
	s := m.s
	if err := checkStack(s); err != nil {
		return err
	}
	v, _ := s.Pop()
	f, ok := v.(float64)
	if !ok {
		return newError("top element is not a float64")
	}
	s.Push(f != 0)
	return nil
}

// FloatToInt converts the top float64 to int
func FloatToInt(m *Machine) error {
	s := m.s
	if err := checkStack(s); err != nil {
		return err
	}
	v, _ := s.Pop()
	f, ok := v.(float64)
	if !ok {
		return newError("top element is not a float64")
	}
	s.Push(int64(math.Round(f)))
	return nil
}

// FloatToString converts the top float64 to string
func FloatToString(m *Machine) error {
	s := m.s
	if err := checkStack(s); err != nil {
		return err
	}
	v, _ := s.Pop()
	f, ok := v.(float64)
	if !ok {
		return newError("top element is not a float64")
	}
	s.Push(fmt.Sprintf("%f", f))
	return nil
}

// FloatToTimestamp converts the top float64 (Unix timestamp) to time.Time
func FloatToTimestamp(m *Machine) error {
	s := m.s
	if err := checkStack(s); err != nil {
		return err
	}
	v, _ := s.Pop()
	f, ok := v.(float64)
	if !ok {
		return newError("top element is not a float64")
	}
	sec, dec := math.Modf(f)
	s.Push(time.Unix(int64(sec), int64(dec*1e9)))
	return nil
}

// StringToBoolean converts the top string to a boolean
func StringToBoolean(m *Machine) error {
	s := m.s
	if s.IsEmpty() {
		return newError("stack is empty")
	}

	val, _ := s.Pop()

	str, ok := val.(string)
	if !ok {
		return newError("top element is not a string")
	}

	boolVal, err := strconv.ParseBool(str)
	if err != nil {
		return err
	}

	s.Push(boolVal)
	return nil
}

// StringToInt converts the top string to an integer
func StringToInt(m *Machine) error {
	s := m.s
	if s.IsEmpty() {
		return newError("stack is empty")
	}

	val, _ := s.Pop()

	str, ok := val.(string)
	if !ok {
		return newError("top element is not a string")
	}

	intVal, err := strconv.Atoi(str)
	if err != nil {
		return err
	}

	s.Push(intVal)
	return nil
}

// StringToFloat converts the top string to a float64
func StringToFloat(m *Machine) error {
	s := m.s
	if s.IsEmpty() {
		return newError("stack is empty")
	}

	val, _ := s.Pop()

	str, ok := val.(string)
	if !ok {
		return newError("top element is not a string")
	}

	floatVal, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return err
	}

	s.Push(floatVal)
	return nil
}

// StringToTimestamp converts the top string to a time.Time
func StringToTimestamp(m *Machine) error {
	s := m.s
	if s.IsEmpty() {
		return newError("stack is empty")
	}

	val, _ := s.Pop()

	str, ok := val.(string)
	if !ok {
		return newError("top element is not a string")
	}

	timeVal, err := time.Parse(time.RFC3339, str)
	if err != nil {
		return err
	}

	s.Push(timeVal)
	return nil
}

// TimestampToInteger converts the top time.Time to an integer (Unix timestamp)
func TimestampToInteger(m *Machine) error {
	s := m.s
	if s.IsEmpty() {
		return newError("stack is empty")
	}

	val, _ := s.Pop()

	timeVal, ok := val.(time.Time)
	if !ok {
		return newError("top element is not a time.Time")
	}

	s.Push(timeVal.Unix())
	return nil
}

// TimestampToFloat converts the top time.Time to a float64 (Unix timestamp with fractional seconds)
func TimestampToFloat(m *Machine) error {
	s := m.s
	if s.IsEmpty() {
		return newError("stack is empty")
	}

	val, _ := s.Pop()

	timeVal, ok := val.(time.Time)
	if !ok {
		return newError("top element is not a time.Time")
	}

	s.Push(float64(timeVal.UnixNano()) / 1e9)
	return nil
}

// TimestampToString converts the top time.Time to a string
func TimestampToString(m *Machine) error {
	s := m.s
	if s.IsEmpty() {
		return newError("stack is empty")
	}

	val, _ := s.Pop()

	timeVal, ok := val.(time.Time)
	if !ok {
		return newError("top element is not a time.Time")
	}

	s.Push(timeVal.Format(time.RFC3339))
	return nil
}

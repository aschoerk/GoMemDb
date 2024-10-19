package machine

import (
	"errors"
	"time"
)

func typeError() error {
	return errors.New("impossible type conversion")
}

// Boolean comparisons
func BoolLessThan(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bBool, okB := b.(bool)
	aBool, okA := a.(bool)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(!aBool && bBool)
	return nil
}

func BoolGreaterThan(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bBool, okB := b.(bool)
	aBool, okA := a.(bool)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aBool && !bBool)
	return nil
}

func BoolLessThanOrEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bBool, okB := b.(bool)
	aBool, okA := a.(bool)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(!aBool || bBool)
	return nil
}

func BoolGreaterThanOrEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bBool, okB := b.(bool)
	aBool, okA := a.(bool)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aBool || !bBool)
	return nil
}

func BoolEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bBool, okB := b.(bool)
	aBool, okA := a.(bool)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aBool == bBool)
	return nil
}

func BoolNotEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bBool, okB := b.(bool)
	aBool, okA := a.(bool)
	if !okB || !okA {
		m.s.Push(false)

	}
	m.s.Push(aBool != bBool)
	return nil
}

// Integer comparisons
func IntLessThan(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bInt, okB := b.(int64)
	aInt, okA := a.(int64)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aInt < bInt)
	return nil
}

func IntGreaterThan(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bInt, okB := b.(int64)
	aInt, okA := a.(int64)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aInt > bInt)
	return nil
}

func IntLessThanOrEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bInt, okB := b.(int64)
	aInt, okA := a.(int64)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aInt <= bInt)
	return nil
}

func IntGreaterThanOrEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bInt, okB := b.(int64)
	aInt, okA := a.(int64)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aInt >= bInt)
	return nil
}

func IntEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bInt, okB := b.(int64)
	aInt, okA := a.(int64)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aInt == bInt)
	return nil
}

func IntNotEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bInt, okB := b.(int64)
	aInt, okA := a.(int64)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aInt != bInt)
	return nil
}

// Float64 comparisons
func Float64LessThan(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bFloat, okB := b.(float64)
	aFloat, okA := a.(float64)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aFloat < bFloat)
	return nil
}

func Float64GreaterThan(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bFloat, okB := b.(float64)
	aFloat, okA := a.(float64)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aFloat > bFloat)
	return nil
}

func Float64LessThanOrEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bFloat, okB := b.(float64)
	aFloat, okA := a.(float64)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aFloat <= bFloat)
	return nil
}

func Float64GreaterThanOrEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bFloat, okB := b.(float64)
	aFloat, okA := a.(float64)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aFloat >= bFloat)
	return nil
}

func Float64Equal(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bFloat, okB := b.(float64)
	aFloat, okA := a.(float64)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aFloat == bFloat)
	return nil
}

func Float64NotEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bFloat, okB := b.(float64)
	aFloat, okA := a.(float64)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aFloat != bFloat)
	return nil
}

// String comparisons
func StringLessThan(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bStr, okB := b.(string)
	aStr, okA := a.(string)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aStr < bStr)
	return nil
}

func StringGreaterThan(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bStr, okB := b.(string)
	aStr, okA := a.(string)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aStr > bStr)
	return nil
}

func StringLessThanOrEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bStr, okB := b.(string)
	aStr, okA := a.(string)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aStr <= bStr)
	return nil
}

func StringGreaterThanOrEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bStr, okB := b.(string)
	aStr, okA := a.(string)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aStr >= bStr)
	return nil
}

func StringEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bStr, okB := b.(string)
	aStr, okA := a.(string)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aStr == bStr)
	return nil
}

func StringNotEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bStr, okB := b.(string)
	aStr, okA := a.(string)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aStr != bStr)
	return nil
}

// Time comparisons
func TimeLessThan(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bTime, okB := b.(time.Time)
	aTime, okA := a.(time.Time)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aTime.Before(bTime))
	return nil
}

func TimeGreaterThan(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bTime, okB := b.(time.Time)
	aTime, okA := a.(time.Time)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aTime.After(bTime))
	return nil
}

func TimeLessThanOrEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bTime, okB := b.(time.Time)
	aTime, okA := a.(time.Time)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aTime.Before(bTime) || aTime.Equal(bTime))
	return nil
}

func TimeGreaterThanOrEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bTime, okB := b.(time.Time)
	aTime, okA := a.(time.Time)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aTime.After(bTime) || aTime.Equal(bTime))
	return nil
}

func TimeEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bTime, okB := b.(time.Time)
	aTime, okA := a.(time.Time)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(aTime.Equal(bTime))
	return nil
}

func TimeNotEqual(m *Machine) error {
	b, _ := m.s.Pop()
	a, _ := m.s.Pop()
	bTime, okB := b.(time.Time)
	aTime, okA := a.(time.Time)
	if !okB || !okA {
		m.s.Push(false)
		return nil
	}
	m.s.Push(!aTime.Equal(bTime))
	return nil
}

func CompareBool(m *Machine) error {
	stack := m.s
	b2, _ := stack.Pop()
	b1, _ := stack.Pop()

	bool1 := b1.(bool)
	bool2 := b2.(bool)

	if !bool1 && bool2 {
		stack.Push(-1)
	} else if bool1 == bool2 {
		stack.Push(0)
	} else {
		stack.Push(1)
	}
	return nil
}

func CompareInt64(m *Machine) error {
	stack := m.s
	i2, _ := stack.Pop()
	i1, _ := stack.Pop()

	int1 := i1.(int64)
	int2 := i2.(int64)

	if int1 < int2 {
		stack.Push(-1)
	} else if int1 == int2 {
		stack.Push(0)
	} else {
		stack.Push(1)
	}
	return nil
}

func CompareFloat64(m *Machine) error {
	stack := m.s
	f2, _ := stack.Pop()
	f1, _ := stack.Pop()

	float1 := f1.(float64)
	float2 := f2.(float64)

	if float1 < float2 {
		stack.Push(-1)
	} else if float1 == float2 {
		stack.Push(0)
	} else {
		stack.Push(1)
	}
	return nil
}

func CompareString(m *Machine) error {
	stack := m.s
	s2, _ := stack.Pop()
	s1, _ := stack.Pop()

	str1 := s1.(string)
	str2 := s2.(string)

	if str1 < str2 {
		stack.Push(-1)
	} else if str1 == str2 {
		stack.Push(0)
	} else {
		stack.Push(1)
	}
	return nil
}

func CompareTimestamp(m *Machine) error {
	stack := m.s
	t2, _ := stack.Pop()
	t1, _ := stack.Pop()

	time1 := t1.(time.Time)
	time2 := t2.(time.Time)

	if time1.Before(time2) {
		stack.Push(-1)
	} else if time1.Equal(time2) {
		stack.Push(0)
	} else {
		stack.Push(1)
	}
	return nil
}

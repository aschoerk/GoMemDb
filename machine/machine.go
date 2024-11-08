package machine

import (
	. "database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"runtime"

	"github.com/aschoerk/go-sql-mem/data"
)

type Command func(m *Machine) error

// Machine represents our command evaluator
type Machine struct {
	commands []Command
	s        Stack
	p        []Value
	r        data.Tuple
	r2       data.Tuple
	ix       int
}

func (m *Machine) AddCommand(c Command) {
	m.commands = append(m.commands, c)
}

func (m *Machine) AddCommandBeforeLast(c Command) {
	m.commands = append(m.commands[:len(m.commands)-1], c, m.commands[len(m.commands)-1], c)
}

func (m *Machine) ReturnPlaceHolder(ix int) (Value, error) {

	if len(m.p) <= ix || ix < 0 {
		return -1, errors.New("invalid ix")
	}
	return (m.p)[ix], nil
}

func AddPushPlaceHolder(m *Machine, ix int) {
	m.AddCommand(func(m *Machine) error {
		if ix < 0 || ix >= len(m.p) {
			return fmt.Errorf("invalid placeholder ix: %d", ix)
		}
		m.s.Push((m.p)[ix])
		return nil
	})
}

func AddPushAttribute(m *Machine, tableIx int, ix int) {
	m.AddCommand(func(m *Machine) error {
		if ix == -1 {
			m.s.Push(m.r.Id())
		} else {
			val := m.r.SafeData(tableIx, ix)
			m.s.Push(val)
		}
		return nil
	})
}

func AddPushAttribute2(m *Machine, tableIx int, ix int) {
	m.AddCommand(func(m *Machine) error {
		if ix == -1 {
			m.s.Push(m.r2.Id())
		} else {
			val := m.r2.SafeData(tableIx, ix)
			m.s.Push(val)
		}
		return nil
	})
}

func AddPushConstant(m *Machine, val Value) {
	m.AddCommand(func(m *Machine) error {
		m.s.Push(val)
		return nil
	})
}

func AddConversion(m *Machine, conversion func(m *Machine) error, preLast bool) {
	if preLast {
		m.AddCommandBeforeLast(conversion)
	} else {
		m.AddCommand(conversion)
	}
}

// NewMachine creates a new Machine instance
func NewMachine(args []Value) *Machine {
	return &Machine{
		commands: make([]Command, 0),
		s:        NewStack(),
		p:        args,
	}
}

func getFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func ReturnIfNotEqualZero(m *Machine) error {
	res, ok := m.s.Pop()
	if !ok {
		return errors.New("empty stack")
	}
	if res.(int) != 0 {
		m.s.Push(res)
		m.ix = len(m.commands)
	} else {
		if m.ix == len(m.commands) {
			m.s.Push(0)
		}
	}
	return nil
}

func ReturnInverseIfNotEqualZero(m *Machine) error {
	res, ok := m.s.Pop()
	if !ok {
		return errors.New("empty stack")
	}
	i := res.(int)
	if i != 0 {
		m.s.Push(-i)
		m.ix = len(m.commands)
	} else {
		if m.ix == len(m.commands) {
			m.s.Push(0)
		}
	}
	return nil
}

func (m *Machine) Execute(placeHolders []Value, record1 data.Tuple, record2 data.Tuple) (Value, error) {
	defer func() {
		fmt.Println("\n**********************************************")
	}()
	m.s.Clear()
	m.p = placeHolders
	m.r = record1
	m.r2 = record2
	m.ix = 0
	fmt.Println("\n**********************************************")
	fmt.Printf("Starting %d commands \n", len(m.commands))
	for {
		if m.ix >= len(m.commands) {
			break
		}
		command := m.commands[m.ix]
		fmt.Println(getFunctionName(command))
		m.ix++
		err := command(m)
		if err != nil {
			return nil, err
		}
	}
	if m.s.IsEmpty() || m.s.Size() != 1 {
		return nil, fmt.Errorf("Stack is not handled completely or is empty after Execution")
	}
	res, _ := m.s.Pop()
	return res, nil
}

func IsNullCommand(m *Machine) error {
	if m.s.IsEmpty() {
		return errors.New("stack is empty")
	}
	top, _ := m.s.Pop()
	m.s.Push(top == nil)
	return nil
}

func IsNotNullCommand(m *Machine) error {
	if m.s.IsEmpty() {
		return errors.New("stack is empty")
	}
	top, _ := m.s.Pop()
	m.s.Push(top != nil)
	return nil
}

func InvertTopBool(m *Machine) error {
	if m.s.IsEmpty() {
		return errors.New("stack is empty")
	}

	// Pop the top element
	top, _ := m.s.Pop()

	// Check if it's a pointer to bool
	boolPtr, ok := top.(bool)
	if !ok {
		return errors.New("top element is not a pointer to bool")
	}

	m.s.Push(!boolPtr)

	return nil
}

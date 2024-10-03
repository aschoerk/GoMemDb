package machine

import (
	"fmt"
	"reflect"
)

// Stack is an interface that defines the standard operations of a stack data structure.
type Stack interface {
	// Push adds an element to the top of the stack.
	Push(x interface{})

	// Pop removes and returns the top element of the stack.
	// It returns false as the second return value if the stack is empty.
	Pop() (interface{}, bool)

	// Peek returns the top element of the stack without removing it.
	// It returns false as the second return value if the stack is empty.
	Peek() (interface{}, bool)

	// IsEmpty returns true if the stack is empty, false otherwise.
	IsEmpty() bool

	// Size returns the number of elements in the stack.
	Size() int

	// Clear removes all elements from the stack.
	Clear()
}

type sliceStack struct {
	items []interface{}
}

func NewStack() Stack {
	return &sliceStack{}
}

func (s *sliceStack) Push(x interface{}) {
	fmt.Printf("Pushing %v type: %s\n", x, reflect.TypeOf(x))
	s.items = append(s.items, x)
}

func (s *sliceStack) Pop() (interface{}, bool) {
	if s.IsEmpty() {
		return nil, false
	}
	index := len(s.items) - 1
	item := s.items[index]
	s.items = s.items[:index]
	fmt.Printf("Popped %v type %s\n", item, reflect.TypeOf(item))
	return item, true
}

func (s *sliceStack) Peek() (interface{}, bool) {
	if s.IsEmpty() {
		return nil, false
	}
	return s.items[len(s.items)-1], true
}

func (s *sliceStack) IsEmpty() bool {
	return len(s.items) == 0
}

func (s *sliceStack) Size() int {
	return len(s.items)
}

func (s *sliceStack) Clear() {
	s.items = []interface{}{}
}

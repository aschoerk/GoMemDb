package tests

import "testing"

func catchPanic(t *testing.T) {
	if r := recover(); r != nil {
		t.Errorf("Test panicked: %v", r)
	}
}

package tests

import (
	"fmt"
	"slices"
)

func mainx() {
	// Sample data: slice of slices of interface{}
	data := [][]interface{}{
		{1, "apple", 3.14},
		{2, "banana", 2.71},
		{3, "cherry", 1.41},
		{1, "date", 0.58},
	}

	// Sort the data based on the first element of each inner slice
	slices.SortFunc(data, func(a, b []interface{}) int {
		// Type assert the first element of each slice to int
		aVal, aOk := a[0].(int)
		bVal, bOk := b[0].(int)

		// If type assertion fails, consider the slice with failed assertion as "greater"
		if !aOk && !bOk {
			return 0
		} else if !aOk {
			return 1
		} else if !bOk {
			return -1
		}

		// Compare the int values
		return aVal - bVal
	})

	// Print the sorted data
	for _, row := range data {
		fmt.Println(row)
	}
}

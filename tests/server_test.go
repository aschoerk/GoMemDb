package tests

import (
	"testing"

	"github.com/aschoerk/go-sql-mem/driver"
)

func TestServerTest(t *testing.T) {
	driver.StartServer()
}

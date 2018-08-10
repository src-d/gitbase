package integration

import (
	"testing"
)

func TestParse(t *testing.T) {
	Suite.SetUpTest(t)
	Suite.TestParse(t)
}

func TestNativeParse(t *testing.T) {
	Suite.SetUpTest(t)
	Suite.TestNativeParse(t)
}

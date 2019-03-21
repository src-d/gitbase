package memory

import "testing"

func TestNonZero(t *testing.T) {
	if TotalMemory() == 0 {
		t.Fatal("TotalMemory returned 0")
	}
}

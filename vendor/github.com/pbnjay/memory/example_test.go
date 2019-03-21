package memory_test

import (
	"fmt"

	"github.com/pbnjay/memory"
)

func ExampleTotalMemory() {
	fmt.Printf("Total system memory: %d\n", memory.TotalMemory())
}

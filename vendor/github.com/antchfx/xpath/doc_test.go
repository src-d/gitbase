package xpath_test

import (
	"fmt"

	"github.com/antchfx/xpath"
)

// XPath package example.
func Example() {
	expr, err := xpath.Compile("count(//book)")
	if err != nil {
		panic(err)
	}
	var root xpath.NodeNavigator
	// using Evaluate() method
	val := expr.Evaluate(root) // it returns float64 type
	fmt.Println(val.(float64))

	// using Evaluate() method
	expr = xpath.MustCompile("//book")
	val = expr.Evaluate(root) // it returns NodeIterator type.
	iter := val.(*xpath.NodeIterator)
	for iter.MoveNext() {
		fmt.Println(iter.Current().Value())
	}

	// using Select() method
	iter = expr.Select(root) // it always returns NodeIterator object.
	for iter.MoveNext() {
		fmt.Println(iter.Current().Value())
	}
}

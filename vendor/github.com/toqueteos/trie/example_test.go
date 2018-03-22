package trie_test

import (
	"fmt"

	"github.com/cespare/trie"
)

// This example shows simple usage of a Trie as a []byte set.
func ExampleTrie() {
	t := trie.New()
	t.Insert([]byte("hello"))
	t.Insert([]byte("world"))
	for _, s := range []string{"hello", "world", "he", "h", "worlds", ""} {
		fmt.Println(t.Contains([]byte(s)))
	}
	// Output:
	// true
	// true
	// false
	// false
	// false
	// false
}

// This example demonstrates walking through the nodes of a Trie.
func ExampleNode() {
	t := trie.New()
	for _, s := range []string{"folk", "foxes", "fox"} {
		t.Insert([]byte(s))
	}
	n := t.Root()
	fmt.Println(n.Terminal()) // false
	next, ok := n.Walk('a')
	fmt.Println(ok) // false
	for _, c := range []byte("fox") {
		next, ok = n.Walk(c)
		if !ok {
			panic("unexpected")
		}
		fmt.Println(ok) // true
		n = next
	}
	fmt.Println(n.Terminal()) // true
	fmt.Println(n.Leaf())     // false
	for _, c := range []byte("es") {
		next, ok = n.Walk(c)
		if !ok {
			panic("unexpected")
		}
		n = next
	}
	fmt.Println(n.Terminal()) // true
	fmt.Println(n.Leaf())     // true

	// Output:
	// false
	// false
	// true
	// true
	// true
	// true
	// false
	// true
	// true
}

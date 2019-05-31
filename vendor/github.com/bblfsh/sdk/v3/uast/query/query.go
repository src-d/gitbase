package query

import (
	"github.com/bblfsh/sdk/v3/uast/nodes"
)

type Interface interface {
	// Prepare parses the query and prepares it for repeated execution.
	Prepare(query string) (Query, error)
	// Execute prepares and runs a query for a given subtree.
	Execute(root nodes.External, query string) (Iterator, error)
}

type Query interface {
	// Execute runs a query for a given subtree.
	Execute(root nodes.External) (Iterator, error)
}

type Iterator = nodes.Iterator

// AllNodes iterates over all nodes and returns them as a slice.
func AllNodes(it Iterator) []nodes.External {
	var out []nodes.External
	for it.Next() {
		out = append(out, it.Node())
	}
	return out
}

// Count counts the nodes in the iterator. Iterator will be exhausted as a result.
func Count(it Iterator) int {
	var n int
	for it.Next() {
		n++
	}
	return n
}

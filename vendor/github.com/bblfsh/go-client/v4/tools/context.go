package tools

import (
	"fmt"

	"github.com/bblfsh/sdk/v3/uast/nodes"
	"github.com/bblfsh/sdk/v3/uast/query"
	"github.com/bblfsh/sdk/v3/uast/query/xpath"
)

// NewContext creates a new query context.
func NewContext(root nodes.Node) *Context {
	return &Context{
		root:  root,
		xpath: xpath.New(),
	}
}

type Context struct {
	root  nodes.Node
	xpath query.Interface
}

// Filter filters the tree and returns the iterator of nodes that satisfy the given query.
func (c *Context) Filter(query string) (query.Iterator, error) {
	if query == "" {
		query = "//*"
	}
	return c.xpath.Execute(c.root, query)
}

// FilterNode filters the tree and returns a single node that satisfy the given query.
func (c *Context) FilterNode(query string) (nodes.Node, error) {
	it, err := c.Filter(query)
	if err != nil {
		return nil, err
	}
	if !it.Next() {
		return nil, nil
	}
	nd, _ := it.Node().(nodes.Node)
	return nd, nil
}

// FilterValue evaluates a query and returns a results as a value.
func (c *Context) FilterValue(query string) (nodes.Value, error) {
	nd, err := c.FilterNode(query)
	if err != nil {
		return nil, err
	}
	v, ok := nd.(nodes.Value)
	if !ok {
		return nil, fmt.Errorf("expected value, got: %T", nd)
	}
	return v, nil
}

// FilterNode evaluates a query and returns a results as a boolean value.
func (c *Context) FilterBool(query string) (bool, error) {
	val, err := c.FilterValue(query)
	if err != nil {
		return false, err
	}
	v, _ := val.(nodes.Bool)
	return bool(v), nil
}

// FilterNumber evaluates a query and returns a results as a float64 value.
func (c *Context) FilterNumber(query string) (float64, error) {
	val, err := c.FilterNode(query)
	if err != nil {
		return 0, err
	}
	switch val := val.(type) {
	case nodes.Float:
		return float64(val), nil
	case nodes.Int:
		return float64(val), nil
	case nodes.Uint:
		return float64(val), nil
	}
	return 0, fmt.Errorf("expected number, got: %T", val)
}

// FilterInt evaluates a query and returns a results as an int value.
func (c *Context) FilterInt(query string) (int, error) {
	val, err := c.FilterNode(query)
	if err != nil {
		return 0, err
	}
	switch val := val.(type) {
	case nodes.Float:
		return int(val), nil
	case nodes.Int:
		return int(val), nil
	case nodes.Uint:
		return int(val), nil
	}
	return 0, fmt.Errorf("expected int, got: %T", val)
}

// FilterString evaluates a query and returns a results as a string value.
func (c *Context) FilterString(query string) (string, error) {
	val, err := c.FilterNode(query)
	if err != nil {
		return "", err
	}
	v, ok := val.(nodes.String)
	if !ok {
		return "", fmt.Errorf("expected string, got: %T", val)
	}
	return string(v), nil
}

// Filter filters the tree and returns the iterator of nodes that satisfy the given query.
func Filter(node nodes.Node, query string) (query.Iterator, error) {
	return NewContext(node).Filter(query)
}

// FilterNode filters the tree and returns a single node that satisfy the given query.
func FilterNode(node nodes.Node, query string) (nodes.Node, error) {
	return NewContext(node).FilterNode(query)
}

// FilterValue evaluates a query and returns a results as a value.
func FilterValue(node nodes.Node, query string) (nodes.Value, error) {
	return NewContext(node).FilterValue(query)
}

// FilterNode evaluates a query and returns a results as a boolean value.
func FilterBool(node nodes.Node, query string) (bool, error) {
	return NewContext(node).FilterBool(query)
}

// FilterNumber evaluates a query and returns a results as a float64 value.
func FilterNumber(node nodes.Node, query string) (float64, error) {
	return NewContext(node).FilterNumber(query)
}

// FilterInt evaluates a query and returns a results as an int value.
func FilterInt(node nodes.Node, query string) (int, error) {
	return NewContext(node).FilterInt(query)
}

// FilterString evaluates a query and returns a results as a string value.
func FilterString(node nodes.Node, query string) (string, error) {
	return NewContext(node).FilterString(query)
}

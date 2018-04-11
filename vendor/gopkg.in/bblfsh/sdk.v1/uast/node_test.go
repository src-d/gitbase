package uast

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	fixtureDir = "fixtures"
)

func TestToNodeErrUnsupported(t *testing.T) {
	require := require.New(t)
	p := &ObjectToNode{}
	n, err := p.ToNode(struct{}{})
	require.Error(err)
	require.Nil(n)
	require.True(ErrUnsupported.Is(err))
}

func TestToNodeErrEmptyAST(t *testing.T) {
	topLevelIsRootNode := false
	testToNodeErrEmptyAST(t, topLevelIsRootNode)
	topLevelIsRootNode = true
	testToNodeErrEmptyAST(t, topLevelIsRootNode)
}

func testToNodeErrEmptyAST(t *testing.T, topIsRoot bool) {
	require := require.New(t)
	empty := make(map[string]interface{})
	p := &ObjectToNode{TopLevelIsRootNode: topIsRoot}
	n, err := p.ToNode(empty)
	require.Error(err)
	require.Nil(n)
	require.True(ErrEmptyAST.Is(err))
}

func TestToNodeErrUnexpectedObjectSize(t *testing.T) {
	require := require.New(t)
	multiRoot := make(map[string]interface{})
	multiRoot["a"] = 0
	multiRoot["b"] = 0
	p := &ObjectToNode{}
	n, err := p.ToNode(multiRoot)
	require.Error(err)
	require.Nil(n)
	require.True(ErrUnexpectedObjectSize.Is(err))
}

func TestToNodeWithTopLevelRoot(t *testing.T) {
	require := require.New(t)

	root, err := getRootAtTopLevelFromFixture()
	require.Nil(err)

	p := &ObjectToNode{
		TopLevelIsRootNode: true,
		InternalTypeKey:    "internalClass",
		LineKey:            "line",
	}

	n, err := p.ToNode(root)
	require.NoError(err)
	require.NotNil(n)
}

// Returns a fixture of an AST with its root at the top level, by
// reusing the already existing fixture at java_example_1; it strips a
// few object from the top levels of the fixture (the CompilationUnit,
// then the types, then picks the first type) util we are left with a
// AST node at its top level.
func getRootAtTopLevelFromFixture() (map[string]interface{}, error) {
	ast, err := getFixture("java_example_1.json")
	if err != nil {
		return nil, err
	}

	// strip the CompilationUnit object
	compilationUnit, ok := ast["CompilationUnit"]
	if !ok {
		return nil, fmt.Errorf("key not found: CompilationUnit")
	}
	ast, ok = compilationUnit.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid cast: compilationUnit to map[string]interface{}")
	}

	// get the list of types
	types, ok := ast["types"]
	if !ok {
		return nil, fmt.Errorf("key not found: types")
	}
	list, ok := types.([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid cast: types to []interface{}")
	}

	if len(list) == 0 {
		return nil, fmt.Errorf("empty list of types")
	}

	first, ok := list[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid cast: first to map[string]interface{}")
	}

	return first, nil
}

func TestToNoderJava(t *testing.T) {
	require := require.New(t)

	f, err := getFixture("java_example_1.json")
	require.NoError(err)

	tn := &ObjectToNode{
		InternalTypeKey: "internalClass",
		LineKey:         "line",
	}
	n, err := tn.ToNode(f)
	require.NoError(err)
	require.NotNil(n)
}

// Test for promoting a specific property-list elements to its own node
func TestPropertyListPromotionSpecific(t *testing.T) {
	require := require.New(t)

	f, err := getFixture("java_example_1.json")
	require.NoError(err)

	p := &ObjectToNode{
		InternalTypeKey: "internalClass",
		LineKey:         "line",
	}
	n, err := p.ToNode(f)
	require.NoError(err)
	require.NotNil(n)

	child := findChildWithInternalType(n, "CompilationUnit.types")
	require.Nil(child)

	p = &ObjectToNode{
		InternalTypeKey: "internalClass",
		LineKey:         "line",
		PromotedPropertyLists: map[string]map[string]bool{
			"CompilationUnit": {"types": true},
		},
		PromoteAllPropertyLists: false,
	}

	n, err = p.ToNode(f)
	require.NoError(err)
	require.NotNil(n)

	child = findChildWithInternalType(n, "CompilationUnit.types")
	require.NotNil(child)
}

// Test for promoting a property with a string value to its own node
func TestPropertyString(t *testing.T) {
	require := require.New(t)

	f, err := getFixture("java_example_1.json")
	require.NoError(err)

	p := &ObjectToNode{
		InternalTypeKey: "internalClass",
		LineKey:         "line",
	}
	n, err := p.ToNode(f)
	require.NoError(err)
	require.NotNil(n)

	child := findChildWithInternalType(n, "CompilationUnit.internalClass")
	require.Nil(child)

	p = &ObjectToNode{
		InternalTypeKey: "internalClass",
		LineKey:         "line",
		PromotedPropertyStrings: map[string]map[string]bool{
			"CompilationUnit": {"internalClass": true},
		},
	}

	n, err = p.ToNode(f)
	require.NoError(err)
	require.NotNil(n)

	child = findChildWithInternalType(n, "CompilationUnit.internalClass")
	require.NotNil(child)
	require.Equal(child.Token, "CompilationUnit")
}

// Test promoting all property-list elements to its own node
func TestPropertyListPromotionAll(t *testing.T) {
	require := require.New(t)

	f, err := getFixture("java_example_1.json")
	require.NoError(err)

	p := &ObjectToNode{
		InternalTypeKey: "internalClass",
		LineKey:         "line",
	}
	n, err := p.ToNode(f)
	require.NoError(err)
	require.NotNil(n)
	child := findChildWithInternalType(n, "CompilationUnit.types")
	require.Nil(child)

	p = &ObjectToNode{
		InternalTypeKey:         "internalClass",
		LineKey:                 "line",
		PromoteAllPropertyLists: true,
	}

	n, err = p.ToNode(f)
	require.NoError(err)
	require.NotNil(n)

	child = findChildWithInternalType(n, "CompilationUnit.types")
	require.NotNil(child)
}

func TestSpecificTokens(t *testing.T) {
	require := require.New(t)

	f, err := getFixture("java_example_1.json")
	require.NoError(err)

	c := &ObjectToNode{
		InternalTypeKey: "internalClass",
		LineKey:         "line",
		SpecificTokenKeys: map[string]string{
			"CompilationUnit": "specificToken",
		},
	}
	n, err := c.ToNode(f)
	require.NoError(err)
	require.NotNil(n)
	require.True(n.Token == "SomeSpecificToken")
}

func TestSyntheticTokens(t *testing.T) {
	require := require.New(t)

	f, err := getFixture("java_example_1.json")
	require.NoError(err)

	c := &ObjectToNode{
		InternalTypeKey: "internalClass",
		LineKey:         "line",
		SyntheticTokens: map[string]string{
			"CompilationUnit": "TestToken",
		},
	}
	n, err := c.ToNode(f)
	require.NoError(err)
	require.NotNil(n)
	child := findChildWithInternalType(n, "CompilationUnit")

	require.Nil(child)
	n, err = c.ToNode(f)
	require.NoError(err)
	require.NotNil(n)
	require.True(n.Token == "TestToken")
}

func TestComposedPositionKeys(t *testing.T) {
	require := require.New(t)

	ast := map[string]interface{}{
		"type":  "sample",
		"start": "66",
		"loc": map[string]interface{}{
			"start": map[string]interface{}{
				"line":   "4",
				"column": "31",
			},
			"end": map[string]interface{}{
				"line":   "4",
				"column": "43",
			},
		},
		"end": "78",
	}

	c := &ObjectToNode{
		InternalTypeKey:    "type",
		TopLevelIsRootNode: true,
		OffsetKey:          "start",
		EndOffsetKey:       "end",
		LineKey:            "loc.start.line",
		EndLineKey:         "loc.end.line",
		ColumnKey:          "loc.start.column",
		EndColumnKey:       "loc.end.column",
	}
	n, err := c.ToNode(ast)
	require.NoError(err)
	require.NotNil(n)

	require.True(n.StartPosition.Offset == 66)
	require.True(n.StartPosition.Line == 4)
	require.True(n.StartPosition.Col == 31)
	require.True(n.EndPosition.Offset == 78)
	require.True(n.EndPosition.Line == 4)
	require.True(n.EndPosition.Col == 43)
}

func TestNilPropertiesAreIgnored(t *testing.T) {
	require := require.New(t)

	ast := map[string]interface{}{
		"type": "file",
		"group": map[string]interface{}{
			"not_nil":   42,
			"nil":       nil,
			"nil_slice": []interface{}(nil),
			"nil_map":   map[string]interface{}(nil),
		},
	}

	c := &ObjectToNode{
		TopLevelIsRootNode: true,
		InternalTypeKey:    "type",
	}

	n, err := c.ToNode(ast)
	require.NoError(err)
	require.NotNil(n)
	require.Len(n.Children, 1)
	child := n.Children[0]
	require.Equal(child.Properties["not_nil"], "42")
}

func TestIsNode(t *testing.T) {
	require := require.New(t)

	ast := map[string]interface{}{
		"type": "file",
		"keep": 1,
		"children": []interface{}{
			map[string]interface{}{"type": "foo", "keep": 1},
			map[string]interface{}{"type": "bar"},
		},
	}

	c := &ObjectToNode{
		InternalTypeKey:    "type",
		TopLevelIsRootNode: true,
		IsNode: func(m map[string]interface{}) bool {
			_, ok := m["keep"]
			return ok
		},
	}

	n, err := c.ToNode(ast)
	require.NoError(err)
	require.NotNil(n)
	require.Equal(n.InternalType, "file")
	require.Len(n.Children, 1)
	require.Equal(n.Children[0].InternalType, "foo")
}

func TestIsNodeMultipleRoot(t *testing.T) {
	require := require.New(t)

	ast := map[string]interface{}{
		"type": "file",
		"children": []interface{}{
			map[string]interface{}{"type": "foo", "keep": 1},
			map[string]interface{}{"type": "bar", "keep": 1},
		},
	}

	c := &ObjectToNode{
		InternalTypeKey:    "type",
		TopLevelIsRootNode: true,
		IsNode: func(m map[string]interface{}) bool {
			_, ok := m["keep"]
			return ok
		},
	}

	n, err := c.ToNode(ast)
	require.NotNil(err)
	require.Nil(n)
	require.True(ErrUnsupported.Is(err))
}

func TestIsNodeSkip(t *testing.T) {
	require := require.New(t)

	ast := map[string]interface{}{
		"type": "file",
		"children": []interface{}{
			map[string]interface{}{"type": "foo", "keep": 1},
			map[string]interface{}{"type": "bar"},
		},
	}

	c := &ObjectToNode{
		InternalTypeKey:    "type",
		TopLevelIsRootNode: true,
		IsNode: func(m map[string]interface{}) bool {
			_, ok := m["keep"]
			return ok
		},
	}

	n, err := c.ToNode(ast)
	require.NoError(err)
	require.NotNil(n)
	require.Equal(n.InternalType, "foo")
	require.Len(n.Children, 0)
}

func TestModifier(t *testing.T) {
	require := require.New(t)

	ast := map[string]interface{}{
		"type": "file",
		"foo":  "qux",
	}

	c := &ObjectToNode{
		InternalTypeKey:    "type",
		TopLevelIsRootNode: true,
		Modifier: func(m map[string]interface{}) error {
			m["foo"] = "bar"
			return nil
		},
	}

	n, err := c.ToNode(ast)
	require.NoError(err)
	require.NotNil(n)
	require.Equal(n.Properties["foo"], "bar")
}

func TestOnToNode(t *testing.T) {
	require := require.New(t)

	ast := map[string]interface{}{}

	c := &ObjectToNode{
		InternalTypeKey: "internalClass",
		OnToNode: func(v interface{}) (interface{}, error) {
			return getFixture("java_example_1.json")
		},
	}

	n, err := c.ToNode(ast)
	require.NoError(err)
	require.NotNil(n)
	require.Len(n.Children, 18)
}

func TestToNodePropsArray(t *testing.T) {
	require := require.New(t)

	ast := map[string]interface{}{}
	astJSON := `{"array": ["a", "b"], "kind": "VariableDeclarationList"}`
	err := json.Unmarshal([]byte(astJSON), &ast)
	require.NoError(err)

	c := &ObjectToNode{
		TopLevelIsRootNode: true,
		InternalTypeKey:    "kind",
	}

	n, err := c.ToNode(ast)
	require.NoError(err)
	require.NotNil(n)
	require.Equal("VariableDeclarationList", n.InternalType)
	require.Len(n.Properties, 1)
	require.Equal(`["a","b"]`, n.Properties["array"])
}

func TestToNodePropsMap(t *testing.T) {
	require := require.New(t)

	ast := map[string]interface{}{}
	astJSON := `{"kind": "SomeType"}`
	err := json.Unmarshal([]byte(astJSON), &ast)
	require.NoError(err)

	c := &ObjectToNode{
		TopLevelIsRootNode: true,
		InternalTypeKey:    "kind",
		Modifier: func(n map[string]interface{}) error {
			n["map"] = map[string]bool{"value": true}
			return nil
		},
	}

	n, err := c.ToNode(ast)
	require.NoError(err)
	require.NotNil(n)
	require.Equal("SomeType", n.InternalType)
	require.Len(n.Properties, 1)
	require.Equal(`{"value":true}`, n.Properties["map"])
}

func findChildWithInternalType(n *Node, internalType string) *Node {
	for _, child := range n.Children {
		if child.InternalType == internalType {
			return child
		}
	}
	return nil
}

func getFixture(name string) (map[string]interface{}, error) {
	path := filepath.Join(fixtureDir, name)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	d := json.NewDecoder(f)
	data := map[string]interface{}{}
	if err := d.Decode(&data); err != nil {
		_ = f.Close()
		return nil, err
	}

	if err := f.Close(); err != nil {
		return nil, err
	}

	return data, nil
}

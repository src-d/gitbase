// Copyright 2017 Sourced Technologies SL
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy
// of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

// Package uast defines a UAST (Universal Abstract Syntax Tree) representation
// and operations to manipulate them.
package uast

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/bblfsh/sdk/v3/uast/nodes"
	"github.com/bblfsh/sdk/v3/uast/role"
)

func init() {
	// Register all types from the package under this namespace
	RegisterPackage(NS,
		Position{},
		Positions{},
		GenNode{},
		Identifier{},
		String{},
		Bool{},
		QualifiedIdentifier{},
		Comment{},
		Group{},
		FunctionGroup{},
		Block{},
		Alias{},
		Import{},
		RuntimeImport{},
		RuntimeReImport{},
		InlineImport{},
		Argument{},
		FunctionType{},
		Function{},
	)
}

// Special field keys for nodes.Object
const (
	KeyType  = "@type"  // the type of UAST node (InternalType in v1)
	KeyToken = "@token" // token of the UAST node (Native and Annotated nodes only)
	KeyRoles = "@role"  // roles of UAST node (Annotated nodes only); for representations see RoleList
	KeyPos   = "@pos"   // positional information is stored in this field, see Positions
)

const (
	// NS is a namespace for the UAST types.
	NS = "uast"

	// TypePosition is a node type for positional information in AST. See AsPosition.
	TypePosition = NS + ":Position"
	// TypePositions is a node type for a root node of positional information in AST. See AsPositions.
	TypePositions = NS + ":Positions"
	// TypeOperator is a node type for an operator AST node. See Operator.
	TypeOperator = NS + ":Operator"
	// KeyPosOff is a name for a Position object field that stores a bytes offset.
	KeyPosOff = "offset"
	// KeyPosLine is a name for a Position object field that stores a source line.
	KeyPosLine = "line"
	// KeyPosCol is a name for a Position object field that stores a source column.
	KeyPosCol = "col"

	KeyStart = "start" // StartPosition
	KeyEnd   = "end"   // EndPosition
)

// Position represents a position in a source code file.
type Position struct {
	// Offset is the position as an absolute byte offset. It is a 0-based index.
	Offset uint32 `json:"offset"`
	// Line is the line number. It is a 1-based index.
	Line uint32 `json:"line"`
	// Col is the column number — the byte offset of the position relative to
	// a line. It is a 1-based index.
	Col uint32 `json:"col"`
}

// HasOffset checks if a position has a valid offset value.
func (p Position) HasOffset() bool {
	return p.Offset != 0 || (p.Line == 1 && p.Col == 1)
}

// HasLineCol checks if a position has a valid line-column pair.
func (p Position) HasLineCol() bool {
	return p.Line != 0 && p.Col != 0
}

// Valid checks if position value is valid.
func (p Position) Valid() bool {
	return p != (Position{})
}

// Less reports whether position p is strictly less than p2.
//
// If both positions have offsets, they will be used for comparison.
// Otherwise, line-column pair will be used.
//
// Invalid positions are sorted last.
func (p Position) Less(p2 Position) bool {
	if !p.Valid() {
		return false
	} else if !p2.Valid() {
		return true
	}
	if p.HasOffset() && p2.HasOffset() {
		return p.Offset < p2.Offset
	}
	if p.Line != p2.Line {
		if p.Line != 0 && p2.Line != 0 {
			return p.Line < p2.Line
		}
		return p.Line != 0
	}
	return p.Col != 0 && p.Col < p2.Col
}

// Positions is a container that stores all positional information for a UAST node.
//
// The string key is a name of a position, for example KeyStart is a start position
// of a node and KeyEnd is an end position of a node. Driver may provide additional
// positional information for other tokens that the node consists of.
type Positions map[string]Position

// Keys returns a sorted slice of position names.
func (p Positions) Keys() []string {
	arr := make([]string, 0, len(p))
	for k := range p {
		arr = append(arr, k)
	}
	sort.Strings(arr)
	return arr
}

// Start returns a start position of the node.
func (p Positions) Start() *Position {
	if p, ok := p[KeyStart]; ok {
		return &p
	}
	return nil
}

// End returns an end position of the node.
func (p Positions) End() *Position {
	if p, ok := p[KeyEnd]; ok {
		return &p
	}
	return nil
}

// ToObject converts a positions map to a generic UAST node.
func (p Positions) ToObject() nodes.Object {
	n, err := toNodeReflect(reflect.ValueOf(p))
	if err != nil {
		panic(err)
	}
	return n.(nodes.Object)
}

// AsPosition transforms a generic AST node to a Position object.
func AsPosition(m nodes.Object) *Position {
	if TypeOf(m) != TypePosition {
		return nil
	}
	var p Position
	if err := NodeAs(m, &p); err != nil {
		panic(err)
	}
	return &p
}

// PositionsOf returns a complete positions map for the given UAST node.
// The function will return nil for non-object nodes like arrays and values. To get
// positions for these nodes, PositionsOf should be called on their parent node.
func PositionsOf(n nodes.Node) Positions {
	m, ok := n.(nodes.Object)
	if !ok {
		return nil
	}
	o, _ := m[KeyPos].(nodes.Object)
	if len(o) == 0 {
		return nil
	}
	ps := make(Positions, len(o))
	for k, v := range o {
		po, _ := v.(nodes.Object)
		if p := AsPosition(po); p != nil {
			ps[k] = *p
		}
	}
	return ps
}

// ToObject converts Position to a generic AST node.
func (p Position) ToObject() nodes.Object {
	n, err := toNodeReflect(reflect.ValueOf(&p))
	if err != nil {
		panic(err)
	}
	return n.(nodes.Object)
}

// RoleList converts a set of roles into a list node.
func RoleList(roles ...role.Role) nodes.Array {
	arr := make(nodes.Array, 0, len(roles))
	for _, r := range roles {
		arr = append(arr, nodes.String(r.String()))
	}
	return arr
}

// RolesOf is a helper for getting node UAST roles (see KeyRoles).
// The function will returns nil roles array for non-object nodes like arrays and values.
func RolesOf(n nodes.Node) role.Roles {
	m, ok := n.(nodes.Object)
	if !ok {
		return nil
	}
	arr, ok := m[KeyRoles].(nodes.Array)
	if !ok || len(arr) == 0 {
		if tp := TypeOf(m); tp == "" || strings.HasPrefix(tp, NS+":") {
			return nil
		}
		return role.Roles{role.Unannotated}
	}
	out := make(role.Roles, 0, len(arr))
	for _, v := range arr {
		if r, ok := v.(nodes.String); ok {
			out = append(out, role.FromString(string(r)))
		}
	}
	return out
}

// TokenOf is a helper for getting node token (see KeyToken).
//
// The token is an exact code snippet that represents a given AST node. It only works for
// primitive nodes like identifiers and string literals, and is only available in Native
// and Annotated parsing modes. For Semantic mode, see ContentOf.
//
// It returns an empty string if the node is not an object, or there is no token.
func TokenOf(n nodes.Node) string {
	switch n := n.(type) {
	case nodes.String:
		return string(n)
	case nodes.Value:
		return fmt.Sprint(n)
	case nodes.Object:
		t := n[KeyToken]
		if t == nil {
			return ""
		}
		return TokenOf(t)
	}
	return ""
}

// Tokens collects all tokens of the tree recursively (pre-order). See TokenOf.
func Tokens(n nodes.Node) []string {
	var tokens []string
	nodes.WalkPreOrder(n, func(n nodes.Node) bool {
		if obj, ok := n.(nodes.Object); ok {
			if tok := TokenOf(obj); tok != "" {
				tokens = append(tokens, tok)
			}
		}
		return true
	})
	return tokens
}

// ContentOf returns any relevant string content of a node. It returns a Name for
// Identifiers, Value for Strings, etc and uses TokenOf for non-Semantic nodes.
//
// The result may not exactly match the source file since values in Semantic nodes
// are normalized.
//
// It returns an empty string if the node has no string content.
func ContentOf(n nodes.Node) string {
	if obj, ok := n.(nodes.Object); ok {
		typ, _ := obj[KeyType].(nodes.String)

		if field, ok := typeContentKey[string(typ)]; ok {
			// allow nested objects
			return ContentOf(obj[field])
		}
	}
	// fallback to token
	return TokenOf(n)
}

// HashNoPos hashes the node, but skips positional information.
func HashNoPos(n nodes.External) nodes.Hash {
	h := nodes.NewHasher()
	h.KeyFilter = func(key string) bool {
		return key != KeyPos
	}
	return h.HashOf(n)
}

// Any is an alias type for any UAST node.
type Any interface{}

// Scope is a temporary definition of a scope semantic type.
type Scope = Any

// GenNode is embedded into every UAST node to store positional information.
type GenNode struct {
	Positions Positions `json:"@pos,omitempty"`
}

// Identifier is a name of an entity.
//
// What is considered an Identifier:
// - variable, type, function names;
// - builtin type names;
// - package name consisting of a single name element;
// - goto labels;
//
// Not considered an Identifier:
// - qualified names (see QualifiedIdentifier);
// - path-like or url-like package names (see String);
type Identifier struct {
	GenNode
	// Name of an entity. Can be any valid UTF8 string.
	Name string `json:"Name" uast:",content"`
}

// Roles returns a list of UAST node roles that apply to this node.
func (Identifier) Roles() []role.Role {
	return []role.Role{
		role.Identifier,
	}
}

// String is an unescaped UTF8 string literal.
//
// What is considered a String literal:
// - escaped string literals;
// - raw string literals;
// - path-like or url-like package names;
//
// Not considered a String literal:
// - identifiers (see Identifier);
// - qualified names (see QualifiedIdentifier);
// - numeric and boolean literals;
// - special regexp literals;
type String struct {
	GenNode
	// Value is a UTF8 string literal value.
	//
	// Drivers should remove any quotes and unescape the value according to the language rules.
	Value string `json:"Value" uast:",content"`

	// Format is an optional language-specific string that describes the format of the literal.
	//
	// This field can be empty for the most common string literal type of a specific language.
	// The priority is given to a one-line literal that escapes newline characters.
	//
	// TODO: define some well-known formats and maybe make it an enum
	Format string `json:"Format"`
}

// QualifiedIdentifier is a name of an entity that consists of multiple simple identifiers,
// organized in a hierarchy, similar to filesystem paths.
//
// What is considered a QualifiedIdentifier:
// - qualified names that consist of Identifier-like elements;
//
// Not considered a QualifiedIdentifier:
// - path-like or url-like package names (see String);
// - selector expressions (a->b and a.b in C++);
type QualifiedIdentifier struct {
	GenNode
	// Names is a list of simple identifiers starting from a root level of hierarchy
	// and ending with leaf identifier. Names should not be empty.
	Names []Identifier `json:"Names"`
}

// Comment is a no-op node that can span multiple lines and provides a human-readable
// description for code around it.
//
// TODO: currently some annotations are also considered a Comment; need to clarify this
type Comment struct {
	GenNode

	// Block is set to true for block-style comments.
	//
	// TODO: should be a string similar to Format field in String literal;
	//       may have more than 2 possible values (line, block, doc?)
	Block bool `json:"Block"`

	// Text is an unescaped UTF8 string with the comment text.
	//
	// Drivers must trim any comment-related tokens as well as whitespaces and
	// stylistic characters at the beginning of ToObjecteach line. See Prefix, Suffix, Tab.
	//
	// Example:
	//    /*
	//     * some comment
	//     */
	//
	//    only "some comment" is considered a text
	Text string `json:"Text" uast:",content"`

	// Prefix is a set of whitespaces and stylistic characters that appear before
	// the first line of an actual comment text.
	//
	// Example:
	//    /*
	//     * some comment
	//     */
	//
	//    the "\n" after the "/*" token is considered a prefix
	Prefix string `json:"Prefix"`

	// Suffix is a set of whitespaces and stylistic characters that appear after
	// the last line of an actual comment text.
	//
	// Example:
	//    /*
	//     * some comment
	//     */
	//
	//    the "\n " before the "*/" token is considered a suffix
	Suffix string `json:"Suffix"`

	// Tab is a set of whitespace and stylistic characters that appears at the beginning
	// of each comment line, except the first one, which uses Prefix.
	//
	// Example:
	//    /*
	//     * some comment
	//     */
	//
	//    the " *" before the comment text is considered a tab
	//
	// TODO(dennwc): rename to Indent?
	Tab string `json:"Tab"`
}

// Group is a no-op UAST node that groups multiple nodes together.
//
// Drivers may use it when for grouping statements that are represented by a single statement
// in the native AST.
//
// For example, a language may describe a way to define multiple variables in one statement.
// This statement should be split into separate UAST nodes that become a children of a single Group.
//
// Groups should never convey any semantic meaning.
type Group struct {
	GenNode
	// Nodes is a list of UAST nodes in a group.
	Nodes []Any `json:"Nodes"`
}

// FunctionGroup is a special group node that joins multiple UAST nodes related to a function
// declaration.
//
// FunctionGroup usually contains at least an Alias node that specifies the function name and
// may contain additional nodes such as annotations and comments and docs related to it.
//
// See Function for more details about function declarations.
type FunctionGroup Group

// Block is a logical code block. It groups multiple statements and enforces a sequential execution
// of these statements.
//
// When the Block should be used:
// - for function bodies;
// - when the statement defines a new scope;
type Block struct {
	GenNode
	Statements []Any `json:"Statements"`

	// TODO: block is logical and should have a reference to a corresponding scope; should be used in "if"s, etc
	// Scope *Scope
}

// Alias provides a way to assign a permanent name to an entity, or give an alternative name.
//
// Aliases are immutable and the only way to redefine it is to shadow it in the child scope.
//
// What is considered an Alias:
// - a name of a function in a function declaration;
// - a name of a constant and its value;
// - a name of a preprocessor macros and its substitution;
// - variable declaration; // TODO: should point to some Variable node
//
// Not considered an Alias:
// - value assignments to a variable, even if it defines a variable;
type Alias struct {
	GenNode
	// Name assigned to an entity.
	//
	// TODO: define a different node to handle QualifiedIdentifier as a name
	Name Identifier `json:"Name"`

	// A UAST node to assign a name to.
	Node Any `json:"Node"`

	// TODO: should include a pointer to a scope where an alias is defined
	// Target *Scope
}

// Import is a statement that can load other modules into the program or library.
//
// This is a declarative import statement. Its position in the UAST does not affect
// the way and the time when the module is imported and the side-effects are executed
// only once a package is initialized.
//
// This describes imports in Go, Java, and C#, for example.
//
// For more specific types see RuntimeImport, RuntimeReImport, InlineImport.
type Import struct {
	GenNode
	// Path is a path of a modules or package to load.
	//
	// May have a value of:
	// - String (specifies relative or absolute module path);
	// - QualifiedIdentifier (specifies a canonical module name);
	// - Alias (contains any of the above and defines a local package name within a file/scope);
	Path Any `json:"Path"`

	// All is set to true when the statement defines all exported symbols from
	// a module in the local scope (usually file).
	All    bool  `json:"All"`
	Names  []Any `json:"Names"`
	Target Scope `json:"Target"`
}

// RuntimeImport is a type of an import statement that imports a module only when an execution
// reaches this UAST node. The import side effects are executed only once, regardless of how many
// times a statement is reached.
//
// This describes imports in PHP, Python and JS for example.
//
// For other import types, see Import.
type RuntimeImport Import

// RuntimeReImport is a subset of RuntimeImport statement that will re-execute
// an import and its side-effects statement each time an execution reaches the
// statement.
//
// This describes imports in PHP and Python for example.
//
// For other import types, see Import.
type RuntimeReImport RuntimeImport

// InlineImport is a subset of import statement that acts like a preprocessor - all statements in
// the imported module are copied into a position of the UAST node.
//
// This describes #include in C and C++.
//
// For other import types, see Import.
type InlineImport Import

// Argument is a named argument or return of a function.
type Argument struct {
	GenNode
	// Name is an optional name of an argument.
	Name *Identifier `json:"Name"`

	// Type is an optional type of an argument.
	Type Any `json:"Type"`

	// Init is an optional expression used to initialize the argument
	// in case no value is provided.
	Init Any `json:"Init"`

	// Variadic is set for the last argument of a function with a
	// variadic number of arguments.
	Variadic bool `json:"Variadic"`

	// MapVariadic is set for the last argument of a function that accepts a
	// map/dictionary value that is mapped to function arguments.
	MapVariadic bool `json:"MapVariadic"`

	// Receiver is set to true if an argument is a receiver of a method call.
	Receiver bool `json:"Receiver"`
}

// FunctionType is a signature of a function.
type FunctionType struct {
	GenNode
	// Arguments is a set of arguments the function accepts.
	//
	// Methods defined on structures and classes must have the first argument
	// that corresponds to a method's receiver ("this" in most languages).
	Arguments []Argument `json:"Arguments"`

	// Returns is a set of values returned by a function.
	//
	// Languages with an implicit return should specify a single return with an
	// unspecified type.
	Returns []Argument `json:"Returns"`
}

// Function is a declaration of a function with a specific signature and implementation.
//
// Name is not a part of function declaration. Use Alias as a parent node to specify
// the name of a function.
//
// What is considered a Function:
// - function declaration;
// - anonymous functions;
type Function struct {
	GenNode
	// Type is a signature of a function. Should always be set.
	Type FunctionType `json:"Type"`

	// Body is an optional implementation of a function. should point to a Block with
	// a set of statements. Each code path in those statements should end with return.
	//
	// TODO: we don't have return statements yet
	Body *Block `json:"Body"`
}

// Bool is a boolean literal.
type Bool struct {
	GenNode
	Value bool `json:"Value" uast:",content"`
}

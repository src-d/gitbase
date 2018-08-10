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
	"strings"

	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/role"
)

func init() {
	// Register all types from the package under this namespace
	RegisterPackage(NS,
		Position{},
		Positions{},
		GenNode{},
		Identifier{},
		String{},
		QualifiedIdentifier{},
		Comment{},
		Group{},
		FunctionGroup{},
		Block{},
		Alias{},
		Import{},
		RuntimeImport{},
		RuntimeReImport{},
		Argument{},
		FunctionType{},
		Function{},
	)
}

// Special field keys for nodes.Object
const (
	KeyType  = "@type"  // InternalType
	KeyToken = "@token" // Token
	KeyRoles = "@role"  // Roles, for representations see RoleList
	KeyPos   = "@pos"   // All positional information is stored in this field
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
	// Col is the column number (the byte offset of the position relative to
	// a line. It is a 1-based index.
	Col uint32 `json:"col"`
}

// Positions is a container object that stores all positional information for a node.
type Positions map[string]Position

func (p Positions) Start() *Position {
	if p, ok := p[KeyStart]; ok {
		return &p
	}
	return nil
}
func (p Positions) End() *Position {
	if p, ok := p[KeyEnd]; ok {
		return &p
	}
	return nil
}
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

// PositionsOf returns an object with all positional information for a node.
func PositionsOf(m nodes.Object) Positions {
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
func RolesOf(m nodes.Object) role.Roles {
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
func TokenOf(m nodes.Object) string {
	t := m[KeyToken]
	s, ok := t.(nodes.String)
	if ok {
		return string(s)
	}
	v, _ := t.(nodes.Value)
	if v != nil {
		return fmt.Sprint(v)
	}
	return ""
}

// Tokens collects all tokens of the tree recursively (pre-order).
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

// Any is an alias type for any UAST node.
type Any interface{}

// Scope is a temporary definition of a scope semantic type.
type Scope = Any

type GenNode struct {
	Positions Positions `json:"@pos,omitempty"`
}

type Identifier struct {
	GenNode
	Name string `json:"Name"`
}

type String struct {
	GenNode
	Value  string `json:"Value"`
	Format string `json:"Format"` // TODO: make an enum later
}

type QualifiedIdentifier struct {
	GenNode
	Names []Identifier `json:"Names"`
}

type Comment struct {
	GenNode
	Text   string `json:"Text"`
	Prefix string `json:"Prefix"`
	Suffix string `json:"Suffix"`
	Tab    string `json:"Tab"`
	Block  bool   `json:"Block"`
}

type Group struct {
	GenNode
	Nodes []Any `json:"Nodes"`
}

type FunctionGroup Group

type Block struct {
	GenNode
	Statements []Any `json:"Statements"`
	// Scope *Scope
}

type Alias struct {
	GenNode
	Name Identifier `json:"Name"`
	Node Any        `json:"Node"`
	// Target *Scope
}

type Import struct {
	GenNode
	Path   Any   `json:"Path"`
	All    bool  `json:"All"`
	Names  []Any `json:"Names"`
	Target Scope `json:"Target"`
}

type RuntimeImport Import

type RuntimeReImport RuntimeImport

//type InlineImport Import

type Argument struct {
	GenNode
	Name        *Identifier `json:"Name"`
	Type        Any         `json:"Type"`
	Init        Any         `json:"Init"`
	Variadic    bool        `json:"Variadic"`
	MapVariadic bool        `json:"MapVariadic"`
	Receiver    bool        `json:"Receiver"`
}

type FunctionType struct {
	GenNode
	Arguments []Argument `json:"Arguments"`
	Returns   []Argument `json:"Returns"`
}

type Function struct {
	GenNode
	Type FunctionType `json:"Type"`
	Body *Block       `json:"Body"`
}

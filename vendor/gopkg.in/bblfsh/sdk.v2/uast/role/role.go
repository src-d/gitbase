package role

//go:generate proteus  -f $GOPATH/src -p gopkg.in/bblfsh/sdk.v2/uast/role

// Role is the main UAST annotation. It indicates that a node in an AST can
// be interpreted as acting with certain language-independent role.
//
//proteus:generate
//go:generate stringer -type=Role
type Role int16

// Roles is an ordered list of roles.
type Roles []Role

var lookupRole = make(map[string]Role)

func init() {
	for i := 0; i < len(_Role_index)-1; i++ {
		s, e := _Role_index[i], _Role_index[i+1]
		lookupRole[_Role_name[s:e]] = Role(i)
	}
}

// FromString converts a string representation of the Role to its numeric value.
func FromString(s string) Role {
	r, ok := lookupRole[s]
	if !ok {
		return Invalid
	}
	return r
}

const (
	// Invalid Role is assigned as a zero value since protobuf enum definition must start at 0.
	Invalid Role = iota

	// Identifier is any form of identifier, used for variable names, functions, packages, etc.
	Identifier

	// Qualified is a kind of property identifiers may have, when it's composed
	// of multiple simple identifiers.
	Qualified

	// Operator is any form of operator.
	Operator

	// Binary is any form of binary operator, in contrast with unary operators.
	Binary

	// Unary is any form of unary operator, in contrast with binary operators.
	Unary

	// Left is a left hand side in a binary expression.
	Left

	// Right is a right hand side if a binary expression.
	Right

	// Infix should mark the nodes which are parents of expression nodes using infix notation, e.g.: a+b.
	// Nodes without Infix or Postfix mark are considered in prefix order by default.
	Infix

	// Postfix should mark the nodes which are parents of nodes using postfix notation, e.g.: ab+.
	// Nodes without Infix or Postfix mark are considered in prefix order by default.
	Postfix

	// Bitwise is any form of bitwise operation.
	Bitwise

	// Boolean is any form of boolean operation.
	Boolean

	// Unsigned is an form of unsigned operation.
	Unsigned

	// LeftShift is a left shift operation (i.e. `<<`, `rol`, etc.)
	LeftShift

	// RightShift is a right shift operation (i.e. `>>`, `ror`, etc.)
	RightShift

	// Or is an OR operation (i.e. `||`, `or`, `|`, etc.)
	Or

	// Xor is an exclusive OR operation  (i.e. `~`, `^`, etc.)
	Xor

	// And is an AND operation (i.e. `&&`, `&`, `and`, etc.)
	And

	// Expression is a construct computed to produce some value.
	Expression

	// Statement is some action to be carried out.
	Statement

	// Equal is an eaquality predicate (i.e. `=`, `==`, etc.)
	Equal

	// Not is a negation operation. It may be used to annotate a complement of an operator.
	Not

	// LessThan is a comparison predicate that checks if the lhs value is smaller than the rhs value (i. e. `<`.)
	LessThan

	// LessThanOrEqual is a comparison predicate that checks if the lhs value is smaller or equal to the rhs value (i.e. `<=`.)
	LessThanOrEqual

	// GreaterThan is a comparison predicate that checks if the lhs value is greather than the rhs value (i. e. `>`.)
	GreaterThan

	// GreaterThanOrEqual is a comparison predicate that checks if the lhs value is greather than or equal to the rhs value (i.e. 1>=`.)
	GreaterThanOrEqual

	// Identical is an identity predicate (i. e. `===`, `is`, etc.)
	Identical

	// Contains is a membership predicate that checks if the lhs value is a member of the rhs container (i.e. `in` in Python.)
	Contains

	// Increment is an arithmetic operator that increments a value (i. e. `++i`.)
	Increment

	// Decrement is an arithmetic operator that decrements a value (i. e. `--i`.)
	Decrement

	// Negative is an arithmetic operator that negates a value (i.e. `-x`.)
	Negative

	// Positive is an arithmetic operator that makes a value positive. It's usually redundant (i.e. `+x`.)
	Positive

	// Dereference is an operation that gets the actual value of a pointer or reference (i.e. `*x`.)
	Dereference

	// TakeAddress is an operation that gets the memory address of a value (i. e. `&x`.)
	TakeAddress

	// File is the root node of a single file AST.
	File

	// Add is an arithmetic operator (i.e. `+`.)
	Add

	// Substract in an arithmetic operator (i.e. `-`.)
	Substract

	// Multiply is an arithmetic operator (i.e. `*`.)
	Multiply

	// Divide is an arithmetic operator (i.e. `/`.)
	Divide

	// Modulo is an arithmetic operator (i.e. `%`, `mod`, etc.)
	Modulo

	// Package indicates that a package level property.
	Package

	// Declaration is a construct to specify properties of an identifier.
	Declaration

	// Import indicates an import level property.
	Import

	// Pathname is a qualified name of some construct.
	Pathname

	// Alias is an alternative name for some construct.
	Alias

	// Function is a sequence of instructions packaged as a unit.
	Function

	// Body is a sequence of instructions in a block.
	Body

	// Name is an identifier used to reference a value.
	Name

	// Receiver is the target of a construct (message, function, etc.)
	Receiver

	// Argument is variable used as input/output in a function.
	Argument

	// Value is an expression that cannot be evaluated any further.
	Value

	// ArgsList is variable number of arguments (i.e. `...`, `Object...`, `*args`, etc.)
	ArgsList

	// Base is the parent type of which another type inherits.
	Base

	// Implements is the type (usually an interface) that another type implements.
	Implements

	// Instance is a concrete occurrence of an object.
	Instance

	// Subtype is a type that can be used to substitute another type.
	Subtype

	// Subpackage is a package that is below another package in the hierarchy.
	Subpackage

	// Module is a set of funcitonality grouped.
	Module

	// Friend is an access granter for some private resources.
	Friend

	// World is a set of every component.
	World

	// If is used for if-then[-else] statements or expressions.
	// An if-then tree will look like:
	//
	// 	If, Statement {
	//		**[non-If nodes] {
	//			If, Condition {
	//				[...]
	//                      }
	//		}
	//		**[non-If* nodes] {
	//			If, Then {
	//				[...]
	//			}
	//		}
	//		**[non-If* nodes] {
	//			If, Else {
	//				[...]
	//			}
	//		}
	//	}
	//
	// The Else node is optional. The order of Condition, Then and
	// Else is not defined.
	If

	// Condition is a condition in an IfStatement or IfExpression.
	Condition

	// Then is the clause executed when the Condition is true.
	Then

	// Else is the clause executed when the Condition is false.
	Else

	// Switch is used to represent a broad of switch flavors. An expression
	// is evaluated and then compared to the values returned by different
	// case expressions, executing a body associated to the first case that
	// matches. Similar constructions that go beyond expression comparison
	// (such as pattern matching in Scala's match) should not be annotated
	// with Switch.
	Switch

	// Case is a clause whose expression is compared with the condition.
	Case

	// Default is a clause that is called when no other clause is matches.
	Default

	// For is a loop with an initialization, a condition, an update and a body.
	For

	// Initialization is the assignment of an initial value to a variable
	// (i.e. a for loop variable initialization.)
	Initialization

	// Update is the assignment of a new value to a variable
	// (i.e. a for loop variable update.)
	Update

	// Iterator is the element that iterates over something.
	Iterator

	// While is a loop construct with a condition and a body.
	While

	// DoWhile is a loop construct with a body and a condition.
	DoWhile

	// Break is a construct for early exiting a block.
	Break

	// Continue is a construct for continuation with the next iteration of a loop.
	Continue

	// Goto is an unconditional transfer of control statement.
	Goto

	// Block is a group of statements. If the source language has block scope,
	// it should be annotated both with Block and BlockScope.
	Block

	// Scope is a range in which a variable can be referred.
	Scope

	// Return is a return statement. It might have a child expression or not
	// as with naked returns in Go or return in void methods in Java.
	Return

	// Try is a statement for exception handling.
	Try

	// Catch is a clause to capture exceptions.
	Catch

	// Finally is a clause for a block executed after a block with exception handling.
	Finally

	// Throw is a statement that creates an exception.
	Throw

	// Assert checks if an expression is true and if it is not, it signals
	// an error/exception, possibly stopping the execution.
	Assert

	// Call is any call, whether it is a function, procedure, method or macro.
	// In its simplest form, a call will have a single child with a function
	// name (callee). Arguments are marked with Argument and Positional or Name.
	// In OO languages there is usually a Receiver too.
	Call

	// Callee is the callable being called. It might be the name of a
	// function or procedure, it might be a method, it might a simple name
	// or qualified with a namespace.
	Callee

	// Positional is an element which position has meaning (i.e. a positional argument in a call).
	Positional

	// Noop is a construct that does nothing.
	Noop

	// Literal is a literal value.
	Literal

	// Byte is a single-byte element.
	Byte

	// ByteString is a raw byte string.
	ByteString

	// Character is an encoded character.
	Character

	// List is a sequence.
	List

	// Map is a collection of key, value pairs.
	Map

	// Null is an empty value.
	Null

	// Number is a numeric value. This applies to any numeric value
	// whether it is integer or float, any base, scientific notation or not,
	// etc.
	Number

	// Regexp is a regular expression.
	Regexp

	// Set is a collection of values.
	Set

	// String is a sequence of characters.
	String

	// Tuple is an finite ordered sequence of elements.
	Tuple

	// Type is a classification of data.
	Type

	// Entry is a collection element.
	Entry

	// Key is the index value of a map.
	Key

	// Primitive is a language builtin.
	Primitive

	// Assignment is an assignment operator.
	Assignment

	// This represents the self-reference of an object instance in
	// one of its methods. This corresponds to the `this` keyword
	// (e.g. Java, C++, PHP), `self` (e.g. Smalltalk, Perl, Swift) and `Me`
	// (e.g. Visual Basic).
	This

	// Comment is a code comment.
	Comment

	// Documentation is a node that represents documentation of another node,
	// such as function or package. Documentation is usually in the form of
	// a string in certain position (e.g. Python docstring) or comment
	// (e.g. Javadoc, godoc).
	Documentation

	// Whitespace is a node containing whitespace(s).
	Whitespace

	// Incomplete express that the semantic meaning of the node roles doesn't express
	// the full semantic information. Added in BIP-002.
	Incomplete

	// Unannotated will be automatically added by the SDK for nodes that did not receive
	// any annotations with the current version of the driver's `annotations.go` file.
	// Added in BIP-002.
	Unannotated

	// Visibility is an access granter role, usually together with an specifier role
	Visibility

	// Annotation is syntactic metadata
	Annotation

	// Anonymous is an unbound construct
	Anonymous

	// Enumeration is a distinct type that represents a set of named constants
	Enumeration

	// Arithmetic is a type of operation
	Arithmetic

	// Relational is a type of operation
	Relational

	// Variable is a symbolic name associatend with a value
	Variable
)

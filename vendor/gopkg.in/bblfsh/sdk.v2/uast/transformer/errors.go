package transformer

import (
	"bytes"
	"fmt"

	"gopkg.in/src-d/go-errors.v1"
)

var (
	// ErrVariableRedeclared is returned when a transformation defines the same variable twice.
	ErrVariableRedeclared = errors.NewKind("variable %q redeclared (%v vs %v)")
	// ErrVariableNotDefined is returned when an operation excepts a variable to be defined, but it was not set
	// in current transformation state.
	//
	// Receiving this error might mean that transformations have an incorrect order and tries to use a variable
	// in Lookup or If, for example, before another transformation step that sets this variable is executed.
	// If this is the case, see Fields vs Obj comparison in regards to execution order.
	ErrVariableNotDefined = errors.NewKind("variable %q is not defined")
	// ErrExpectedObject is returned when transformation expected an object in the tree or variable, but got other type.
	ErrExpectedObject = errors.NewKind("expected object, got %T")
	// ErrExpectedList is returned when transformation expected an array in the tree or variable, but got other type.
	ErrExpectedList = errors.NewKind("expected list, got %T")
	// ErrExpectedValue is returned when transformation expected a value in the tree or variable, but got other type.
	ErrExpectedValue = errors.NewKind("expected value, got %T")
	// ErrUnhandledValueIn is returned when Lookup fails to find a value in the map. It's recommended to define all
	// values in the lookup map. If it's not possible, use nil value as a key to set default case for Lookup.
	ErrUnhandledValueIn = errors.NewKind("unhandled value: %v in %v")
	// ErrUnexpectedNode is an internal error returned by SDK when a transformation that creates a value receives another
	// value as an argument. This should not happen.
	ErrUnexpectedNode = errors.NewKind("expected node to be nil, got: %v")
	// ErrUnexpectedValue is returned when the value from state does not match constraints enforced by Construct.
	ErrUnexpectedValue = errors.NewKind("unexpected value: %v")
	// ErrUnexpectedType is returned in both Check and Construct when unexpected type is received as an argument or variable.
	ErrUnexpectedType = errors.NewKind("unexpected type: exp %T vs got %T")
	// ErrAmbiguousValue is returned when Lookup map contains the same value mapped to different keys.
	ErrAmbiguousValue = errors.NewKind("map has ambiguous value %v")
	// ErrUnusedField is returned when a transformation is not defined as partial, but does not process a specific key
	// found in object. This usually means that an AST has a field that is not covered by transformation code and it
	// should be added to the mapping.
	ErrUnusedField = errors.NewKind("field was not used: %v")
	// ErrDuplicateField is returned when trying to create a Fields definition with two items with the same name.
	ErrDuplicateField = errors.NewKind("duplicate field: %v")
	// ErrUndefinedField is returned when trying to create an object with a field that is not defined in the type spec.
	ErrUndefinedField = errors.NewKind("undefined field: %v")

	errAnd     = errors.NewKind("op %d (%T)")
	errKey     = errors.NewKind("key %q")
	errElem    = errors.NewKind("elem %d (%T)")
	errAppend  = errors.NewKind("append")
	errMapping = errors.NewKind("mapping %q")

	errCheck     = errors.NewKind("check")
	errConstruct = errors.NewKind("construct")
)

var _ error = (*MultiError)(nil)

// NewMultiError creates an error that contains multiple other errors.
// If a slice is empty, it returns nil. If there is only one error, it is returned directly.
func NewMultiError(errs ...error) error {
	if len(errs) == 0 {
		return nil
	} else if len(errs) == 1 {
		return errs[0]
	}
	return &MultiError{Errs: errs}
}

// MultiError is an error that groups multiple other errors.
type MultiError struct {
	Errs []error
}

func (e *MultiError) Error() string {
	if len(e.Errs) == 1 {
		return e.Errs[0].Error()
	}
	buf := bytes.NewBuffer(nil)
	fmt.Fprintf(buf, "received %d errors:\n", len(e.Errs))
	for _, err := range e.Errs {
		fmt.Fprintf(buf, "\t%v\n", err)
	}
	return buf.String()
}

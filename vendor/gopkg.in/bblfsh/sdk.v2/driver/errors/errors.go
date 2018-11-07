package errors

import (
	"strings"

	"gopkg.in/src-d/go-errors.v1"
)

var (
	// ErrDriverFailure is returned when the driver is malfunctioning.
	ErrDriverFailure = errors.NewKind("driver failure")

	// ErrSyntax is returned when driver cannot parse the source file.
	// Can be omitted for native driver implementations.
	ErrSyntax = errors.NewKind("syntax error")
)

// Join multiple errors into a single error value.
// If there are only one error, it will be returned directly.
// Zero or more than one error will be wrapped into ErrMulti.
func Join(errs []error) error {
	if len(errs) == 1 {
		return errs[0]
	}
	return &ErrMulti{Errors: errs}
}

// ErrMulti joins multiple errors.
type ErrMulti struct {
	Errors []error
}

func (e *ErrMulti) Error() string {
	var buf strings.Builder
	for _, err := range e.Errors {
		buf.WriteString(err.Error())
		buf.WriteString("\n")
	}
	return buf.String()
}

// Package driver contains all the logic to build a driver.
package driver

import (
	"bytes"
	"context"

	"gopkg.in/bblfsh/sdk.v2/driver/manifest"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
)

type Mode int

const (
	ModeNative = Mode(1 << iota)
	ModePreprocessed
	ModeAnnotated
	ModeSemantic
)

const ModeDefault = ModeSemantic

// Module is an interface for a generic module instance.
type Module interface {
	Start() error
	Close() error
}

// Driver is an interface for a language driver that returns UAST.
type Driver interface {
	Parse(ctx context.Context, mode Mode, src string) (nodes.Node, error)
}

// DriverModule is an interface for a driver instance.
type DriverModule interface {
	Module
	Driver
	Manifest() (manifest.Manifest, error)
}

// Native is a base interface of a language driver that returns a native AST.
type Native interface {
	Module
	Parse(ctx context.Context, src string) (nodes.Node, error)
}

// ErrMulti joins multiple errors.
type ErrMulti struct {
	Header string
	Errors []string
}

func (e ErrMulti) Error() string {
	buf := bytes.NewBuffer(nil)
	if e.Header != "" {
		buf.WriteString(e.Header + ":\n")
	}
	for _, s := range e.Errors {
		buf.WriteString(s)
		buf.WriteString("\n")
	}
	return buf.String()
}

func MultiError(errs []string) error {
	return &ErrMulti{Errors: errs}
}

func PartialParse(ast nodes.Node, errs []string) error {
	return &ErrPartialParse{
		ErrMulti: ErrMulti{Header: "partial parse", Errors: errs},
		AST:      ast,
	}
}

// ErrPartialParse is returned when driver was not able to parse the whole source file.
type ErrPartialParse struct {
	ErrMulti
	AST nodes.Node
}

// Package driver contains all the logic to build a driver.
package driver

import (
	"context"
	"fmt"

	"gopkg.in/src-d/go-errors.v1"

	derrors "gopkg.in/bblfsh/sdk.v2/driver/errors"
	"gopkg.in/bblfsh/sdk.v2/driver/manifest"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
)

var (
	// ErrDriverFailure is returned when the driver is malfunctioning.
	ErrDriverFailure = derrors.ErrDriverFailure

	// ErrSyntax is returned when driver cannot parse the source file.
	// Can be omitted for native driver implementations.
	ErrSyntax = derrors.ErrSyntax

	// ErrTransformFailure is returned if one of the UAST transformations fails.
	ErrTransformFailure = errors.NewKind("transform failed")

	// ErrModeNotSupported is returned if a UAST transformation mode is not supported by the driver.
	ErrModeNotSupported = errors.NewKind("transform mode not supported")
)

// ErrMulti joins multiple errors.
type ErrMulti = derrors.ErrMulti

// Join multiple errors into a single error value.
func JoinErrors(errs []error) error {
	return derrors.Join(errs)
}

type Mode int

const (
	ModeNative = Mode(1 << iota)
	ModePreprocessed
	ModeAnnotated
	ModeSemantic
)

const ModeDefault = ModeSemantic

// Parse mode parses a UAST mode string to an enum value.
func ParseMode(mode string) (Mode, error) {
	switch mode {
	case "native":
		return ModeNative, nil
	case "annotated":
		return ModeAnnotated, nil
	case "semantic":
		return ModeSemantic, nil
	}
	return 0, fmt.Errorf("unsupported mode: %q", mode)
}

// Module is an interface for a generic module instance.
type Module interface {
	Start() error
	Close() error
}

type ParseOptions struct {
	Mode     Mode
	Language string
	Filename string
}

// Driver is an interface for a language driver that returns UAST.
type Driver interface {
	// Parse reads the input string and constructs an AST representation of it.
	//
	// Language can be specified by providing ParseOptions. If the language is not set,
	// it will be set during the Parse call if implementation supports language detection.
	//
	// Depending on the mode, AST may be transformed to different UAST variants.
	// ErrModeNotSupported is returned for unsupported transformation modes.
	//
	// Syntax errors are indicated by returning ErrSyntax.
	// In this case a non-empty UAST may be returned, if driver supports partial parsing.
	//
	// Native driver failures are indicated by ErrDriverFailure and UAST transformation are indicated by ErrTransformFailure.
	// All other errors indicate a protocol or server failure.
	Parse(ctx context.Context, src string, opts *ParseOptions) (nodes.Node, error)
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
	// Parse reads the input string and constructs an AST representation of it.
	// All errors are considered ErrSyntax, unless they are wrapped into ErrDriverFailure.
	Parse(ctx context.Context, src string) (nodes.Node, error)
}

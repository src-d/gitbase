// Package driver contains all the logic to build a driver.
package driver

import (
	"context"
	"fmt"
	"time"

	"gopkg.in/src-d/go-errors.v1"

	derrors "github.com/bblfsh/sdk/v3/driver/errors"
	"github.com/bblfsh/sdk/v3/driver/manifest"
	"github.com/bblfsh/sdk/v3/uast/nodes"
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

const (
	// ManifestLocation is the path of the manifest file in the driver image.
	ManifestLocation = "/opt/driver/etc/" + manifest.Filename
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

	// Version returns a version of the driver or the server, depending where is this interface is implemented.
	Version(ctx context.Context) (Version, error)

	// Languages returns a list of manifests for languages supported by this driver or the server.
	Languages(ctx context.Context) ([]manifest.Manifest, error)
}

// Version information for driver or the server.
type Version struct {
	Version string    // the version label for the driver, e.g., 'v1.2.3-tag' or 'undefined'
	Build   time.Time // the timestamp when the driver was built
}

// DriverModule is an interface for a driver instance.
type DriverModule interface {
	Module
	Driver
}

// Native is a base interface of a language driver that returns a native AST.
type Native interface {
	Module
	// Parse reads the input string and constructs an AST representation of it.
	// All errors are considered ErrSyntax, unless they are wrapped into ErrDriverFailure.
	Parse(ctx context.Context, src string) (nodes.Node, error)
}

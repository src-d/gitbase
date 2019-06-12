package driver

import (
	"context"
	"fmt"

	"github.com/opentracing/opentracing-go"

	"github.com/bblfsh/sdk/v3/driver/manifest"
	"github.com/bblfsh/sdk/v3/uast/nodes"
)

// NewDriver returns a new Driver instance based on the given ObjectToNode and list of transformers.
func NewDriverFrom(d Native, m *manifest.Manifest, t Transforms) (DriverModule, error) {
	if d == nil {
		return nil, fmt.Errorf("no driver implementation")
	} else if m == nil {
		return nil, fmt.Errorf("no manifest")
	}
	return &driverImpl{d: d, m: m, t: t}, nil
}

// Driver implements a bblfsh driver, a driver is on charge of transforming a
// source code into an AST and a UAST. To transform the AST into a UAST, a
// `uast.ObjectToNode`` and a series of `tranformer.Transformer` are used.
//
// The `Parse` and `NativeParse` requests block the driver until the request is
// done, since the communication with the native driver is a single-channel
// synchronous communication over stdin/stdout.
type driverImpl struct {
	d Native

	m *manifest.Manifest
	t Transforms
}

func (d *driverImpl) Start() error {
	return d.d.Start()
}

func (d *driverImpl) Close() error {
	return d.d.Close()
}

// Parse process a protocol.ParseRequest, calling to the native driver. It a
// parser request is done to the internal native driver and the the returned
// native AST is transform to UAST.
func (d *driverImpl) Parse(rctx context.Context, src string, opts *ParseOptions) (nodes.Node, error) {
	sp, ctx := opentracing.StartSpanFromContext(rctx, "bblfsh.driver.Parse")
	defer sp.Finish()

	if opts == nil {
		opts = &ParseOptions{}
	}
	ast, err := d.d.Parse(ctx, src)
	if err != nil {
		if !ErrDriverFailure.Is(err) {
			// all other errors are considered syntax errors
			err = ErrSyntax.Wrap(err)
		} else {
			ast = nil
		}
		return ast, err
	}
	if opts.Language == "" {
		opts.Language = d.m.Language
	}

	ast, err = d.t.Do(ctx, opts.Mode, src, ast)
	if err != nil {
		err = ErrTransformFailure.Wrap(err)
	}
	return ast, err
}

// Version returns driver version.
func (d *driverImpl) Version(ctx context.Context) (Version, error) {
	return Version{
		Version: d.m.Version,
		Build:   d.m.Build,
	}, nil
}

// Languages returns a single driver manifest for the language supported by the driver.
func (d *driverImpl) Languages(ctx context.Context) ([]manifest.Manifest, error) {
	return []manifest.Manifest{*d.m}, nil // TODO: clone
}

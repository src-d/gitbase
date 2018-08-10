package driver

import (
	"context"
	"fmt"

	"gopkg.in/bblfsh/sdk.v2/driver/manifest"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
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
func (d *driverImpl) Parse(ctx context.Context, mode Mode, src string) (nodes.Node, error) {
	ast, err := d.d.Parse(ctx, src)
	if err != nil {
		return nil, err
	}
	return d.t.Do(mode, src, ast)
}

// Manifest returns a driver manifest.
func (d *driverImpl) Manifest() (manifest.Manifest, error) {
	return *d.m, nil // TODO: clone
}

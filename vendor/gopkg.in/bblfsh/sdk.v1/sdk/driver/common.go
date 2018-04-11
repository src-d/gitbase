// Package driver contains all the logic to build a driver.
package driver

import (
	"gopkg.in/bblfsh/sdk.v1/manifest"
	"gopkg.in/bblfsh/sdk.v1/uast"
	"gopkg.in/bblfsh/sdk.v1/uast/transformer"
)

var (
	// DriverBinary default location of the driver binary. Should not
	// override this variable unless you know what are you doing.
	DriverBinary = "/opt/driver/bin/driver"
	// NativeBinary default location of the native driver binary. Should not
	// override this variable unless you know what are you doing.
	NativeBinary = "/opt/driver/bin/native"
	// ManifestLocation location of the manifest file. Should not override
	// this variable unless you know what are you doing.
	ManifestLocation = "/opt/driver/etc/" + manifest.Filename
)

// Run is a common main function used as an entry point for drivers.
// It panics in case of an error.
func Run(o *uast.ObjectToNode, t []transformer.Tranformer) {
	d, err := NewDriver(o, t)
	if err != nil {
		panic(err)
	}

	s := NewServer(d)
	if err := s.Start(); err != nil {
		panic(err)
	}
}

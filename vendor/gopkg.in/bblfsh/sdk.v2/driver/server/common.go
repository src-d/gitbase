package server

import (
	"gopkg.in/bblfsh/sdk.v2/driver"
	"gopkg.in/bblfsh/sdk.v2/driver/manifest"
	"gopkg.in/bblfsh/sdk.v2/driver/native"
)

var DefaultDriver driver.Native = native.NewDriver("")

var (
	// ManifestLocation location of the manifest file. Should not override
	// this variable unless you know what are you doing.
	ManifestLocation = "/opt/driver/etc/" + manifest.Filename
)

// Run is a common main function used as an entry point for drivers.
// It panics in case of an error.
func Run(t driver.Transforms) {
	RunNative(DefaultDriver, t)
}

// RunNative is like Run but allows to provide a custom driver native driver implementation.
func RunNative(d driver.Native, t driver.Transforms) {
	m, err := manifest.Load(ManifestLocation)
	if err != nil {
		panic(err)
	}
	dr, err := driver.NewDriverFrom(d, m, t)
	if err != nil {
		panic(err)
	}
	s := NewServer(dr)
	if err := s.Start(); err != nil {
		panic(err)
	}
}

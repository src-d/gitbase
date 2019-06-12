// Package buildmanifest provides shared types for driver build manifests.
package buildmanifest

import "gopkg.in/yaml.v2"

const (
	// Filename of a build manifest relative to the package directory.
	Filename = "build.yml"
	// CurrentFormat is the SDK version corresponding to the current manifest format.
	//
	// See Manifest.Format.
	CurrentFormat = "2"
)

// Artifact is a file copied from one path to another during the build.
type Artifact struct {
	Path string `yaml:"path"`
	Dest string `yaml:"dest"`
}

// Manifest used for the declarative build system for drivers.
type Manifest struct {
	// SDK version corresponding to manifest file format. Used to track future changes to this file.
	// The current version is given by CurrentFormat.
	Format string `yaml:"sdk"`

	// Native is a build manifest for steps related to a native driver build and runtime.
	Native struct {
		// Image is a Docker image used as the native driver runtime in the final image.
		// The format is image:version.
		Image string `yaml:"image"`

		// ImageAssets is a list of files that will be copied from native driver's source directory
		// to the final driver image. Note that those files won't be modified by the build.
		ImageAssets []Artifact `yaml:"static"`

		// Deps is a list of apt/apk commands executed in the final driver image.
		// This directive should be avoided since a different Docker image may be used instead of it.
		Deps []string `yaml:"deps"`

		Build struct {
			// Gopath directive sets GOPATH for the native driver build. Only used by Go drivers.
			// Should be a single absolute path inside the build container.
			Gopath string `yaml:"gopath"`

			// Image is a Docker image used to build the native driver. The format is image:version.
			Image string `yaml:"image"`

			// Deps is a list of shell commands to pull native driver dependencies.
			// Note that those commands are executed before copying the driver files to the
			// container, so they can be cached. See Build also.
			Deps []string `yaml:"deps"`

			// BuildAssets is a list of files that are copied from the native driver source to the
			// build container for the native driver. It is similar to Docker ADD command.
			BuildAssets []Artifact `yaml:"add"`

			// Build is a list of shell commands to build the native driver. Those commands
			// can access files copied by BuildAssets directives. Files produced by the build should
			// be mentioned in the Artifacts directive to be copied to the final image.
			Build []string `yaml:"run"`

			// Artifacts is a list of files copied from the native build container to the
			// final driver image.
			Artifacts []Artifact `yaml:"artifacts"`
		} `yaml:"build"`

		Test struct {
			// Deps is a list of shell commands to install native driver test dependencies.
			Deps []string `yaml:"deps"`

			// Test is a list of shell commands to test the native driver. Those commands
			// are run over the build container for a native driver.
			Test []string `yaml:"run"`
		} `yaml:"test"`
	} `yaml:"native"`

	// Runtime is a manifest for the driver server runtime (Go). Used to build the Go server binary.
	Runtime struct {
		// Version of Go used to build and run the driver server.
		Version string `yaml:"version"`
	} `yaml:"go-runtime"`
}

// Decode updates m by decoding a YAML manifest from data.
func (m *Manifest) Decode(data []byte) error {
	return yaml.Unmarshal(data, m)
}

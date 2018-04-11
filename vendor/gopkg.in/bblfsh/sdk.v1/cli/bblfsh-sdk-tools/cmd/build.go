package cmd

import (
	"os"
	"path/filepath"
	"time"

	"gopkg.in/bblfsh/sdk.v1/manifest"
)

const BuildCommandDescription = "" +
	"updates the manifest with the build information"

type BuildCommand struct {
	Args struct {
		Version string `positional-arg-name:"version" description:"version been build"`
	} `positional-args:"yes"`

	Output string `long:"output" description:"file to be written."`
	manifestCommand
}

func (c *BuildCommand) Execute(args []string) error {
	if err := c.manifestCommand.Execute(args); err != nil {
		return err
	}

	c.fillManifest(c.Manifest)

	if c.Output == "" {
		return c.Manifest.Encode(os.Stdout)
	}
	if err := os.MkdirAll(filepath.Dir(c.Output), 0755); err != nil {
		return err
	}

	f, err := os.Create(c.Output)
	if err != nil {
		return err
	}

	return c.Manifest.Encode(f)
}

func (c *BuildCommand) fillManifest(m *manifest.Manifest) {
	m.Version = c.Args.Version

	now := time.Now().UTC()
	m.Build = &now
}

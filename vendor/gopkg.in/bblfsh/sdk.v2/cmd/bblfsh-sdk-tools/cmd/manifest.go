package cmd

import (
	"path/filepath"

	"gopkg.in/bblfsh/sdk.v2/cmd"
	"gopkg.in/bblfsh/sdk.v2/driver/manifest"
)

type manifestCommand struct {
	cmd.Command
	Manifest *manifest.Manifest
}

func (c *manifestCommand) Execute(args []string) error {
	return c.readManifest()
}

func (c *manifestCommand) readManifest() error {
	m, err := manifest.Load(filepath.Join(c.Root, manifest.Filename))
	if err != nil {
		return err
	}

	c.Manifest = m
	return nil
}

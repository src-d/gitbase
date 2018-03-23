package cmd

import (
	"path/filepath"

	"gopkg.in/bblfsh/sdk.v1/cli"
	"gopkg.in/bblfsh/sdk.v1/manifest"
)

type manifestCommand struct {
	cli.Command
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

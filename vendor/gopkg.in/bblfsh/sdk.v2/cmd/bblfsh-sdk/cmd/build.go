package cmd

import (
	"fmt"

	"gopkg.in/bblfsh/sdk.v2/build"
	"gopkg.in/bblfsh/sdk.v2/cmd"
)

const BuildCommandDescription = "builds the driver"

type BuildCommand struct {
	cmd.Command
}

func (c *BuildCommand) Execute(args []string) error {
	name := ""
	if len(args) != 0 {
		name = args[0]
	}
	d, err := build.NewDriver(c.Root)
	if err != nil {
		return err
	}
	id, err := d.Build(name)
	if err != nil {
		return err
	}
	fmt.Println(id)
	return nil
}

const TestCommandDescription = "tests the driver using fixtures"

type TestCommand struct {
	cmd.Command
	Bblfshd string `long:"bblfshd" description:"bblfshd version to test with"`
}

func (c *TestCommand) Execute(args []string) error {
	d, err := build.NewDriver(c.Root)
	if err != nil {
		return err
	}
	image := ""
	if len(args) != 0 {
		image = args[0]
	}
	return d.Test(c.Bblfshd, image)
}

const TagCommandDescription = "returns a version tag for the driver"

type TagCommand struct {
	cmd.Command
}

func (c *TagCommand) Execute(args []string) error {
	d, err := build.NewDriver(c.Root)
	if err != nil {
		return err
	}
	tag, err := d.VersionTag()
	if err != nil {
		return err
	}
	fmt.Println(tag)
	return nil
}

const ReleaseCommandDescription = "prepare driver for the release"

type ReleaseCommand struct {
	cmd.Command
}

func (c *ReleaseCommand) Execute(args []string) error {
	d, err := build.NewDriver(c.Root)
	if err != nil {
		return err
	}
	return d.FillManifest("")
}

const PushCommandDescription = "push driver image to docker registry (CI only)"

type PushCommand struct {
	cmd.Command
}

func (c *PushCommand) Execute(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("image name should be specified")
	}
	d, err := build.NewDriver(c.Root)
	if err != nil {
		return err
	}
	return d.Push(args[0])
}

package cmd

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"

	"gopkg.in/bblfsh/sdk.v2/cmd"
)

const InitCommandDescription = "initializes a driver for a given language and OS"

type InitCommand struct {
	Args struct {
		Language string `positional-arg-name:"language"  description:"target language of the driver"`
	} `positional-args:"yes"`

	UpdateCommand
}

func (c *InitCommand) Execute(args []string) error {
	if err := c.processManifest(); err != nil {
		return err
	}

	if err := c.UpdateCommand.Execute(args); err != nil {
		return err
	}
	for _, file := range []string{
		"Dockerfile",
		"Gopkg.lock",
	} {
		git := exec.Command("git", "add", file)
		git.Dir = c.Root
		if out, err := git.CombinedOutput(); err != nil {
			cmd.Warning.Println("cannot add a file to git:", err, "\n"+string(out))
		}
	}
	return nil
}

func (c *InitCommand) processManifest() error {
	if c.Args.Language == "" {
		return fmt.Errorf("`language` argument is mandatory")
	}

	cmd.Notice.Printf("initializing driver %q, creating new manifest\n", c.Args.Language)

	c.Root = filepath.Join(c.Root, strings.ToLower(c.Args.Language)+"-driver")

	cmd.Notice.Printf("initializing new repo %q\n", c.Root)
	cmd := exec.Command("git", "init", c.Root)
	if err := cmd.Run(); err != nil {
		return err
	}

	return c.processTemplateAsset(manifestTpl, c.Args, false)
}

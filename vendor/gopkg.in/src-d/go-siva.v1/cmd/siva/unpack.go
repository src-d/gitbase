package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"gopkg.in/src-d/go-siva.v1"

	"github.com/dustin/go-humanize"
)

const writeFlagsDefault = os.O_WRONLY | os.O_CREATE | os.O_TRUNC | os.O_EXCL
const writeFlagsOverwrite = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
const defaultPerms = 0755

type CmdUnpack struct {
	cmd
	Overwrite   bool   `short:"o" description:"Overwrites the files if already exists"`
	IgnorePerms bool   `short:"i" description:"Ignore files permisisions"`
	Match       string `short:"m" description:"Only extract files matching the given regexp"`

	Output struct {
		Path string `positional-arg-name:"target" description:"taget directory"`
	} `positional-args:"yes"`

	flags        int
	regexp       *regexp.Regexp
	matchingFunc func(string) bool
}

func (c *CmdUnpack) Execute(args []string) error {
	if err := c.validate(); err != nil {
		return err
	}

	if err := c.buildReader(); err != nil {
		return err
	}

	defer c.close()
	if err := c.do(); err != nil {
		return err
	}

	return nil
}

func (c *CmdUnpack) validate() error {
	err := c.cmd.validate()
	if err != nil {
		return err
	}

	if _, err := os.Stat(c.Args.File); err != nil {
		return fmt.Errorf("Invalid input file %q, %s\n", c.Args.File, err)
	}

	if c.Output.Path == "" {
		c.Output.Path = "."
	}

	c.flags = writeFlagsDefault
	if c.Overwrite {
		c.flags = writeFlagsOverwrite
	}

	return c.buildMatchingFunc()
}

func (c *CmdUnpack) buildMatchingFunc() error {
	c.matchingFunc = func(string) bool { return true }
	if c.Match == "" {
		return nil
	}

	var err error
	c.regexp, err = regexp.Compile(c.Match)
	if err != nil {
		return fmt.Errorf("Invalid match regexp %q, %s\n", c.Match, err.Error())
	}

	c.matchingFunc = func(name string) bool {
		return c.regexp.MatchString(name)
	}

	return nil
}

func (c *CmdUnpack) do() error {
	i, err := c.r.Index()
	if err != nil {
		return err
	}

	for _, entry := range i.Filter() {
		if !c.matchingFunc(entry.Name) {
			continue
		}

		if err := c.extract(entry); err != nil {
			return err
		}
	}

	return nil
}

func (c *CmdUnpack) extract(entry *siva.IndexEntry) error {
	src, err := c.r.Get(entry)
	if err != nil {
		return err
	}

	dst, err := c.createFile(entry)
	if err != nil {
		return err
	}

	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("unable to write %q : %s\n", entry.Name, err)
	}

	c.println(entry.Name, humanize.Bytes(entry.Size))
	return nil
}

func (c *CmdUnpack) createFile(entry *siva.IndexEntry) (*os.File, error) {
	dstName := filepath.Join(c.Output.Path, entry.Name)

	dir := filepath.Dir(dstName)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("unable to create dir %q: %s\n", dir, err)
	}

	perms := os.FileMode(defaultPerms)
	if !c.IgnorePerms {
		perms = entry.Mode.Perm()
	}

	dst, err := os.OpenFile(dstName, c.flags, perms)
	if err != nil {
		return nil, fmt.Errorf("unable to open %q for writing: %s\n", dstName, err)
	}

	return dst, nil
}

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/gitql/gitql"
	gitqlgit "github.com/gitql/gitql/git"
	"github.com/gitql/gitql/internal/format"
	"github.com/gitql/gitql/sql"

	"gopkg.in/src-d/go-git.v4"
)

type CmdQuery struct {
	cmd

	Path   string `short:"p" long:"path" description:"Path where the git repository is located"`
	Format string `short:"f" long:"format" default:"pretty" description:"Ouptut format. Formats supported: pretty, csv, json."`
	Args   struct {
		SQL string `positional-arg-name:"sql" required:"true" description:"SQL query to execute"`
	} `positional-args:"yes"`

	r  *git.Repository
	db sql.Database
}

func (c *CmdQuery) Execute(args []string) error {
	if err := c.validate(); err != nil {
		return err
	}

	if err := c.buildDatabase(); err != nil {
		return err
	}

	return c.executeQuery()
}

func (c *CmdQuery) validate() error {
	var err error
	c.Path, err = findDotGitFolder(c.Path)
	return err
}

func (c *CmdQuery) buildDatabase() error {
	c.print("opening %q repository...\n", c.Path)

	var err error
	c.r, err = git.NewFilesystemRepository(c.Path)
	if err != nil {
		return err
	}

	empty, err := c.r.IsEmpty()
	if err != nil {
		return err
	}

	if empty {
		return errors.New("error: the repository is empty")
	}

	head, err := c.r.Head()
	if err != nil {
		return err
	}

	c.print("current HEAD %q\n", head.Hash())

	name := filepath.Base(filepath.Join(c.Path, ".."))
	c.db = gitqlgit.NewDatabase(name, c.r)
	return nil
}

func (c *CmdQuery) executeQuery() error {
	c.print("executing %q at %q\n", c.Args.SQL, c.db.Name())

	fmt.Println(c.Args.SQL)
	e := gitql.New()
	e.AddDatabase(c.db)
	schema, iter, err := e.Query(c.Args.SQL)
	if err != nil {
		return err
	}

	return c.printQuery(schema, iter)
}

func (c *CmdQuery) printQuery(schema sql.Schema, iter sql.RowIter) error {
	f, err := format.NewFormat(c.Format, os.Stdout)
	if err != nil {
		return err
	}

	headers := []string{}
	for _, f := range schema {
		headers = append(headers, f.Name)
	}

	if err := f.WriteHeader(headers); err != nil {
		return err
	}

	for {
		row, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := f.Write(row.Fields()); err != nil {
			return err
		}
	}

	return f.Close()
}

func findDotGitFolder(path string) (string, error) {
	if path == "" {
		var err error
		path, err = os.Getwd()
		if err != nil {
			return "", err
		}
	}

	git := filepath.Join(path, ".git")
	_, err := os.Stat(git)
	if err == nil {
		return git, nil
	}

	if !os.IsNotExist(err) {
		return "", err
	}

	next := filepath.Join(path, "..")
	if next == path {
		return "", errors.New("unable to find a git repository")
	}

	return findDotGitFolder(next)
}

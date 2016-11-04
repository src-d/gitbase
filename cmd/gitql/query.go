package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	gitqlgit "github.com/gitql/gitql/git"
	"github.com/gitql/gitql/sql"

	"gopkg.in/src-d/go-git.v4"
	"github.com/gitql/gitql"
	"io"
	"github.com/olekukonko/tablewriter"
)

type CmdQuery struct {
	cmd

	Path string `short:"p" long:"path" description:"Path where the git repository is located"`
	Args struct {
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

	if err := c.executeQuery(); err != nil {
		return err
	}

	return nil
}

func (c *CmdQuery) validate() error {
	var err error
	c.Path, err = findDotGitFolder(c.Path)
	if err != nil {
		return err
	}

	return nil
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

	c.printQuery(schema, iter)

	return nil
}

func (c *CmdQuery) printQuery(schema sql.Schema, iter sql.RowIter) {
	w := tablewriter.NewWriter(os.Stdout)
	headers := []string{}
	for _, f := range schema {
		headers = append(headers, f.Name)
	}
	w.SetHeader(headers)
	for {
		row, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		rowStrings := []string{}
		for _, v := range row.Fields() {
			rowStrings = append(rowStrings, fmt.Sprintf("%v", v))
		}
		w.Append(rowStrings)
	}
	w.Render()
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

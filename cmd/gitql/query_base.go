package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/gitql/gitql"
	gitqlgit "github.com/gitql/gitql/git"
	"github.com/gitql/gitql/internal/format"
	"github.com/gitql/gitql/sql"

	"gopkg.in/src-d/go-git.v4"
)

type cmdQueryBase struct {
	cmd

	Path string `short:"p" long:"path" description:"Path where the git repository is located"`

	db sql.Database
	e  *gitql.Engine
}

func (c *cmdQueryBase) validate() error {
	var err error
	c.Path, err = findDotGitFolder(c.Path)
	return err
}

func (c *cmdQueryBase) buildDatabase() error {
	c.print("opening %q repository...\n", c.Path)

	var err error
	r, err := git.NewFilesystemRepository(c.Path)
	if err != nil {
		return err
	}

	empty, err := r.IsEmpty()
	if err != nil {
		return err
	}

	if empty {
		return errors.New("error: the repository is empty")
	}

	head, err := r.Head()
	if err != nil {
		return err
	}

	c.print("current HEAD %q\n", head.Hash())

	name := filepath.Base(filepath.Join(c.Path, ".."))

	c.db = gitqlgit.NewDatabase(name, r)
	c.e = gitql.New()
	c.e.AddDatabase(c.db)

	return nil
}

func (c *cmdQueryBase) executeQuery(sql string) (sql.Schema, sql.RowIter, error) {
	c.print("executing %q at %q\n", sql, c.db.Name())

	return c.e.Query(sql)
}

func (c *cmdQueryBase) printQuery(schema sql.Schema, iter sql.RowIter, formatId string) error {
	f, err := format.NewFormat(formatId, os.Stdout)
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

		dataRow := make([]interface{}, len(row))
		for i := range row {
			dataRow[i] = interface{}(row[i])
		}

		if err := f.Write(dataRow); err != nil {
			return err
		}
	}

	if err := iter.Close(); err != nil {
		_ = f.Close()
		return err
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

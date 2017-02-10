package main

import (
	"io"
	"os"
	"path/filepath"

	"github.com/gitql/gitql"
	gitqlgit "github.com/gitql/gitql/git"
	"github.com/gitql/gitql/internal/format"
	"github.com/gitql/gitql/sql"

	"srcd.works/go-git.v4"
)

type cmdQueryBase struct {
	cmd

	Path string `short:"p" long:"path" description:"Path where the git repository is located"`

	db sql.Database
	e  *gitql.Engine
}

func (c *cmdQueryBase) buildDatabase() error {
	c.print("opening %q repository...\n", c.Path)

	var err error
	r, err := git.PlainOpen(c.Path)
	if err != nil {
		return err
	}

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

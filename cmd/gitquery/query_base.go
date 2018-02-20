package main

import (
	"io"
	"os"
	"path/filepath"

	"github.com/src-d/gitquery"
	"github.com/src-d/gitquery/internal/format"

	gogit "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/utils/ioutil"
	sqle "gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

type cmdQueryBase struct {
	cmd

	Path []string `short:"p" long:"path" description:"Path where the git repository is located, can be used several times"`

	engine *sqle.Engine
	name   string
}

func (c *cmdQueryBase) buildDatabase() error {
	if c.engine == nil {
		c.engine = sqle.New()
	}

	c.print("opening %q repository...\n", c.Path)

	var err error

	pool := gitquery.NewRepositoryPool()

	for _, path := range c.Path {
		r, err := gogit.PlainOpen(path)
		if err != nil {
			return err
		}

		pool.Add(path, r)

		c.name = filepath.Base(filepath.Join(path, ".."))
	}

	c.engine.AddDatabase(gitquery.NewDatabase(c.name, &pool))
	return err
}

func (c *cmdQueryBase) executeQuery(sql string) (sql.Schema, sql.RowIter, error) {
	c.print("executing %q at %q\n", sql, c.name)
	return c.engine.Query(sql)
}

func (c *cmdQueryBase) printQuery(schema sql.Schema, rows sql.RowIter, formatId string) (err error) {
	defer ioutil.CheckClose(rows, &err)

	f, err := format.NewFormat(formatId, os.Stdout)
	if err != nil {
		return err
	}
	defer ioutil.CheckClose(f, &err)

	columnNames := make([]string, len(schema))
	for i, column := range schema {
		columnNames[i] = column.Name
	}

	if err := f.WriteHeader(columnNames); err != nil {
		return err
	}

	for {
		row, err := rows.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		if err := f.Write(row); err != nil {
			return err
		}
	}

	return nil
}

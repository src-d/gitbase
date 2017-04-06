package main

import (
	"database/sql"
	"os"
	"path/filepath"

	"gopkg.in/sqle/gitquery.v0"
	"gopkg.in/sqle/gitquery.v0/internal/format"
	"gopkg.in/sqle/sqle.v0"

	gogit "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/utils/ioutil"
)

type cmdQueryBase struct {
	cmd

	Path string `short:"p" long:"path" description:"Path where the git repository is located"`

	db   *sql.DB
	name string
}

func (c *cmdQueryBase) buildDatabase() error {
	c.print("opening %q repository...\n", c.Path)

	var err error
	r, err := gogit.PlainOpen(c.Path)
	if err != nil {
		return err
	}

	c.name = filepath.Base(filepath.Join(c.Path, ".."))
	sqle.DefaultEngine.AddDatabase(gitquery.NewDatabase(c.name, r))
	c.db, err = sql.Open(sqle.DriverName, "")
	return err
}

func (c *cmdQueryBase) executeQuery(sql string) (*sql.Rows, error) {
	c.print("executing %q at %q\n", sql, c.name)
	return c.db.Query(sql)
}

func (c *cmdQueryBase) printQuery(rows *sql.Rows, formatId string) (err error) {
	defer ioutil.CheckClose(rows, &err)

	f, err := format.NewFormat(formatId, os.Stdout)
	if err != nil {
		return err
	}
	defer ioutil.CheckClose(f, &err)

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	if err := f.WriteHeader(cols); err != nil {
		return err
	}

	vals := make([]interface{}, len(cols))
	valPtrs := make([]interface{}, len(cols))
	for i := 0; i < len(cols); i++ {
		valPtrs[i] = &vals[i]
	}

	for {
		if !rows.Next() {
			break
		}

		if err := rows.Scan(valPtrs...); err != nil {
			return err
		}

		if err := f.Write(vals); err != nil {
			return err
		}
	}

	return rows.Err()
}

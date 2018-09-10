package gitbase

import (
	"io"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

var noRows emptyRowIter

type emptyRowIter struct{}

func (emptyRowIter) Next() (sql.Row, error) { return nil, io.EOF }
func (emptyRowIter) Close() error           { return nil }

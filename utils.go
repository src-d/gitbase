package gitbase

import (
	"io"

	"github.com/src-d/go-mysql-server/sql"
)

var noRows emptyRowIter

type emptyRowIter struct{}

func (emptyRowIter) Next() (sql.Row, error) { return nil, io.EOF }
func (emptyRowIter) Close() error           { return nil }

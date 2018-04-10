package gitbase

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

const expectedString = `Table(foo)
 ├─ Column(col1, TEXT, nullable=true)
 └─ Column(col2, INT64, nullable=false)
`

func TestTableString(t *testing.T) {
	require := require.New(t)
	schema := sql.Schema{
		{"col1", sql.Text, nil, true, ""},
		{"col2", sql.Int64, nil, false, ""},
	}
	require.Equal(expectedString, printTable("foo", schema))
}

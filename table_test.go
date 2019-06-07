package gitbase

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/src-d/go-mysql-server/sql"
)

const expectedString = `Table(foo)
 ├─ Column(col1, TEXT, nullable=true)
 └─ Column(col2, INT64, nullable=false)
`

func TestTableString(t *testing.T) {
	require := require.New(t)
	schema := sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
		{Name: "col2", Type: sql.Int64},
	}
	require.Equal(expectedString, printTable("foo", schema, nil, nil, nil))
}

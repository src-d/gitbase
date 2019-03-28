package rule

import (
	"fmt"
	"io"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestParallelProject(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()
	child := mem.NewTable("test", sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
		{Name: "col2", Type: sql.Text, Nullable: true},
	})

	var input, expected []sql.Row
	for i := 1; i < 500; i++ {
		input = append(input, sql.Row{
			fmt.Sprintf("col1_%d", i), fmt.Sprintf("col2_%d", i),
		})

		expected = append(expected, sql.Row{fmt.Sprintf("col2_%d", i)})
	}

	for _, row := range input {
		require.NoError(child.Insert(sql.NewEmptyContext(), row))
	}

	p := newParallelProject(
		[]sql.Expression{expression.NewGetField(1, sql.Text, "col2", true)},
		plan.NewResolvedTable(child),
		runtime.NumCPU(),
	)

	iter, err := p.RowIter(ctx)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.ElementsMatch(expected, rows)

	_, err = iter.Next()
	require.Equal(io.EOF, err)
}

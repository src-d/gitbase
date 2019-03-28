package rule

import (
	"testing"

	"github.com/src-d/gitbase"
	"github.com/src-d/gitbase/internal/function"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestParallelizeUASTProjections(t *testing.T) {
	require := require.New(t)

	tables := gitbase.NewDatabase("foo", gitbase.NewRepositoryPool(0)).Tables()

	uastFn, err := function.NewUAST(
		expression.NewGetFieldWithTable(0, sql.Blob, "files", "blob_content", false),
	)
	require.NoError(err)

	uastModeFn := function.NewUASTMode(
		expression.NewLiteral("semantic", sql.Text),
		expression.NewGetFieldWithTable(0, sql.Blob, "files", "blob_content", false),
		expression.NewLiteral("Go", sql.Text),
	)

	node := plan.NewProject(
		[]sql.Expression{
			uastModeFn,
			uastFn,
		},
		plan.NewExchange(
			5,
			plan.NewProject(
				[]sql.Expression{
					expression.NewAlias(
						uastFn,
						"foo",
					),
				},
				plan.NewResolvedTable(tables[gitbase.FilesTableName]),
			),
		),
	)

	a := analyzer.NewBuilder(nil).WithParallelism(4).Build()

	result, err := ParallelizeUASTProjections(sql.NewEmptyContext(), a, node)
	require.NoError(err)

	expected := newParallelProject(
		[]sql.Expression{
			uastModeFn,
			uastFn,
		},
		plan.NewExchange(
			5,
			plan.NewProject(
				[]sql.Expression{
					expression.NewAlias(
						uastFn,
						"foo",
					),
				},
				plan.NewResolvedTable(tables[gitbase.FilesTableName]),
			),
		),
		4,
	)

	require.Equal(expected, result)
}

package gitbase

import (
	"context"
	"testing"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestAllReposIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	require.Len(chainableIterRows(t, ctx, NewAllReposIter(nil)), 2)
}

func TestAllRemotesIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	require.Len(chainableIterRows(t, ctx, NewAllRemotesIter(nil)), 2)

	require.Len(
		chainableIterRows(t, ctx, NewAllRemotesIter(
			expression.NewNot(
				expression.NewEquals(
					expression.NewGetField(2, sql.Text, "push_url", false),
					expression.NewLiteral("git@github.com:git-fixtures/submodule.git", sql.Text),
				),
			),
		)),
		1,
	)
}

func TestRepoRemotesIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	require.Len(chainableIterRows(
		t, ctx,
		NewRepoRemotesIter(NewAllReposIter(nil), nil),
	), 2)

	require.Len(chainableIterRows(
		t, ctx,
		NewRepoRemotesIter(
			NewAllReposIter(nil),
			expression.NewNot(
				expression.NewEquals(
					expression.NewGetField(3, sql.Text, "push_url", false),
					expression.NewLiteral("git@github.com:git-fixtures/submodule.git", sql.Text),
				),
			),
		),
	), 1)
}

func TestAllRefsIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewAllRefsIter(nil),
	)

	it, err := NewRowRepoIter(ctx, new(referenceIter))
	require.NoError(err)
	expected, err := sql.RowIterToRows(it)
	require.NoError(err)

	require.ElementsMatch(expected, rows)

	rows = chainableIterRows(
		t, ctx,
		NewAllRefsIter(
			expression.NewEquals(
				expression.NewGetField(1, sql.Text, "name", false),
				expression.NewLiteral("HEAD", sql.Text),
			),
		),
	)

	require.Len(rows, 2)
}

func TestRepoRefsIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewRepoRefsIter(
			NewAllReposIter(nil),
			nil,
		),
	)

	expected := chainableIterRows(
		t, ctx,
		NewAllRefsIter(nil),
	)

	for i := range rows {
		rows[i] = rows[i][1:]
	}

	require.ElementsMatch(expected, rows)

	rows = chainableIterRows(
		t, ctx,
		NewRepoRefsIter(
			NewAllReposIter(nil),
			expression.NewEquals(
				expression.NewGetField(2, sql.Text, "name", false),
				expression.NewLiteral("HEAD", sql.Text),
			),
		),
	)

	require.Len(rows, 2)
}

func TestRemoteRefsIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewRemoteRefsIter(
			NewAllRemotesIter(nil),
			nil,
		),
	)

	expected := chainableIterRows(
		t, ctx,
		NewAllRefsIter(nil),
	)

	for i := range rows {
		rows[i] = rows[i][6:]
	}

	require.ElementsMatch(expected, rows)

	rows = chainableIterRows(
		t, ctx,
		NewRemoteRefsIter(
			NewAllRemotesIter(nil),
			expression.NewEquals(
				expression.NewGetField(7, sql.Text, "name", false),
				expression.NewLiteral("HEAD", sql.Text),
			),
		),
	)

	require.Len(rows, 2)
}

func TestAllCommitsIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewAllCommitsIter(nil),
	)

	it, err := NewRowRepoIter(ctx, new(commitIter))
	require.NoError(err)
	expected, err := sql.RowIterToRows(it)
	require.NoError(err)

	require.ElementsMatch(expected, rows)

	rows = chainableIterRows(
		t, ctx,
		NewAllCommitsIter(
			expression.NewEquals(
				expression.NewGetField(2, sql.Text, "author_email", false),
				expression.NewLiteral("mcuadros@gmail.com", sql.Text),
			),
		),
	)

	require.Len(rows, 12)
}

func TestRefCommitsIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewRefCommitsIter(
			NewAllRefsIter(nil),
			nil,
		),
	)
	require.Len(rows, 44)

	rows = chainableIterRows(
		t, ctx,
		NewRefCommitsIter(
			NewAllRefsIter(expression.NewEquals(
				expression.NewGetField(1, sql.Text, "name", false),
				expression.NewLiteral("HEAD", sql.Text),
			)),
			expression.NewEquals(
				expression.NewGetField(5, sql.Text, "author_email", false),
				expression.NewLiteral("mcuadros@gmail.com", sql.Text),
			),
		),
	)
	require.Len(rows, 11)
}

func TestRefHEADCommitsIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewRefHEADCommitsIter(NewAllRefsIter(nil), nil, false),
	)

	it, err := NewRowRepoIter(ctx, new(referenceIter))
	require.NoError(err)
	expected, err := sql.RowIterToRows(it)
	require.NoError(err)

	require.Len(rows, len(expected))
	for _, row := range rows {
		require.Equal(row[2 /* ref hash */], row[3 /* commit hash */])
	}

	rows = chainableIterRows(
		t, ctx,
		NewRefHEADCommitsIter(
			NewAllRefsIter(nil),
			expression.NewEquals(
				expression.NewGetField(5, sql.Text, "author_email", false),
				expression.NewLiteral("mcuadros@gmail.com", sql.Text),
			),
			false,
		),
	)

	require.Len(rows, 7)
	for _, row := range rows {
		require.Equal(row[2 /* ref hash */], row[3 /* commit hash */])
	}
}

func TestAllTreeEntriesIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewAllTreeEntriesIter(nil),
	)

	it, err := NewRowRepoIter(ctx, new(treeEntryIter))
	require.NoError(err)
	expected, err := sql.RowIterToRows(it)
	require.NoError(err)

	require.ElementsMatch(expected, rows)

	rows = chainableIterRows(
		t, ctx,
		NewAllTreeEntriesIter(
			expression.NewEquals(
				expression.NewGetField(3, sql.Text, "name", false),
				expression.NewLiteral("LICENSE", sql.Text),
			),
		),
	)

	require.Len(rows, 8)
}

func TestCommitTreeEntriesIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewCommitTreeEntriesIter(
			NewAllCommitsIter(nil),
			nil,
			false,
		),
	)

	require.Len(rows, 52)
}

func TestTreeEntryBlobsIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewTreeEntryBlobsIter(
			// FIXME: instead of using NewAllTreeEntriesIter, use the chained
			// one with commits, since the implementation of the other is wrong.
			NewCommitTreeEntriesIter(
				NewAllCommitsIter(nil),
				nil,
				false,
			),
			nil,
		),
	)

	for i := range rows {
		rows[i] = rows[i][len(CommitsSchema):]
		require.Equal(rows[i][1], rows[i][4])
	}

	require.Len(rows, 52)

	rows = chainableIterRows(
		t, ctx,
		NewTreeEntryBlobsIter(
			// FIXME: instead of using NewAllTreeEntriesIter, use the chained
			// one with commits, since the implementation of the other is wrong.
			NewCommitTreeEntriesIter(
				NewAllCommitsIter(nil),
				nil,
				false,
			),
			expression.NewLessThan(
				expression.NewGetField(len(CommitsSchema)+len(TreeEntriesSchema)+1, sql.Int64, "size", false),
				expression.NewLiteral(int64(150), sql.Int64),
			),
		),
	)

	for i := range rows {
		rows[i] = rows[i][len(CommitsSchema):]
		require.Equal(rows[i][1], rows[i][4])
	}

	require.Len(rows, 11)
}

func chainableIterRows(t *testing.T, ctx *sql.Context, iter ChainableIter) []sql.Row {
	it, err := NewRowRepoIter(ctx, NewChainableRowRepoIter(ctx, iter))
	require.NoError(t, err)
	rows, err := sql.RowIterToRows(it)
	require.NoError(t, err)
	return rows
}

func setupIter(t *testing.T) (*sql.Context, func()) {
	require.NoError(t, fixtures.Init())

	pool := NewRepositoryPool()
	for _, f := range fixtures.ByTag("worktree") {
		pool.AddGit(f.Worktree().Root())
	}

	session := NewSession(&pool)
	ctx := sql.NewContext(context.TODO(), session, opentracing.NoopTracer{})
	cleanup := func() {
		require.NoError(t, fixtures.Clean())
	}

	return ctx, cleanup
}

package gitbase

import (
	"context"
	"testing"

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

func TestSquashContextCancelled(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	var cancel context.CancelFunc
	ctx.Context, cancel = context.WithCancel(ctx.Context)
	cancel()

	iters := []ChainableIter{
		NewAllReposIter(nil),
		NewAllRemotesIter(nil),
		NewAllRefsIter(nil, false),
		NewAllCommitsIter(nil, false),
		NewAllTreeEntriesIter(nil),
	}

	session, err := getSession(ctx)
	require.NoError(err)

	for _, it := range iters {
		iter, err := it.New(ctx, session.Pool)
		require.NoError(err)

		err = iter.Advance()
		require.Error(err)
		require.True(ErrSessionCanceled.Is(err))
	}
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

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	require.Len(chainableIterRows(t, ctx, NewAllRemotesIter(nil)), 2)

	ctx, cleanup3 := setupIterWithErrors(t, true, false)
	defer cleanup3()

	chainableIterRowsError(t, ctx, NewAllRemotesIter(nil))
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

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	require.Len(chainableIterRows(
		t, ctx,
		NewRepoRemotesIter(NewAllReposIter(nil), nil),
	), 2)

	ctx, cleanup3 := setupIterWithErrors(t, true, false)
	defer cleanup3()

	chainableIterRowsError(
		t, ctx,
		NewRepoRemotesIter(NewAllReposIter(nil), nil),
	)
}

func TestAllRefsIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewAllRefsIter(nil, false),
	)

	expectedRowsLen := len(rows)

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
			false,
		),
	)

	require.Len(rows, 2)

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	rows = chainableIterRows(
		t, ctx,
		NewAllRefsIter(nil, false),
	)

	require.Len(rows, expectedRowsLen)

	ctx, cleanup3 := setupIterWithErrors(t, true, false)
	defer cleanup3()

	chainableIterRowsError(
		t, ctx,
		NewAllRefsIter(nil, false),
	)
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
			false,
		),
	)

	expectedRowsLen := len(rows)

	expected := chainableIterRows(
		t, ctx,
		NewAllRefsIter(nil, false),
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
			false,
		),
	)

	require.Len(rows, 2)

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	rows = chainableIterRows(
		t, ctx,
		NewRepoRefsIter(
			NewAllReposIter(nil),
			nil,
			false,
		),
	)

	require.Equal(expectedRowsLen, len(rows))

	ctx, cleanup3 := setupIterWithErrors(t, true, false)
	defer cleanup3()

	chainableIterRowsError(
		t, ctx,
		NewRepoRefsIter(
			NewAllReposIter(nil),
			nil,
			false,
		),
	)
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

	expectedRowsLen := len(rows)

	expected := chainableIterRows(
		t, ctx,
		NewAllRefsIter(nil, false),
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

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	rows = chainableIterRows(
		t, ctx,
		NewRemoteRefsIter(
			NewAllRemotesIter(nil),
			nil,
		),
	)

	require.Len(rows, expectedRowsLen)

	ctx, cleanup3 := setupIterWithErrors(t, true, false)
	defer cleanup3()

	chainableIterRowsError(
		t, ctx,
		NewRemoteRefsIter(
			NewAllRemotesIter(nil),
			nil,
		),
	)
}

func TestAllCommitsIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewAllCommitsIter(nil, false),
	)

	expectedRowsLen := len(rows)

	it, err := NewRowRepoIter(ctx, new(commitIter))
	require.NoError(err)
	expected, err := sql.RowIterToRows(it)
	require.NoError(err)

	require.ElementsMatch(expected, rows)

	rows = chainableIterRows(
		t, ctx,
		NewAllCommitsIter(
			expression.NewEquals(
				expression.NewGetField(3, sql.Text, "commit_author_email", false),
				expression.NewLiteral("mcuadros@gmail.com", sql.Text),
			),
			false,
		),
	)

	require.Len(rows, 12)

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	rows = chainableIterRows(
		t, ctx,
		NewAllCommitsIter(nil, false),
	)

	require.Len(rows, expectedRowsLen)

	ctx, cleanup3 := setupIterWithErrors(t, true, false)
	defer cleanup3()

	chainableIterRowsError(
		t, ctx,
		NewAllCommitsIter(nil, false),
	)
}

func TestRepoCommitsIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewRepoCommitsIter(
			NewAllReposIter(nil),
			nil,
		),
	)

	expected := chainableIterRows(
		t, ctx,
		NewAllCommitsIter(nil, false),
	)

	for i := range rows {
		rows[i] = rows[i][1:]
	}

	require.ElementsMatch(expected, rows)

	rows = chainableIterRows(
		t, ctx,
		NewRepoCommitsIter(
			NewAllReposIter(nil),
			expression.NewEquals(
				expression.NewGetField(2, sql.Text, "commit_hash", false),
				expression.NewLiteral("918c48b83bd081e863dbe1b80f8998f058cd8294", sql.Text),
			),
		),
	)

	require.Len(rows, 1)
}

func TestRefHEADCommitsIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewRefHEADCommitsIter(NewAllRefsIter(nil, false), nil, false),
	)

	expectedRowsLen := len(rows)

	it, err := NewRowRepoIter(ctx, new(referenceIter))
	require.NoError(err)
	expected, err := sql.RowIterToRows(it)
	require.NoError(err)

	require.Len(rows, len(expected))
	for _, row := range rows {
		require.Equal(row[2 /* ref hash */], row[4 /* commit hash */])
	}

	rows = chainableIterRows(
		t, ctx,
		NewRefHEADCommitsIter(
			NewAllRefsIter(nil, false),
			expression.NewEquals(
				expression.NewGetField(6, sql.Text, "commit_author_email", false),
				expression.NewLiteral("mcuadros@gmail.com", sql.Text),
			),
			false,
		),
	)

	require.Len(rows, 7)
	for _, row := range rows {
		require.Equal(row[2 /* ref hash */], row[4 /* commit hash */])
	}

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	rows = chainableIterRows(
		t, ctx,
		NewRefHEADCommitsIter(NewAllRefsIter(nil, false), nil, false),
	)

	require.Len(rows, expectedRowsLen)

	ctx, cleanup3 := setupIterWithErrors(t, true, false)
	defer cleanup3()

	chainableIterRowsError(
		t, ctx,
		NewRefHEADCommitsIter(NewAllRefsIter(nil, false), nil, false),
	)
}

func TestAllTreeEntriesIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewAllTreeEntriesIter(nil),
	)

	expectedRowsLen := len(rows)

	it, err := NewRowRepoIter(ctx, new(treeEntryIter))
	require.NoError(err)
	expected, err := sql.RowIterToRows(it)
	require.NoError(err)

	require.ElementsMatch(expected, rows)

	rows = chainableIterRows(
		t, ctx,
		NewAllTreeEntriesIter(
			expression.NewEquals(
				expression.NewGetField(1, sql.Text, "tree_entry_name", false),
				expression.NewLiteral("LICENSE", sql.Text),
			),
		),
	)

	require.Len(rows, 8)

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	rows = chainableIterRows(
		t, ctx,
		NewAllTreeEntriesIter(nil),
	)

	require.Len(rows, expectedRowsLen)

	ctx, cleanup3 := setupIterWithErrors(t, true, false)
	defer cleanup3()

	chainableIterRowsError(
		t, ctx,
		NewAllTreeEntriesIter(nil),
	)
}

func TestRepoTreeEntriesIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewRepoTreeEntriesIter(
			NewAllReposIter(nil),
			nil,
		),
	)

	expected := chainableIterRows(
		t, ctx,
		NewAllTreeEntriesIter(nil),
	)

	for i := range rows {
		rows[i] = rows[i][1:]
	}

	require.ElementsMatch(expected, rows)

	rows = chainableIterRows(
		t, ctx,
		NewRepoTreeEntriesIter(
			NewAllReposIter(nil),
			expression.NewEquals(
				expression.NewGetField(2, sql.Text, "tree_entry_name", false),
				expression.NewLiteral("LICENSE", sql.Text),
			),
		),
	)

	require.Len(rows, 8)
}

func TestCommitTreesIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewCommitTreesIter(
			NewAllCommitsIter(nil, true),
			nil,
			false,
		),
	)

	require.Len(rows, 24)

	expectedRowsLen := len(rows)

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	rows = chainableIterRows(
		t, ctx,
		NewCommitTreesIter(
			NewAllCommitsIter(nil, false),
			nil,
			false,
		),
	)

	require.Len(rows, expectedRowsLen)

	ctx, cleanup3 := setupIterWithErrors(t, true, false)
	defer cleanup3()

	chainableIterRowsError(
		t, ctx,
		NewCommitTreesIter(
			NewAllCommitsIter(nil, false),
			nil,
			false,
		),
	)
}

func TestTreeEntryBlobsIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewTreeEntryBlobsIter(
			NewAllTreeEntriesIter(nil),
			nil,
			false,
		),
	)

	require.Len(rows, 39)

	expectedRowsLen := len(rows)

	rows = chainableIterRows(
		t, ctx,
		NewTreeEntryBlobsIter(
			NewAllTreeEntriesIter(nil),
			expression.NewLessThan(
				expression.NewGetField(len(TreeEntriesSchema)+2, sql.Int64, "blob_size", false),
				expression.NewLiteral(int64(150), sql.Int64),
			),
			false,
		),
	)

	require.Len(rows, 10)

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	rows = chainableIterRows(
		t, ctx,
		NewTreeEntryBlobsIter(
			NewAllTreeEntriesIter(nil),
			nil,
			false,
		),
	)

	require.Len(rows, expectedRowsLen)

	ctx, cleanup3 := setupIterWithErrors(t, true, false)
	defer cleanup3()

	chainableIterRowsError(
		t, ctx,
		NewTreeEntryBlobsIter(
			NewAllTreeEntriesIter(nil),
			nil,
			false,
		),
	)
}

func TestRepoBlobsIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewRepoBlobsIter(
			NewAllReposIter(nil),
			nil,
			false,
		),
	)

	iter, err := NewRowRepoIter(ctx, new(blobIter))
	require.NoError(err)

	expected, err := sql.RowIterToRows(iter)
	require.NoError(err)

	for i := range rows {
		rows[i] = rows[i][1:]
	}

	require.ElementsMatch(expected, rows)

	rows = chainableIterRows(
		t, ctx,
		NewRepoBlobsIter(
			NewAllReposIter(nil),
			expression.NewEquals(
				expression.NewGetField(2, sql.Text, "hash", false),
				expression.NewLiteral("d3ff53e0564a9f87d8e84b6e28e5060e517008aa", sql.Text),
			),
			false,
		),
	)

	require.Len(rows, 1)
}

func TestCommitBlobsIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewCommitBlobsIter(
			NewAllCommitsIter(nil, true),
			nil,
		),
	)

	require.Len(rows, 52)
}

func chainableIterRowsError(t *testing.T, ctx *sql.Context, iter ChainableIter) {
	it, err := NewChainableRowIter(ctx, iter)
	require.NoError(t, err)
	_, err = sql.RowIterToRows(it)
	require.Error(t, err)
}

func chainableIterRows(t *testing.T, ctx *sql.Context, iter ChainableIter) []sql.Row {
	it, err := NewChainableRowIter(ctx, iter)
	require.NoError(t, err)
	rows, err := sql.RowIterToRows(it)
	require.NoError(t, err)
	return rows
}

func setupIter(t *testing.T) (*sql.Context, func()) {
	return setupIterWithErrors(t, false, false)
}

func setupIterWithErrors(t *testing.T, badRepo bool, skipErrors bool) (*sql.Context, func()) {
	require.NoError(t, fixtures.Init())

	pool := NewRepositoryPool()
	if badRepo {
		// TODO: add repo with errors
		pool.Add(gitRepo("bad_repo", "bad_path"))
	}

	for _, f := range fixtures.ByTag("worktree") {
		path := f.Worktree().Root()
		ok, err := IsGitRepo(path)
		require.NoError(t, err)
		if ok {
			pool.AddGit(f.Worktree().Root())
		}
	}

	session := NewSession(pool, WithSkipGitErrors(skipErrors))
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))
	cleanup := func() {
		require.NoError(t, fixtures.Clean())
	}

	return ctx, cleanup
}

func TestIndexRefCommitsIter(t *testing.T) {
	require := require.New(t)

	ctx, index, cleanup := setupWithIndex(t, new(refCommitsTable))
	defer cleanup()

	it, err := NewChainableRowIter(
		ctx,
		NewRefCommitCommitsIter(NewAllRefCommitsIter(nil), nil),
	)
	require.NoError(err)

	expected, err := sql.RowIterToRows(it)
	require.NoError(err)

	iter := NewRefCommitCommitsIter(
		NewIndexRefCommitsIter(index, nil),
		nil,
	)

	it, err = NewChainableRowIter(ctx, iter)
	require.NoError(err)

	rows, err := sql.RowIterToRows(it)
	require.NoError(err)

	require.ElementsMatch(expected, rows)
}

func TestIndexCommitsIter(t *testing.T) {
	require := require.New(t)

	ctx, index, cleanup := setupWithIndex(t, new(commitsTable))
	defer cleanup()

	it, err := NewChainableRowIter(
		ctx,
		NewCommitTreesIter(NewAllCommitsIter(nil, false), nil, false),
	)
	require.NoError(err)

	expected, err := sql.RowIterToRows(it)
	require.NoError(err)

	iter := NewCommitTreesIter(
		NewIndexCommitsIter(index, nil),
		nil,
		false,
	)

	it, err = NewChainableRowIter(ctx, iter)
	require.NoError(err)

	rows, err := sql.RowIterToRows(it)
	require.NoError(err)

	require.ElementsMatch(expected, rows)
}

func TestIndexCommitTreesIter(t *testing.T) {
	require := require.New(t)

	ctx, index, cleanup := setupWithIndex(t, new(commitTreesTable))
	defer cleanup()

	it, err := NewChainableRowIter(
		ctx,
		NewTreeTreeEntriesIter(NewAllCommitTreesIter(nil), nil, false),
	)
	require.NoError(err)

	expected, err := sql.RowIterToRows(it)
	require.NoError(err)

	iter := NewTreeTreeEntriesIter(
		NewIndexCommitTreesIter(index, nil),
		nil,
		false,
	)

	it, err = NewChainableRowIter(ctx, iter)
	require.NoError(err)

	rows, err := sql.RowIterToRows(it)
	require.NoError(err)

	require.ElementsMatch(expected, rows)
}

func TestIndexCommitBlobsIter(t *testing.T) {
	require := require.New(t)

	ctx, index, cleanup := setupWithIndex(t, new(commitBlobsTable))
	defer cleanup()

	it, err := NewChainableRowIter(
		ctx,
		NewCommitBlobBlobsIter(NewAllCommitBlobsIter(nil), nil, false),
	)
	require.NoError(err)

	expected, err := sql.RowIterToRows(it)
	require.NoError(err)

	iter := NewCommitBlobBlobsIter(
		NewIndexCommitBlobsIter(index, nil),
		nil,
		false,
	)

	it, err = NewChainableRowIter(ctx, iter)
	require.NoError(err)

	rows, err := sql.RowIterToRows(it)
	require.NoError(err)

	require.ElementsMatch(expected, rows)
}

func TestIndexTreeEntriesIter(t *testing.T) {
	require := require.New(t)

	ctx, index, cleanup := setupWithIndex(t, new(treeEntriesTable))
	defer cleanup()

	it, err := NewChainableRowIter(
		ctx,
		NewTreeEntryBlobsIter(NewAllTreeEntriesIter(nil), nil, false),
	)
	require.NoError(err)

	expected, err := sql.RowIterToRows(it)
	require.NoError(err)

	iter := NewTreeEntryBlobsIter(
		NewIndexTreeEntriesIter(index, nil),
		nil,
		false,
	)

	it, err = NewChainableRowIter(ctx, iter)
	require.NoError(err)

	rows, err := sql.RowIterToRows(it)
	require.NoError(err)

	require.ElementsMatch(expected, rows)
}

func setupWithIndex(
	t *testing.T,
	table Indexable,
) (*sql.Context, sql.IndexLookup, CleanupFunc) {
	t.Helper()
	ctx, _, cleanup := setup(t)
	index := &lookup{tableIndexValues(t, table, ctx)}
	return ctx, index, cleanup
}

type lookup struct {
	values sql.IndexValueIter
}

func (l lookup) Values() (sql.IndexValueIter, error) {
	return l.values, nil
}

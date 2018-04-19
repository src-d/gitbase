package gitbase

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
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
		NewAllRefsIter(nil),
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
		),
	)

	require.Len(rows, 2)

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	rows = chainableIterRows(
		t, ctx,
		NewAllRefsIter(nil),
	)

	require.Len(rows, expectedRowsLen)

	ctx, cleanup3 := setupIterWithErrors(t, true, false)
	defer cleanup3()

	chainableIterRowsError(
		t, ctx,
		NewAllRefsIter(nil),
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
		),
	)

	expectedRowsLen := len(rows)

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

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	rows = chainableIterRows(
		t, ctx,
		NewRepoRefsIter(
			NewAllReposIter(nil),
			nil,
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
		NewAllCommitsIter(nil),
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
				expression.NewGetField(2, sql.Text, "author_email", false),
				expression.NewLiteral("mcuadros@gmail.com", sql.Text),
			),
		),
	)

	require.Len(rows, 12)

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	rows = chainableIterRows(
		t, ctx,
		NewAllCommitsIter(nil),
	)

	require.Len(rows, expectedRowsLen)

	ctx, cleanup3 := setupIterWithErrors(t, true, false)
	defer cleanup3()

	chainableIterRowsError(
		t, ctx,
		NewAllCommitsIter(nil),
	)
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

	expectedRowsLen := len(rows)

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

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	rows = chainableIterRows(
		t, ctx,
		NewRefCommitsIter(
			NewAllRefsIter(nil),
			nil,
		),
	)

	require.Len(rows, expectedRowsLen)

	ctx, cleanup3 := setupIterWithErrors(t, true, false)
	defer cleanup3()

	chainableIterRowsError(
		t, ctx,
		NewRefCommitsIter(
			NewAllRefsIter(nil),
			nil,
		),
	)
}

func TestRefHEADCommitsIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewRefHEADCommitsIter(NewAllRefsIter(nil), nil, false),
	)

	expectedRowsLen := len(rows)

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

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	rows = chainableIterRows(
		t, ctx,
		NewRefHEADCommitsIter(NewAllRefsIter(nil), nil, false),
	)

	require.Len(rows, expectedRowsLen)

	ctx, cleanup3 := setupIterWithErrors(t, true, false)
	defer cleanup3()

	chainableIterRowsError(
		t, ctx,
		NewRefHEADCommitsIter(NewAllRefsIter(nil), nil, false),
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
				expression.NewGetField(3, sql.Text, "name", false),
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

	require.Len(rows, 67)

	expectedRowsLen := len(rows)

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	rows = chainableIterRows(
		t, ctx,
		NewCommitTreeEntriesIter(
			NewAllCommitsIter(nil),
			nil,
			false,
		),
	)

	require.Len(rows, expectedRowsLen)

	ctx, cleanup3 := setupIterWithErrors(t, true, false)
	defer cleanup3()

	chainableIterRowsError(
		t, ctx,
		NewCommitTreeEntriesIter(
			NewAllCommitsIter(nil),
			nil,
			false,
		),
	)
}

func TestCommitMainTreeEntriesIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewCommitMainTreeEntriesIter(
			NewAllCommitsIter(nil),
			nil,
			false,
		),
	)

	require.Len(rows, 52)

	expectedRowsLen := len(rows)

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	rows = chainableIterRows(
		t, ctx,
		NewCommitMainTreeEntriesIter(
			NewAllCommitsIter(nil),
			nil,
			false,
		),
	)

	require.Len(rows, expectedRowsLen)

	ctx, cleanup3 := setupIterWithErrors(t, true, false)
	defer cleanup3()

	chainableIterRowsError(
		t, ctx,
		NewCommitMainTreeEntriesIter(
			NewAllCommitsIter(nil),
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
			NewCommitTreeEntriesIter(
				NewAllCommitsIter(nil),
				nil,
				false,
			),
			nil,
			false,
		),
	)

	require.Len(rows, 67)

	expectedRowsLen := len(rows)

	rows = chainableIterRows(
		t, ctx,
		NewTreeEntryBlobsIter(
			NewCommitTreeEntriesIter(
				NewAllCommitsIter(nil),
				nil,
				false,
			),
			expression.NewLessThan(
				expression.NewGetField(len(CommitsSchema)+len(TreeEntriesSchema)+1, sql.Int64, "size", false),
				expression.NewLiteral(int64(150), sql.Int64),
			),
			false,
		),
	)

	require.Len(rows, 12)

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	rows = chainableIterRows(
		t, ctx,
		NewTreeEntryBlobsIter(
			NewCommitTreeEntriesIter(
				NewAllCommitsIter(nil),
				nil,
				false,
			),
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
			NewCommitTreeEntriesIter(
				NewAllCommitsIter(nil),
				nil,
				false,
			),
			nil,
			false,
		),
	)
}

func TestRecursiveTreeFileIter(t *testing.T) {
	require := require.New(t)
	require.NoError(fixtures.Init())
	defer func() {
		require.NoError(fixtures.Clean())
	}()

	repo, err := git.PlainOpen(fixtures.ByTag("worktree").One().Worktree().Root())
	require.NoError(err)

	hash := plumbing.NewHash("a8d315b2b1c615d43042c3a62402b8a54288cf5c")
	tree, err := repo.TreeObject(hash)
	require.NoError(err)

	r := &Repository{
		Repo: repo,
		ID:   "test_repo",
	}

	session := NewSession(nil)
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))
	iter := newRecursiveTreeFileIter(ctx, r, tree)

	var result [][]interface{}
	for {
		f, t, err := iter.Next()
		if err == io.EOF {
			break
		}
		require.NoError(err)

		result = append(result, []interface{}{
			f.Name, f.Hash.String(), t.Hash.String(),
		})
	}

	expected := [][]interface{}{
		{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
		{"CHANGELOG", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
		{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
		{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
		{"go/example.go", "880cd14280f4b9b6ed3986d6671f907d7cc2a198", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
		{"json/long.json", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
		{"json/short.json", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
		{"php/crappy.php", "9a48f23120e880dfbe41f7c9b7b708e9ee62a492", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
		{"vendor/foo.go", "9dea2395f5403188298c1dabe8bdafe562c491e3", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
		{"example.go", "880cd14280f4b9b6ed3986d6671f907d7cc2a198", "a39771a7651f97faf5c72e08224d857fc35133db"},
		{"long.json", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9", "5a877e6a906a2743ad6e45d99c1793642aaf8eda"},
		{"short.json", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956", "5a877e6a906a2743ad6e45d99c1793642aaf8eda"},
		{"crappy.php", "9a48f23120e880dfbe41f7c9b7b708e9ee62a492", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},
		{"foo.go", "9dea2395f5403188298c1dabe8bdafe562c491e3", "cf4aa3b38974fb7d81f367c0830f7d78d65ab86b"},
	}

	require.Equal(expected, result)

	// TODO: add repo that has errors walking trees
}

func TestCommitBlobsIter(t *testing.T) {
	require := require.New(t)
	ctx, cleanup := setupIter(t)
	defer cleanup()

	rows := chainableIterRows(
		t, ctx,
		NewCommitBlobsIter(
			NewRefHEADCommitsIter(
				NewAllRefsIter(nil),
				nil,
				true,
			),
			nil,
			false,
		),
	)

	require.Len(rows, 42)
	expectedRowsLen := len(rows)

	ctx, cleanup2 := setupIterWithErrors(t, true, true)
	defer cleanup2()

	rows = chainableIterRows(
		t, ctx,
		NewCommitBlobsIter(
			NewRefHEADCommitsIter(
				NewAllRefsIter(nil),
				nil,
				true,
			),
			nil,
			false,
		),
	)

	require.Len(rows, expectedRowsLen)

	ctx, cleanup3 := setupIterWithErrors(t, true, false)
	defer cleanup3()

	chainableIterRowsError(
		t, ctx,
		NewCommitBlobsIter(
			NewRefHEADCommitsIter(
				NewAllRefsIter(nil),
				nil,
				true,
			),
			nil,
			false,
		),
	)
}

func chainableIterRowsError(t *testing.T, ctx *sql.Context, iter ChainableIter) {
	it, err := NewRowRepoIter(ctx, NewChainableRowRepoIter(ctx, iter))
	require.NoError(t, err)
	_, err = sql.RowIterToRows(it)
	require.Error(t, err)
}

func chainableIterRows(t *testing.T, ctx *sql.Context, iter ChainableIter) []sql.Row {
	it, err := NewRowRepoIter(ctx, NewChainableRowRepoIter(ctx, iter))
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
		pool.Add("bad_repo", "bad_path", gitRepo)
	}

	for _, f := range fixtures.ByTag("worktree") {
		pool.AddGit(f.Worktree().Root())
	}

	session := NewSession(pool, WithSkipGitErrors(skipErrors))
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))
	cleanup := func() {
		require.NoError(t, fixtures.Clean())
	}

	return ctx, cleanup
}

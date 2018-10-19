package gitbase_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/src-d/gitbase/cmd/gitbase/command"
	"github.com/src-d/gitbase/internal/rule"

	"github.com/src-d/gitbase"
	"github.com/src-d/gitbase/internal/function"
	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	sqle "gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	sqlfunction "gopkg.in/src-d/go-mysql-server.v0/sql/expression/function"
	"gopkg.in/src-d/go-mysql-server.v0/sql/index/pilosa"
)

func TestIntegration(t *testing.T) {
	engine := newBaseEngine()
	require.NoError(t, fixtures.Init())
	defer func() {
		require.NoError(t, fixtures.Clean())
	}()

	path := fixtures.ByTag("worktree").One().Worktree().Root()

	pool := gitbase.NewRepositoryPool(cache.DefaultMaxSize)
	require.NoError(t, pool.AddGitWithID("worktree", path))

	testCases := []struct {
		query  string
		result []sql.Row
	}{
		{
			`SELECT COUNT(c.commit_hash), c.commit_hash
			FROM ref_commits r
			INNER JOIN commit_blobs c
				ON r.ref_name = 'HEAD' AND r.commit_hash = c.commit_hash
			INNER JOIN blobs b
				ON c.blob_hash = b.blob_hash
			GROUP BY c.commit_hash`,
			[]sql.Row{
				{int32(4), "1669dce138d9b841a518c64b10914d88f5e488ea"},
				{int32(3), "35e85108805c84807bc66a02d91535e1e24b38b9"},
				{int32(9), "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				{int32(8), "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				{int32(3), "a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69"},
				{int32(6), "af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
				{int32(2), "b029517f6300c2da0f4b651b8642506cd6aaf45d"},
				{int32(3), "b8e471f58bcbca63b07bda20e428190409c2db47"},
			},
		},
		{
			`SELECT ref_name FROM refs ORDER BY ref_name`,
			[]sql.Row{
				{"HEAD"},
				{"refs/heads/master"},
				{"refs/remotes/origin/branch"},
				{"refs/remotes/origin/master"},
			},
		},
		{
			`SELECT c.commit_hash
			FROM ref_commits r
			INNER JOIN commits c
				ON r.ref_name = 'HEAD'
				AND r.commit_hash = c.commit_hash`,
			[]sql.Row{
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294"},
				{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
				{"1669dce138d9b841a518c64b10914d88f5e488ea"},
				{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69"},
				{"b8e471f58bcbca63b07bda20e428190409c2db47"},
				{"35e85108805c84807bc66a02d91535e1e24b38b9"},
				{"b029517f6300c2da0f4b651b8642506cd6aaf45d"},
			},
		},
		{
			`SELECT COUNT(first_commit_year), first_commit_year
			FROM (
				SELECT YEAR(c.commit_author_when) AS first_commit_year
				FROM ref_commits rc
				INNER JOIN commits c
					ON rc.commit_hash = c.commit_hash
				ORDER BY c.commit_author_when
				LIMIT 1
			) repo_years
			GROUP BY first_commit_year`,
			[]sql.Row{{int32(1), int32(2015)}},
		},
		{
			`SELECT COUNT(*) as num_commits, month, repo_id, committer_email
			FROM (
				SELECT
					MONTH(committer_when) as month,
					rc.repository_id as repo_id,
					committer_email
				FROM ref_commits rc
				INNER JOIN commits c ON rc.commit_hash = c.commit_hash
				WHERE YEAR(committer_when) = 2015 AND rc.ref_name = 'refs/heads/master'
			) as t
			GROUP BY committer_email, month, repo_id`,
			[]sql.Row{
				{int32(6), int32(3), "worktree", "mcuadros@gmail.com"},
				{int32(1), int32(4), "worktree", "mcuadros@gmail.com"},
				{int32(1), int32(3), "worktree", "daniel@lordran.local"},
			},
		},
		{
			`SELECT * FROM (
				SELECT COUNT(c.commit_hash) AS num, c.commit_hash
				FROM ref_commits r
				INNER JOIN commits c
					ON r.commit_hash = c.commit_hash
				GROUP BY c.commit_hash
			) t WHERE num > 1`,
			[]sql.Row{
				{int32(3), "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				{int32(4), "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				{int32(4), "af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
				{int32(4), "1669dce138d9b841a518c64b10914d88f5e488ea"},
				{int32(4), "a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69"},
				{int32(4), "b8e471f58bcbca63b07bda20e428190409c2db47"},
				{int32(4), "35e85108805c84807bc66a02d91535e1e24b38b9"},
				{int32(4), "b029517f6300c2da0f4b651b8642506cd6aaf45d"},
			},
		},
		{
			`SELECT count(1), refs.repository_id
				FROM refs
				NATURAL JOIN commits
				NATURAL JOIN commit_blobs
				WHERE refs.ref_name = 'HEAD'
				GROUP BY refs.repository_id`,
			[]sql.Row{
				{int32(9), "worktree"},
			},
		},
		{
			`SELECT c.commit_hash, COUNT(*) as num_files
			FROM commit_files c
			NATURAL JOIN files f
			GROUP BY c.commit_hash`,
			[]sql.Row{
				{"b8e471f58bcbca63b07bda20e428190409c2db47", int32(3)},
				{"b029517f6300c2da0f4b651b8642506cd6aaf45d", int32(2)},
				{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", int32(6)},
				{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", int32(3)},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", int32(8)},
				{"1669dce138d9b841a518c64b10914d88f5e488ea", int32(4)},
				{"35e85108805c84807bc66a02d91535e1e24b38b9", int32(3)},
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", int32(9)},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", int32(9)},
			},
		},
		{
			`SELECT MONTH(committer_when) as month,
				r.repository_id as repo_id,
				committer_email
			FROM ref_commits r
			INNER JOIN commits c
				ON YEAR(c.committer_when) = 2015
				AND r.commit_hash = c.commit_hash
			WHERE r.ref_name = 'HEAD'`,
			[]sql.Row{
				{int32(4), "worktree", "mcuadros@gmail.com"},
				{int32(3), "worktree", "mcuadros@gmail.com"},
				{int32(3), "worktree", "mcuadros@gmail.com"},
				{int32(3), "worktree", "mcuadros@gmail.com"},
				{int32(3), "worktree", "mcuadros@gmail.com"},
				{int32(3), "worktree", "mcuadros@gmail.com"},
				{int32(3), "worktree", "mcuadros@gmail.com"},
				{int32(3), "worktree", "daniel@lordran.local"},
			},
		},
		{
			`SELECT
				c.commit_hash AS hash,
				c.commit_message AS message,
				commit_author_name AS author,
				te.tree_entry_name AS file
			FROM
				commits c
			NATURAL JOIN commit_trees
			NATURAL JOIN tree_entries as te
			WHERE te.repository_id='worktree'
			LIMIT 8`,
			[]sql.Row{
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "some code in a branch\n", "Máximo Cuadros Ortiz", ".gitignore"},
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "some code in a branch\n", "Máximo Cuadros Ortiz", "CHANGELOG"},
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "some code in a branch\n", "Máximo Cuadros Ortiz", "LICENSE"},
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "some code in a branch\n", "Máximo Cuadros Ortiz", "README"},
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "some code in a branch\n", "Máximo Cuadros Ortiz", "binary.jpg"},
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "some code in a branch\n", "Máximo Cuadros Ortiz", "go"},
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "some code in a branch\n", "Máximo Cuadros Ortiz", "json"},
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "some code in a branch\n", "Máximo Cuadros Ortiz", "php"},
			},
		},
		{
			`SELECT
				c.commit_hash AS hash,
				c.commit_message AS message,
				te.tree_entry_name AS file,
				te.tree_hash AS thash
			FROM
				commits c
			NATURAL JOIN commit_trees
			NATURAL JOIN tree_entries as te
			WHERE te.repository_id='worktree'
			LIMIT 8`,
			[]sql.Row{
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "some code in a branch\n", ".gitignore", "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "some code in a branch\n", "CHANGELOG", "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "some code in a branch\n", "LICENSE", "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "some code in a branch\n", "README", "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "some code in a branch\n", "binary.jpg", "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "some code in a branch\n", "go", "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "some code in a branch\n", "json", "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "some code in a branch\n", "php", "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
			},
		},
		{
			`SELECT
				file_path, array_length(uast_extract(uast(blob_content, language(file_path)), "@type"))
			FROM
				files
			WHERE
				language(file_path)="Go"
			LIMIT 1`,
			[]sql.Row{
				{"go/example.go", int32(1)},
			},
		},
	}

	runTests := func(t *testing.T) {
		for _, tt := range testCases {
			t.Run(tt.query, func(t *testing.T) {
				require := require.New(t)

				session := gitbase.NewSession(pool)
				ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

				_, iter, err := engine.Query(ctx, tt.query)
				require.NoError(err)
				rows, err := sql.RowIterToRows(iter)
				require.NoError(err)

				require.ElementsMatch(tt.result, rows)
			})
		}
	}

	t.Run("without squash", runTests)

	a := analyzer.NewBuilder(engine.Catalog).
		AddPostAnalyzeRule(rule.SquashJoinsRule, rule.SquashJoins).
		Build()

	engine.Analyzer = a
	t.Run("with squash", runTests)
}

func TestUastQueries(t *testing.T) {
	engine, pool, cleanup := setup(t)
	defer cleanup()

	testCases := []struct {
		query string
		rows  int
	}{
		{`SELECT uast_xpath(uast(blob_content, language(tree_entry_name, blob_content)), '//Identifier') as uast,
			tree_entry_name
		FROM tree_entries te
		INNER JOIN blobs b
		ON b.blob_hash = te.blob_hash
		WHERE te.tree_entry_name = 'example.go'`, 1},
		{`SELECT uast_xpath(uast_mode('semantic', blob_content, language(tree_entry_name, blob_content)), '//Identifier') as uast,
			tree_entry_name
		FROM tree_entries te
		INNER JOIN blobs b
		ON b.blob_hash = te.blob_hash
		WHERE te.tree_entry_name = 'example.go'`, 1},
		{`SELECT uast_xpath(uast_mode('annotated', blob_content, language(tree_entry_name, blob_content)), '//*[@roleIdentifier]') as uast,
			tree_entry_name
		FROM tree_entries te
		INNER JOIN blobs b
		ON b.blob_hash = te.blob_hash
		WHERE te.tree_entry_name = 'example.go'`, 1},
		{`SELECT uast_xpath(uast_mode('native', blob_content, language(tree_entry_name, blob_content)), '//*[@ast_type=\'FunctionDef\']') as uast,
			tree_entry_name
		FROM tree_entries te
		INNER JOIN blobs b
		ON b.blob_hash = te.blob_hash
		WHERE te.tree_entry_name = 'example.go'`, 1},
	}

	_ = testCases

	for _, c := range testCases {
		t.Run(c.query, func(t *testing.T) {
			require := require.New(t)

			session := gitbase.NewSession(pool)
			ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

			_, iter, err := engine.Query(ctx, c.query)
			require.NoError(err)

			rows, err := sql.RowIterToRows(iter)
			require.NoError(err)
			require.Len(rows, c.rows)
		})
	}
}

func TestSquashCorrectness(t *testing.T) {
	engine, pool, cleanup := setup(t)
	defer cleanup()

	squashEngine := newSquashEngine()

	queries := []string{
		`SELECT * FROM repositories`,
		`SELECT * FROM refs`,
		`SELECT * FROM remotes`,
		`SELECT * FROM commits`,
		`SELECT * FROM tree_entries`,
		`SELECT * FROM blobs`,
		`SELECT * FROM files`,
		`SELECT * FROM repositories r INNER JOIN refs ON r.repository_id = refs.repository_id`,
		`SELECT * FROM repositories r INNER JOIN remotes ON r.repository_id = remotes.repository_id`,
		`SELECT * FROM refs r INNER JOIN remotes re ON r.repository_id = re.repository_id`,
		`SELECT * FROM refs r INNER JOIN commits c ON r.commit_hash = c.commit_hash`,
		`SELECT * FROM ref_commits r INNER JOIN commits c ON r.commit_hash = c.commit_hash`,
		`SELECT * FROM refs r INNER JOIN commit_trees t ON r.commit_hash = t.commit_hash`,
		`SELECT * FROM refs r INNER JOIN commit_blobs b ON r.commit_hash = b.commit_hash`,
		`SELECT * FROM refs r
		INNER JOIN commit_blobs cb
			ON r.commit_hash = cb.commit_hash
		INNER JOIN blobs b
			ON cb.blob_hash = b.blob_hash`,
		`SELECT * FROM commits c INNER JOIN commit_trees t ON c.commit_hash = t.tree_hash`,
		`SELECT * FROM commits c INNER JOIN tree_entries te ON c.tree_hash = te.tree_hash`,
		`SELECT * FROM commits c
		INNER JOIN commit_blobs cb
			ON c.commit_hash = cb.commit_hash
		INNER JOIN blobs b
			ON cb.blob_hash = b.blob_hash`,
		`SELECT * FROM tree_entries te INNER JOIN blobs b ON te.blob_hash = b.blob_hash`,

		`SELECT * FROM commit_files NATURAL JOIN files`,
		`SELECT * FROM commit_files c INNER JOIN files f ON c.tree_hash = f.tree_hash`,

		`SELECT * FROM repositories r
		INNER JOIN refs re
			ON r.repository_id = re.repository_id
		INNER JOIN commits c
			ON re.commit_hash = c.commit_hash
		WHERE re.ref_name = 'HEAD'`,

		`SELECT * FROM commits c
		INNER JOIN commit_trees t
			ON c.commit_hash = t.commit_hash
		INNER JOIN tree_entries te
			ON t.tree_hash = te.tree_hash
		INNER JOIN blobs b
			ON te.blob_hash = b.blob_hash
		WHERE te.tree_entry_name = 'LICENSE'`,

		`SELECT * FROM repositories,
		commits c INNER JOIN tree_entries te
			ON c.tree_hash = te.tree_hash`,

		`SELECT * FROM refs r
		INNER JOIN ref_commits c
			ON r.ref_name = c.ref_name
			AND c.repository_id = r.repository_id`,

		`SELECT * FROM refs r
		INNER JOIN ref_commits c
			ON r.commit_hash = c.commit_hash
			AND r.ref_name = c.ref_name
			AND c.repository_id = r.repository_id`,

		`SELECT COUNT(r.*) as repos FROM repositories r`,

		`SELECT repository_id, num_files FROM (
			SELECT COUNT(f.*) num_files, f.repository_id
			FROM ref_commits r
			INNER JOIN commit_files cf
				ON r.commit_hash = cf.commit_hash
				AND r.repository_id = cf.repository_id
			INNER JOIN files f
				ON cf.repository_id = f.repository_id
				AND cf.blob_hash = f.blob_hash
				AND cf.tree_hash = f.tree_hash
				AND cf.file_path = f.file_path
			WHERE r.ref_name = 'HEAD'
			GROUP BY f.repository_id
		) t
		ORDER BY num_files DESC
		LIMIT 10`,

		// Squash with non-squashable joins
		`SELECT * FROM refs NATURAL JOIN blobs`,
		`SELECT * FROM remotes NATURAL JOIN commits`,
		`SELECT *
		FROM repositories
		NATURAL JOIN refs
		NATURAL JOIN blobs
		NATURAL JOIN files`,
	}

	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			expected := queryResults(t, engine, pool, q)
			result := queryResults(t, squashEngine, pool, q)
			require.Len(t, result, len(expected))
			require.ElementsMatch(
				t,
				expected,
				result,
			)
		})
	}
}

func queryResults(
	t *testing.T,
	e *sqle.Engine,
	pool *gitbase.RepositoryPool,
	q string,
) []sql.Row {
	session := gitbase.NewSession(pool)
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

	_, iter, err := e.Query(ctx, q)
	require.NoError(t, err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(t, err)

	return rows
}

func TestMissingHeadRefs(t *testing.T) {
	require := require.New(t)

	path := filepath.Join(
		os.Getenv("GOPATH"),
		"src", "github.com", "src-d", "gitbase",
		"_testdata",
	)

	pool := gitbase.NewRepositoryPool(cache.DefaultMaxSize)
	require.NoError(
		filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if gitbase.IsSivaFile(path) {
				require.NoError(pool.AddSivaFile(path))
			}

			return nil
		}),
	)

	engine := newBaseEngine()

	session := gitbase.NewSession(pool)
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))
	_, iter, err := engine.Query(ctx, "SELECT * FROM refs")
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 56)
}

func BenchmarkQueries(b *testing.B) {
	queries := []struct {
		name  string
		query string
	}{
		{
			"simple query",
			`SELECT * FROM repositories r
			INNER JOIN refs rr
			ON r.repository_id = rr.repository_id`,
		},
		{
			"select by specific id",
			`SELECT * FROM ref_commits r
			INNER JOIN commits c
				ON c.commit_hash = r.commit_hash
			WHERE c.commit_hash = '6ecf0ef2c2dffb796033e5a02219af86ec6584e5'
				AND r.ref_name = 'refs/heads/master'`,
		},
		{
			"select file by name",
			`SELECT * FROM files WHERE file_path = 'LICENSE'`,
		},
		{
			"select files by language",
			`SELECT * FROM files WHERE language(file_path, blob_content) = 'Go'`,
		},
		{
			"query with commit_blobs",
			`SELECT COUNT(c.commit_hash), c.commit_hash
			FROM ref_commits r
			INNER JOIN commit_blobs c
				ON r.ref_name = 'HEAD' AND r.commit_hash = c.commit_hash
			INNER JOIN blobs b
				ON c.blob_hash = b.blob_hash
			GROUP BY c.commit_hash`,
		},
		{
			"query with history_idx and 3 joins",
			`SELECT COUNT(first_commit_year), first_commit_year
			FROM (
				SELECT YEAR(c.commit_author_when) AS first_commit_year
				FROM ref_commits r
				INNER JOIN commits c
					ON r.commit_hash = c.commit_hash
				ORDER BY c.commit_author_when
				LIMIT 1
			) repo_years
			GROUP BY first_commit_year`,
		},
		{
			"query with history_idx",
			`SELECT * FROM (
				SELECT COUNT(c.commit_hash) AS num, c.commit_hash
				FROM ref_commits r
				INNER JOIN commits c
					ON r.commit_hash = c.commit_hash
				GROUP BY c.commit_hash
			) t WHERE num > 1`,
		},
		{
			"join tree entries and blobs",
			`SELECT * FROM tree_entries te
			INNER JOIN blobs b
			ON te.blob_hash = b.blob_hash`,
		},
		{
			"join tree entries and blobs with filters",
			`SELECT * FROM tree_entries te
			INNER JOIN blobs b
			ON te.blob_hash = b.blob_hash
			WHERE te.tree_entry_name = 'LICENSE'`,
		},
		{
			"join refs and blobs",
			`SELECT * FROM refs r
			INNER JOIN commit_blobs cb
				ON r.commit_hash = cb.commit_hash
			INNER JOIN blobs b
				ON cb.blob_hash = b.blob_hash`,
		},
		{
			"join refs and blobs with filters",
			`SELECT * FROM refs r
			INNER JOIN commit_blobs cb
				ON r.commit_hash = cb.commit_hash
			INNER JOIN blobs b
				ON cb.blob_hash = b.blob_hash
			WHERE r.ref_name = 'refs/heads/master'`,
		},
	}

	indexesEngine, pool, cleanup := setup(b)
	defer cleanup()

	tmpDir, err := ioutil.TempDir(os.TempDir(), "pilosa-idx-gitbase")
	require.NoError(b, err)
	defer os.RemoveAll(tmpDir)
	indexesEngine.Catalog.RegisterIndexDriver(pilosa.NewDriver(tmpDir))

	ctx := sql.NewContext(
		context.TODO(),
		sql.WithSession(gitbase.NewSession(pool)),
	)

	engine := newBaseEngine()
	squashEngine := newSquashEngine()
	squashIndexEngine := newSquashEngine()

	tmpDir2, err := ioutil.TempDir(os.TempDir(), "pilosa-idx-gitbase")
	require.NoError(b, err)
	defer os.RemoveAll(tmpDir2)
	squashIndexEngine.Catalog.RegisterIndexDriver(pilosa.NewDriver(tmpDir2))

	cleanupIndexes := createTestIndexes(b, indexesEngine, ctx)
	defer cleanupIndexes()

	cleanupIndexes2 := createTestIndexes(b, squashIndexEngine, ctx)
	defer cleanupIndexes2()

	for _, qq := range queries {
		b.Run(qq.name, func(b *testing.B) {
			b.Run("base", func(b *testing.B) {
				benchmarkQuery(b, qq.query, engine, ctx)
			})

			b.Run("indexes", func(b *testing.B) {
				benchmarkQuery(b, qq.query, indexesEngine, ctx)
			})

			b.Run("squash", func(b *testing.B) {
				benchmarkQuery(b, qq.query, squashEngine, ctx)
			})

			b.Run("squash indexes", func(b *testing.B) {
				benchmarkQuery(b, qq.query, squashIndexEngine, ctx)
			})
		})
	}
}

func benchmarkQuery(b *testing.B, query string, engine *sqle.Engine, ctx *sql.Context) {
	for i := 0; i < b.N; i++ {
		_, rows, err := engine.Query(ctx, query)
		require.NoError(b, err)

		_, err = sql.RowIterToRows(rows)
		require.NoError(b, err)
	}
}

func TestIndexes(t *testing.T) {
	t.Skip()
	engine, pool, cleanup := setup(t)
	defer cleanup()

	tmpDir, err := ioutil.TempDir(os.TempDir(), "pilosa-idx-gitbase")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	engine.Catalog.RegisterIndexDriver(pilosa.NewDriver(tmpDir))

	ctx := sql.NewContext(
		context.TODO(),
		sql.WithSession(gitbase.NewSession(pool)),
	)

	baseEngine := newBaseEngine()
	squashEngine := newSquashEngine()

	tmpDir2, err := ioutil.TempDir(os.TempDir(), "pilosa-idx-gitbase")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir2)
	squashEngine.Catalog.RegisterIndexDriver(pilosa.NewDriver(tmpDir2))

	cleanupIndexes := createTestIndexes(t, engine, ctx)
	defer cleanupIndexes()

	cleanupIndexes2 := createTestIndexes(t, squashEngine, ctx)
	defer cleanupIndexes2()

	testCases := []string{
		`SELECT ref_name, commit_hash FROM refs WHERE ref_name = 'refs/heads/master'`,
		`SELECT remote_name, remote_push_url FROM remotes WHERE remote_name = 'origin'`,
		`SELECT commit_hash, commit_author_email FROM commits WHERE commit_hash = '918c48b83bd081e863dbe1b80f8998f058cd8294'`,
		`SELECT commit_hash, ref_name FROM ref_commits WHERE ref_name = 'refs/heads/master'`,
		`SELECT commit_hash, tree_hash FROM commit_trees WHERE commit_hash = '918c48b83bd081e863dbe1b80f8998f058cd8294'`,
		`SELECT commit_hash, blob_hash FROM commit_blobs WHERE commit_hash = '918c48b83bd081e863dbe1b80f8998f058cd8294'`,
		`SELECT tree_entry_name, blob_hash FROM tree_entries WHERE tree_entry_name = 'LICENSE'`,
		`SELECT blob_hash, blob_size FROM blobs WHERE blob_hash = 'd5c0f4ab811897cadf03aec358ae60d21f91c50d'`,
		`SELECT file_path, blob_hash FROM files WHERE file_path = 'LICENSE'`,
		`SELECT b.* FROM tree_entries t
		INNER JOIN blobs b ON t.blob_hash = b.blob_hash
		WHERE t.tree_entry_name = 'LICENSE'`,
		`SELECT c.* FROM ref_commits r
		INNER JOIN commits c ON c.commit_hash = r.commit_hash
		WHERE r.ref_name = 'refs/heads/master'`,
		`SELECT t.* FROM commits c
		INNER JOIN tree_entries t ON c.tree_hash = t.tree_hash
		WHERE c.commit_hash = '918c48b83bd081e863dbe1b80f8998f058cd8294'`,
		`SELECT t.* FROM commit_trees c
		INNER JOIN tree_entries t ON c.tree_hash = t.tree_hash
		WHERE c.commit_hash = '918c48b83bd081e863dbe1b80f8998f058cd8294'`,
		`SELECT b.* FROM commit_blobs c
		INNER JOIN blobs b ON c.blob_hash = b.blob_hash
		WHERE c.commit_hash = '918c48b83bd081e863dbe1b80f8998f058cd8294'`,
		`SELECT f.* FROM commit_files c
		NATURAL JOIN files f
		WHERE c.commit_hash = '918c48b83bd081e863dbe1b80f8998f058cd8294'`,
	}

	for _, tt := range testCases {
		t.Run(tt, func(t *testing.T) {
			require := require.New(t)

			_, iter, err := engine.Query(ctx, tt)
			require.NoError(err)

			rows, err := sql.RowIterToRows(iter)
			require.NoError(err)

			_, iter, err = baseEngine.Query(ctx, tt)
			require.NoError(err)

			expected, err := sql.RowIterToRows(iter)
			require.NoError(err)

			_, iter, err = squashEngine.Query(ctx, tt)
			require.NoError(err)

			squashRows, err := sql.RowIterToRows(iter)
			require.NoError(err)

			require.ElementsMatch(expected, rows)
			require.ElementsMatch(expected, squashRows)
		})
	}
}

func col(t testing.TB, schema sql.Schema, name string) sql.Expression {
	for i, col := range schema {
		if col.Name == name {
			return expression.NewGetFieldWithTable(i, col.Type, col.Source, col.Name, col.Nullable)
		}
	}

	t.Fatalf("unknown column %s in schema", name)
	return nil
}

type indexData struct {
	id    string
	table string
	exprs []string
}

func createTestIndexes(t testing.TB, engine *sqle.Engine, ctx *sql.Context) func() {
	var indexes = []indexData{
		{
			id:    "refs_idx",
			table: gitbase.ReferencesTableName,
			exprs: []string{"ref_name"},
		},
		{
			id:    "remotes_idx",
			table: gitbase.RemotesTableName,
			exprs: []string{"remote_name"},
		},
		{
			id:    "ref_commits_idx",
			table: gitbase.RefCommitsTableName,
			exprs: []string{"ref_name"},
		},
		{
			id:    "commits_idx",
			table: gitbase.CommitsTableName,
			exprs: []string{"commit_hash"},
		},
		{
			id:    "commit_trees_idx",
			table: gitbase.CommitTreesTableName,
			exprs: []string{"commit_hash"},
		},
		{
			id:    "commit_blobs_idx",
			table: gitbase.CommitBlobsTableName,
			exprs: []string{"commit_hash"},
		},
		{
			id:    "tree_entries_idx",
			table: gitbase.TreeEntriesTableName,
			exprs: []string{"tree_entry_name"},
		},
		{
			id:    "blobs_idx",
			table: gitbase.BlobsTableName,
			exprs: []string{"blob_hash"},
		},
		{
			id:    "commit_files_idx",
			table: gitbase.CommitFilesTableName,
			exprs: []string{"commit_hash"},
		},
		{
			id:    "files_idx",
			table: gitbase.FilesTableName,
			exprs: []string{"file_path"},
		},
		{
			id:    "files_lang_idx",
			table: gitbase.FilesTableName,
			exprs: []string{"language(file_path, blob_content)"},
		},
	}

	for _, idx := range indexes {
		createIndex(t, engine, idx, ctx)
	}

	return func() {
		for _, idx := range indexes {
			defer deleteIndex(t, engine, idx)
		}
	}
}

func createIndex(
	t testing.TB,
	e *sqle.Engine,
	data indexData,
	ctx *sql.Context,
) {
	t.Helper()

	query := fmt.Sprintf(
		`CREATE INDEX %s ON %s USING pilosa (%s) WITH (async = false)`,
		data.id, data.table, strings.Join(data.exprs, ", "),
	)

	_, _, err := e.Query(ctx, query)
	require.NoError(t, err)
}

func deleteIndex(
	t testing.TB,
	e *sqle.Engine,
	data indexData,
) {
	t.Helper()
	done, err := e.Catalog.DeleteIndex("foo", data.id, true)
	require.NoError(t, err)
	<-done
}

func setup(t testing.TB) (*sqle.Engine, *gitbase.RepositoryPool, func()) {
	t.Helper()
	require.NoError(t, fixtures.Init())
	cleanup := func() {
		require.NoError(t, fixtures.Clean())
	}

	pool := gitbase.NewRepositoryPool(cache.DefaultMaxSize)
	for _, f := range fixtures.ByTag("worktree") {
		pool.AddGitWithID("worktree", f.Worktree().Root())
	}

	return newBaseEngine(), pool, cleanup
}

func newSquashEngine() *sqle.Engine {
	engine := newBaseEngine()

	engine.Catalog.RegisterFunctions(sqlfunction.Defaults)
	engine.Analyzer = analyzer.NewBuilder(engine.Catalog).
		AddPostAnalyzeRule(rule.SquashJoinsRule, rule.SquashJoins).
		Build()

	return engine
}

func newBaseEngine() *sqle.Engine {
	foo := gitbase.NewDatabase("foo")
	engine := command.NewDatabaseEngine(false, "test", 0, false)

	engine.AddDatabase(foo)
	engine.Catalog.RegisterFunctions(function.Functions)
	return engine
}

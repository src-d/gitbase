package gitbase_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/src-d/gitbase"
	"github.com/src-d/gitbase/cmd/gitbase/command"
	"github.com/src-d/gitbase/internal/function"
	"github.com/src-d/gitbase/internal/rule"

	"github.com/src-d/go-borges/plain"
	"github.com/src-d/go-borges/siva"
	fixtures "github.com/src-d/go-git-fixtures"
	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/auth"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/analyzer"
	"github.com/src-d/go-mysql-server/sql/index/pilosa"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
)

func TestIntegration(t *testing.T) {
	defer func() {
		require.NoError(t, fixtures.Clean())
	}()

	path := fixtures.ByTag("worktree").One().Worktree().Root()
	pathLib := path + "-lib"
	pathRepo := filepath.Join(pathLib, "worktree")

	err := os.MkdirAll(pathLib, 0777)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(pathLib))
	}()

	err = os.Rename(path, pathRepo)
	require.NoError(t, err)

	lib := plain.NewLibrary("plain", nil)
	loc, err := plain.NewLocation("location", osfs.New(pathLib), nil)
	require.NoError(t, err)
	lib.AddLocation(loc)

	pool := gitbase.NewRepositoryPool(cache.NewObjectLRUDefault(), lib)
	engine := newBaseEngine(pool)

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
				{int64(4), "1669dce138d9b841a518c64b10914d88f5e488ea"},
				{int64(3), "35e85108805c84807bc66a02d91535e1e24b38b9"},
				{int64(9), "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				{int64(8), "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				{int64(3), "a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69"},
				{int64(6), "af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
				{int64(2), "b029517f6300c2da0f4b651b8642506cd6aaf45d"},
				{int64(3), "b8e471f58bcbca63b07bda20e428190409c2db47"},
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
			[]sql.Row{{int64(1), int32(2015)}},
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
				{int64(6), int32(3), "worktree", "mcuadros@gmail.com"},
				{int64(1), int32(4), "worktree", "mcuadros@gmail.com"},
				{int64(1), int32(3), "worktree", "daniel@lordran.local"},
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
				{int64(3), "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				{int64(4), "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				{int64(4), "af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
				{int64(4), "1669dce138d9b841a518c64b10914d88f5e488ea"},
				{int64(4), "a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69"},
				{int64(4), "b8e471f58bcbca63b07bda20e428190409c2db47"},
				{int64(4), "35e85108805c84807bc66a02d91535e1e24b38b9"},
				{int64(4), "b029517f6300c2da0f4b651b8642506cd6aaf45d"},
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
				{int64(9), "worktree"},
			},
		},
		{
			`SELECT c.commit_hash, COUNT(*) as num_files
			FROM commit_files c
			NATURAL JOIN files f
			GROUP BY c.commit_hash`,
			[]sql.Row{
				{"b8e471f58bcbca63b07bda20e428190409c2db47", int64(3)},
				{"b029517f6300c2da0f4b651b8642506cd6aaf45d", int64(2)},
				{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", int64(6)},
				{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", int64(3)},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", int64(8)},
				{"1669dce138d9b841a518c64b10914d88f5e488ea", int64(4)},
				{"35e85108805c84807bc66a02d91535e1e24b38b9", int64(3)},
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", int64(9)},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", int64(9)},
			},
		},
		{
			`SELECT commit_hash, file_path
			FROM commit_files
			WHERE commit_hash='1669dce138d9b841a518c64b10914d88f5e488ea' OR file_path = 'go/example.go'`,
			[]sql.Row{
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "go/example.go"},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "go/example.go"},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "go/example.go"},
				{"1669dce138d9b841a518c64b10914d88f5e488ea", ".gitignore"},
				{"1669dce138d9b841a518c64b10914d88f5e488ea", "CHANGELOG"},
				{"1669dce138d9b841a518c64b10914d88f5e488ea", "LICENSE"},
				{"1669dce138d9b841a518c64b10914d88f5e488ea", "binary.jpg"},
			},
		},
		{
			`SELECT commit_hash, file_path
			FROM commit_files
			WHERE commit_hash='1669dce138d9b841a518c64b10914d88f5e488ea' AND file_path = 'binary.jpg'`,
			[]sql.Row{
				{"1669dce138d9b841a518c64b10914d88f5e488ea", "binary.jpg"},
			},
		},
		{
			`SELECT commit_hash, file_path
			FROM commit_files
			WHERE NOT commit_hash = '1669dce138d9b841a518c64b10914d88f5e488ea' AND file_path = 'go/example.go'`,
			[]sql.Row{
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "go/example.go"},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "go/example.go"},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "go/example.go"},
			},
		},
		{
			`SELECT blob_hash, tree_hash
			FROM commit_files
			WHERE tree_hash='eba74343e2f15d62adedfd8c883ee0262b5c8021' OR blob_hash = 'd3ff53e0564a9f87d8e84b6e28e5060e517008aa'`,
			[]sql.Row{
				{"32858aad3c383ed1ff0a0f9bdf231d54a00c9e88", "eba74343e2f15d62adedfd8c883ee0262b5c8021"},
				{"d3ff53e0564a9f87d8e84b6e28e5060e517008aa", "eba74343e2f15d62adedfd8c883ee0262b5c8021"},
				{"c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "eba74343e2f15d62adedfd8c883ee0262b5c8021"},
				{"d5c0f4ab811897cadf03aec358ae60d21f91c50d", "eba74343e2f15d62adedfd8c883ee0262b5c8021"},
				{"d3ff53e0564a9f87d8e84b6e28e5060e517008aa", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
				{"d3ff53e0564a9f87d8e84b6e28e5060e517008aa", "fb72698cab7617ac416264415f13224dfd7a165e"},
				{"d3ff53e0564a9f87d8e84b6e28e5060e517008aa", "c2d30fa8ef288618f65f6eed6e168e0d514886f4"},
				{"d3ff53e0564a9f87d8e84b6e28e5060e517008aa", "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd"},
				{"d3ff53e0564a9f87d8e84b6e28e5060e517008aa", "c2d30fa8ef288618f65f6eed6e168e0d514886f4"},
				{"d3ff53e0564a9f87d8e84b6e28e5060e517008aa", "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
			},
		},
		{
			`SELECT blob_hash, tree_hash
			FROM commit_files
			WHERE tree_hash='eba74343e2f15d62adedfd8c883ee0262b5c8021' AND blob_hash = 'd3ff53e0564a9f87d8e84b6e28e5060e517008aa'`,
			[]sql.Row{
				{"d3ff53e0564a9f87d8e84b6e28e5060e517008aa", "eba74343e2f15d62adedfd8c883ee0262b5c8021"},
			},
		},
		{
			`SELECT commit_hash, tree_hash
			FROM commits
			WHERE commit_author_email='daniel@lordran.local' OR commit_hash='b029517f6300c2da0f4b651b8642506cd6aaf45d'`,
			[]sql.Row{
				{"b029517f6300c2da0f4b651b8642506cd6aaf45d", "aa9b383c260e1d05fbbf6b30a02914555e20c725"},
				{"b8e471f58bcbca63b07bda20e428190409c2db47", "c2d30fa8ef288618f65f6eed6e168e0d514886f4"},
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
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "vendor stuff\n", "Máximo Cuadros Ortiz", ".gitignore"},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "vendor stuff\n", "Máximo Cuadros Ortiz", "CHANGELOG"},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "vendor stuff\n", "Máximo Cuadros Ortiz", "LICENSE"},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "vendor stuff\n", "Máximo Cuadros Ortiz", "binary.jpg"},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "vendor stuff\n", "Máximo Cuadros Ortiz", "go"},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "vendor stuff\n", "Máximo Cuadros Ortiz", "json"},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "vendor stuff\n", "Máximo Cuadros Ortiz", "php"},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "vendor stuff\n", "Máximo Cuadros Ortiz", "vendor"},
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
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "vendor stuff\n", ".gitignore", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "vendor stuff\n", "CHANGELOG", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "vendor stuff\n", "LICENSE", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "vendor stuff\n", "binary.jpg", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "vendor stuff\n", "go", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "vendor stuff\n", "json", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "vendor stuff\n", "php", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "vendor stuff\n", "vendor", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
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
		{
			`
			SELECT language, COUNT(repository_id) AS repository_count
			FROM (
				SELECT DISTINCT r.repository_id, LANGUAGE(t.tree_entry_name, b.blob_content) AS language
				FROM   refs r
		        JOIN commits c ON r.commit_hash = c.commit_hash
		        NATURAL JOIN commit_trees
		        NATURAL JOIN tree_entries t
				NATURAL JOIN blobs b
				WHERE language IS NOT NULL
			) AS q1
			GROUP  BY language
			ORDER  BY repository_count DESC
			`,
			[]sql.Row{
				{"Text", int64(1)},
				{"Ignore List", int64(1)},
			},
		},
		{
			`
			SELECT
		  		repository_id,
		  		COUNT(commit_author_email) as contributor_count
			FROM (
		  		SELECT DISTINCT
		      		repository_id,
		      		commit_author_email
		  		FROM commits
			) as q
			GROUP BY repository_id
			ORDER BY contributor_count DESC
			LIMIT 10
			`,
			[]sql.Row{{"worktree", int64(2)}},
		},
		{
			`SELECT cf.file_path
			FROM refs r
			INNER JOIN commit_files cf
			ON r.commit_hash = cf.commit_hash
				AND r.repository_id = cf.repository_id
			WHERE r.ref_name = 'HEAD' AND IS_VENDOR(cf.file_path)`,
			[]sql.Row{{".gitignore"}, {"vendor/foo.go"}},
		},
		{
			`
			SELECT f.file_path, c.commit_author_when
			FROM repositories r
			NATURAL JOIN commits c
			NATURAL JOIN commit_files cf
			NATURAL JOIN files f
			WHERE r.repository_id = 'worktree'
				AND c.commit_hash = '6ecf0ef2c2dffb796033e5a02219af86ec6584e5'
				AND f.file_path NOT REGEXP '^vendor.*'
				AND NOT IS_BINARY(f.blob_content)
			LIMIT 10
			`,
			[]sql.Row{
				{".gitignore", time.Date(2015, time.April, 5, 21, 30, 47, 0, time.UTC)},
				{"CHANGELOG", time.Date(2015, time.April, 5, 21, 30, 47, 0, time.UTC)},
				{"LICENSE", time.Date(2015, time.April, 5, 21, 30, 47, 0, time.UTC)},
				{"binary.jpg", time.Date(2015, time.April, 5, 21, 30, 47, 0, time.UTC)},
				{"go/example.go", time.Date(2015, time.April, 5, 21, 30, 47, 0, time.UTC)},
				{"json/long.json", time.Date(2015, time.April, 5, 21, 30, 47, 0, time.UTC)},
				{"json/short.json", time.Date(2015, time.April, 5, 21, 30, 47, 0, time.UTC)},
				{"php/crappy.php", time.Date(2015, time.April, 5, 21, 30, 47, 0, time.UTC)},
			},
		},
		{
			`
			SELECT repository_id, commit_author_when,
			CASE WHEN ARRAY_LENGTH(commit_parents) > 1 THEN 'Merge' ELSE 'Commit' END AS commit_type
			FROM commits
			ORDER BY 2
			`,
			[]sql.Row{
				{"worktree", time.Date(2015, time.March, 31, 11, 42, 21, 0, time.UTC), "Commit"},
				{"worktree", time.Date(2015, time.March, 31, 11, 44, 52, 0, time.UTC), "Commit"},
				{"worktree", time.Date(2015, time.March, 31, 11, 46, 24, 0, time.UTC), "Commit"},
				{"worktree", time.Date(2015, time.March, 31, 11, 47, 14, 0, time.UTC), "Merge"},
				{"worktree", time.Date(2015, time.March, 31, 11, 48, 14, 0, time.UTC), "Merge"},
				{"worktree", time.Date(2015, time.March, 31, 11, 51, 51, 0, time.UTC), "Commit"},
				{"worktree", time.Date(2015, time.March, 31, 11, 56, 18, 0, time.UTC), "Commit"},
				{"worktree", time.Date(2015, time.March, 31, 12, 00, 8, 0, time.UTC), "Commit"},
				{"worktree", time.Date(2015, time.April, 5, 21, 30, 47, 0, time.UTC), "Commit"},
			},
		},
		{
			`
			SELECT repo,
			CASE
				WHEN day_index = 2 THEN '1 - Monday'
				WHEN day_index = 3 THEN '2 - Tuesday'
				WHEN day_index = 4 THEN '3 - Wednesday'
				WHEN day_index = 5 THEN '4 - Thursday'
				WHEN day_index = 6 THEN '5 - Friday'
				WHEN day_index = 7 THEN '6 - Saturday'
				ELSE '7 - Sunday'
			END AS day,
			CASE
				WHEN n_parents > 1 THEN 'Merge commit'
				ELSE 'Non-merge commit'
			END AS commit_type
			FROM (
				SELECT
					repository_id AS repo,
					committer_name AS developer,
					DAYOFWEEK(committer_when) AS day_index,
					ARRAY_LENGTH(commit_parents) AS n_parents
				FROM commits
				) t
			`,
			[]sql.Row{
				{"worktree", "7 - Sunday", "Non-merge commit"},
				{"worktree", "2 - Tuesday", "Non-merge commit"},
				{"worktree", "2 - Tuesday", "Non-merge commit"},
				{"worktree", "2 - Tuesday", "Merge commit"},
				{"worktree", "2 - Tuesday", "Non-merge commit"},
				{"worktree", "2 - Tuesday", "Merge commit"},
				{"worktree", "2 - Tuesday", "Non-merge commit"},
				{"worktree", "2 - Tuesday", "Non-merge commit"},
				{"worktree", "2 - Tuesday", "Non-merge commit"},
			},
		},
		{
			`
			SELECT added, deleted, commit_author_when, commit_hash, repository_id,
			CASE WHEN deleted < (added+deleted)*0.1 THEN 'Added' WHEN added < (added+deleted)*0.1 THEN 'Deleted' ELSE 'Changed' END AS commit_type
			FROM (
					SELECT
						JSON_EXTRACT(stats, "$.Total.Additions") as added,
						JSON_EXTRACT(stats, "$.Total.Deletions") as deleted,
						commit_author_when,
						commit_hash,
						repository_id
					FROM (
						SELECT
							repository_id,
							commit_author_when,
							commit_hash,
							commit_stats(repository_id, commit_hash) as stats
						FROM refs
						natural join ref_commits
						natural join commits
						ORDER BY commit_author_when
					) q
				) q2`,
			[]sql.Row{
				{float64(1), float64(0), time.Date(2015, time.March, 31, 12, 00, 8, 0, time.UTC), "e8d3ffab552895c19b9fcf7aa264d277cde33881", "worktree", "Added"},
				{float64(0), float64(0), time.Date(2015, time.April, 5, 21, 30, 47, 0, time.UTC), "6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "worktree", "Changed"},
				{float64(0), float64(0), time.Date(2015, time.April, 5, 21, 30, 47, 0, time.UTC), "6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "worktree", "Changed"},
				{float64(0), float64(0), time.Date(2015, time.April, 5, 21, 30, 47, 0, time.UTC), "6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "worktree", "Changed"},
			},
		},
		{
			`
			SELECT
				JSON_UNQUOTE(JSON_EXTRACT(SPLIT(committer_email, '@'), '$[1]')) as domain,
				COUNT(*) as n
			FROM commits
			WHERE committer_email LIKE '%%@%%' and committer_email NOT LIKE '%%@github.com'
			GROUP BY domain
			ORDER BY n DESC
			`,
			[]sql.Row{
				{"gmail.com", int64(8)},
				{"lordran.local", int64(1)},
			},
		},
		{
			`
			SELECT file_path
			FROM commit_files
			NATURAL JOIN refs
			WHERE ref_name = 'HEAD'
			AND LANGUAGE(file_path) = 'Go';
			`,
			[]sql.Row{
				{"go/example.go"},
				{"vendor/foo.go"},
			},
		},
		{
			`
				SELECT repository_id, JSON_EXTRACT(bl, "$.author[0]"),
					   ARRAY_LENGTH(JSON_EXTRACT(bl, "$.file"))
				FROM (
					SELECT repository_id, BLAME(repository_id, commit_hash) as bl
					FROM commits
					WHERE  commit_hash = '918c48b83bd081e863dbe1b80f8998f058cd8294'
				) as p
			`,
			[]sql.Row{{"worktree", "mcuadros@gmail.com", int32(7235)}},
		},
	}

	var pid uint64
	runTests := func(t *testing.T) {
		for _, tt := range testCases {
			t.Run(tt.query, func(t *testing.T) {
				require := require.New(t)

				session := gitbase.NewSession(pool)
				ctx := sql.NewContext(context.TODO(), sql.WithSession(session), sql.WithPid(pid))
				pid++

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

	var pid uint64
	for _, c := range testCases {
		pid++
		t.Run(c.query, func(t *testing.T) {
			require := require.New(t)

			session := gitbase.NewSession(pool)
			ctx := sql.NewContext(context.TODO(), sql.WithSession(session), sql.WithPid(pid))

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

	squashEngine := newSquashEngine(pool)

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

		`SELECT
			SUBSTRING(repository_id_part, 1, ARRAY_LENGTH(SPLIT(repository_id_part, ''))-4) AS repository_id,
			commit_author_when,
			commit_type
		FROM (
		SELECT
			SUBSTRING(remote_fetch_url, 18) AS repository_id_part,
			commit_author_when,
			CASE ARRAY_LENGTH(commit_parents) WHEN 0 THEN 'Commit' WHEN 1 THEN 'Commit' ELSE 'Merge' END AS commit_type
		FROM remotes
		NATURAL JOIN refs
		NATURAL JOIN commits
		) AS q
		LIMIT 1000`,

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

	cwd, err := os.Getwd()
	require.NoError(err)

	path := filepath.Join(cwd, "_testdata")

	lib, err := siva.NewLibrary("", osfs.New(path), &siva.LibraryOptions{
		RootedRepo: true,
	})
	require.NoError(err)

	pool := gitbase.NewRepositoryPool(cache.NewObjectLRUDefault(), lib)
	engine := newBaseEngine(pool)

	session := gitbase.NewSession(pool)
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))
	_, iter, err := engine.Query(ctx, "SELECT * FROM refs")
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 57)
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

	engine := newBaseEngine(pool)
	squashEngine := newSquashEngine(pool)
	squashIndexEngine := newSquashEngine(pool)

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

	baseEngine := newBaseEngine(pool)
	squashEngine := newSquashEngine(pool)

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

	// create library directory and move repo inside

	path := fixtures.ByTag("worktree").One().Worktree().Root()
	pathLib := path + "-lib"
	pathRepo := filepath.Join(pathLib, "worktree")

	err := os.MkdirAll(pathLib, 0777)
	require.NoError(t, err)

	err = os.Rename(path, pathRepo)
	require.NoError(t, err)

	lib := plain.NewLibrary("plain", nil)
	loc, err := plain.NewLocation("location", osfs.New(pathLib), nil)
	require.NoError(t, err)
	lib.AddLocation(loc)

	pool := gitbase.NewRepositoryPool(cache.NewObjectLRUDefault(), lib)

	cleanup := func() {
		require.NoError(t, fixtures.Clean())
		require.NoError(t, os.RemoveAll(pathLib))
	}

	return newBaseEngine(pool), pool, cleanup
}

func newSquashEngine(pool *gitbase.RepositoryPool) *sqle.Engine {
	engine := newBaseEngine(pool)
	engine.Analyzer = analyzer.NewBuilder(engine.Catalog).
		AddPostAnalyzeRule(rule.SquashJoinsRule, rule.SquashJoins).
		Build()
	return engine
}

func newBaseEngine(pool *gitbase.RepositoryPool) *sqle.Engine {
	foo := gitbase.NewDatabase("foo", pool)
	au := new(auth.None)
	engine := command.NewDatabaseEngine(au, "test", 0, false)

	engine.AddDatabase(foo)
	engine.Catalog.MustRegister(function.Functions...)
	return engine
}

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/fatih/color"
	"github.com/src-d/gitbase"
	"github.com/src-d/gitbase/internal/function"
	"github.com/src-d/gitbase/internal/rule"
	sqle "gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

var suite = benchmarks{
	{
		"Count repositories",
		`
		SELECT COUNT(DISTINCT id) AS repository_count
		FROM repositories`,
	},
	{
		"Last commit messages in HEAD for every repository",
		`
		SELECT c.message
		FROM
			refs r
			JOIN commits c ON r.hash = c.hash
		WHERE
			r.name = 'refs/heads/HEAD';`,
	},
	{
		"All commit messages in HEAD history for every repository",
		`
		SELECT c.message
		FROM
			commits c
			JOIN refs r ON r.hash = c.hash
		WHERE
			r.name = 'refs/heads/HEAD' AND
			history_idx(r.hash, c.hash) >= 0;`,
	},
	{
		"Top 10 repositories by commit count in HEAD",
		`
		SELECT
			repository_id,
			commit_count
		FROM (
			SELECT
				r.repository_id,
				count(*) AS commit_count
			FROM
				refs r
				JOIN commits c ON history_idx(r.hash, c.hash) >= 0
			WHERE
				r.name = 'refs/heads/HEAD'
			GROUP BY r.repository_id
		) AS q
		ORDER BY commit_count DESC
		LIMIT 10;`,
	},
	{
		"Count repository HEADs",
		`
		SELECT
			COUNT(DISTINCT r.repository_id) AS head_count
		FROM
			refs r
		WHERE name = 'refs/heads/HEAD';`,
	},
	{
		"Repository count by language presence (HEAD, no forks)",
		`
		SELECT *
		FROM (
			SELECT
				language,
				COUNT(repository_id) AS repository_count
			FROM (
				SELECT DISTINCT
					r.repository_id AS repository_id,
					language(t.name, b.content) AS language
				FROM
					refs r
					JOIN commits c ON r.hash = c.hash
					JOIN tree_entries t ON commit_has_tree(c.hash, t.tree_hash)
					JOIN blobs b ON t.entry_hash = b.hash
				WHERE
					r.name = 'refs/heads/HEAD'
			) AS q1
			GROUP BY language
		) AS q2
		ORDER BY repository_count DESC;`,
	},
	{
		"Top 10 repositories by contributor count (all branches)",
		`
		SELECT
			repository_id,
			contributor_count
		FROM (
			SELECT
				repository_id,
				COUNT(DISTINCT c.author_email) AS contributor_count
			FROM
				refs r
				JOIN commits c ON history_idx(r.hash, c.hash) >= 0
			GROUP BY repository_id
		) AS q
		ORDER BY contributor_count DESC
		LIMIT 10;`,
	},
	{
		"Created projects per year",
		`
		SELECT
			year,
			COUNT(DISTINCT hash) AS project_count
		FROM (
			SELECT
				hash,
				YEAR(author_date) AS year
			FROM
				refs r
				JOIN commits c ON r.hash = c.hash
			WHERE
				r.name = 'refs/heads/HEAD'
		) AS q
		GROUP BY year
		ORDER BY year DESC;`,
	},
}

func main() {
	path, err := getDatasetPath()
	if err != nil {
		fatal("[ERROR] unable to get dataset path: %s", err)
	}

	pool, err := newPool(path)
	if err != nil {
		fatal("[ERROR] unable to create repository pool: %s", err)
	}

	engine := newEngine()

	suite.run(pool, engine)
}

type benchmark struct {
	name  string
	query string
}

func (b benchmark) run(pool *gitbase.RepositoryPool, e *sqle.Engine) error {
	info("[RUN] %s", b.name)
	info(b.query)
	session := gitbase.NewSession(pool)
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))
	start := time.Now()

	_, iter, err := e.Query(ctx, string(b.query))
	if err != nil {
		return err
	}

	var rows int
	for {
		_, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		rows++
	}

	ok("[PASSED] returned %d row(s) (%v)", rows, time.Since(start))
	return nil
}

type benchmarks []benchmark

func (bm benchmarks) run(pool *gitbase.RepositoryPool, e *sqle.Engine) {
	var failed int

	start := time.Now()
	for _, b := range bm {
		start := time.Now()
		if err := b.run(pool, e); err != nil {
			failed++
			fail("[FAILED] reason: %s (%s)", err, time.Since(start))
		}
	}

	passed := len(bm) - failed

	info(
		"[SUMMARY] %d out of %d tests passed (%v)",
		passed, len(bm), time.Since(start),
	)

	if failed > 0 {
		fatal("[FAILED] finished with errors")
	} else {
		ok("[PASSED] finished without errors")
	}
}

func newPool(path string) (*gitbase.RepositoryPool, error) {
	pool := gitbase.NewRepositoryPool()
	err := pool.AddSivaDir(path)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func newEngine() *sqle.Engine {
	engine := sqle.New()
	engine.AddDatabase(gitbase.NewDatabase("benchmark"))
	engine.Catalog.RegisterFunctions(function.Functions)
	engine.Analyzer.AddRule(rule.SquashJoinsRule, rule.SquashJoins)
	return engine
}

func getDatasetPath() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	return filepath.Join(wd, ".pga", "siva", "latest"), nil
}

func info(msg string, args ...interface{}) {
	fmt.Printf(msg+"\n", args...)
}

func fail(msg string, args ...interface{}) {
	color.Red(msg+"\n", args...)
}

func ok(msg string, args ...interface{}) {
	color.Green(msg+"\n", args...)
}

func fatal(msg string, args ...interface{}) {
	fail(msg, args...)
	os.Exit(1)
}

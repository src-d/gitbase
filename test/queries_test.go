package test

import "gopkg.in/src-d/go-mysql-server.v0/sql"

type query struct {
	name           string
	statement      string
	expectedSchema sql.Schema
	expectedRows   int
	expectedErr    bool
}

var queries []*query = []*query{
	&query{
		name:        "All commits in HEAD's histories",
		statement:   query1,
		expectedErr: false,
	},
	&query{
		name:        "All commits referenced by HEAD",
		statement:   query2,
		expectedErr: false,
	},
	&query{
		name:        "All commits in HEAD's histories (until 4 previous commits)",
		statement:   query3,
		expectedErr: false,
	},
	&query{name: "Number of blobs per commit", statement: query4, expectedErr: false},
	&query{
		name:        "Number of blobs per commit per repository in the history of the commits referenced by master",
		statement:   query5,
		expectedErr: false,
	},
	&query{
		name:        "Number of commits per month per user and pe repo in year 2017",
		statement:   query6,
		expectedErr: false,
	},
	&query{
		name:        "Commits pointed by more than one references",
		statement:   query7,
		expectedErr: false,
	},
	&query{
		name:        "Number of projects created per year",
		statement:   query8,
		expectedErr: false,
	},
	&query{
		name:        "Number of committer per project",
		statement:   query9,
		expectedErr: false,
	},
}

const (
	query1 = `SELECT * FROM commits INNER JOIN refs ON history_idx(refs.name, commits.hash) >= 0 AND refs.name = 'HEAD';`

	query2 = `SELECT * FROM commits INNER JOIN refs ON refs.hash = commits.hash WHERE refs.name = 'HEAD';`

	query3 = `
    SELECT
	refs.repository_id,
	refs.name,
	refs.hash AS ref_hash,
	commits.hash AS commit_hash
    FROM
	commits
    INNER JOIN
	refs
    ON
	history_idx(refs.name, commits.hash) BETWEEN 0 AND 4
    WHERE
	refs.name = 'HEAD';`

	query4 = `
    SELECT
	commits.hash AS commit_hash,
	COUNT(blobs.hash) AS blobs_amount
    FROM
	commits
    INNER JOIN
	blobs
    ON
	commit_contains(commits.hash, blobs.hash)
    GROUP BY
	commits.hash;`

	query5 = `
    SELECT
	refs.repository_id AS repository_id,
	commits.hash AS commit_hash,
	COUNT(blobs.hash) AS blobs_amount
    FROM
	refs
    INNER JOIN
	commits ON history_idx(refs.hash, commits.hash) AND refs.name = 'refs/head/master'
    INNER JOIN
	blobs ON commit_contains(commits.hash, blobs.hash)
    GROUP BY
	refs.repository_id,commits.hash;`

	query6 = `
    SELECT
	refs.repository_id AS repository_id,
	commits.committer_email AS committer,
	commits.hash AS commit_hash,
	COUNT(CASE WHEN month(commits.committer_date) = 1 THEN 1 ELSE NULL END) AS january,
	COUNT(CASE WHEN month(commits.committer_date) = 2 THEN 1 ELSE NULL END) AS february,
	COUNT(CASE WHEN month(commits.committer_date) = 3 THEN 1 ELSE NULL END) AS march,
	COUNT(CASE WHEN month(commits.committer_date) = 4 THEN 1 ELSE NULL END) AS april,
	COUNT(CASE WHEN month(commits.committer_date) = 5 THEN 1 ELSE NULL END) AS may,
	COUNT(CASE WHEN month(commits.committer_date) = 6 THEN 1 ELSE NULL END) AS june,
	COUNT(CASE WHEN month(commits.committer_date) = 7 THEN 1 ELSE NULL END) AS july,
	COUNT(CASE WHEN month(commits.committer_date) = 8 THEN 1 ELSE NULL END) AS august,
	COUNT(CASE WHEN month(commits.committer_date) = 9 THEN 1 ELSE NULL END) AS september,
	COUNT(CASE WHEN month(commits.committer_date) = 10 THEN 1 ELSE NULL END) AS october,
	COUNT(CASE WHEN month(commits.committer_date) = 11 THEN 1 ELSE NULL END) AS november,
	COUNT(CASE WHEN month(commits.committer_date) = 11 THEN 1 ELSE NULL END) AS december
    FROM
	commits
    INNER JOIN
	refs ON history_idx(refs.name, commits.hash) >= 0 AND year(commits.committer_date) = 2017
    GROUP BY
	refs.repository_id, commits.committer_email, commits.hash;`

	query7 = `
    SELECT
	refs.repository_id AS repository_id,
	refs.hash AS commit_hash,
	COUNT(refs.name) AS refs_amount
    FROM
	refs
    GROUP BY
	refs.repository_id, refs.hash
    HAVING
	COUNT(refs.name) > 1;`

	query8 = `
    SELECT
	min(year(commits.committer_date)) AS year,
	COUNT(DISTINCT(refs.repository_id)) AS repos_amount
    FROM
	refs
    INNER JOIN
	commits ON history_idx(refs.hash, commits.hash) AND refs.name = 'refs/head/master'
    GROUP BY
	min(year(commits.committer_date));`

	query9 = `
    SELECT
	refs.repository_id AS repository_id,
	COUNT(DISTINCT(commits.author_name)) AS committers_amount
    FROM
	refs
    INNER JOIN
	commits ON history_idx(refs.hash, commits.hash) AND refs.name = 'refs/head/master'
    GROUP BY
	refs.repository_id
    ORDER BY
	COUNT(DISTINCT(commits.author_name)) DESC;`
)

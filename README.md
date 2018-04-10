# gitbase <a href="https://travis-ci.org/src-d/gitbase"><img alt="Build Status" src="https://travis-ci.org/src-d/gitbase.svg?branch=master" /></a> <a href="https://codecov.io/gh/src-d/gitbase"><img alt="codecov" src="https://codecov.io/gh/src-d/gitbase/branch/master/graph/badge.svg" /></a> <a href="https://godoc.org/gopkg.in/src-d/gitbase.v0"><img alt="GoDoc" src="https://godoc.org/gopkg.in/src-d/gitbase.v0?status.svg" /></a>

Query git repositories with a MySQL interface.

## Installation

Check the [Releases](https://github.com/src-d/gitbase/releases) page to download the gitbase binary.

## Usage

```bash
Usage:
  gitbase [OPTIONS] <server | version>

Help Options:
  -h, --help  Show this help message

Available commands:
  server   Start SQL server.
  version  Show the version information.
```

A MySQL client is needed to connect to the server. For example:

```bash
$ mysql -q -u root -h 127.0.0.1
MySQL [(none)]> SELECT hash, author_email, author_name FROM commits LIMIT 2;
SELECT hash, author_email, author_name FROM commits LIMIT 2;
+------------------------------------------+---------------------+-----------------------+
| hash                                     | author_email        | author_name           |
+------------------------------------------+---------------------+-----------------------+
| 003dc36e0067b25333cb5d3a5ccc31fd028a1c83 | user1@test.io       | Santiago M. Mola      |
| 01ace9e4d144aaeb50eb630fed993375609bcf55 | user2@test.io       | Antonio Navarro Perez |
+------------------------------------------+---------------------+-----------------------+
2 rows in set (0.01 sec)
```

## Tables

You can execute the `SHOW TABLES` statement to get a list of the available tables.
To get all the columns and types of a specific table, you can write `DESCRIBE TABLE [tablename]`.

gitbase exposes the following tables:

|     Name     |                                               Columns                                               |
|:------------:|:---------------------------------------------------------------------------------------------------:|
| repositories |id                                                                                                   |
| remotes      |repository_id, name, push_url,fetch_url,push_refspec,fetch_refspec                                   | 
|    commits   | hash, author_name, author_email, author_when, comitter_name, comitter_email, comitter_when, message, tree_hash |
|     blobs    | hash, size, content                                                                                 |
|  refs        | repository_id, name, hash                                                                           |
| tree_entries | tree_hash, entry_hash, mode, name                                                                   |

## Functions

To make some common tasks easier for the user, there are some functions to interact with the previous mentioned tables:

|     Name     |                                               Description                                           |
|:------------:|:---------------------------------------------------------------------------------------------------:|
|commit_has_blob(commit_hash,blob_hash)bool| get if the specified commit contains the specified blob                 |
|commit_has_tree(commit_hash,tree_hash)bool| get if the specified commit contains the specified tree                 |
|history_idx(start_hash, target_hash)int| get the index of a commit in the history of another commit                 |
|is_remote(reference_name)bool| check if the given reference name is from a remote one                               |
|is_tag(reference_name)bool| check if the given reference name is a tag                                              |

## Unstable features

- **Table squashing:** there is an optimization that collects inner joins between tables with a set of supported conditions and converts them into a single node that retrieves the data in chained steps (getting first the commits and then the blobs of every commit instead of joinin all commits and all blobs, for example). It can be enabled with the environment variable `UNSTABLE_SQUASH_ENABLE`.

## Examples

### Get all the HEAD references from all the repositories
```sql
SELECT * FROM refs WHERE name = 'HEAD'

```

### Commits that appears in more than one reference

```sql
SELECT * FROM (
	SELECT COUNT(c.hash) AS num, c.hash
	FROM refs r
	INNER JOIN commits c
		ON history_idx(r.hash, c.hash) >= 0
	GROUP BY c.hash
) t WHERE num > 1
```

###  Get the number of blobs per HEAD commit
```sql
SELECT COUNT(c.hash), c.hash
FROM refs r
INNER JOIN commits c
	ON r.name = 'HEAD' AND history_idx(r.hash, c.hash) >= 0
INNER JOIN blobs b
	ON commit_has_blob(c.hash, b.hash)
GROUP BY c.hash
```

### Get commits per commiter, per month in 2015

```sql
SELECT COUNT(*) as num_commits, month, repo_id, committer_email
	FROM (
		SELECT
			MONTH(committer_when) as month,
			r.id as repo_id,
			committer_email
		FROM repositories r
		INNER JOIN refs ON refs.repository_id = r.id AND refs.name = 'HEAD'
		INNER JOIN commits c ON YEAR(committer_when) = 2015 AND history_idx(refs.hash, c.hash) >= 0
	) as t
GROUP BY committer_email, month, repo_id
```

## License

gitbase is licensed under the [MIT License](/LICENSE).

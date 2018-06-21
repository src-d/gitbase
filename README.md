![**gitbase** is a SQL database interface to Git repositories.](https://cdn.rawgit.com/src-d/artwork/efe2dd7d/gitbase/files/gitbase-github-readme-header.png)

-**gitbase**, is a SQL database interface to Git repositories.

# gitbase [![GitHub version](https://badge.fury.io/gh/src-d%2Fgitbase.svg)](https://github.com/mcuadros/ofelia/releases) [![Build Status](https://travis-ci.org/src-d/gitbase.svg?branch=master)](https://travis-ci.org/src-d/gitbase) [![codecov](https://codecov.io/gh/src-d/gitbase/branch/master/graph/badge.svg)](https://codecov.io/gh/src-d/gitbase) [![GoDoc](https://godoc.org/gopkg.in/src-d/gitbase.v0?status.svg)](https://godoc.org/gopkg.in/src-d/gitbase.v0) [![Go Report Card](https://goreportcard.com/badge/github.com/src-d/gitbase)](https://goreportcard.com/report/github.com/src-d/gitbase)

It can be used to perform SQL queries about the Git history and
about the [Universal AST](https://doc.bblf.sh/) of the code itself. gitbase is being built to work on top of any number of git repositories.

gitbase implements the *MySQL* wire protocol, it can be accessed using any MySQL
client or library from any language.

## Status

The project is currently in **alpha** stage, meaning it's still lacking performance in a number of cases but we are working hard on getting a performant system able to processes
thousands of repositories in a single node. Stay tuned!

## Examples

To see the SQL subset currently supported take a look at [this list](https://github.com/src-d/go-mysql-server/blob/5620932d8b3ca58edd6bfa4c168073d4c1ff665f/SUPPORTED.md) from [src-d/go-mysql-server](https://github.com/src-d/go-mysql-server).

[src-d/go-mysql-server](https://github.com/src-d/go-mysql-server) is the project where the SQL engine used by ***gitbase*** is implemented.

#### Get all the HEAD references from all the repositories

```sql
SELECT * FROM refs WHERE ref_name = 'HEAD'
```

#### Commits that appears in more than one reference

```sql
SELECT * FROM (
    SELECT COUNT(c.commit_hash) AS num, c.commit_hash
    FROM ref_commits r
    INNER JOIN commits c
        ON r.commit_hash = c.commit_hash
    GROUP BY c.commit_hash
) t WHERE num > 1
```

####  Get the number of blobs per HEAD commit

```sql
SELECT COUNT(c.commit_hash), c.commit_hash
FROM ref_commits as r
INNER JOIN commits c
    ON r.ref_name = 'HEAD' AND r.commit_hash = c.commit_hash
INNER JOIN commit_blobs cb
    ON cb.commit_hash = c.commit_hash
GROUP BY c.commit_hash
```

#### Get commits per commiter, per month in 2015

```sql
SELECT COUNT(*) as num_commits, month, repo_id, committer_email
FROM (
    SELECT
        MONTH(committer_when) as month,
        r.repository_id as repo_id,
        committer_email
    FROM ref_commits r
    INNER JOIN commits c
            ON YEAR(c.committer_when) = 2015 AND r.commit_hash = c.commit_hash
    WHERE r.ref_name = 'HEAD'
) as t
GROUP BY committer_email, month, repo_id
```

## Installation

### Installing from binaries

Check the [Release](https://github.com/src-d/gitbase/releases) page to download the gitbase binary.

### Installing from source

Because gitbase uses [bblfsh's client-go](https://github.com/bblfsh/client-go), which uses cgo, you need to install some dependencies by hand instead of just using `go get`.

_Note_: we use `go get -d` so the code is not compiled yet, as it would
fail before `make dependencies` is executed successfully.

```
go get -d github.com/src-d/gitbase
cd $GOPATH/src/github.com/src-d/gitbase
make dependencies
```

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

You can start a server by providing a path which contains multiple git repositories `/path/to/repositories` with this command:

```
$ gitbase server -v -g /path/to/repositories
```

A MySQL client is needed to connect to the server. For example:

```bash
$ mysql -q -u root -h 127.0.0.1
MySQL [(none)]> SELECT commit_hash, commit_author_email, commit_author_name FROM commits LIMIT 2;
SELECT commit_hash, commit_author_email, commit_author_name FROM commits LIMIT 2;
+------------------------------------------+---------------------+-----------------------+
| commit_hash                              | commit_author_email | commit_author_name    |
+------------------------------------------+---------------------+-----------------------+
| 003dc36e0067b25333cb5d3a5ccc31fd028a1c83 | user1@test.io       | Santiago M. Mola      |
| 01ace9e4d144aaeb50eb630fed993375609bcf55 | user2@test.io       | Antonio Navarro Perez |
+------------------------------------------+---------------------+-----------------------+
2 rows in set (0.01 sec)
```

### Environment variables

| Name                             | Description                                         |
|:---------------------------------|:----------------------------------------------------|
| `BBLFSH_ENDPOINT`                | bblfshd endpoint, default "127.0.0.1:9432"          |
| `GITBASE_BLOBS_MAX_SIZE`         | maximum blob size to return in MiB, default 5 MiB   |
| `GITBASE_BLOBS_ALLOW_BINARY`     | enable retrieval of binary blobs, default `false`   |
| `GITBASE_UNSTABLE_SQUASH_ENABLE` | **UNSTABLE** check *Unstable features*              |
| `GITBASE_SKIP_GIT_ERRORS`        | do not stop queries on git errors, default disabled |

## Tables

You can execute the `SHOW TABLES` statement to get a list of the available tables.
To get all the columns and types of a specific table, you can write `DESCRIBE TABLE [tablename]`.

gitbase exposes the following tables:

|     Name     |                                               Columns                                                             |
|:-------------|:------------------------------------------------------------------------------------------------------------------|
| repositories | repository_id                                                                                                     |
| remotes      | repository_id, remote_name, remote_push_url, remote_fetch_url, remote_push_refspec, remote_fetch_refspec          |
| commits      | repository_id, commit_hash, commit_author_name, commit_author_email, commit_author_when, committer_name, committer_email, committer_when, commit_message, tree_hash |
| blobs        | repository_id, blob_hash, blob_size, blob_content                                                                                               |
| refs         | repository_id, ref_name, commit_hash                                                                                         |
| ref_commits | repository_id, ref_name, commit_hash, index |
| tree_entries | repository_id, tree_hash, blob_hash, tree_entry_mode, tree_entry_name                                                                                 |
| references   | repository_id, ref_name, commit_hash                                                                                         |
| commit_trees | repository_id, commit_hash, tree_hash                                                                                        |
| commit_blobs | repository_id, commit_hash, blob_hash |
| files | repository_id, file_path, blob_hash, tree_hash, tree_entry_mode, blob_content, blob_size |

## Functions

To make some common tasks easier for the user, there are some functions to interact with the previous mentioned tables:

|     Name     |                                               Description                                           |
|:-------------|:----------------------------------------------------------------------------------------------------|
|is_remote(reference_name)bool| check if the given reference name is from a remote one                               |
|is_tag(reference_name)bool| check if the given reference name is a tag                                              |
|language(path, [blob])text| gets the language of a file given its path and the optional content of the file         |
|uast(blob, [lang, [xpath]])json_blob| returns an array of UAST nodes as blobs                                       |
|uast_xpath(json_blob, xpath)| performs an XPath query over the given UAST nodes                                     |

## Unstable features

- **Table squashing:** there is an optimization that collects inner joins between tables with a set of supported conditions and converts them into a single node that retrieves the data in chained steps (getting first the commits and then the blobs of every commit instead of joining all commits and all blobs, for example). It can be enabled with the environment variable `GITBASE_UNSTABLE_SQUASH_ENABLE`.

## License

Apache License Version 2.0, see [LICENSE](LICENSE)

<p align="center"> 
  <img src="https://rawgit.com/sqle/gitql/master/gitql-logo.svg">
</p>

<p align="center"> 

  <a href="https://codebeat.co/projects/github-com-sqle-gitql"><img alt="codebeat badge" src="https://codebeat.co/badges/d797a013-326b-4686-a1a7-adcb3039f65c" /></a>

  <a href="https://travis-ci.org/sqle/gitql"><img alt="Build Status" src="https://travis-ci.org/sqle/gitql.svg?branch=master" /></a>
 
  <a href="https://codecov.io/gh/sqle/gitql"><img alt="codecov" src="https://codecov.io/gh/sqle/gitql/branch/master/graph/badge.svg" /></a>
  
  <a href="https://godoc.org/gopkg.in/sqle/gitql.v0"><img alt="GoDoc" src="https://godoc.org/gopkg.in/sqle/gitql.v0?status.svg" /></a>
</p>

<a href="https://asciinema.org/a/102733?autoplay=1" target="_blank"><img src="https://asciinema.org/a/102733.png" width="979"/></a>

## Installation

Check the [Releases](https://github.com/sqle/gitql/releases) page to download
the gitql binary.

## Usage

```bash
Usage:
  gitql [OPTIONS] <query | shell | version>

Help Options:
  -h, --help  Show this help message

Available commands:
  query    Execute a SQL query a repository.
  shell    Start an interactive session.
  version  Show the version information.
```

For example:

```bash
$ cd my_git_repo
$ gitql query 'SELECT hash, author_email, author_name FROM commits LIMIT 2;' 
SELECT hash, author_email, author_name FROM commits LIMIT 2;
+------------------------------------------+---------------------+-----------------------+
|                   HASH                   |    AUTHOR EMAIL     |      AUTHOR NAME      |
+------------------------------------------+---------------------+-----------------------+
| 003dc36e0067b25333cb5d3a5ccc31fd028a1c83 | user1@test.io       | Santiago M. Mola      |
| 01ace9e4d144aaeb50eb630fed993375609bcf55 | user2@test.io       | Antonio Navarro Perez |
+------------------------------------------+---------------------+-----------------------+
```

You can use the interactive shell like you usually do to explore tables in postgreSQL per example:

```bash
$ gitql shell

           gitQL SHELL
           -----------
You must end your queries with ';'

!> SELECT hash, author_email, author_name FROM commits LIMIT 2;

--> Executing query: SELECT hash, author_email, author_name FROM commits LIMIT 2;

+------------------------------------------+---------------------+-----------------------+
|                   HASH                   |    AUTHOR EMAIL     |      AUTHOR NAME      |
+------------------------------------------+---------------------+-----------------------+
| 003dc36e0067b25333cb5d3a5ccc31fd028a1c83 | user1@test.io       | Santiago M. Mola      |
| 01ace9e4d144aaeb50eb630fed993375609bcf55 | user2@test.io       | Antonio Navarro Perez |
+------------------------------------------+---------------------+-----------------------+
!>  
```

## Tables

gitql exposes the following tables:

|     Name     |                                               Columns                                               |
|:------------:|:---------------------------------------------------------------------------------------------------:|
|    commits   | hash, author_name, author_email, author_time, comitter_name, comitter_email, comitter_time, message |
|     blobs    | hash, size                                                                                          |
|  references  | hash,hash, name, is_branch, is_note, is_remote, is_tag, target                                      |
|     tags     | hash, name, tagger_email, tagger_name, tagger_when, message, target                                 |
| tree_entries | tree_hash, entry_hash, mode, name                                                                   |

## SQL syntax

We are continuously adding more functionality to gitql. We support a subset of the SQL standard, currently including:

|                        |                                     Supported                                     |
|:----------------------:|:---------------------------------------------------------------------------------:|
| Comparison expressions |                                !=, ==, >, <, >=,<=                                |
|  Grouping expressions  |                                    COUNT, FIRST                                   |
|  Standard expressions  |                              ALIAS, LITERAL, STAR (*)                             |
|       Statements       | CROSS JOIN, DESCRIBE, FILTER (WHERE), GROUP BY, LIMIT, SELECT, SHOW TABLES, SORT  |

## License

gitql is licensed under the [MIT License](https://github.com/sqle/gitql/blob/master/LICENSE).

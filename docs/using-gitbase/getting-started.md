# Getting started

## Prerequisites

`gitbase` optional dependencies that should be running on your system if you're planning on using certain functionality.

- [bblfsh](https://github.com/bblfsh/bblfshd) >= 2.14.0 (only if you're planning to use the `UAST` functionality provided in gitbase)

## Installing gitbase

The easiest way to run the gitbase server is using Docker; However, you have the options of using the binary or installing from source.

### Running with Docker

You can use the official image from [docker hub](https://hub.docker.com/r/srcd/gitbase/tags/) to quickly run gitbase:
```
docker run --rm --name gitbase -p 3306:3306 -v /my/git/repos:/opt/repos srcd/gitbase:latest
```

**Note:** remember to replace `/my/git/repos` with the local path where your repositories are stored in your computer.

If you want to use [bblfsh](https://github.com/bblfsh/bblfshd) with running in Docker you can do so by linking the 2 containers.
Fist you need to start following [the bblfsh quick start](https://github.com/bblfsh/bblfshd#quick-start). After that you can run gitbase using:
```
docker run --rm --name gitbase -p 3306:3306 --link bblfshd:bblfshd -e BBLFSH_ENDPOINT=bblfshd:9432 -v /my/git/repos/go:/opt/repos srcd/gitbase:latest
```

### Download and use the binary

Check the [Releases](https://github.com/src-d/gitbase/releases) page to download the gitbase binary.

For more info about command line arguments, [go here](/docs/using-gitbase/configuration.md#command-line-arguments).

You can start a server by providing a path which contains multiple git repositories with this command:

```
gitbase server -v -d /path/to/repositories
```

**Note:** remember to replace `/path/to/repositories` with the local path where your repositories are stored in your computer.

### Installing from source

On Linux and macOS:

```
go get -u github.com/src-d/gitbase/...
```

#### Oniguruma support

On linux and macOS you can choose to build gitbase with oniguruma support, resulting in faster results for queries using the `language` UDF.

macOS:

```
brew install oniguruma
```

Linux:

- Debian-based distros:
```
sudo apt-get install libonig2 libonig-dev
```
- Arch linux:
```
pacman -S oniguruma
```

Then build gitbase like this:

```
go build -tags oniguruma -o gitbase ./cmd/gitbase/main.go
```

**Note:** prebuilt binaries do not include oniguruma support.

On Windows:

Because gitbase uses [bblfsh's client-go](https://github.com/bblfsh/client-go), which uses cgo, you need to install some dependencies by hand instead of just using `go get`. Use this instead:

```
go get -d github.com/src-d/gitbase
cd $GOPATH/src/github.com/src-d/gitbase
make dependencies
```

## Connecting to the server

When the gitbase server is started a MySQL client is needed to connect to the server. For example:

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

If you're using a MySQL client version 8.0 or higher, see the following section to solve some problems you may encounter.

## Troubleshooting

```
ERROR 2012 (HY000): Client asked for auth caching_sha2_password, but server wants auth mysql_native_password
```

As of MySQL 8.0 [the default authentication method is `caching_sha2_password`](https://dev.mysql.com/doc/refman/8.0/en/caching-sha2-pluggable-authentication.html) instead of `mysql_native_password`. You can solve this using the following command instead:

```
mysql -q -u root -h 127.0.0.1 --default-auth=mysql_native_password
```

## Library format specification

By default the directories added to gitbase should contain git repositories and it detects if they are standard or bare format. Each directory added can only contain one type of repository. If you want to specify the format you have two ways to do it:

If all the directories are in the same format you can set it globally with these parameters:

* `--format`: it can be either `git` for filesystem repositories or `siva` for siva archives
* `--bare`: specifies that git archives are bare, can only be used with `git` format
* `--non-bare`: specifies that git archives are standard, can only be used with `git` format
* `--bucket`: sets the number of characters to use for bucketing, used with `siva` libraries
* `--non-rooted`: disables rooted repositories management in `siva` libraries

If you are mixing formats you can specify each directory as a `file://` URL with these parameters:

* `format`: can be `git` or `siva`
* `bare`: `true`, `false` or `auto`
* `bucket`: the characters to use for directory bucketing
* `rooted`: `true` or `false`

For example:

```
-d 'file:///path/to/git?format=git&bare=true' -d 'file:///path/to/sivas?format=siva&rooted=false&bucket=0'
```

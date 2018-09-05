# Getting started

## Prerequisites

`gitbase` has two optional dependencies that should be running on your system if you're planning on using certain functionality.

- [bblfsh](https://github.com/bblfsh/bblfshd) >= 2.6.1 (only if you're planning to use the `UAST` functionality provided in gitbase)
- [pilosa](https://github.com/pilosa/pilosa) 0.9.0 (only if you're planning on using indexes)

## Installing gitbase

The easiest way to run the gitbase server is using Docker, however you have the options of using the binary or installing from source.

### Running with Docker

You can use the official image from [docker hub](https://hub.docker.com/r/srcd/gitbase/tags/) to quickly run gitbase:
```
$ docker run --rm --name gitbase -p 3306:3306 -v /my/git/repos:/opt/repos srcd/gitbase:latest
```

If you want to speedup gitbase using indexes you must run a pilosa container:
```
$ docker run -it --rm --name pilosa -p 10101:10101 pilosa/pilosa:v0.9.0
```

Then link the gitbase container to the pilosa one:
```
$ docker run --rm --name gitbase -p 3306:3306 --link pilosa:pilosa -e PILOSA_ENDPOINT="http://pilosa:10101" -v /my/git/repos:/opt/repos srcd/gitbase:latest
```

### Download and use the binary

Check the [Releases](https://github.com/src-d/gitbase/releases) page to download the gitbase binary.

For more info about command line arguments, [go here](/docs/using-gitbase/configuration.md#command-line-arguments).

You can start a server by providing a path which contains multiple git repositories `/path/to/repositories` with this command:

```
$ gitbase server -v -d /path/to/repositories
```

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

```
sudo apt-get install libonig2
```

Then build gitbase like this:

```
go build -tags oniguruma -o gitbase ./cmd/gitbase/main.go
```

On Windows:

Because gitbase uses [bblfsh's client-go](https://github.com/bblfsh/client-go), which uses cgo, you need to install some dependencies by hand instead of just using `go get`. Use this instead:

```
$ go get -d github.com/src-d/gitbase
$ cd $GOPATH/src/github.com/src-d/gitbase
$ make dependencies
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

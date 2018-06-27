# Getting started

## Prerequisites

**gitbase** has two optional dependencies that should be running on your system if you're planning on using certain functionality.

- [bblfsh](https://github.com/bblfsh/bblfshd) >= 2.5.0 (only if you're planning to use the `UAST` functionality provided in gitbase).
- [pilosa](https://github.com/pilosa/pilosa) 0.9.0 (only if you're planning on using indexes).

## Download an use the binary

Check the [Release](https://github.com/src-d/gitbase/releases) page to download the gitbase binary.

For more info about executable parameters, [go here](/docs/using-gitbase/configuration.md#executable-parameters).

You can start a server by providing a path which contains multiple git repositories `/path/to/repositories` with this command:

```
$ gitbase server -v -g /path/to/repositories -u gitbase
```

## Installing from source

Because gitbase uses [bblfsh's client-go](https://github.com/bblfsh/client-go), which uses cgo, you need to install some dependencies by hand instead of just using `go get`.

_Note_: we use `go get -d` so the code is not compiled yet, as it would
fail before `make dependencies` is executed successfully.

```
go get -d github.com/src-d/gitbase
cd $GOPATH/src/github.com/src-d/gitbase
make dependencies
```

## Running with docker

You can use the official image from [docker hub](https://hub.docker.com/r/srcd/gitbase/tags/) to quickly run gitbase:
```
docker run --rm --name gitbase -p 3306:3306 -v /my/git/repos:/opt/repos srcd/gitbase:latest
```

If you want to speedup gitbase using indexes, you must run a pilosa container:
```
docker run -it --rm --name pilosa -p 10101:10101 pilosa/pilosa:v0.9.0
```

Then link the gitbase container to the pilosa one:
```
docker run --rm --name gitbase -p 3306:3306 --link pilosa:pilosa -e PILOSA_ENDPOINT="http://pilosa:10101" -v /my/git/repos:/opt/repos srcd/gitbase:latest
```

## Connecting to the server

When the server is started, a MySQL client is needed to connect to the server. For example:

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

If gitbase is running in a container from the official image, you must use `gitbase` as user:
```
mysql -q -u gitbase -h 127.0.0.1
```

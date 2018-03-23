# sdk [![Build Status](https://travis-ci.org/bblfsh/sdk.svg?branch=master)](https://travis-ci.org/bblfsh/sdk) [![codecov](https://codecov.io/gh/bblfsh/sdk/branch/master/graph/badge.svg)](https://codecov.io/gh/bblfsh/sdk) [![license](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](https://github.com/bblfsh/sdk/blob/master/LICENSE) [![GitHub release](https://img.shields.io/github/release/bblfsh/sdk.svg)](https://github.com/bblfsh/sdk/releases)

Babelfish SDK contains the tools and libraries
required to create a Babelfish driver for a programming language.

## Build

### Dependencies

The Babelfish SDK has the following dependencies:

* [Docker](https://www.docker.com/get-docker)
* [Go](https://golang.org/dl/)

Make sure that you've correctly set your [GOROOT and
GOPATH](https://golang.org/doc/code.html#Workspaces) environment variables.

### Install

Babelfish SDK gets installed using either Go:

```bash
$ go get -t -v gopkg.in/bblfsh/sdk.v1/...
```

or make command:

```bash
$ make install
```

These commands will install both `bblfsh-sdk` and `bblfsh-sdk-tools` programs
at `$GOPATH/bin/`.

### Contribute

The SDK provides many templates for language drivers.
These templates are converted to Go code that ends up in both `bblfsh-sdk` and `bblfsh-sdk-tools` automatically. Use `make` convert templates to code:

```bash
$ make
go get -v github.com/jteeuwen/go-bindata/...
go get -v golang.org/x/tools/cmd/cover/...
cat protocol/internal/testdriver/main.go | sed -e 's|\([[:space:]]\+\).*//REPLACE:\(.*\)|\1\2|g' \
	> etc/skeleton/driver/main.go.tpl
chmod -R go=r ${GOPATH}/src/github.com/bblfsh/sdk/etc/build; \
go-bindata \
	-pkg build \
	-modtime 1 \
	-nocompress \
	-prefix ${GOPATH}/src/github.com/bblfsh/sdk/etc/build \
	-o assets/build/bindata.go \
	${GOPATH}/src/github.com/bblfsh/sdk/etc/build/...
chmod -R go=r ${GOPATH}/src/github.com/bblfsh/sdk/etc/skeleton; \
go-bindata \
	-pkg skeleton \
	-modtime 1 \
	-nocompress \
	-prefix ${GOPATH}/src/github.com/bblfsh/sdk/etc/skeleton \
	-o assets/skeleton/bindata.go \
	${GOPATH}/src/github.com/bblfsh/sdk/etc/skeleton/...
```

You can validate this process has been properly done before submitting changes:

```bash
$ make validate-commit
```

If the code has not been properly generated,
this command will show a diff of the changes that have not been processed
and will end up with a message like:

```
generated bindata is out of sync
make: *** [Makefile:66: validate-commit] Error 2
```

Review the process if this happens.

On the other hand, If you need to regenerate *[proto](https://developers.google.com/protocol-buffers/)*  and *[proteus](https://github.com/src-d/proteus)* files, you must run `go generate` from *protocol/* directory:

```bash
$ cd protocol/
$ go generate
```

It regenerates all *[proto](https://developers.google.com/protocol-buffers/)* and *[proteus](https://github.com/src-d/proteus)* files under *[protocol/](https://github.com/bblfsh/sdk/tree/master/protocol)* and *[uast/](https://github.com/bblfsh/sdk/tree/master/uast)* directories.

## Usage

Babelfish SDK helps both setting up the initial structure of a new driver
and keeping that structure up to date.

### Creating the driver's initial structure

Let's say we're creating a driver for `mylang`. The first step is initializing a git
repository for the driver:

```bash
$ cd $GOPATH/src/github.com/bblfsh
$ git init mylang-driver
$ cd mylang-driver
```

Now the driver should be bootstrapped with `bblfsh-sdk`. This will create some
directories and files required by every driver. They will be overwritten if they
exist, like the README.md file in the example below.

```bash
$ bblfsh-sdk init mylang alpine
initializing driver "mylang", creating new manifest
creating file "manifest.toml"
creating file "Makefile"
creating file "driver/main.go"
creating file "driver/normalizer/normalizer.go"
creating file ".git/hooks/pre-commit"
creating file ".gitignore"
creating file ".travis.yml"
creating file "Dockerfile.build.tpl"
creating file "driver/normalizer/normalizer_test.go"
creating file "Dockerfile.tpl"
creating file "LICENSE"
managed file "README.md" has changed, discarding changes
$ git add -A
$ git commit -m 'initialize repository'
```

Note that this adds a pre-commit [git
hook](https://git-scm.com/book/en/v2/Customizing-Git-Git-Hooks), which will verify
these files are up to date before every commit and will disallow commits if some
of the managed files are changed. You can by-pass this with `git commit
--no-verify`.

You can find the driver skeleton used here at [`etc/sekeleton`](etc/skeleton).

### Keeping managed files updated

Whenever the managed files are updated, drivers need to update them.
`bblfsh-sdk` can be used to perform some of this updates in managed files.
For example, if the README template is updated,
running `bblfsh-sdk update` will overwrite it.

```bash
$ bblfsh-sdk update
managed file "README.md" has changed, discarding changes
```

`bblfsh-sdk` doesn't update the SDK itself.

For further details of how to construct a language driver,
take a look at [Implementing the driver](https://doc.bblf.sh/driver/sdk.html#implementing-the-driver)
section in documentation.


## License

GPLv3, see [LICENSE](LICENSE)


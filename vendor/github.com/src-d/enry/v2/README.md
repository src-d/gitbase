# enry [![GoDoc](https://godoc.org/github.com/src-d/enry?status.svg)](https://godoc.org/github.com/src-d/enry) [![Build Status](https://travis-ci.com/src-d/enry.svg?branch=master)](https://travis-ci.com/src-d/enry) [![codecov](https://codecov.io/gh/src-d/enry/branch/master/graph/badge.svg)](https://codecov.io/gh/src-d/enry)

File programming language detector and toolbox to ignore binary or vendored files. *enry*, started as a port to _Go_ of the original [linguist](https://github.com/github/linguist) _Ruby_ library, that has an improved *2x performance*.


Installation
------------

The recommended way to install enry is

```
go get github.com/src-d/enry/cmd/enry
```

To build enry's CLI you must run

    make build

this will generate a binary in the project's root directory called `enry`. You can then move this binary to anywhere in your `PATH`.

This project is now part of [source{d} Engine](https://sourced.tech/engine),
which provides the simplest way to get started with a single command.
Visit [sourced.tech/engine](https://sourced.tech/engine) for more information.

### Faster regexp engine (optional)

[Oniguruma](https://github.com/kkos/oniguruma) is CRuby's regular expression engine.
It is very fast and performs better than the one built into Go runtime. *enry* supports swapping
between those two engines thanks to [rubex](https://github.com/moovweb/rubex) project.
The typical overall speedup from using Oniguruma is 1.5-2x. However, it requires CGo and the external shared library.
On macOS with brew, it is

```
brew install oniguruma
```

On Ubuntu, it is

```
sudo apt install libonig-dev
```

To build enry with Oniguruma regexps use the `oniguruma` build tag

```
go get -v -t --tags oniguruma ./...
```

and then rebuild the project.

Examples
------------

```go
lang, safe := enry.GetLanguageByExtension("foo.go")
fmt.Println(lang, safe)
// result: Go true

lang, safe := enry.GetLanguageByContent("foo.m", []byte("<matlab-code>"))
fmt.Println(lang, safe)
// result: Matlab true

lang, safe := enry.GetLanguageByContent("bar.m", []byte("<objective-c-code>"))
fmt.Println(lang, safe)
// result: Objective-C true

// all strategies together
lang := enry.GetLanguage("foo.cpp", []byte("<cpp-code>"))
// result: C++ true
```

Note that the returned boolean value `safe` is set either to `true`, if there is only one possible language detected, or to `false` otherwise.

To get a list of possible languages for a given file, you can use the plural version of the detecting functions.

```go
langs := enry.GetLanguages("foo.h",  []byte("<cpp-code>"))
// result: []string{"C", "C++", "Objective-C}

langs := enry.GetLanguagesByExtension("foo.asc", []byte("<content>"), nil)
// result: []string{"AGS Script", "AsciiDoc", "Public Key"}

langs := enry.GetLanguagesByFilename("Gemfile", []byte("<content>"), []string{})
// result: []string{"Ruby"}
```


CLI
------------

You can use enry as a command,

```bash
$ enry --help
  enry v1.5.0 build: 10-02-2017_14_01_07 commit: 95ef0a6cf3, based on linguist commit: 37979b2
  enry, A simple (and faster) implementation of github/linguist
  usage: enry <path>
         enry [-json] [-breakdown] <path>
         enry [-json] [-breakdown]
         enry [-version]
```

and it'll return an output similar to *linguist*'s output,

```bash
$ enry
55.56%    Shell
22.22%    Ruby
11.11%    Gnuplot
11.11%    Go
```

but not only the output; its flags are also the same as *linguist*'s ones,

```bash
$ enry --breakdown
55.56%    Shell
22.22%    Ruby
11.11%    Gnuplot
11.11%    Go

Gnuplot
plot-histogram.gp

Ruby
linguist-samples.rb
linguist-total.rb

Shell
parse.sh
plot-histogram.sh
run-benchmark.sh
run-slow-benchmark.sh
run.sh

Go
parser/main.go
```

even the JSON flag,

```bash
$ enry --json
{"Gnuplot":["plot-histogram.gp"],"Go":["parser/main.go"],"Ruby":["linguist-samples.rb","linguist-total.rb"],"Shell":["parse.sh","plot-histogram.sh","run-benchmark.sh","run-slow-benchmark.sh","run.sh"]}
```

Note that even if enry's CLI is compatible with linguist's, its main point is that **_enry doesn't need a git repository to work!_**

Java bindings
------------

Generated Java bindings using a C-shared library and JNI are located under [`java`](https://github.com/src-d/enry/blob/master/java)

Development
------------

*enry* re-uses parts of original [linguist](https://github.com/github/linguist) to generate internal data structures. In order to update to the latest upstream and generate all the necessary code you must run:

    git clone https://github.com/github/linguist.git .linguist
    # update commit in generator_test.go (to re-generate .gold fixtures)
    # https://github.com/src-d/enry/blob/13d3d66d37a87f23a013246a1b0678c9ee3d524b/internal/code-generator/generator/generator_test.go#L18
    go generate

We update enry when changes are done in linguist's master branch on the following files:

* [languages.yml](https://github.com/github/linguist/blob/master/lib/linguist/languages.yml)
* [heuristics.yml](https://github.com/github/linguist/blob/master/lib/linguist/heuristics.yml)
* [vendor.yml](https://github.com/github/linguist/blob/master/lib/linguist/vendor.yml)
* [documentation.yml](https://github.com/github/linguist/blob/master/lib/linguist/documentation.yml)

Currently we don't have any procedure established to automatically detect changes in the linguist project and regenerate the code.
So we update the generated code as needed, without any specific criteria.

If you want to update *enry* because of changes in linguist, you can run the *go
generate* command and do a pull request that only contains the changes in
generated files (those files in the subdirectory [data](https://github.com/src-d/enry/blob/master/data)).

To run the tests,

    make test


Divergences from linguist
------------

`enry` [CLI tool](#cli) does *not* require a full Git repository to be present in the filesystem in order to report languages.

Using [linguist/samples](https://github.com/github/linguist/tree/master/samples)
as a set for the tests, the following issues were found:

* [Heuristics for ".es" extension](https://github.com/github/linguist/blob/e761f9b013e5b61161481fcb898b59721ee40e3d/lib/linguist/heuristics.yml#L103) in JavaScript could not be parsed, due to unsupported backreference in RE2 regexp engine

* As of (Linguist v5.3.2)[https://github.com/github/linguist/releases/tag/v5.3.2] it is using [flex-based scanner in C for tokenization](https://github.com/github/linguist/pull/3846). Enry still uses [extract_token](https://github.com/github/linguist/pull/3846/files#diff-d5179df0b71620e3fac4535cd1368d15L60) regex-based algorithm. See [#193](https://github.com/src-d/enry/issues/193).

* Bayesian classifier can't distinguish "SQL" from "PLpgSQL. See [#194](https://github.com/src-d/enry/issues/194).

* Detection of [generated files](https://github.com/github/linguist/blob/bf95666fc15e49d556f2def4d0a85338423c25f3/lib/linguist/generated.rb#L53) is not supported yet.
 (Thus they are not excluded from CLI output). See [#213](https://github.com/src-d/enry/issues/213).

* XML detection strategy is not implemented. See [#192](https://github.com/src-d/enry/issues/192).

* Overriding languages and types though `.gitattributes` is not yet supported. See [#18](https://github.com/src-d/enry/issues/18).

* `enry` CLI output does NOT exclude `.gitignore`ed files and git submodules, as linguist does

In all the cases above that have an issue number - we plan to update enry to match Linguist behaviour.


Benchmarks
------------

Enry's language detection has been compared with Linguist's one. In order to do that, Linguist's project directory [*linguist/samples*](https://github.com/github/linguist/tree/master/samples) was used as a set of files to run benchmarks against.

We got these results:

![histogram](benchmarks/histogram/distribution.png)

The histogram represents the number of files for which spent time in language
detection was in the range of the time interval indicated in the x axis.

So you can see that most of the files were detected quicker in enry.

We found some few cases where enry turns slower than linguist. This is due to
Golang's regexp engine being slower than Ruby's, which uses the [oniguruma](https://github.com/kkos/oniguruma) library, written in C.

You can find scripts and additional information (like software and hardware used
and benchmarks' results per sample file) in [*benchmarks*](https://github.com/src-d/enry/blob/master/benchmarks) directory.


### Benchmark Dependencies
As benchmarks depend on Ruby and Github-Linguist gem make sure you have:
 - Ruby (e.g using [`rbenv`](https://github.com/rbenv/rbenv)), [`bundler`](https://bundler.io/) installed
 - Docker
 - [native dependencies](https://github.com/github/linguist/#dependencies) installed
 - Build the gem `cd .linguist && bundle install && rake build_gem && cd -`
 - Install it `gem install --no-rdoc --no-ri --local .linguist/github-linguist-*.gem`


### How to reproduce current results

If you want to reproduce the same benchmarks as reported above:
 - Make sure all [dependencies](#benchmark-dependencies) are installed
 - Install [gnuplot](http://gnuplot.info) (in order to plot the histogram)
 - Run `ENRY_TEST_REPO="$PWD/.linguist" benchmarks/run.sh` (takes ~15h)

It will run the benchmarks for enry and linguist, parse the output, create csv files and plot the histogram. This takes some time.

### Quick
To run quicker benchmarks you can either:

    make benchmarks

to get average times for the main detection function and strategies for the whole samples set or:

    make benchmarks-samples

if you want to see measures per sample file.


Why Enry?
------------

In the movie [My Fair Lady](https://en.wikipedia.org/wiki/My_Fair_Lady), [Professor Henry Higgins](http://www.imdb.com/character/ch0011719/?ref_=tt_cl_t2) is one of the main characters. Henry is a linguist and at the very beginning of the movie enjoys guessing the origin of people based on their accent.

`Enry Iggins` is how [Eliza Doolittle](http://www.imdb.com/character/ch0011720/?ref_=tt_cl_t1), [pronounces](https://www.youtube.com/watch?v=pwNKyTktDIE) the name of the Professor during the first half of the movie.


License
------------

Apache License, Version 2.0. See [LICENSE](LICENSE)

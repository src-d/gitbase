# Contributing

Please check our [Contributor's Guidelines](https://github.com/pilosa/pilosa/CONTRIBUTING.md).

1. Fork this repo and add it as upstream: `git remote add upstream git@github.com:pilosa/go-pilosa.git`.
2. Make sure all tests pass (use `make test-all`) and be sure that the tests cover all statements in your code (we aim for 100% test coverage).
3. Commit your code to a feature branch and send a pull request to the `master` branch of our repo.

## Running tests

You can run unit tests with:
```
make test
```

And both unit and integration tests with:
```
make test-all
```

Check the test coverage:
```
make cover
```

## Generating protobuf classes

Protobuf classes are already checked in to source control, so this step is only needed when the upstream `public.proto` changes.

Before running the following step, make sure you have the [Protobuf compiler](https://github.com/google/protobuf) and [Go protobuf support](https://github.com/golang/protobuf)  is installed:

```
make generate
```

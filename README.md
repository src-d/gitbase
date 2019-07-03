# Introduction

**gitbase**, is a SQL database interface to Git repositories.

This project is now part of [source{d} Community Edition](https://sourced.tech/products/community-edition/), which provides the simplest way to get started with a single command. Visit [https://docs.sourced.tech/community-edition](https://docs.sourced.tech/community-edition) for more information.

It can be used to perform SQL queries about the Git history and about the [Universal AST](https://doc.bblf.sh/) of the code itself. gitbase is being built to work on top of any number of git repositories.

gitbase implements the _MySQL_ wire protocol, it can be accessed using any MySQL client or library from any language.

[src-d/go-mysql-server](https://github.com/src-d/go-mysql-server) is the SQL engine implementation used by `gitbase`.

## Status

The project is currently in **alpha** stage, meaning it's still lacking performance in a number of cases but we are working hard on getting a performant system able to process thousands of repositories in a single node. Stay tuned!

## Examples

You can see some [query examples](using-gitbase/examples.md) in [gitbase documentation](https://github.com/src-d/gitbase/tree/2fb1fcd137eff9da63125f60323c3456661c928e/docs/README.md).

## Motivation and scope

gitbase was born to ease the analysis of git repositories and their source code.

Also, making it MySQL compatible, we provide the maximum compatibility between languages and existing tools.

It comes as a single self-contained binary and it can be used as a standalone service. The service is able to process local repositories and integrates with existing tools and frameworks to simplify source code analysis on a large scale. The integration with Apache Spark is planned and is currently under active development.

## Further reading

From here, you can directly go to [getting started](using-gitbase/getting-started.md).

## License

Apache License Version 2.0, see [LICENSE](https://github.com/src-d/gitbase/tree/2fb1fcd137eff9da63125f60323c3456661c928e/LICENSE/README.md)


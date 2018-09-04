# Introduction

**gitbase**, is a SQL database interface to Git repositories.

It can be used to perform SQL queries about the Git history and about the [Universal AST](https://doc.bblf.sh/) of the code itself. gitbase is being built to work on top of any number of git repositories.

gitbase implements the _MySQL_ wire protocol, it can be accessed using any MySQL client or library from any language.

[src-d/go-mysql-server](https://github.com/src-d/go-mysql-server) is the SQL engine implementation used by `gitbase`.

## Status

The project is currently in **alpha** stage, meaning it's still lacking performance in a number of cases but we are working hard on getting a performant system able to process thousands of repositories in a single node. Stay tuned!

## Examples

You can see some [query examples](using-gitbase/examples.md) in [gitbase documentation](https://github.com/src-d/gitbase/tree/f68dd7f644c24e3acba490a20c6c735b7770c54b/docs/README.md).

## Motivation and scope

gitbase was born to ease the analysis of git repositories and their source code.

Also, making it MySQL compatible, we provide the maximum compatibility between languages and existing tools.

It comes as a single self-contained binary and it can be used as a standalone service. The service is able to process local repositories or integrate with existing tools and frameworks \(e.g. Apache Spark\) to make source code analysis on a large scale.

## Further reading

From here, you can directly go to [getting started](using-gitbase/getting-started.md).

## License

Apache License Version 2.0, see [LICENSE](https://github.com/src-d/gitbase/tree/f68dd7f644c24e3acba490a20c6c735b7770c54b/LICENSE/README.md)


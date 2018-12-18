## Supported languages

Gitbase supports many programming languages depends on the use case.
For instance the `language(path, [blob])` function supports all languages which [enry's package](https://github.com/src-d/enry) can autodetect.
More details about aliases, groups, extensions, etc. you can find in [enry's repo](https://github.com/src-d/enry/blob/master/data/alias.go),
or go directly to [linguist defines](https://github.com/github/linguist/blob/master/lib/linguist/languages.yml).

If your use case requires _Universal Abstract Syntax Tree_ then most likely one of the following functions will be interesting for you:
- `uast(blob, [lang, [xpath]])`
- `uast_mode(mode, blob, lang)`
- `uast_xpath(blob, xpath)`
- `uast_extract(blob, key)`
- `uast_children(blob)`

The _UAST_ functions support programming languages which already have implemented [babelfish](https://docs.sourced.tech/babelfish) driver.
The list of currently supported languages on babelfish, you can find [here](https://docs.sourced.tech/babelfish/languages#supported-languages).
Drivers which are still in development can be find [here](https://docs.sourced.tech/babelfish/languages#in-development).

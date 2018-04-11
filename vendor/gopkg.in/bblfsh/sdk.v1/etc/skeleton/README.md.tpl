# {{.Manifest.Language}}-driver  ![Driver Status](https://img.shields.io/badge/status-{{.Manifest.Status | escape_shield}}-{{template "color-status" .}}.svg) [![Build Status](https://travis-ci.org/bblfsh/{{.Manifest.Language}}-driver.svg?branch=master)](https://travis-ci.org/bblfsh/{{.Manifest.Language | escape_shield }}-driver) ![Native Version](https://img.shields.io/badge/{{.Manifest.Language}}%20version-{{.Manifest.Runtime.NativeVersion | escape_shield}}-aa93ea.svg) ![Go Version](https://img.shields.io/badge/go%20version-{{.Manifest.Runtime.GoVersion | escape_shield}}-63afbf.svg)

{{.Manifest.Documentation.Description}}

{{if .Manifest.Documentation.Caveats -}}
Caveats
-------

{{.Manifest.Documentation.Caveats}}
{{end -}}


Development Environment
-----------------------

Requirements:
- `docker`
- [`bblfsh-sdk`](https://github.com/bblfsh/sdk) _(go get -u gopkg.in/bblfsh/sdk.v1/...)_
- UAST converter dependencies _(go get -t -v ./...)_

To initialize the build system execute: `bblfsh-sdk prepare-build`, at the root of the project. This will install the SDK at `.sdk` for this driver.

To execute the tests just execute `make test`, this will execute the test over the native and the go components of the driver. Use `make test-native` to run the test only over the native component or `make test-driver` to run the test just over the go component.

The build is done executing `make build`. To evaluate the result using a docker container, execute:
`docker run -it bblfsh/{{.Manifest.Language}}-driver:dev-<commit[:7]>-dirty`


License
-------

GPLv3, see [LICENSE](LICENSE)


{{define "color-status" -}}
{{if eq .Manifest.Status "planning" -}}
e08dd1
{{- else if eq .Manifest.Status "pre-alpha" -}}
d6ae86
{{- else if eq .Manifest.Status "alpha" -}}
db975c
{{- else if eq .Manifest.Status "beta" -}}
dbd25c
{{- else if eq .Manifest.Status "stable" -}}
9ddb5c
{{- else if eq .Manifest.Status "mature" -}}
60db5c
{{- else -}}
d1d1d1
{{- end}}
{{- end}}

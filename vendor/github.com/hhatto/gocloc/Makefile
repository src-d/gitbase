.PHONY: test build

build:
	mkdir -p bin
	GO111MODULE=on go build -o ./bin/gocloc cmd/gocloc/main.go

update-package:
	GO111MODULE=on go get -u github.com/hhatto/gocloc

run-example:
	GO111MODULE=on go run examples/languages.go
	GO111MODULE=on go run examples/files.go

test:
	GO111MODULE=on go test -v

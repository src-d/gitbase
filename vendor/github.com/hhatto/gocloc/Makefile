.PHONY: test build

build:
	go build
	mkdir -p bin
	go build -o ./bin/gocloc cmd/gocloc/main.go

update-vendor:
	dep ensure

update-package:
	go get -u github.com/hhatto/gocloc

run-example:
	go run examples/languages.go
	go run examples/files.go

test:
	go test -v

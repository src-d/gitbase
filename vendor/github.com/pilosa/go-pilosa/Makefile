
.PHONY: all cover fast-cover generate-proto test test-all

VERSION := $(shell git describe --tags 2> /dev/null || echo unknown)

all: test

cover:
	go test -cover -tags="integration fullcoverage"

fast-cover:
	go test -cover -tags="integration"

generate:
	protoc --go_out=. gopilosa_pbuf/public.proto

test:
	go test

test-all:
	go test -tags=integration -v

test-all-race:
	go test -race -tags=integration -v

release:
	printf "package pilosa\nconst Version = \"$(VERSION)\"" > version.go
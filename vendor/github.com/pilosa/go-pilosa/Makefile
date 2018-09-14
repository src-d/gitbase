
.PHONY: all cover fast-cover generate-proto test test-all

VERSION := $(shell git describe --tags 2> /dev/null || echo unknown)

all: test

cover:
	go test -cover -tags="integration fullcoverage" $(TESTFLAGS)

fast-cover:
	go test -cover -tags="integration" $(TESTFLAGS)

generate:
	# This ensures that we don't forget to change the package name if we copy the proto definition from pilosa in order to update it.
	# It is important that the packages have different names because proto can misbehave if pilosa and go-pilosa are imported into the same codebase.
	sed -i -e 's/package internal;/package gopilosa_pbuf;/g' gopilosa_pbuf/public.proto
	protoc --go_out=. gopilosa_pbuf/public.proto

test:
	go test $(TESTFLAGS)

test-all:
	go test -tags=integration $(TESTFLAGS)

test-all-race:
	go test -race -tags=integration $(TESTFLAGS)

release:
	printf "package pilosa\nconst Version = \"$(VERSION)\"" > version.go

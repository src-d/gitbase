package integration

import (
	// we ensure the download of test dependencies even if the package was
	// installed without "go get -t ..."
	_ "github.com/pmezard/go-difflib/difflib"
	_ "github.com/stretchr/testify/require"
	_ "google.golang.org/grpc"
)

const (
	Endpoint   = "SERVER_ENDPOINT"
	Language   = "LANGUAGE"
	DriverPath = "DRIVER_PATH"
)

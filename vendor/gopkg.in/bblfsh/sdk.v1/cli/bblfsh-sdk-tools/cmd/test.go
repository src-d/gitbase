package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"gopkg.in/bblfsh/sdk.v1/sdk/driver/integration"
)

const (
	IntegrationPackage     = "gopkg.in/bblfsh/sdk.v1/sdk/driver/integration"
	TestCommandDescription = "" +
		"test runs all the integration tests for a given driver"
)

type TestCommand struct {
	manifestCommand
	Endpoint string `long:"endpoint" default:"localhost:9432" description:"grpc endpoint to run the test against."`
}

func (c *TestCommand) Execute(args []string) error {
	if err := c.manifestCommand.Execute(args); err != nil {
		return err
	}

	path, err := filepath.Abs(c.Root)
	if err != nil {
		return err
	}

	cmd := exec.Command("go", "test", "-v", IntegrationPackage)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("%s=%s", integration.Endpoint, c.Endpoint),
		fmt.Sprintf("%s=%s", integration.DriverPath, path),
		fmt.Sprintf("%s=%s", integration.Language, c.Manifest.Language),
	)

	return cmd.Run()
}

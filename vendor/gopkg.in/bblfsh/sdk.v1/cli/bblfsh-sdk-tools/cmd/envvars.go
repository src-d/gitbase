package cmd

import (
	"fmt"
	"strings"

	"gopkg.in/bblfsh/sdk.v1/manifest"
)

const EnvVarsCommandDescription = "" +
	"prints the manifest as a list of variables ready to be evaluated by bash or make"

type EnvVarsCommand struct {
	manifestCommand
}

func (c *EnvVarsCommand) Execute(args []string) error {
	if err := c.manifestCommand.Execute(args); err != nil {
		return err
	}

	c.processManifest(c.Manifest)
	return nil
}

func (c *EnvVarsCommand) processManifest(m *manifest.Manifest) {
	c.processValue("LANGUAGE", m.Language)
	c.processValue("RUNTIME_OS", string(m.Runtime.OS))

	nv := strings.Join(m.Runtime.NativeVersion, ":")
	c.processValue("RUNTIME_NATIVE_VERSION", nv)
	c.processValue("RUNTIME_GO_VERSION", m.Runtime.GoVersion)
}

func (c *EnvVarsCommand) processValue(key, value string) {
	fmt.Printf("%s=%s\n", key, value)
}

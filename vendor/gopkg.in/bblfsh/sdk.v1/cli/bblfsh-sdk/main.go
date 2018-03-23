package main

import (
	"fmt"
	"os"

	"gopkg.in/bblfsh/sdk.v1/cli/bblfsh-sdk/cmd"

	"github.com/jessevdk/go-flags"
)

var version string
var build string

func main() {
	parser := flags.NewNamedParser("bblfsh-sdk", flags.Default)
	parser.AddCommand("prepare-build", cmd.PrepareBuildCommandDescription, "", &cmd.PrepareBuildCommand{})
	parser.AddCommand("update", cmd.UpdateCommandDescription, "", &cmd.UpdateCommand{})
	parser.AddCommand("init", cmd.InitCommandDescription, "", &cmd.InitCommand{})

	if _, err := parser.Parse(); err != nil {
		if _, ok := err.(*flags.Error); ok {
			parser.WriteHelp(os.Stdout)
			fmt.Printf("\nBuild information\n  commit: %s\n  date:%s\n", version, build)
		}

		os.Exit(1)
	}
}

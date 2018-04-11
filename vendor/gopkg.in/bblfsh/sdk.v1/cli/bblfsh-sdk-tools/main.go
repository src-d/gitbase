package main

import (
	"fmt"
	"os"

	"gopkg.in/bblfsh/sdk.v1/cli/bblfsh-sdk-tools/cmd"

	"github.com/jessevdk/go-flags"
)

var version string
var build string

func main() {
	parser := flags.NewNamedParser("bblfsh-sdk-tools", flags.Default)
	parser.AddCommand("envvars", cmd.EnvVarsCommandDescription, "", &cmd.EnvVarsCommand{})
	parser.AddCommand("build", cmd.BuildCommandDescription, "", &cmd.BuildCommand{})
	parser.AddCommand("fixtures", cmd.FixturesCommandDescription, "", &cmd.FixturesCommand{})
	parser.AddCommand("test", cmd.TestCommandDescription, "", &cmd.TestCommand{})
	parser.AddCommand("ast2gv", cmd.Ast2GraphvizCommandDescription, "", &cmd.Ast2GraphvizCommand{})

	if _, err := parser.Parse(); err != nil {
		if _, ok := err.(*flags.Error); ok {
			parser.WriteHelp(os.Stdout)
			fmt.Printf("\nBuild information\n  commit: %s\n  date:%s\n", version, build)
		}

		os.Exit(1)
	}
}

package main

import (
	"fmt"
	"os"

	"gopkg.in/bblfsh/sdk.v2/cmd/bblfsh-sdk/cmd"

	"github.com/jessevdk/go-flags"
)

var version string
var build string

func main() {
	parser := flags.NewNamedParser("bblfsh-sdk", flags.Default)
	parser.AddCommand("update", cmd.UpdateCommandDescription, "", &cmd.UpdateCommand{})
	parser.AddCommand("init", cmd.InitCommandDescription, "", &cmd.InitCommand{})
	parser.AddCommand("build", cmd.BuildCommandDescription, "", &cmd.BuildCommand{})
	parser.AddCommand("test", cmd.TestCommandDescription, "", &cmd.TestCommand{})
	parser.AddCommand("tag", cmd.TagCommandDescription, "", &cmd.TagCommand{})
	parser.AddCommand("release", cmd.ReleaseCommandDescription, "", &cmd.ReleaseCommand{})
	parser.AddCommand("push", cmd.PushCommandDescription, "", &cmd.PushCommand{})
	parser.AddCommand("fixtures", cmd.FixturesCommandDescription, "", &cmd.FixturesCommand{})
	parser.AddCommand("ast2gv", cmd.Ast2GraphvizCommandDescription, "", &cmd.Ast2GraphvizCommand{})

	if _, err := parser.Parse(); err != nil {
		if _, ok := err.(*flags.Error); ok {
			parser.WriteHelp(os.Stdout)
			fmt.Printf("\nBuild information\n  commit: %s\n  date:%s\n", version, build)
		}

		os.Exit(1)
	}
}

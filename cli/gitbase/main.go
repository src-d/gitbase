package main

import (
	"os"

	"github.com/src-d/gitbase/cli/gitbase/cmd"

	"github.com/jessevdk/go-flags"
)

const (
	name = "gitbase"
)

func main() {
	parser := flags.NewNamedParser(name, flags.Default)

	parser.AddCommand("server", cmd.ServerDescription, cmd.ServerHelp, &cmd.Server{
		UnstableSquash: os.Getenv("UNSTABLE_SQUASH_ENABLE") != "",
		SkipGitErrors:  os.Getenv("GITBASE_SKIP_GIT_ERRORS") != "",
	})

	parser.AddCommand("version", cmd.VersionDescription, cmd.VersionHelp, &cmd.Version{
		Name: name,
	})

	_, err := parser.Parse()
	if err != nil {
		if e, ok := err.(*flags.Error); ok && e.Type == flags.ErrCommandRequired {
			parser.WriteHelp(os.Stdout)
		}

		os.Exit(1)
	}
}

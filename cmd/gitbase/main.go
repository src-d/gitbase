package main

import (
	"os"

	"github.com/src-d/gitbase/cmd/gitbase/command"

	"github.com/jessevdk/go-flags"
)

const (
	name = "gitbase"
)

func main() {
	parser := flags.NewNamedParser(name, flags.Default)

	parser.AddCommand("server", command.ServerDescription, command.ServerHelp,
		&command.Server{
			UnstableSquash: os.Getenv("GITBASE_UNSTABLE_SQUASH_ENABLE") != "",
			SkipGitErrors:  os.Getenv("GITBASE_SKIP_GIT_ERRORS") != "",
		})

	parser.AddCommand("version", command.VersionDescription, command.VersionHelp,
		&command.Version{
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

package main

import (
	"os"

	"github.com/src-d/gitbase/cmd/gitbase/command"

	"github.com/jessevdk/go-flags"
	"github.com/sirupsen/logrus"
)

const (
	name    = "gitbase"
	version = "undefined"
	build   = "undefined"
)

func main() {
	parser := flags.NewNamedParser(name, flags.Default)

	_, err := parser.AddCommand("server", command.ServerDescription, command.ServerHelp,
		&command.Server{
			SkipGitErrors: os.Getenv("GITBASE_SKIP_GIT_ERRORS") != "",
			Version:       version,
		})
	if err != nil {
		logrus.Fatal(err)
	}

	_, err = parser.AddCommand("version", command.VersionDescription, command.VersionHelp,
		&command.Version{
			Name:    name,
			Version: version,
			Build:   build,
		})
	if err != nil {
		logrus.Fatal(err)
	}

	_, err = parser.Parse()
	if err != nil {
		if e, ok := err.(*flags.Error); ok && e.Type == flags.ErrCommandRequired {
			parser.WriteHelp(os.Stdout)
		}

		os.Exit(1)
	}
}

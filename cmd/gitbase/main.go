package main

import (
	"fmt"
	"os"

	"runtime"
	"runtime/debug"

	"github.com/src-d/gitbase/cmd/gitbase/command"

	"github.com/jessevdk/go-flags"
	"github.com/sirupsen/logrus"
)

var (
	name    = "gitbase"
	version = "undefined"
	build   = "undefined"
)

func main() {
	debug.SetPanicOnFault(true)
	runtime.GOMAXPROCS(1)

	parser := flags.NewNamedParser(name, flags.Default)
	parser.UnknownOptionHandler = func(option string, arg flags.SplitArgument, args []string) ([]string, error) {
		if option != "g" {
			return nil, fmt.Errorf("unknown flag `%s'", option)
		}

		if len(args) == 0 {
			return nil, fmt.Errorf("unknown flag `%s'", option)
		}

		return append(append(args, "-d"), args[0]), nil
	}

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

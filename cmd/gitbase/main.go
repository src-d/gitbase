package main

import (
	"os"

	"github.com/jessevdk/go-flags"
)

const (
	name = "gitbase"
)

func main() {
	parser := flags.NewNamedParser(name, flags.Default)
	parser.AddCommand("server", "Start SQL server.", "", &cmdServer{})
	parser.AddCommand("version", "Show the version information.", "", &cmdVersion{})

	_, err := parser.Parse()
	if err != nil {
		if e, ok := err.(*flags.Error); ok && e.Type == flags.ErrCommandRequired {
			parser.WriteHelp(os.Stdout)
		}

		os.Exit(1)
	}
}

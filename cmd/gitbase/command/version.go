package command

import "fmt"

const (
	VersionDescription = "Show the version information"
	VersionHelp        = VersionDescription
)

var (
	version = "undefined"
	build   = "undefined"
)

// Version represents the `version` command of gitbase cli tool.
type Version struct {
	// Name of the cli binary
	Name string
}

// Execute prints the build information provided by the compilation tools, it
// honors the go-flags.Commander interface.
func (c *Version) Execute(args []string) error {
	fmt.Printf("%s (%s) - build %s\n", c.Name, version, build)
	return nil
}

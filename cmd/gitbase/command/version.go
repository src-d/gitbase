package command

import "fmt"

const (
	VersionDescription = "Show the version information"
	VersionHelp        = VersionDescription
)

// Version represents the `version` command of gitbase cli tool.
type Version struct {
	// Name of the cli binary
	Name string
	// Version of the cli binary
	Version string
	// Build of the cli binary
	Build string
}

// Execute prints the build information provided by the compilation tools, it
// honors the go-flags.Commander interface.
func (c *Version) Execute(args []string) error {
	fmt.Printf("%s (%s) - build %s\n", c.Name, c.Version, c.Build)
	return nil
}

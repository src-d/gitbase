package cli

import "github.com/fatih/color"

var (
	Warning = color.New(color.FgRed)
	Notice  = color.New(color.FgGreen)
	Debug   = color.New(color.FgBlue)
)

type Command struct {
	Verbose bool   `long:"verbose" description:"show verbose debug information"`
	Root    string `long:"root" default:"." description:"root of the driver"`
}

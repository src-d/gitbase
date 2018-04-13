package main

import "fmt"

var (
	version string
	build   string
)

type cmdVersion struct{}

func (c *cmdVersion) Execute(args []string) error {
	fmt.Printf("%s (%s) - build %s\n", name, version, build)
	return nil
}

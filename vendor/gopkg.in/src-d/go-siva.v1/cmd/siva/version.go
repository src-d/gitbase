package main

import (
	"fmt"
)

var version string
var build string

type CmdVersion struct{}

func (c *CmdVersion) Execute(args []string) error {
	fmt.Printf("siva (%s) - build %s\n", version, build)

	return nil
}

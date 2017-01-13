package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strings"
)

type CmdRepl struct {
	cmdQueryUtils
}

func (c *CmdRepl) Execute(args []string) error {
	if err := c.validate(); err != nil {
		return err
	}

	if err := c.buildDatabase(); err != nil {
		return err
	}

	fmt.Print("\n            gitQL REPL\n" +
		"            ----------\n" +
		"You must end your queries with ';'\n\n")

	s := bufio.NewScanner(os.Stdin)

	s.Split(ScanQueries)

	for {
		fmt.Print("!> ")

		s.Scan()

		query := s.Text()

		query = strings.Replace(query, "\n", " ", -1)
		query = strings.TrimSpace(query)

		fmt.Printf("\n--> Executing query: %s\n\n", query)

		schema, rowIter, err := c.executeQuery(query)
		if err != nil {
			c.printError(err)
			continue
		}

		err = c.printQuery(schema, rowIter, "pretty")
		if err != nil {
			c.printError(err)
		}
	}
}

func (c *CmdRepl) printError(err error) {
	fmt.Printf("ERROR: %v\n\n", err)
}

func ScanQueries(data []byte, atEOF bool) (int, []byte, error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, ';'); i >= 0 {
		// We have a full newline-terminated line.
		return i + 1, dropCR(data[0:i]), nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), dropCR(data), nil
	}
	// Request more data.
	return 0, nil, nil
}

// dropCR drops a terminal \r from the data.
func dropCR(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\r' {
		return data[0 : len(data)-1]
	}
	return data
}

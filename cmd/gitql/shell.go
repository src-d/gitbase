package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strings"
)

type CmdShell struct {
	cmdQueryBase
}

func (c *CmdShell) Execute(args []string) error {
	if err := c.validate(); err != nil {
		return err
	}

	if err := c.buildDatabase(); err != nil {
		return err
	}

	fmt.Print(`
           gitQL SHELL
           -----------
You must end your queries with ';'

`)

	s := bufio.NewScanner(os.Stdin)

	s.Split(scanQueries)

	for {
		fmt.Print("!> ")

		if !s.Scan() {
			break
		}

		query := s.Text()

		query = strings.Replace(query, "\n", " ", -1)
		query = strings.TrimSpace(query)

		fmt.Printf("\n--> Executing query: %s\n\n", query)

		schema, rowIter, err := c.executeQuery(query)
		if err != nil {
			c.printError(err)
			continue
		}

		if err := c.printQuery(schema, rowIter, "pretty"); err != nil {
			c.printError(err)
		}
	}

	return s.Err()
}

func (c *CmdShell) printError(err error) {
	fmt.Printf("ERROR: %v\n\n", err)
}

func scanQueries(data []byte, atEOF bool) (int, []byte, error) {
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

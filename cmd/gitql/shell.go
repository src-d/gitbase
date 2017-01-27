package main

import (
	"fmt"
	"strings"

	"github.com/chzyer/readline"
)

const (
	initPrompt      = "!> "
	multilinePrompt = "!>>> "
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

	rl, err := readline.NewEx(&readline.Config{
		Prompt:                 initPrompt,
		HistoryFile:            "/tmp/gitql-history",
		DisableAutoSaveHistory: true,
	})
	if err != nil {
		return err
	}

	rl.Terminal.Print(fmt.Sprint(`
           gitQL SHELL
           -----------
You must end your queries with ';'

`))

	var cmds []string
	for {
		line, err := rl.Readline()
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		cmds = append(cmds, line)
		if !strings.HasSuffix(line, ";") {
			rl.SetPrompt(multilinePrompt)
			continue
		}

		query := strings.Join(cmds, " ")
		cmds = cmds[:0]
		rl.SetPrompt(initPrompt)
		rl.SaveHistory(query)

		rl.Terminal.Print(fmt.Sprintf("\n--> Executing query: %s\n\n", query))

		schema, rowIter, err := c.executeQuery(query)
		if err != nil {
			rl.Terminal.Print(fmt.Sprintf("ERROR: %v\n\n", err))
			continue
		}

		if err := c.printQuery(schema, rowIter, "pretty"); err != nil {
			rl.Terminal.Print(fmt.Sprintf("ERROR: %v\n\n", err))
			continue
		}
	}

	return rl.Close()
}

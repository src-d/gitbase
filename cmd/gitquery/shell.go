package main

import (
	"fmt"
	"strings"

	"github.com/chzyer/readline"
	"github.com/fatih/color"
)

const (
	initPrompt      = "!> "
	multilinePrompt = "!>>> "
)

type CmdShell struct {
	cmdQueryBase
}

func (c *CmdShell) Execute(args []string) error {
	if err := c.buildDatabase(); err != nil {
		return err
	}

	blue := color.New(color.FgHiBlue).SprintfFunc()
	white := color.New(color.FgWhite).SprintfFunc()
	red := color.New(color.FgHiRed).PrintfFunc()

	prompt := blue(initPrompt)
	mlPrompt := blue(multilinePrompt)

	rl, err := readline.NewEx(&readline.Config{
		Prompt:                 prompt,
		HistoryFile:            fmt.Sprintf("/tmp/%s-history", name),
		DisableAutoSaveHistory: true,
	})
	if err != nil {
		return err
	}

	fmt.Println("          ", white("git")+blue("query"), "shell")
	fmt.Println("           -----------")
	fmt.Println("You must end your queries with ';'")
	fmt.Println("")

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
			rl.SetPrompt(mlPrompt)
			continue
		}

		query := strings.Join(cmds, " ")
		cmds = cmds[:0]
		rl.SetPrompt(prompt)
		rl.SaveHistory(query)

		rl.Terminal.Print(fmt.Sprintf("\n--> Executing query: %s\n\n", query))

		rows, err := c.executeQuery(query)
		if err != nil {
			red("ERROR: %v\n\n", err)
			continue
		}

		if err := c.printQuery(rows, "pretty"); err != nil {
			red("ERROR: %v\n\n", err)
			continue
		}
	}

	return rl.Close()
}

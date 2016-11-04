package main

import (
	"github.com/mvader/gitql/repl"
	"gopkg.in/readline.v1"
)

func main() {

	rl, err := readline.NewEx(&readline.Config{Prompt: ">>>", AutoComplete: repl.GitSqlAutocomplete{}})
	if err != nil {
		panic(err)
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err != nil {
			// io.EOF
			break
		}
		println(line)
	}
}

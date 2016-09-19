package repl

import (
	"strings"
	"unicode/utf8"
)

type GitSqlAutocomplete struct {
}

func (g GitSqlAutocomplete) Do(line []rune, pos int) (newLine [][]rune, length int) {

	var str = string(line)

	if strings.HasPrefix("select", str) && strings.Compare("select", str) != 0 {
		var completedString = strings.TrimPrefix("select", str)
		return [][]rune{
			[]rune(completedString),
		}, utf8.RuneCountInString(str)
	} else if !strings.HasSuffix(str, " ") {
		return [][]rune{
			[]rune(" "),
		}, utf8.RuneCountInString(str) + 1
	} else {
		return nil, pos
	}
}
